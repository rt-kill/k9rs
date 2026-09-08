//! A single self-contained scroll relationship, shared by every scrollable
//! surface (resource tables, logs, yaml/describe, the help overlay).
//!
//! Why this exists as its own component: scroll bounds used to be decided in
//! the ACTION layer from a *guess* of the viewport — `crossterm::terminal::size()`
//! minus hardcoded chrome constants, in LOGICAL-line units — while the only
//! component that knows the true drawable `Rect` (and, for logs, the wrap-
//! expanded PHYSICAL row layout) is the render widget. The two clamps silently
//! disagreed on resize and wrap; the acute symptom was "log wrap on can't
//! scroll to the last line".
//!
//! `Viewport` is the same cure as [`crate::app::anim::Anim`]: the component
//! that actually knows the truth (the widget, which holds the `Rect`) OWNS and
//! PUBLISHES it via [`Viewport::set_metrics`]; the action layer only expresses
//! INTENT ([`Viewport::line_down`], [`Viewport::page_up`], [`Viewport::end`], …)
//! and never reconstructs geometry. INVARIANT: the persisted offset is always in
//! `[0, content_rows - viewport_rows]` — every mutator keeps it there (the intent
//! methods clamp against the last-published extent; [`Viewport::set_metrics`] /
//! [`Viewport::apply_render`] re-clamp on write-back). A widget may re-clamp its
//! throwaway per-frame snapshot defensively; that is not a second authority.
//!
//! How the extent REACHES the Viewport differs by surface, but the action layer
//! never guesses in any of them: LOGS must have the widget report the wrap-
//! expanded PHYSICAL row count (only it knows the wrap layout); the resource
//! tables report their measured inner height back; the simple dialog surfaces
//! (yaml/describe/help/context) derive their fixed layout chrome in the render
//! (which holds the `Rect`). All render-owned, none action-owned.
//!
//! **Units are PHYSICAL rows** (wrap-expanded). For non-wrapping surfaces one
//! logical line is one row, so physical == logical and adopting `Viewport` is a
//! rename. For logs the widget reports the wrap-expanded count, so `end()`
//! reaches `content_rows - viewport_rows` — the true last row — BY CONSTRUCTION.
//! The "can't reach the last line" bug is unrepresentable because the action
//! layer never holds a line-to-row count to get wrong.

/// The scroll relationship of one viewport: where the window starts, how big it
/// is, how much content there is, and whether it sticks to the bottom. The last
/// two are written back by the render pass; the offset is moved by intent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Viewport {
    /// Index of the top visible row, in PHYSICAL (wrap-expanded) rows.
    offset: usize,
    /// Stick-to-bottom (logs autoscroll). Static surfaces leave it false; a
    /// bare `end()` sets it, where it simply means "pinned to the last page".
    follow: bool,
    /// Total addressable rows — WRITTEN BACK by the render pass each frame.
    /// Wrap-expanded physical count for logs; line count for everything else.
    content_rows: usize,
    /// The inner row height the widget actually got — WRITTEN BACK by the
    /// render pass. This IS the page size; the action layer never guesses it.
    viewport_rows: usize,
}

impl Viewport {
    /// A viewport that starts pinned to the bottom (logs tail by default).
    pub fn tailing() -> Self {
        Self { follow: true, ..Self::default() }
    }

    /// A viewport pre-seeded with a page height, for the one frame before the
    /// first render publishes the real one (matches the table's old
    /// `page_size: 40` seed, so paging before the first paint isn't a no-op).
    pub fn seeded(viewport_rows: usize) -> Self {
        Self { viewport_rows, ..Self::default() }
    }

    fn max_scroll(&self) -> usize {
        self.content_rows.saturating_sub(self.viewport_rows)
    }

    /// The top visible row — the only value the render pass reads to window.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// The published viewport height. Used as the page size and (by the table)
    /// as the cursor's page step. Never guessed from `terminal::size()`.
    pub fn viewport_rows(&self) -> usize {
        self.viewport_rows
    }

    /// Whether the viewport is sticking to the bottom (logs autoscroll intent).
    pub fn following(&self) -> bool {
        self.follow
    }

    /// RENDER-PASS write-back: publish the true extent measured from the real
    /// `Rect` (wrap-expanded for logs) and the inner height. Re-clamps `offset`
    /// to the new bounds; when `autoscroll` is set, snaps to the bottom (the
    /// caller passes `following()`, and logs additionally gate it on not being
    /// in the initial tail-load). This is the sole clamp of the PERSISTED offset.
    pub fn set_metrics(&mut self, content_rows: usize, viewport_rows: usize, autoscroll: bool) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        self.offset = if autoscroll { self.max_scroll() } else { self.offset.min(self.max_scroll()) };
    }

    /// CURSOR-table render write-back: the widget already computed the revealed
    /// `offset` from the real area (the cursor is primary, the viewport trails
    /// it), so persist that offset plus the freshly-measured extent. Free-scroll
    /// surfaces use `set_metrics` + intent; this is the cursor-coupled analogue.
    pub fn apply_render(&mut self, offset: usize, content_rows: usize, viewport_rows: usize) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        self.offset = offset.min(self.max_scroll());
    }

    // --- Intent (action layer). None of these takes or computes geometry. ---

    /// Scroll up `n` rows. Any explicit upward move drops follow (the user is
    /// looking at history now) — the single place that clears it.
    pub fn line_up(&mut self, n: usize) {
        self.follow = false;
        self.offset = self.offset.saturating_sub(n);
    }

    /// Scroll down `n` rows, bounded by the last published extent.
    pub fn line_down(&mut self, n: usize) {
        self.offset = (self.offset + n).min(self.max_scroll());
    }

    /// Page up by the published viewport height (never a config guess).
    pub fn page_up(&mut self) {
        self.line_up(self.viewport_rows.max(1));
    }

    /// Page down by the published viewport height.
    pub fn page_down(&mut self) {
        self.line_down(self.viewport_rows.max(1));
    }

    /// Jump to the top.
    pub fn home(&mut self) {
        self.follow = false;
        self.offset = 0;
    }

    /// Go to the bottom and stick there (autoscroll). For static surfaces this
    /// just pins to the last page; the snap itself lands on the next
    /// `set_metrics`, and is applied here too so a never-yet-rendered view is
    /// already at the bottom.
    pub fn end(&mut self) {
        self.follow = true;
        self.offset = self.max_scroll();
    }

    /// Toggle stick-to-bottom (logs `s`).
    pub fn toggle_follow(&mut self) {
        self.follow = !self.follow;
        if self.follow {
            self.offset = self.max_scroll();
        }
    }

    /// Center the viewport on a row (content-view search "next/prev match").
    /// Clears follow — the user jumped somewhere specific.
    pub fn center_on(&mut self, row: usize) {
        self.follow = false;
        let half = self.viewport_rows / 2;
        self.offset = row.saturating_sub(half).min(self.max_scroll());
    }

    /// Jump so `row` is the top visible row (log/content search "go to match").
    /// Clears follow.
    pub fn scroll_to(&mut self, row: usize) {
        self.follow = false;
        self.offset = row.min(self.max_scroll());
    }

    /// Content scrolled off the top (log ring eviction): shift the offset up so
    /// the paused view stays roughly put, without touching follow. Best-effort:
    /// `n` is a logical line count while the offset is physical rows (and, for a
    /// filter, over the filtered set), so under wrap/filter it can drift a few
    /// rows — harmless, since the next `set_metrics` re-clamps every frame.
    pub fn shift_up(&mut self, n: usize) {
        self.offset = self.offset.saturating_sub(n);
    }

    /// CURSOR surfaces (the resource table): scroll the minimum needed to keep
    /// `row` visible in the window. The cursor is primary; the viewport trails
    /// it. Uses the last published `viewport_rows`, exactly like the table's
    /// previous `page_size`-based reveal.
    pub fn reveal(&mut self, row: usize) {
        if row < self.offset {
            self.offset = row;
        } else if self.viewport_rows > 0 && row >= self.offset + self.viewport_rows {
            self.offset = row + 1 - self.viewport_rows;
        }
    }
}

#[cfg(test)]
#[path = "../tests/app/viewport.rs"]
mod tests;
