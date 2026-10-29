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
//! `Viewport` is the same cure as [`crate::anim::Anim`]: the component
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
//! One surface — logs — has content that moves UNDERNEATH the window: the ring
//! evicts from the front while you read. A row number cannot survive that, so
//! the log render additionally reports WHICH line the top row was showing
//! ([`LineAnchor`], written by [`Viewport::apply_render_anchored`]) and the next
//! frame parks on the line rather than the row. That record is only ever read
//! by the render that wrote it; any intent invalidates it in `Viewport::move_to`,
//! so there is still exactly ONE authority — this type — and exactly one place
//! that decides where the window starts.
//!
//! **Units are PHYSICAL rows** (wrap-expanded). For non-wrapping surfaces one
//! logical line is one row, so physical == logical and adopting `Viewport` is a
//! rename. For logs the widget reports the wrap-expanded count, so `end()`
//! reaches `content_rows - viewport_rows` — the true last row — BY CONSTRUCTION.
//! The "can't reach the last line" bug is unrepresentable because the action
//! layer never holds a line-to-row count to get wrong.

/// WHICH line the top of the window was showing, recorded by the render pass.
///
/// A row number only means something against the content that was there when
/// it was measured, and log content moves underneath the viewport constantly:
/// the ring evicts from the front, a range restart empties it, a grep changes
/// which lines are visible, `w` re-flows every line to a different height.
/// Each of those changes *which line* row N names, so a persisted row silently
/// becomes a persisted lie — you park on a stack trace and drift off it.
///
/// The cure is to persist the LINE. `seq` is the store-absolute line sequence
/// (`evicted + index`), which is monotonic across eviction and therefore still
/// identifies the line after it scrolls out; `skip` is how many wrapped rows
/// into that line the window started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineAnchor {
    pub seq: u64,
    pub skip: usize,
}

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
    /// The render pass's record of WHICH line `offset` was showing — see
    /// [`LineAnchor`]. `None` on every surface whose content cannot move under
    /// it, and on any viewport whose offset has been moved since the record was
    /// written, which is exactly when it would be a lie.
    anchor: Option<LineAnchor>,
}

impl Viewport {
    /// A viewport that starts pinned to the bottom (logs tail by default).
    pub(crate) fn tailing() -> Self {
        Self { follow: true, ..Self::default() }
    }

    /// The viewport for a fresh stream over the same surface: extent reset,
    /// user preference kept.
    ///
    /// A log range change (`0`-`6`) clears the store, so the offset and the
    /// published extent are meaningless afterwards — but whether the user
    /// wants the view to tail is a preference they set with `s`, not a
    /// property of the stream being replaced.
    pub fn restarted(&self) -> Self {
        if self.follow {
            Self::tailing()
        } else {
            Self::default()
        }
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
    pub(crate) fn viewport_rows(&self) -> usize {
        self.viewport_rows
    }

    /// Whether the viewport is sticking to the bottom (logs autoscroll intent).
    pub fn following(&self) -> bool {
        self.follow
    }

    /// The render's line record, if the position has not moved since it was
    /// written. The log render parks on this instead of [`Self::offset`].
    pub fn anchor(&self) -> Option<LineAnchor> {
        self.anchor
    }

    /// The ONE place the offset is written outside a line-anchored write-back.
    ///
    /// Moving the window invalidates the render's line record: it said "row
    /// `offset` is line X", and X is not the line we are on any more. Every
    /// mutator below goes through here, so "moved the offset, kept a stale
    /// anchor" is not a state this type can be left in.
    fn move_to(&mut self, offset: usize) {
        self.offset = offset;
        self.anchor = None;
    }

    /// RENDER-PASS write-back: publish the true extent measured from the real
    /// `Rect` (wrap-expanded for logs) and the inner height. Re-clamps `offset`
    /// to the new bounds; when `autoscroll` is set, snaps to the bottom (the
    /// caller passes `following()`, and logs additionally gate it on not being
    /// in the initial tail-load). This is the sole clamp of the PERSISTED offset.
    pub fn set_metrics(&mut self, content_rows: usize, viewport_rows: usize, autoscroll: bool) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        let offset =
            if autoscroll { self.max_scroll() } else { self.offset.min(self.max_scroll()) };
        self.move_to(offset);
    }

    /// CURSOR-table render write-back: the widget already computed the revealed
    /// `offset` from the real area (the cursor is primary, the viewport trails
    /// it), so persist that offset plus the freshly-measured extent. Free-scroll
    /// surfaces use `set_metrics` + intent; this is the cursor-coupled analogue.
    pub fn apply_render(&mut self, offset: usize, content_rows: usize, viewport_rows: usize) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        let offset = offset.min(self.max_scroll());
        self.move_to(offset);
    }

    /// RENDER-PASS write-back for a surface whose content moves underneath it
    /// (logs): the offset actually windowed, the extent measured, AND which
    /// line the top row was showing.
    ///
    /// The anchor is what the NEXT frame parks on, so eviction above the
    /// window, a wrap re-flow, and a grep that changes the visible set all
    /// re-base the position EXACTLY — instead of leaving a row number that
    /// now names some other line. The clamp is defensive only: the widget
    /// derived `offset` and the anchor from the same extent it reports here,
    /// so they agree by construction.
    pub fn apply_render_anchored(
        &mut self,
        offset: usize,
        anchor: LineAnchor,
        content_rows: usize,
        viewport_rows: usize,
    ) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        self.offset = offset.min(self.max_scroll());
        self.anchor = Some(anchor);
    }

    // --- Intent (action layer). None of these takes or computes geometry. ---

    /// Scroll up `n` rows. Any explicit upward move drops follow (the user is
    /// looking at history now).
    pub fn line_up(&mut self, n: usize) {
        self.follow = false;
        self.move_to(self.offset.saturating_sub(n));
    }

    /// Scroll down `n` rows, bounded by the last published extent. Landing on
    /// the last row RE-ENGAGES follow.
    ///
    /// That re-engagement is what makes `follow` mean one thing:
    /// **pinned to the bottom**. Without it, scrolling up and then back down
    /// to the very bottom left `follow` false, so incoming lines silently
    /// pushed the view off the end — the log looked like it was tailing right
    /// up until it wasn't. Worse, `s` was then out of phase with what the user
    /// could see: they were at the bottom, so the next toggle read as "turn it
    /// off" when they meant "turn it on".
    pub fn line_down(&mut self, n: usize) {
        self.move_to((self.offset + n).min(self.max_scroll()));
        // `max_scroll() > 0` guards the degenerate extent: when the content
        // fits entirely, offset and max_scroll are both 0, so the test below
        // is trivially true and `j` on a five-line log silently turned
        // autoscroll back on without moving anything — the same `s`-is-out-
        // of-phase confusion this re-engagement was written to fix.
        if self.max_scroll() > 0 && self.offset == self.max_scroll() {
            self.follow = true;
        }
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
        self.move_to(0);
    }

    /// Go to the bottom and stick there (autoscroll). For static surfaces this
    /// just pins to the last page; the snap itself lands on the next
    /// `set_metrics`, and is applied here too so a never-yet-rendered view is
    /// already at the bottom.
    pub fn end(&mut self) {
        self.follow = true;
        self.move_to(self.max_scroll());
    }

    /// Toggle stick-to-bottom (logs `s`).
    ///
    /// Turning it OFF deliberately keeps the anchor: the position did not
    /// move, so the line the user is looking at is still the line they should
    /// keep looking at as the tail grows past them.
    pub fn toggle_follow(&mut self) {
        self.follow = !self.follow;
        if self.follow {
            self.move_to(self.max_scroll());
        }
    }

    /// Center the viewport on a row (content-view search "next/prev match").
    /// Clears follow — the user jumped somewhere specific.
    pub(crate) fn center_on(&mut self, row: usize) {
        self.follow = false;
        let half = self.viewport_rows / 2;
        self.move_to(row.saturating_sub(half).min(self.max_scroll()));
    }

    /// Jump so `row` is the top visible row (log/content search "go to match").
    /// Clears follow.
    pub fn scroll_to(&mut self, row: usize) {
        self.follow = false;
        self.move_to(row.min(self.max_scroll()));
    }

    /// CURSOR surfaces (the resource table): scroll the minimum needed to keep
    /// `row` visible in the window. The cursor is primary; the viewport trails
    /// it. Uses the last published `viewport_rows`, exactly like the table's
    /// previous `page_size`-based reveal.
    pub fn reveal(&mut self, row: usize) {
        if row < self.offset {
            self.move_to(row);
        } else if self.viewport_rows > 0 && row >= self.offset + self.viewport_rows {
            self.move_to(row + 1 - self.viewport_rows);
        }
    }
}

/// Horizontal panning for a surface whose lines run off the right edge.
///
/// This and [`Viewport`] are the same doctrine on two axes — the render
/// measures the extent, the action layer expresses intent and never computes
/// geometry — but deliberately NOT the same type. `follow`, the line anchor,
/// `page_*` and `reveal` all mean something on a column of rows and nothing on
/// a row of columns, and the stick-to-the-end that [`Viewport::line_down`]
/// re-engages would be actively wrong here: nobody wants a log pinned to the
/// right margin of its longest line.
///
/// One asymmetry IS load-bearing. The offset is clamped when the user pans and
/// NEVER on write-back. Vertically an out-of-range offset means blank rows at
/// the bottom, which is a bug; horizontally it means blank space inside a
/// short line, which is what reading past the end of a short line looks like
/// in any editor. Re-clamping each frame would instead yank the view back left
/// the moment you scrolled onto a short line — so the extent below bounds the
/// next KEYPRESS, not the current offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pan {
    /// Display columns skipped off the left of every rendered line.
    offset: usize,
    /// The furthest useful offset — widest rendered line minus the visible
    /// width — WRITTEN BACK by the render pass. Zero when the surface wraps,
    /// because wrapped content has nothing to the right by construction.
    max: usize,
    /// The stride for the PAGING pan (`H`/`L`), in columns: a quarter of the
    /// measured width. `h`/`l` move one column, like vim's `zh`/`zl` — the
    /// big jumps are `^` and `$`, so the per-key step does not have to
    /// compromise between precision and reach.
    step: usize,
}

impl Pan {
    /// Columns to skip off the left of each rendered line.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// RENDER-PASS write-back: the widest line it drew and the width it had.
    pub fn set_extent(&mut self, widest: usize, viewport_cols: usize) {
        self.max = widest.saturating_sub(viewport_cols);
        self.step = (viewport_cols / 4).max(1);
    }

    /// Pan to the far edge of the widest line seen.
    pub fn end(&mut self) {
        self.offset = self.max;
    }

    /// Pan right, bounded by the last measured extent — but NEVER backwards.
    ///
    /// `max` is re-measured every frame from the widest line in the CURRENT
    /// window, so it drops as soon as you scroll onto shorter lines. Clamping
    /// straight to it meant that panning right while parked over short lines
    /// snapped you to column 0: the key that means "go further right" was the
    /// one that threw the position away. Never-clamp-on-write-back removed
    /// the yank from the frame; this removes it from the keypress, which is
    /// where it had been relocated to.
    pub fn right(&mut self) {
        self.offset = (self.offset + 1).min(self.max.max(self.offset));
    }

    /// Pan right by a screen-derived stride (`^`/`$` are the big jumps; this
    /// is the middle gear).
    pub fn page_right(&mut self) {
        self.offset = (self.offset + self.step.max(1)).min(self.max.max(self.offset));
    }

    /// Pan left by the same stride.
    pub fn page_left(&mut self) {
        self.offset = self.offset.saturating_sub(self.step.max(1));
    }

    /// Pan left, bounded by the left margin.
    pub fn left(&mut self) {
        self.offset = self.offset.saturating_sub(1);
    }

    /// Back to the left margin in one press. A quarter-screen step is the
    /// right size for reading along a line and the wrong size for undoing a
    /// four-hundred-column excursion.
    pub fn home(&mut self) {
        self.offset = 0;
    }
}

#[cfg(test)]
#[path = "tests/viewport.rs"]
mod tests;
