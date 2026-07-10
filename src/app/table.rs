use std::collections::HashSet;
use std::sync::Arc;

use unicode_width::UnicodeWidthStr;

use crate::kube::protocol::ObjectKey;
use crate::kube::resources::KubeResource;

use super::types::ItemCounts;

// ---------------------------------------------------------------------------
// TableDataState — lifecycle state for table data
// ---------------------------------------------------------------------------

/// Lifecycle state of a resource table. Replaces the prior triple of
/// `has_data: bool` + `loading: bool` + `error: Option<String>` — same
/// pattern as [`crate::app::ContextSwitchState`] and
/// [`crate::app::EditState`].
///
/// Transitions:
/// - `Initializing` → `Ready` (first snapshot arrives via `set_items_filtered`)
/// - `Ready` → `Initializing` (context switch / namespace switch clears data)
/// - `Ready` → `Failed` (subscription error arrives)
/// - `Failed` → `Initializing` (clear_data resets)
/// - `Initializing` → `Failed` (subscription fails before first snapshot)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TableDataState {
    /// No snapshot received yet. The UI shows a loading spinner.
    #[default]
    Initializing,
    /// At least one snapshot has been received. The table may be empty
    /// (zero matching resources), but the server has responded.
    Ready,
    /// The subscription failed. The UI shows this error message instead
    /// of the loading spinner.
    Failed(String),
}

// ---------------------------------------------------------------------------
// TableNav trait — allows App to dispatch navigation to any StatefulTable
// ---------------------------------------------------------------------------

pub(crate) trait TableNav {
    fn nav_next(&mut self);
    fn nav_prev(&mut self);
    fn nav_page_up(&mut self);
    fn nav_page_down(&mut self);
    fn nav_home(&mut self);
    fn nav_end(&mut self);
    fn nav_clear_filter(&mut self);
    fn nav_toggle_mark(&mut self);
    fn nav_sort_by(&mut self, target: crate::app::SortTarget);
    fn nav_toggle_sort(&mut self);
    fn nav_items_count(&self) -> ItemCounts;
    fn nav_clear_marks(&mut self);
    fn nav_span_mark(&mut self);
    fn nav_select(&mut self, idx: usize);
    fn nav_selected(&self) -> usize;
    fn nav_col_left(&mut self);
    fn nav_col_right(&mut self);
}

impl<T: Clone + KubeResource> TableNav for StatefulTable<T> {
    fn nav_next(&mut self) { self.next(); }
    fn nav_prev(&mut self) { self.previous(); }
    fn nav_page_up(&mut self) { self.page_up(); }
    fn nav_page_down(&mut self) { self.page_down(); }
    fn nav_home(&mut self) { self.home(); }
    fn nav_end(&mut self) { self.end(); }
    fn nav_clear_filter(&mut self) { self.clear_filter(); }
    fn nav_toggle_mark(&mut self) {
        // Clamp first — a snapshot might have shrunk the table since the
        // last render. Without this, `selected` can be past the end and
        // the mark silently doesn't happen.
        if self.filtered_indices.is_empty() {
            return;
        }
        self.clamp_selection();
        let real_idx = self.filtered_indices[self.selected];
        let Some(item) = self.items.get(real_idx) else { return };
        let key = ObjectKey::new(item.namespace(), item.name());
        if !self.marked.remove(&key) {
            self.marked.insert(key);
        }
    }
    fn nav_sort_by(&mut self, target: crate::app::SortTarget) {
        self.sort_by_column(target);
    }
    fn nav_toggle_sort(&mut self) {
        let col = self.sort_column;
        self.sort_by_column(crate::app::SortTarget::Column(col));
    }
    fn nav_items_count(&self) -> ItemCounts {
        ItemCounts { filtered: self.len(), total: self.total() }
    }
    fn nav_clear_marks(&mut self) {
        self.marked.clear();
    }
    fn nav_span_mark(&mut self) {
        if self.filtered_indices.is_empty() { return; }
        let current = self.selected.min(self.filtered_indices.len() - 1);
        // Find the nearest existing mark by checking each visible row against
        // the authoritative `marked` set. One ObjectKey per visible row — only
        // happens once per keypress, not per frame.
        let anchor = self.filtered_indices.iter()
            .enumerate()
            .filter_map(|(pos, &real)| {
                let item = self.items.get(real)?;
                let key = ObjectKey::new(item.namespace(), item.name());
                if self.marked.contains(&key) { Some(pos) } else { None }
            })
            .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
            .unwrap_or(0);
        let (start, end) = if anchor <= current { (anchor, current) } else { (current, anchor) };
        for pos in start..=end {
            if let Some(real) = self.filtered_indices.get(pos).copied() {
                if let Some(item) = self.items.get(real) {
                    let key = ObjectKey::new(item.namespace(), item.name());
                    self.marked.insert(key);
                }
            }
        }
    }
    fn nav_select(&mut self, idx: usize) {
        self.selected = idx.min(self.filtered_indices.len().saturating_sub(1));
        self.adjust_offset();
    }
    fn nav_selected(&self) -> usize { self.selected }
    fn nav_col_left(&mut self) { self.col_left(); }
    fn nav_col_right(&mut self) {
        self.col_right(self.num_cols);
    }
}

// ---------------------------------------------------------------------------
// StatefulTable
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatefulTable<T: Clone> {
    pub items: Vec<T>,
    filtered_indices: Vec<usize>,
    selected: usize,
    offset: usize,
    /// Selected column index (for horizontal cursor / scrolling).
    selected_col: usize,
    /// Horizontal scroll offset — the pixel x-position of the leftmost
    /// visible column edge. Adjusted automatically to keep `selected_col`
    /// visible, similar to how `offset` keeps `selected` row visible.
    col_offset: u16,
    sort_column: usize,
    sort_ascending: bool,
    page_size: usize,
    /// Lifecycle state of this table's data. Replaces the prior triple of
    /// `has_data: bool` + `loading: bool` + `error: Option<String>` with a
    /// single source of truth — same pattern as `ContextSwitchState` and
    /// `EditState`. Impossible states (e.g. "has data AND has error") are
    /// unrepresentable.
    pub data_state: TableDataState,
    /// Number of columns in the table (from headers/descriptor). Used to
    /// clamp `selected_col` so it can't drift past the last column.
    num_cols: usize,
    /// Marked/selected rows, keyed by stable identity (namespace + name) so
    /// marks survive data refreshes and sort changes. The render path checks
    /// this set directly via row_keys — no intermediate bitmap cache.
    pub marked: HashSet<ObjectKey>,
    /// Monotonic data-version counter, bumped by every mutator that changes
    /// `items` or `filtered_indices`. Since `items` is only ever *read*
    /// externally (verified — no direct writes escape this module), these
    /// mutators are the sole write paths, so the counter can't miss a change.
    /// The render path uses it to detect "same data as last frame" in O(1)
    /// and skip re-materializing the view.
    generation: u64,
    /// Memoized render-ready [`PreparedView`], reused while its
    /// [`ViewCacheKey`] still matches (same data version + columns + width
    /// clamp). Cursor moves, scroll, marks, and change-highlight ticks change
    /// none of those, so those frames hit the cache. Only ever populated by
    /// the `ResourceRow` `prepare_view`; stays `None` for other row types.
    /// `Arc` so a hit clones a pointer, not the materialized `Vec`s.
    view_cache: Option<(ViewCacheKey, Arc<PreparedView>)>,
}

/// Cache key for the memoized [`PreparedView`]. The view is pure over the
/// table's data (captured by `generation`), the requested `visible_cols`, and
/// the `max_col_width` clamp. `column_rules` and header names are fixed for a
/// given descriptor, and a descriptor swap always arrives with a fresh data
/// snapshot (→ `generation` bump), so they need no explicit key.
#[derive(Clone, Debug, PartialEq)]
struct ViewCacheKey {
    generation: u64,
    visible_cols: Vec<usize>,
    max_col_width: u16,
}

impl<T: Clone> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            filtered_indices: Vec::new(),
            selected: 0,
            offset: 0,
            selected_col: 0,
            col_offset: 0,
            sort_column: 0,
            sort_ascending: true,
            page_size: 40,
            data_state: TableDataState::Initializing,
            num_cols: 0,
            marked: HashSet::new(),
            generation: 0,
            view_cache: None,
        }
    }
}

impl<T: Clone> StatefulTable<T> {
    pub fn new() -> Self { Self::default() }

    /// Invalidate the memoized view by advancing the data version. Called by
    /// every mutator that touches `items` or `filtered_indices`. Over-bumping
    /// is harmless (it only forces an occasional redundant rebuild); a *missed*
    /// bump would serve a stale view, so callers err toward bumping.
    fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    pub fn len(&self) -> usize { self.filtered_indices.len() }
    pub fn is_empty(&self) -> bool { self.filtered_indices.is_empty() }
    pub fn total(&self) -> usize { self.items.len() }

    // --- Getters for private fields ---

    pub fn selected(&self) -> usize { self.selected }
    pub fn offset(&self) -> usize { self.offset }
    pub fn selected_col(&self) -> usize { self.selected_col }
    pub fn col_offset(&self) -> u16 { self.col_offset }
    pub fn sort_column(&self) -> usize { self.sort_column }
    pub fn sort_ascending(&self) -> bool { self.sort_ascending }
    pub fn page_size(&self) -> usize { self.page_size }
    pub fn num_cols(&self) -> usize { self.num_cols }

    /// Read-only access to filtered indices. External code should use
    /// `len()`, `is_empty()`, or `prepare_view()` when possible.
    pub fn filtered_indices(&self) -> &[usize] { &self.filtered_indices }

    // --- Validated setters for fields the render path must write back ---

    /// Set the page size (visible row count). Called by the render path
    /// after layout to communicate the actual visible height.
    pub fn set_page_size(&mut self, size: usize) {
        self.page_size = size;
    }

    /// Set the number of visible columns. Called by the render path and
    /// event handlers after computing column visibility.
    pub fn set_num_cols(&mut self, n: usize) {
        self.num_cols = n;
    }

    /// Set the vertical scroll offset. Called by the render path after
    /// the widget adjusts scrolling.
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Set the horizontal scroll offset. Called by the render path after
    /// the widget adjusts column scrolling.
    pub fn set_col_offset(&mut self, offset: u16) {
        self.col_offset = offset;
    }

    pub fn next(&mut self) {
        if !self.filtered_indices.is_empty() && self.selected + 1 < self.filtered_indices.len() {
            self.selected += 1;
        }
        self.adjust_offset();
    }

    pub fn previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
        self.adjust_offset();
    }

    pub fn page_up(&mut self) {
        self.selected = self.selected.saturating_sub(self.page_size);
        self.adjust_offset();
    }

    pub fn page_down(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.selected = (self.selected + self.page_size).min(self.filtered_indices.len() - 1);
        }
        self.adjust_offset();
    }

    pub fn home(&mut self) {
        self.selected = 0;
        self.offset = 0;
    }

    pub fn end(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    /// Move the column cursor left.
    pub fn col_left(&mut self) {
        self.selected_col = self.selected_col.saturating_sub(1);
    }

    /// Move the column cursor right. The caller should provide the total
    /// number of visible columns so we don't go past the last one.
    pub fn col_right(&mut self, num_cols: usize) {
        if num_cols > 0 && self.selected_col + 1 < num_cols {
            self.selected_col += 1;
        }
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        self.bump_generation();
        self.data_state = TableDataState::Ready;
        self.items = items;
        self.filtered_indices = (0..self.items.len()).collect();
        self.clamp_selection();
        self.adjust_offset();
    }

    pub fn apply_filter<F: Fn(&T) -> bool>(&mut self, pred: F) {
        self.bump_generation();
        self.filtered_indices = self.items.iter().enumerate()
            .filter(|(_, item)| pred(item))
            .map(|(i, _)| i)
            .collect();
        self.clamp_selection();
        self.adjust_offset();
    }

    /// Clear all data and reset to `Initializing` state.
    pub fn clear_data(&mut self) {
        self.bump_generation();
        self.items.clear();
        self.filtered_indices.clear();
        self.selected = 0;
        self.offset = 0;
        self.data_state = TableDataState::Initializing;
        self.marked.clear();
    }

    pub fn selected_item(&self) -> Option<&T> {
        let idx = *self.filtered_indices.get(self.selected)?;
        self.items.get(idx)
    }

    pub fn visible_items(&self) -> Vec<&T> {
        let end = (self.offset + self.page_size).min(self.filtered_indices.len());
        if self.offset >= self.filtered_indices.len() {
            return Vec::new();
        }
        self.filtered_indices[self.offset..end]
            .iter()
            .filter_map(|&i| self.items.get(i))
            .collect()
    }

    /// Clamp `selected` and `offset` to valid range after any operation
    /// that changes `filtered_indices`. This is the single enforcement
    /// point for the key invariant: `selected < filtered_indices.len()`
    /// (or both zero when empty).
    fn clamp_selection(&mut self) {
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
    }

    fn adjust_offset(&mut self) {
        if self.page_size == 0 { return; }
        if self.selected < self.offset {
            self.offset = self.selected;
        }
        if self.selected >= self.offset + self.page_size {
            self.offset = self.selected - self.page_size + 1;
        }
    }
}


impl<T: Clone + KubeResource> StatefulTable<T> {
    pub fn clear_filter(&mut self) {
        self.bump_generation();
        self.filtered_indices = (0..self.items.len()).collect();
        self.clamp_selection();
        self.adjust_offset();
    }

    /// Toggle sort on a column. Reuses sort_items and rebuild_filter.
    /// Cursor stays at its screen index.
    pub fn sort_by_column(&mut self, target: crate::app::SortTarget) {
        self.bump_generation();
        let actual_col = match target {
            crate::app::SortTarget::Column(c) => c,
            crate::app::SortTarget::Last => {
                // Resolved at apply time using the actual table width —
                // the AGE column's data index varies per resource type.
                self.items.first()
                    .map(|item| item.cells().len().saturating_sub(1))
                    .unwrap_or(0)
            }
        };
        if self.sort_column == actual_col {
            self.sort_ascending = !self.sort_ascending;
        } else {
            self.sort_column = actual_col;
            self.sort_ascending = true;
        }
        self.sort_items();
        self.rebuild_filter();
        self.clamp_selection();
        self.adjust_offset();
    }

    /// Replace items with a new snapshot. Cursor stays at its current
    /// screen index — it does NOT follow item identity. Marks are keyed by
    /// identity so they survive refreshes; any mark whose row no longer
    /// exists in the new snapshot is dropped.
    pub fn set_items_filtered(&mut self, items: Vec<T>) {
        self.bump_generation();
        self.update_loading(items.len());
        self.items = items;
        // Prune marks whose rows no longer exist.
        if !self.marked.is_empty() {
            let present: HashSet<ObjectKey> = self.items.iter()
                .map(|i| ObjectKey::new(i.namespace(), i.name()))
                .collect();
            self.marked.retain(|k| present.contains(k));
        }
        self.sort_items();
        self.rebuild_filter();
        self.clamp_selection();
        self.adjust_offset();
    }

    /// Mark the table as having received data. Transitions from
    /// `Initializing` (or `Failed`) to `Ready`.
    fn update_loading(&mut self, _new_count: usize) {
        self.data_state = TableDataState::Ready;
    }

    /// Sort items by the current sort column with a stable tiebreaker.
    /// Comparison uses `CellValue::cmp()` directly — the typed enum
    /// encodes how each variant should be ordered, so no content sniffing
    /// or `ColumnSortKind` dispatch is needed.
    fn sort_items(&mut self) {
        use crate::kube::resources::row::CellValue;
        if self.items.is_empty() { return; }
        let col = self.sort_column;
        let asc = self.sort_ascending;
        let default = CellValue::Text(String::new());
        self.items.sort_by(|a, b| {
            let a_val = a.cells().get(col).unwrap_or(&default);
            let b_val = b.cells().get(col).unwrap_or(&default);
            let primary = a_val.cmp(b_val);
            let primary = if asc { primary } else { primary.reverse() };
            if primary == std::cmp::Ordering::Equal {
                a.namespace().cmp(b.namespace())
                    .then_with(|| a.name().cmp(b.name()))
            } else {
                primary
            }
        });
    }

    /// Rebuild `filtered_indices` to show every row. Filtering itself is
    /// owned by `App::reapply_nav_filters` (which calls `apply_filter` with
    /// the nav-stack-derived predicates) — the table is just the storage,
    /// not the policy.
    pub fn rebuild_filter(&mut self) {
        self.bump_generation();
        self.filtered_indices.clear();
        self.filtered_indices.extend(0..self.items.len());
    }
}

// ---------------------------------------------------------------------------
// PreparedView — pre-zipped data for the table renderer
// ---------------------------------------------------------------------------

/// The product of [`StatefulTable::prepare_view`]: parallel arrays of
/// visible-column cells, health tags, and identity keys, ready for the
/// table widget. Encapsulates the `filtered_indices → items → visible
/// cells` pipeline so the render code doesn't do manual index arithmetic.
#[derive(Debug)]
pub struct PreparedView {
    pub rows: Vec<Vec<String>>,
    pub health: Vec<crate::kube::resources::row::RowHealth>,
    /// Per-cell rendering style. `cell_style[row][col]` is `Some(h)` if
    /// that specific cell has its own coloring (from overlay rules or
    /// `CellValue::Status`), `None` to inherit the row's style.
    pub cell_style: Vec<Vec<Option<crate::kube::resources::row::RowHealth>>>,
    pub keys: Vec<crate::kube::protocol::ObjectKey>,
    /// Natural per-column display widths (seeded from header widths, expanded
    /// to the widest visible cell, padded and clamped to `max_col_width`).
    /// Computed with the view so the widget consumes a ready number instead of
    /// re-scanning every row every frame.
    pub col_widths: Vec<u16>,
}

/// Natural per-column display widths: seed from header widths, expand to the
/// widest cell in each column, then pad (`+3`: left border + two spaces) and
/// clamp to `max_col_width`. Pure over `(headers, rows, max_col_width)`. Lives
/// with the view materialization so a memoized frame reuses the result instead
/// of the widget re-scanning all rows.
fn column_widths(headers: &[&str], rows: &[Vec<String>], max_col_width: u16) -> Vec<u16> {
    if headers.is_empty() {
        return Vec::new();
    }
    let mut widths: Vec<u16> = headers.iter().map(|h| h.width() as u16 + 2).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.width() as u16);
            }
        }
    }
    for w in &mut widths {
        *w = (*w + 3).min(max_col_width);
    }
    widths
}

impl StatefulTable<crate::kube::resources::row::ResourceRow> {
    /// Prepare the visible rows for rendering, memoized on the table's data
    /// version + requested columns (see [`ViewCacheKey`]). A cache hit clones
    /// an `Arc` and skips the O(rows×cols) materialization in
    /// [`build_view`](Self::build_view) entirely — so a frame driven by a
    /// cursor move, scroll, mark, or change-highlight tick (none of which
    /// change a cache input) pays nothing to rebuild the view.
    pub fn prepare_view(
        &mut self,
        visible_col_indices: &[usize],
        column_rules: &[crate::kube::overlay::ColumnRenderRules],
        headers: &[&str],
        max_col_width: u16,
    ) -> Arc<PreparedView> {
        let key = ViewCacheKey {
            generation: self.generation,
            visible_cols: visible_col_indices.to_vec(),
            max_col_width,
        };
        if let Some((cached_key, view)) = &self.view_cache {
            if *cached_key == key {
                return Arc::clone(view);
            }
        }
        let view = Arc::new(self.build_view(visible_col_indices, column_rules, headers, max_col_width));
        self.view_cache = Some((key, Arc::clone(&view)));
        view
    }

    /// Materialize the render-ready view from scratch — the expensive pass that
    /// [`prepare_view`](Self::prepare_view) memoizes. Evaluates pre-resolved
    /// column render rules against final cell values, following the form-system
    /// pattern: rules are resolved once from shared overlay config (analogous
    /// to `FormSchema`), passed in as `column_rules`, and evaluated generically
    /// against each cell's display value.
    fn build_view(
        &self,
        visible_col_indices: &[usize],
        column_rules: &[crate::kube::overlay::ColumnRenderRules],
        headers: &[&str],
        max_col_width: u16,
    ) -> PreparedView {
        use crate::kube::resources::row::{CellValue, RowHealth};

        let items: Vec<&crate::kube::resources::row::ResourceRow> = self.filtered_indices.iter()
            .filter_map(|&i| self.items.get(i))
            .collect();
        let rows: Vec<Vec<String>> = items.iter().map(|r| {
            visible_col_indices.iter()
                .map(|&ci| r.cells.get(ci).map(|c| c.to_string()).unwrap_or_default())
                .collect()
        }).collect();
        // Per-cell rendering: zip each visible cell with its column's
        // pre-resolved rules, evaluate, then check CellValue::Status.
        // Reuses the display strings already materialized into `rows`
        // (same items × same visible_col_indices ⇒ same shape) instead of
        // stringifying every cell a second time.
        let cell_style = items.iter().zip(rows.iter()).map(|(r, row_strs)| {
            visible_col_indices.iter().zip(row_strs.iter()).map(|(&ci, cell_str)| {
                // 1. Column render rules (from overlay config). Only for
                //    cells that exist — a missing cell's empty placeholder
                //    string must not match a rule.
                if let Some(col_rules) = column_rules.get(ci) {
                    if r.cells.get(ci).is_some() {
                        if let Some(style) = col_rules.evaluate(cell_str) {
                            return Some(style);
                        }
                    }
                }
                // 2. CellValue::Status { health } from the converter.
                if let Some(CellValue::Status { health, .. }) = r.cells.get(ci) {
                    if *health != RowHealth::Normal {
                        return Some(*health);
                    }
                }
                // 3. None = inherit row style.
                None
            }).collect()
        }).collect();
        let health = items.iter().map(|r| r.health).collect();
        let keys = items.iter().map(|r| {
            crate::kube::protocol::ObjectKey::new(
                r.namespace.clone().unwrap_or_default(),
                r.name.clone(),
            )
        }).collect();
        let col_widths = column_widths(headers, &rows, max_col_width);
        PreparedView { rows, health, cell_style, keys, col_widths }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::resources::row::ResourceRow;
    use crate::kube::protocol::ObjectKey;

    /// Build a minimal ResourceRow with the given name and namespace.
    fn row(name: &str, ns: &str) -> ResourceRow {
        ResourceRow {
            name: name.into(),
            namespace: Some(ns.into()),
            ..Default::default()
        }
    }

    /// Build a table pre-loaded with `n` rows named "row-0" .. "row-(n-1)".
    fn table_with_rows(n: usize) -> StatefulTable<ResourceRow> {
        let mut t = StatefulTable::<ResourceRow>::new();
        let items: Vec<ResourceRow> = (0..n).map(|i| row(&format!("row-{}", i), "default")).collect();
        t.set_items(items);
        t
    }

    // ---- basic navigation --------------------------------------------------

    #[test]
    fn nav_next_advances_selection() {
        let mut t = table_with_rows(5);
        assert_eq!(t.selected(), 0);
        t.next();
        assert_eq!(t.selected(), 1);
        t.next();
        assert_eq!(t.selected(), 2);
    }

    #[test]
    fn nav_prev_decrements_selection() {
        let mut t = table_with_rows(5);
        t.next();
        t.next();
        assert_eq!(t.selected(), 2);
        t.previous();
        assert_eq!(t.selected(), 1);
    }

    #[test]
    fn nav_next_clamps_at_end() {
        let mut t = table_with_rows(3);
        t.next();
        t.next();
        assert_eq!(t.selected(), 2);
        t.next();
        assert_eq!(t.selected(), 2, "should not go past last item");
    }

    #[test]
    fn nav_prev_clamps_at_zero() {
        let mut t = table_with_rows(3);
        assert_eq!(t.selected(), 0);
        t.previous();
        assert_eq!(t.selected(), 0, "should not go below zero");
    }

    #[test]
    fn selected_item_returns_correct_row() {
        let mut t = table_with_rows(3);
        t.next();
        let item = t.selected_item().unwrap();
        assert_eq!(item.name, "row-1");
    }

    // ---- page navigation ---------------------------------------------------

    #[test]
    fn page_down_advances_by_page_size() {
        let mut t = table_with_rows(100);
        t.set_page_size(10);
        t.page_down();
        assert_eq!(t.selected(), 10);
    }

    #[test]
    fn page_up_retreats_by_page_size() {
        let mut t = table_with_rows(100);
        t.set_page_size(10);
        // Move to row 25 first.
        for _ in 0..25 { t.next(); }
        assert_eq!(t.selected(), 25);
        t.page_up();
        assert_eq!(t.selected(), 15);
    }

    #[test]
    fn page_down_clamps_at_end() {
        let mut t = table_with_rows(15);
        t.set_page_size(10);
        t.page_down();
        assert_eq!(t.selected(), 10);
        t.page_down();
        assert_eq!(t.selected(), 14, "should clamp to last item");
    }

    #[test]
    fn page_up_clamps_at_zero() {
        let mut t = table_with_rows(15);
        t.set_page_size(10);
        t.next(); // at 1
        t.page_up();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn page_down_adjusts_offset() {
        let mut t = table_with_rows(100);
        t.set_page_size(10);
        t.page_down(); // selected=10
        // offset must have moved so selected is visible
        assert!(t.selected() >= t.offset());
        assert!(t.selected() < t.offset() + t.page_size());
    }

    // ---- home / end --------------------------------------------------------

    #[test]
    fn home_goes_to_zero() {
        let mut t = table_with_rows(20);
        for _ in 0..15 { t.next(); }
        assert_eq!(t.selected(), 15);
        t.home();
        assert_eq!(t.selected(), 0);
        assert_eq!(t.offset(), 0);
    }

    #[test]
    fn end_goes_to_last() {
        let mut t = table_with_rows(20);
        t.end();
        assert_eq!(t.selected(), 19);
    }

    // ---- column navigation -------------------------------------------------

    #[test]
    fn col_right_advances() {
        let mut t = table_with_rows(3);
        t.set_num_cols(5);
        assert_eq!(t.selected_col(), 0);
        t.col_right(5);
        assert_eq!(t.selected_col(), 1);
    }

    #[test]
    fn col_right_clamps_at_last() {
        let mut t = table_with_rows(3);
        t.set_num_cols(3);
        t.col_right(3);
        t.col_right(3);
        assert_eq!(t.selected_col(), 2);
        t.col_right(3);
        assert_eq!(t.selected_col(), 2, "should not go past last column");
    }

    #[test]
    fn col_left_decrements() {
        let mut t = table_with_rows(3);
        t.set_num_cols(5);
        t.col_right(5);
        t.col_right(5);
        assert_eq!(t.selected_col(), 2);
        t.col_left();
        assert_eq!(t.selected_col(), 1);
    }

    #[test]
    fn col_left_clamps_at_zero() {
        let mut t = table_with_rows(3);
        assert_eq!(t.selected_col(), 0);
        t.col_left();
        assert_eq!(t.selected_col(), 0);
    }

    #[test]
    fn col_right_zero_cols_noop() {
        let mut t = table_with_rows(1);
        t.set_num_cols(0);
        t.col_right(0);
        assert_eq!(t.selected_col(), 0);
    }

    // ---- TableNav trait dispatch -------------------------------------------

    #[test]
    fn trait_nav_next_prev() {
        let mut t = table_with_rows(5);
        t.nav_next();
        t.nav_next();
        assert_eq!(t.nav_selected(), 2);
        t.nav_prev();
        assert_eq!(t.nav_selected(), 1);
    }

    #[test]
    fn trait_nav_col() {
        let mut t = table_with_rows(3);
        t.set_num_cols(4);
        t.nav_col_right();
        t.nav_col_right();
        assert_eq!(t.selected_col(), 2);
        t.nav_col_left();
        assert_eq!(t.selected_col(), 1);
    }

    #[test]
    fn trait_nav_select() {
        let mut t = table_with_rows(10);
        t.nav_select(7);
        assert_eq!(t.nav_selected(), 7);
    }

    #[test]
    fn trait_nav_select_clamps() {
        let mut t = table_with_rows(5);
        t.nav_select(100);
        assert_eq!(t.nav_selected(), 4, "should clamp to last item");
    }

    // ---- marking -----------------------------------------------------------

    #[test]
    fn toggle_mark_adds_and_removes() {
        let mut t = table_with_rows(5);
        // Mark row 0.
        t.nav_toggle_mark();
        let key = ObjectKey::new("default", "row-0");
        assert!(t.marked.contains(&key), "row-0 should be marked");

        // Toggle again to unmark.
        t.nav_toggle_mark();
        assert!(!t.marked.contains(&key), "row-0 should be unmarked after second toggle");
    }

    #[test]
    fn mark_multiple_rows() {
        let mut t = table_with_rows(5);
        t.nav_toggle_mark(); // mark row-0
        t.nav_next();
        t.nav_toggle_mark(); // mark row-1
        t.nav_next();
        t.nav_toggle_mark(); // mark row-2

        assert_eq!(t.marked.len(), 3);
        assert!(t.marked.contains(&ObjectKey::new("default", "row-0")));
        assert!(t.marked.contains(&ObjectKey::new("default", "row-1")));
        assert!(t.marked.contains(&ObjectKey::new("default", "row-2")));
    }

    #[test]
    fn clear_marks() {
        let mut t = table_with_rows(5);
        t.nav_toggle_mark();
        t.nav_next();
        t.nav_toggle_mark();
        assert_eq!(t.marked.len(), 2);
        t.nav_clear_marks();
        assert!(t.marked.is_empty());
    }

    #[test]
    fn span_mark_marks_range() {
        let mut t = table_with_rows(10);
        // Mark row-2 as anchor.
        t.nav_select(2);
        t.nav_toggle_mark();
        // Move to row-5 and span-mark.
        t.nav_select(5);
        t.nav_span_mark();
        // Rows 2..=5 should all be marked.
        for i in 2..=5 {
            let key = ObjectKey::new("default", format!("row-{}", i));
            assert!(t.marked.contains(&key), "row-{} should be marked", i);
        }
        assert_eq!(t.marked.len(), 4);
    }

    // ---- sort --------------------------------------------------------------

    #[test]
    fn sort_column_default() {
        let t = table_with_rows(3);
        assert_eq!(t.sort_column(), 0);
        assert!(t.sort_ascending());
    }

    #[test]
    fn sort_by_column_changes_column() {
        let mut t = table_with_rows(3);
        t.sort_by_column(crate::app::SortTarget::Column(2));
        assert_eq!(t.sort_column(), 2);
        assert!(t.sort_ascending(), "first click on new column should be ascending");
    }

    #[test]
    fn sort_by_same_column_toggles_direction() {
        let mut t = table_with_rows(3);
        t.sort_by_column(crate::app::SortTarget::Column(1));
        assert!(t.sort_ascending());
        t.sort_by_column(crate::app::SortTarget::Column(1));
        assert!(!t.sort_ascending(), "second click should toggle to descending");
        t.sort_by_column(crate::app::SortTarget::Column(1));
        assert!(t.sort_ascending(), "third click should toggle back to ascending");
    }

    #[test]
    fn sort_by_different_column_resets_ascending() {
        let mut t = table_with_rows(3);
        t.sort_by_column(crate::app::SortTarget::Column(1));
        t.sort_by_column(crate::app::SortTarget::Column(1)); // descending
        assert!(!t.sort_ascending());
        t.sort_by_column(crate::app::SortTarget::Column(2));
        assert!(t.sort_ascending(), "switching columns should reset to ascending");
    }

    // ---- empty table -------------------------------------------------------

    #[test]
    fn empty_next_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.next();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_prev_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.previous();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_page_up_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.page_up();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_page_down_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.page_down();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_home_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.home();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_end_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.end();
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn empty_toggle_mark_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.nav_toggle_mark();
        assert!(t.marked.is_empty());
    }

    #[test]
    fn empty_span_mark_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.nav_span_mark();
        assert!(t.marked.is_empty());
    }

    #[test]
    fn empty_selected_item_none() {
        let t = StatefulTable::<ResourceRow>::new();
        assert!(t.selected_item().is_none());
    }

    #[test]
    fn empty_visible_items_empty() {
        let t = StatefulTable::<ResourceRow>::new();
        assert!(t.visible_items().is_empty());
    }

    #[test]
    fn empty_sort_no_panic() {
        let mut t = StatefulTable::<ResourceRow>::new();
        t.sort_by_column(crate::app::SortTarget::Column(0));
        assert_eq!(t.sort_column(), 0);
    }

    #[test]
    fn empty_nav_items_count() {
        let t = table_with_rows(0);
        let counts = t.nav_items_count();
        assert_eq!(counts.filtered, 0);
        assert_eq!(counts.total, 0);
    }

    // ---- set_items / clear_data --------------------------------------------

    #[test]
    fn set_items_populates_filtered_indices() {
        let mut t = StatefulTable::<ResourceRow>::new();
        assert_eq!(t.len(), 0);
        t.set_items(vec![row("a", "ns"), row("b", "ns")]);
        assert_eq!(t.len(), 2);
        assert_eq!(t.total(), 2);
        assert!(matches!(t.data_state, TableDataState::Ready));
    }

    #[test]
    fn set_items_clamps_selection_when_shrinking() {
        let mut t = table_with_rows(10);
        t.nav_select(8);
        assert_eq!(t.selected(), 8);
        // Replace with fewer items.
        t.set_items(vec![row("a", "ns"), row("b", "ns")]);
        assert_eq!(t.selected(), 1, "selection should clamp to new last index");
    }

    #[test]
    fn clear_data_resets_everything() {
        let mut t = table_with_rows(5);
        t.nav_toggle_mark();
        t.clear_data();
        assert!(t.items.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.selected(), 0);
        assert_eq!(t.offset(), 0);
        assert!(matches!(t.data_state, TableDataState::Initializing));
        assert!(t.marked.is_empty());
    }

    // ---- filtering ---------------------------------------------------------

    #[test]
    fn apply_filter_reduces_visible_items() {
        let mut t = table_with_rows(10);
        // Filter to rows with even indices.
        t.apply_filter(|r| {
            let idx: usize = r.name.strip_prefix("row-").unwrap().parse().unwrap();
            idx % 2 == 0
        });
        assert_eq!(t.len(), 5);
        assert_eq!(t.total(), 10);
    }

    #[test]
    fn clear_filter_restores_all() {
        let mut t = table_with_rows(10);
        t.apply_filter(|r| r.name == "row-0");
        assert_eq!(t.len(), 1);
        t.clear_filter();
        assert_eq!(t.len(), 10);
    }

    #[test]
    fn filter_clamps_selection() {
        let mut t = table_with_rows(10);
        t.nav_select(9);
        assert_eq!(t.selected(), 9);
        // Filter to just 3 rows.
        t.apply_filter(|r| r.name == "row-0" || r.name == "row-1" || r.name == "row-2");
        assert!(t.selected() <= 2, "selection should be clamped after filter");
    }

    // ---- visible_items -----------------------------------------------------

    #[test]
    fn visible_items_respects_page_size() {
        let mut t = table_with_rows(50);
        t.set_page_size(10);
        let vis = t.visible_items();
        assert_eq!(vis.len(), 10);
    }

    // ---- nav_items_count ---------------------------------------------------

    #[test]
    fn nav_items_count_reflects_filter() {
        let mut t = table_with_rows(10);
        t.apply_filter(|r| r.name.ends_with('0'));
        let counts = t.nav_items_count();
        assert_eq!(counts.filtered, 1);
        assert_eq!(counts.total, 10);
    }

    // ---- prepared-view memoization -----------------------------------------

    #[test]
    fn prepare_view_reuses_cache_on_unchanged_data() {
        let mut t = table_with_rows(5);
        let a = t.prepare_view(&[0], &[], &["NAME"], 64);
        let b = t.prepare_view(&[0], &[], &["NAME"], 64);
        assert!(Arc::ptr_eq(&a, &b), "same data + columns must reuse the cached Arc");
    }

    #[test]
    fn prepare_view_rebuilds_after_data_change() {
        let mut t = table_with_rows(5);
        let a = t.prepare_view(&[0], &[], &["NAME"], 64);
        t.set_items_filtered((0..3).map(|i| row(&format!("row-{i}"), "default")).collect());
        let b = t.prepare_view(&[0], &[], &["NAME"], 64);
        assert!(!Arc::ptr_eq(&a, &b), "a data mutation must invalidate the cache");
        assert_eq!(b.rows.len(), 3);
    }

    #[test]
    fn prepare_view_rebuilds_on_column_change() {
        let mut t = table_with_rows(5);
        let a = t.prepare_view(&[0, 1], &[], &["NAME", "NS"], 64);
        let b = t.prepare_view(&[0], &[], &["NAME"], 64);
        assert!(!Arc::ptr_eq(&a, &b), "changing visible columns must invalidate the cache");
    }

    #[test]
    fn prepare_view_rebuilds_on_width_clamp_change() {
        let mut t = table_with_rows(5);
        let a = t.prepare_view(&[0], &[], &["NAME"], 64);
        let b = t.prepare_view(&[0], &[], &["NAME"], 20);
        assert!(!Arc::ptr_eq(&a, &b), "changing max_col_width must invalidate the cache");
    }

    #[test]
    fn prepare_view_survives_cursor_and_scroll() {
        // The whole point of B-lite: frames driven by navigation, not data,
        // must not pay to re-materialize the view.
        let mut t = table_with_rows(5);
        let a = t.prepare_view(&[0], &[], &["NAME"], 64);
        t.next(); // cursor move
        t.set_offset(2); // scroll
        let b = t.prepare_view(&[0], &[], &["NAME"], 64);
        assert!(Arc::ptr_eq(&a, &b), "cursor/scroll must not invalidate the materialized view");
    }

    // ---- column widths -----------------------------------------------------

    #[test]
    fn column_widths_expand_to_widest_cell() {
        // header "N" seeds 1+2=3; widest cell "wide-value" (10) wins; +3 pad = 13.
        let rows = vec![vec!["a".to_string()], vec!["wide-value".to_string()]];
        assert_eq!(column_widths(&["N"], &rows, 64), vec![13]);
    }

    #[test]
    fn column_widths_clamp_to_max() {
        let rows = vec![vec!["x".repeat(100)]];
        assert_eq!(column_widths(&["N"], &rows, 20), vec![20], "content beyond max is clamped");
    }

    #[test]
    fn column_widths_empty_headers_is_empty() {
        assert!(column_widths(&[], &[vec!["x".to_string()]], 64).is_empty());
    }
}
