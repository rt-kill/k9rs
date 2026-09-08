// ---------------------------------------------------------------------------
// TableDataState — lifecycle state for table data
// ---------------------------------------------------------------------------

/// Lifecycle state of a data source. Replaces the prior triple of
/// `has_data: bool` + `loading: bool` + `error: Option<String>` — same
/// pattern as [`crate::app::ContextSwitchState`] and
/// [`crate::app::EditState`].
///
/// Transitions:
/// - `Initializing` → `Ready` (first baseline arrives)
/// - `Ready` → `Initializing` (refresh / reused store cleared)
/// - `Ready` → `Failed` (subscription error arrives)
/// - `Failed` → `Initializing` (clear resets)
/// - `Initializing` → `Failed` (subscription fails before first baseline)
/// - `Ready` → `Stale` (the daemon's watch stopped feeding this store)
/// - `Stale` → `Ready` (the watch recovered, or any data arrived)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TableDataState {
    /// No data received yet. The UI shows a loading spinner.
    #[default]
    Initializing,
    /// At least one baseline has been received. The table may be empty
    /// (zero matching resources), but the server has responded.
    Ready,
    /// The subscription failed. The UI shows this error message instead
    /// of the loading spinner.
    Failed(String),
    /// Rows are resident and were true when they arrived, but the daemon's
    /// watch is no longer feeding them (cluster-side outage; it is
    /// retrying). Deliberately NOT `Initializing`: these rows are the last
    /// known truth and worth acting on, they are just not current — and
    /// deliberately NOT `Failed`: nothing is broken permanently.
    Stale(String),
}

// ---------------------------------------------------------------------------
// StatefulTable — a plain cursor-over-rows widget table
// ---------------------------------------------------------------------------

/// A simple owned-rows table with a cursor: the contexts panel's state.
///
/// Resource views don't use this — they are nav ELEMENTS whose data lives
/// in shared [`crate::app::store::RowStore`]s and whose cursor/sort live
/// in [`crate::app::element::TableInteraction`], materialized per frame by
/// [`crate::app::store::derive_view`]. This type remains for chrome tables
/// whose rows are small, local, and unshared.
#[derive(Debug, Clone)]
pub struct StatefulTable<T: Clone> {
    items: Vec<T>,
    selected: usize,
    /// Vertical scroll relationship (offset + render-published page height).
    /// The cursor is primary; the viewport trails it via `reveal`.
    viewport: crate::app::viewport::Viewport,
    pub data_state: TableDataState,
}

impl<T: Clone> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            selected: 0,
            viewport: crate::app::viewport::Viewport::seeded(40),
            data_state: TableDataState::Initializing,
        }
    }
}

impl<T: Clone> StatefulTable<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn selected(&self) -> usize {
        self.selected
    }

    pub fn offset(&self) -> usize {
        self.viewport.offset()
    }

    /// Render write-back: publish the real page height (the widget's inner row
    /// count) and reveal the cursor. Called each frame by the context render,
    /// so paging and scroll use the true height, not a hardcoded default.
    pub fn set_page_size(&mut self, size: usize) {
        self.viewport.set_metrics(self.items.len(), size, false);
        self.viewport.reveal(self.selected);
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        self.data_state = TableDataState::Ready;
        self.items = items;
        self.clamp_selection();
        self.adjust_offset();
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn selected_item(&self) -> Option<&T> {
        self.items.get(self.selected)
    }

    pub fn visible_items(&self) -> Vec<&T> {
        let start = self.viewport.offset();
        if start >= self.items.len() {
            return Vec::new();
        }
        let end = (start + self.viewport.viewport_rows()).min(self.items.len());
        self.items[start..end].iter().collect()
    }

    pub fn next(&mut self) {
        if !self.items.is_empty() && self.selected + 1 < self.items.len() {
            self.selected += 1;
        }
        self.adjust_offset();
    }

    pub fn previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
        self.adjust_offset();
    }

    pub fn page_up(&mut self) {
        self.selected = self.selected.saturating_sub(self.viewport.viewport_rows());
        self.adjust_offset();
    }

    pub fn page_down(&mut self) {
        if !self.items.is_empty() {
            self.selected = (self.selected + self.viewport.viewport_rows()).min(self.items.len() - 1);
        }
        self.adjust_offset();
    }

    pub fn home(&mut self) {
        self.selected = 0;
        self.viewport.reveal(0);
    }

    pub fn end(&mut self) {
        if !self.items.is_empty() {
            self.selected = self.items.len() - 1;
        }
        self.adjust_offset();
    }

    /// Single enforcement point for `selected < items.len()` (or both
    /// zero when empty).
    fn clamp_selection(&mut self) {
        if self.items.is_empty() {
            self.selected = 0;
            self.viewport.reveal(0);
        } else if self.selected >= self.items.len() {
            self.selected = self.items.len() - 1;
        }
    }

    fn adjust_offset(&mut self) {
        // Reveal the cursor within the render-published viewport height.
        self.viewport.reveal(self.selected);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/app/table.rs"]
mod tests;
