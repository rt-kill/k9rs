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
    offset: usize,
    page_size: usize,
    pub data_state: TableDataState,
}

impl<T: Clone> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            selected: 0,
            offset: 0,
            page_size: 40,
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
        self.offset
    }

    pub fn set_page_size(&mut self, size: usize) {
        self.page_size = size;
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
        let end = (self.offset + self.page_size).min(self.items.len());
        if self.offset >= self.items.len() {
            return Vec::new();
        }
        self.items[self.offset..end].iter().collect()
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
        self.selected = self.selected.saturating_sub(self.page_size);
        self.adjust_offset();
    }

    pub fn page_down(&mut self) {
        if !self.items.is_empty() {
            self.selected = (self.selected + self.page_size).min(self.items.len() - 1);
        }
        self.adjust_offset();
    }

    pub fn home(&mut self) {
        self.selected = 0;
        self.offset = 0;
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
            self.offset = 0;
        } else if self.selected >= self.items.len() {
            self.selected = self.items.len() - 1;
        }
    }

    fn adjust_offset(&mut self) {
        if self.page_size == 0 {
            return;
        }
        if self.selected < self.offset {
            self.offset = self.selected;
        }
        if self.selected >= self.offset + self.page_size {
            self.offset = self.selected - self.page_size + 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn table(n: usize) -> StatefulTable<usize> {
        let mut t = StatefulTable::new();
        t.set_items((0..n).collect());
        t
    }

    #[test]
    fn cursor_moves_and_clamps() {
        let mut t = table(3);
        t.next();
        t.next();
        assert_eq!(t.selected(), 2);
        t.next(); // at end: stays
        assert_eq!(t.selected(), 2);
        t.previous();
        assert_eq!(t.selected(), 1);
        t.home();
        assert_eq!(t.selected(), 0);
        t.end();
        assert_eq!(t.selected(), 2);
    }

    #[test]
    fn paging_respects_bounds() {
        let mut t = table(100);
        t.set_page_size(10);
        t.page_down();
        assert_eq!(t.selected(), 10);
        t.page_up();
        assert_eq!(t.selected(), 0);
        t.page_up(); // at start: stays
        assert_eq!(t.selected(), 0);
    }

    #[test]
    fn set_items_clamps_selection_and_sets_ready() {
        let mut t = table(10);
        t.end();
        assert_eq!(t.selected(), 9);
        t.set_items(vec![1, 2, 3]);
        assert_eq!(t.selected(), 2);
        assert_eq!(t.data_state, TableDataState::Ready);
        t.set_items(Vec::new());
        assert_eq!(t.selected(), 0);
        assert!(t.selected_item().is_none());
    }

    #[test]
    fn visible_items_windows_by_offset_and_page() {
        let mut t = table(50);
        t.set_page_size(5);
        t.end();
        let visible: Vec<usize> = t.visible_items().into_iter().copied().collect();
        assert_eq!(visible, vec![45, 46, 47, 48, 49]);
        assert_eq!(t.offset(), 45);
    }

    #[test]
    fn empty_table_is_safe_everywhere() {
        let mut t: StatefulTable<usize> = StatefulTable::new();
        t.next();
        t.previous();
        t.page_down();
        t.end();
        assert_eq!(t.selected(), 0);
        assert!(t.visible_items().is_empty());
        assert!(t.selected_item().is_none());
    }
}
