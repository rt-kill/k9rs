use std::collections::HashSet;

use crate::kube::resources::KubeResource;

use super::types::{parse_age_seconds, ItemCounts};

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
    fn nav_reset(&mut self);
    fn nav_toggle_mark(&mut self);
    fn nav_sort_by(&mut self, col: usize);
    fn nav_toggle_sort(&mut self);
    fn nav_items_count(&self) -> ItemCounts;
    fn nav_clear_marks(&mut self);
    fn nav_span_mark(&mut self);
    fn nav_select(&mut self, idx: usize);
    fn nav_selected(&self) -> usize;
}

impl<T: Clone + KubeResource> TableNav for StatefulTable<T> {
    fn nav_next(&mut self) { self.next(); }
    fn nav_prev(&mut self) { self.previous(); }
    fn nav_page_up(&mut self) { self.page_up(); }
    fn nav_page_down(&mut self) { self.page_down(); }
    fn nav_home(&mut self) { self.home(); }
    fn nav_end(&mut self) { self.end(); }
    fn nav_clear_filter(&mut self) { self.clear_filter(); }
    fn nav_reset(&mut self) { self.clear_data(); }
    fn nav_toggle_mark(&mut self) {
        if !self.filtered_indices.is_empty() && self.selected < self.filtered_indices.len() {
            let real_idx = self.filtered_indices[self.selected];
            if self.marked.contains(&real_idx) {
                self.marked.remove(&real_idx);
            } else {
                self.marked.insert(real_idx);
            }
            // Don't advance — Space is a clean toggle. Ctrl+Space handles range selection.
        }
    }
    fn nav_sort_by(&mut self, col: usize) { self.sort_by_column(col); }
    fn nav_toggle_sort(&mut self) {
        let col = self.sort_column;
        self.sort_by_column(col);
    }
    fn nav_items_count(&self) -> ItemCounts {
        ItemCounts { filtered: self.len(), total: self.total() }
    }
    fn nav_clear_marks(&mut self) { self.marked.clear(); }
    fn nav_span_mark(&mut self) {
        if self.filtered_indices.is_empty() { return; }
        let current = self.selected.min(self.filtered_indices.len() - 1);
        let _current_real = self.filtered_indices[current];
        // Find the nearest existing mark to use as anchor
        let anchor = self.marked.iter().copied()
            .filter_map(|idx| self.filtered_indices.iter().position(|&i| i == idx))
            .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
            .unwrap_or(0);
        let (start, end) = if anchor <= current { (anchor, current) } else { (current, anchor) };
        for i in start..=end {
            if i < self.filtered_indices.len() {
                self.marked.insert(self.filtered_indices[i]);
            }
        }
    }
    fn nav_select(&mut self, idx: usize) {
        self.selected = idx.min(self.filtered_indices.len().saturating_sub(1));
        self.adjust_offset();
    }
    fn nav_selected(&self) -> usize { self.selected }
}

// ---------------------------------------------------------------------------
// StatefulTable
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatefulTable<T: Clone> {
    pub items: Vec<T>,
    pub filtered_indices: Vec<usize>,
    pub selected: usize,
    pub offset: usize,
    pub sort_column: usize,
    pub sort_ascending: bool,
    pub page_size: usize,
    pub filter_text: String,
    /// Cached compiled regex for the current filter_text (avoids recompiling on every data update).
    /// When true, filtering is managed by the NavStack's `reapply_nav_filters()`.
    /// `set_items_filtered` will skip its own `filter_text`-based filtering and
    /// show all items, letting `reapply_nav_filters` apply the real filter after.
    pub nav_managed: bool,
    /// Whether this table has received any response from the watcher.
    /// Used to distinguish "loading" (false) from "empty" (true + no items) in the UI.
    pub has_data: bool,
    /// Whether the initial list is still streaming in (InitApply phase).
    /// When true, the title shows a loading indicator alongside the count.
    pub loading: bool,
    /// Error message if the subscription failed (e.g., resource doesn't exist).
    /// When set, the UI shows this instead of the loading spinner.
    pub error: Option<String>,
    /// Previous item count — used to detect when initial loading completes.
    prev_item_count: usize,
    /// Set of marked/selected row indices (real indices into `items`).
    pub marked: HashSet<usize>,
}

impl<T: Clone> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            filtered_indices: Vec::new(),
            selected: 0,
            offset: 0,
            sort_column: 0,
            sort_ascending: true,
            page_size: 40,
            filter_text: String::new(),
            nav_managed: false,
            has_data: false,
            loading: false,
            error: None,
            prev_item_count: 0,
            marked: HashSet::new(),
        }
    }
}

impl<T: Clone> StatefulTable<T> {
    pub fn new() -> Self { Self::default() }

    pub fn len(&self) -> usize { self.filtered_indices.len() }
    pub fn is_empty(&self) -> bool { self.filtered_indices.is_empty() }
    pub fn total(&self) -> usize { self.items.len() }

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

    pub fn set_items(&mut self, items: Vec<T>) {
        self.has_data = true;
        self.items = items;
        self.marked.clear();
        self.filtered_indices = (0..self.items.len()).collect();
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    pub fn apply_filter<F: Fn(&T) -> bool>(&mut self, pred: F) {
        self.filtered_indices = self.items.iter().enumerate()
            .filter(|(_, item)| pred(item))
            .map(|(i, _)| i)
            .collect();
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    /// Clear all data and reset `has_data` to false.
    pub fn clear_data(&mut self) {
        self.items.clear();
        self.filtered_indices.clear();
        self.selected = 0;
        self.offset = 0;
        self.nav_managed = false;
        self.has_data = false;
        self.loading = false;
        self.error = None;
        self.prev_item_count = 0;
        self.marked.clear();
    }

    pub fn clear_filter(&mut self) {
        self.filter_text.clear();
        self.nav_managed = false;
        self.filtered_indices = (0..self.items.len()).collect();
        if self.selected >= self.filtered_indices.len() && !self.filtered_indices.is_empty() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
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
    /// Toggle sort on a column. Reuses sort_items and rebuild_filter.
    /// Cursor stays at its screen index.
    pub fn sort_by_column(&mut self, col: usize) {
        let actual_col = if col == usize::MAX {
            // Resolve sentinel to last column using actual data width,
            // not the static trait fallback (which is meaningless for ResourceRow).
            self.items.first()
                .map(|item| item.cells().len().saturating_sub(1))
                .unwrap_or(0)
        } else {
            col
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
    /// screen index — it does NOT follow item identity.
    pub fn set_items_filtered(&mut self, items: Vec<T>) {
        self.update_loading(items.len());
        self.items = items;
        self.marked.clear(); // Indices invalidated by sort.
        self.sort_items();
        self.rebuild_filter();
        self.clamp_selection();
        self.adjust_offset();
    }

    /// Update loading indicator based on item count changes.
    fn update_loading(&mut self, new_count: usize) {
        if self.loading {
            if new_count <= self.prev_item_count {
                self.loading = false;
            }
        } else if self.prev_item_count == 0 && new_count > 0 {
            self.loading = true;
        }
        self.prev_item_count = new_count;
        self.has_data = true;
    }

    /// Sort items by the current sort column with a stable tiebreaker.
    fn sort_items(&mut self) {
        if self.items.is_empty() { return; }
        let col = self.sort_column;
        let asc = self.sort_ascending;
        // Detect whether this column contains age-formatted values (e.g. "3d5h",
        // "10m") by sampling the first non-empty cell. This avoids depending on
        // header names (which ResourceRow doesn't expose via the trait).
        let is_age_col = self.items.iter()
            .find_map(|item| {
                let val = item.cells().get(col)?.as_str();
                if val.is_empty() || val == "<unknown>" { return None; }
                Some(val.chars().all(|c| c.is_ascii_digit() || matches!(c, 'd' | 'h' | 'm' | 's'))
                    && val.chars().any(|c| matches!(c, 'd' | 'h' | 'm' | 's')))
            })
            .unwrap_or(false);
        self.items.sort_by(|a, b| {
            let a_cells = a.cells();
            let b_cells = b.cells();
            let a_val = a_cells.get(col).map(|c| c.as_str()).unwrap_or("");
            let b_val = b_cells.get(col).map(|c| c.as_str()).unwrap_or("");
            let primary = if is_age_col {
                parse_age_seconds(a_val).cmp(&parse_age_seconds(b_val))
            } else if let (Ok(an), Ok(bn)) = (a_val.parse::<f64>(), b_val.parse::<f64>()) {
                an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                a_val.cmp(b_val)
            };
            let primary = if asc { primary } else { primary.reverse() };
            if primary == std::cmp::Ordering::Equal {
                a.namespace().cmp(b.namespace())
                    .then_with(|| a.name().cmp(b.name()))
            } else {
                primary
            }
        });
    }

    /// Rebuild filtered_indices from current items and filter state.
    pub fn rebuild_filter(&mut self) {
        self.filtered_indices.clear();
        if self.nav_managed {
            self.filtered_indices.extend(0..self.items.len());
        } else if !self.filter_text.is_empty() {
            let pat = crate::util::SearchPattern::new(&self.filter_text);
            for (i, item) in self.items.iter().enumerate() {
                if item.cells().iter().any(|cell| pat.is_match(cell)) {
                    self.filtered_indices.push(i);
                }
            }
        } else {
            self.filtered_indices.extend(0..self.items.len());
        }
    }

    /// Clamp selection to valid range.
    fn clamp_selection(&mut self) {
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
    }
}
