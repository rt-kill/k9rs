use std::collections::HashSet;

use crate::kube::protocol::ObjectKey;
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
        if self.filtered_indices.is_empty() || self.selected >= self.filtered_indices.len() {
            return;
        }
        let real_idx = self.filtered_indices[self.selected];
        let Some(item) = self.items.get(real_idx) else { return };
        let key = ObjectKey::new(item.namespace(), item.name());
        if self.marked.contains(&key) {
            self.marked.remove(&key);
        } else {
            self.marked.insert(key);
        }
        // Maintain the visible-mark cache: a single bit flip at the cursor.
        if let Some(slot) = self.marked_visible.get_mut(self.selected) {
            *slot = !*slot;
        }
        // Don't advance — Space is a clean toggle. Ctrl+Space handles range selection.
    }
    fn nav_sort_by(&mut self, col: usize) { self.sort_by_column(col); }
    fn nav_toggle_sort(&mut self) {
        let col = self.sort_column;
        self.sort_by_column(col);
    }
    fn nav_items_count(&self) -> ItemCounts {
        ItemCounts { filtered: self.len(), total: self.total() }
    }
    fn nav_clear_marks(&mut self) {
        self.marked.clear();
        // All visible bits drop to false in one shot.
        for slot in self.marked_visible.iter_mut() { *slot = false; }
    }
    fn nav_span_mark(&mut self) {
        if self.filtered_indices.is_empty() { return; }
        let current = self.selected.min(self.filtered_indices.len() - 1);
        // Find the nearest existing mark via the cached visible bitmap —
        // no per-position ObjectKey allocation.
        let anchor = self.marked_visible.iter()
            .enumerate()
            .filter_map(|(pos, marked)| if *marked { Some(pos) } else { None })
            .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
            .unwrap_or(0);
        let (start, end) = if anchor <= current { (anchor, current) } else { (current, anchor) };
        for pos in start..=end {
            if let Some(real) = self.filtered_indices.get(pos).copied() {
                if let Some(item) = self.items.get(real) {
                    let key = ObjectKey::new(item.namespace(), item.name());
                    self.marked.insert(key);
                    if let Some(slot) = self.marked_visible.get_mut(pos) {
                        *slot = true;
                    }
                }
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
    /// Marked/selected rows, keyed by stable identity (namespace + name) so
    /// marks survive data refreshes and sort changes.
    pub marked: HashSet<ObjectKey>,
    /// Cached membership-by-visible-position for `marked`. Same length as
    /// `filtered_indices`; `marked_visible[i] == true` iff the row at visible
    /// position `i` is marked. Recomputed exactly when filters or marks
    /// change — never per-frame. Eliminates the per-row `String` clones
    /// that the render path used to do to build a transient HashSet.
    pub marked_visible: Vec<bool>,
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
            has_data: false,
            loading: false,
            error: None,
            prev_item_count: 0,
            marked: HashSet::new(),
            marked_visible: Vec::new(),
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
        self.filtered_indices = (0..self.items.len()).collect();
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        // Reset the marked-visible bitmap to match the new visible set.
        // KubeResource tables that need it actually populated will call
        // `refresh_marked_visible()` separately (we can't do it here without
        // a `KubeResource` bound, and the bound would break the contexts
        // table which uses `set_items` but has no marks).
        self.marked_visible.clear();
        self.marked_visible.resize(self.filtered_indices.len(), false);
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
        // Bitmap layout invariant: same length as filtered_indices. Callers
        // with marks should call `refresh_marked_visible()` afterwards
        // (only `KubeResource`-bounded tables have marks anyway).
        self.marked_visible.clear();
        self.marked_visible.resize(self.filtered_indices.len(), false);
        self.adjust_offset();
    }

    /// Clear all data and reset `has_data` to false.
    pub fn clear_data(&mut self) {
        self.items.clear();
        self.filtered_indices.clear();
        self.selected = 0;
        self.offset = 0;
        self.has_data = false;
        self.loading = false;
        self.error = None;
        self.prev_item_count = 0;
        self.marked.clear();
        self.marked_visible.clear();
    }

    pub fn clear_filter(&mut self) {
        self.filtered_indices = (0..self.items.len()).collect();
        if self.selected >= self.filtered_indices.len() && !self.filtered_indices.is_empty() {
            self.selected = self.filtered_indices.len() - 1;
        }
        // Same invariant as `apply_filter`.
        self.marked_visible.clear();
        self.marked_visible.resize(self.filtered_indices.len(), false);
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
    /// screen index — it does NOT follow item identity. Marks are keyed by
    /// identity so they survive refreshes; any mark whose row no longer
    /// exists in the new snapshot is dropped.
    pub fn set_items_filtered(&mut self, items: Vec<T>) {
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

    /// Rebuild `filtered_indices` to show every row. Filtering itself is
    /// owned by `App::reapply_nav_filters` (which calls `apply_filter` with
    /// the nav-stack-derived predicates) — the table is just the storage,
    /// not the policy. After rebuilding the visible set, refresh the
    /// marked-visible cache so the bitmap stays in sync.
    pub fn rebuild_filter(&mut self) {
        self.filtered_indices.clear();
        self.filtered_indices.extend(0..self.items.len());
        self.refresh_marked_visible();
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

    /// Recompute the `marked_visible` cache from `filtered_indices` + `marked`.
    /// Called from any mutator that touches either side of the join. The render
    /// path borrows the result without doing per-row work.
    pub fn refresh_marked_visible(&mut self) {
        self.marked_visible.clear();
        self.marked_visible.resize(self.filtered_indices.len(), false);
        if self.marked.is_empty() {
            return;
        }
        for (pos, &real_idx) in self.filtered_indices.iter().enumerate() {
            if let Some(item) = self.items.get(real_idx) {
                let key = ObjectKey::new(item.namespace(), item.name());
                if self.marked.contains(&key) {
                    self.marked_visible[pos] = true;
                }
            }
        }
    }
}
