
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, StatefulWidget, Widget},
};
use unicode_width::UnicodeWidthStr;

use crate::ui::theme::Theme;

/// State for the ResourceTable widget.
pub struct ResourceTableState {
    pub selected: usize,
    pub offset: usize,
    pub selected_col: usize,
    pub col_offset: u16,
    pub filtered_count: usize,
}

/// Pre-computed layout for all columns: positions, widths, viewport bounds.
/// Built once per render, threaded through every row — no per-cell recomputation.
struct ColumnLayout {
    widths: Vec<u16>,
    /// Pixel x-position of each column's left edge (before viewport offset).
    positions: Vec<u16>,
    /// Pixel x-position one past the last column's right edge.
    total_width: u16,
    /// Viewport bounds (pixel offsets into the virtual column space).
    viewport_left: u16,
    viewport_right: u16,
    /// Which column is selected.
    sel_col: usize,
    /// The screen x-origin (inner.x of the bordered block).
    origin_x: u16,
}

impl ColumnLayout {
    fn new(widths: Vec<u16>, sel_col: usize, col_offset: u16, viewport_width: u16, origin_x: u16) -> Self {
        let mut positions = Vec::with_capacity(widths.len());
        let mut acc: u16 = 0;
        for &w in &widths {
            positions.push(acc);
            acc = acc.saturating_add(w);
        }
        Self {
            widths,
            positions,
            total_width: acc,
            viewport_left: col_offset,
            viewport_right: col_offset + viewport_width,
            sel_col,
            origin_x,
        }
    }

    /// Is column `i` at least partially visible in the viewport?
    fn is_visible(&self, i: usize) -> bool {
        let start = self.positions[i];
        let end = start + self.widths[i];
        end > self.viewport_left && start < self.viewport_right
    }

    /// Screen x-position for column `i`'s left edge.
    fn screen_x(&self, i: usize) -> u16 {
        self.origin_x + self.positions[i].saturating_sub(self.viewport_left)
    }

    /// How many pixels of column `i` are visible (may be clipped at right edge).
    fn visible_width(&self, i: usize) -> u16 {
        self.widths[i].min(self.viewport_right.saturating_sub(self.positions[i]))
    }

    /// Whether the column border at `i` should be highlighted. A border is
    /// highlighted if it's the selected column's left edge (i == sel_col)
    /// or the selected column's right edge (i == sel_col + 1, since each
    /// column only draws its LEFT border).
    fn is_highlighted_border(&self, i: usize) -> bool {
        i == self.sel_col || i == self.sel_col + 1
    }

    /// Screen x-position for the trailing │ after the last column.
    /// Returns None if it falls outside the viewport.
    fn trailing_border_x(&self) -> Option<u16> {
        if self.total_width > self.viewport_left {
            let x = self.origin_x + self.total_width.saturating_sub(self.viewport_left);
            if x < self.origin_x + (self.viewport_right - self.viewport_left) {
                return Some(x);
            }
        }
        None
    }

    /// Whether the trailing border (after last column) should be highlighted.
    fn is_trailing_highlighted(&self) -> bool {
        self.widths.len().saturating_sub(1) == self.sel_col
    }
}

/// Maximum column width in characters.
const MAX_COL_WIDTH: u16 = 64;

pub struct ResourceTable<'a> {
    headers: Vec<&'a str>,
    rows: &'a [Vec<String>],
    title: &'a str,
    namespace: &'a str,
    sort_col: Option<usize>,
    sort_asc: bool,
    theme: &'a Theme,
    marked: &'a std::collections::HashSet<crate::kube::protocol::ObjectKey>,
    changed_rows: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    row_keys: &'a [crate::kube::protocol::ObjectKey],
    row_health: &'a [crate::kube::resources::row::RowHealth],
}

impl<'a> ResourceTable<'a> {
    pub fn new(
        headers: Vec<&'a str>,
        rows: &'a [Vec<String>],
        title: &'a str,
        theme: &'a Theme,
    ) -> Self {
        static EMPTY_MAP: std::sync::LazyLock<std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>> = std::sync::LazyLock::new(std::collections::HashMap::new);
        static EMPTY_MARKED: std::sync::LazyLock<std::collections::HashSet<crate::kube::protocol::ObjectKey>> = std::sync::LazyLock::new(std::collections::HashSet::new);
        Self {
            headers, rows, title, namespace: "",
            sort_col: None, sort_asc: true, theme,
            marked: &EMPTY_MARKED,
            changed_rows: &EMPTY_MAP,
            row_keys: &[],
            row_health: &[],
        }
    }

    pub fn row_keys(mut self, keys: &'a [crate::kube::protocol::ObjectKey]) -> Self { self.row_keys = keys; self }
    pub fn row_health(mut self, health: &'a [crate::kube::resources::row::RowHealth]) -> Self { self.row_health = health; self }
    pub fn marked(mut self, marked: &'a std::collections::HashSet<crate::kube::protocol::ObjectKey>) -> Self { self.marked = marked; self }
    pub fn sort(mut self, col: Option<usize>, ascending: bool) -> Self { self.sort_col = col; self.sort_asc = ascending; self }
    pub fn namespace(mut self, ns: &'a str) -> Self { self.namespace = ns; self }
    pub fn changed_rows(mut self, changed: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>) -> Self { self.changed_rows = changed; self }

    fn health_at(&self, idx: usize) -> crate::kube::resources::row::RowHealth {
        self.row_health.get(idx).copied().unwrap_or_default()
    }

    /// Compute natural column widths from content. No scaling.
    fn compute_col_widths(&self, rows: &[&Vec<String>]) -> Vec<u16> {
        if self.headers.is_empty() { return Vec::new(); }
        let mut widths: Vec<u16> = self.headers.iter()
            .map(|h| h.width() as u16 + 2) // +2 for sort indicator
            .collect();
        for row in rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.width() as u16);
                }
            }
        }
        // +3 per column: │(1) + pad(1) + content + pad(1).
        for w in &mut widths { *w = (*w + 3).min(MAX_COL_WIDTH); }
        widths
    }

    /// Render a single cell: `│ text  ` (left border + padded content).
    /// The right edge is the next column's left `│` (or the trailing `│`
    /// for the last column). Content is left-padded to fill the full
    /// inner width so background styles extend edge-to-edge.
    fn render_cell(buf: &mut Buffer, x: u16, y: u16, width: u16, text: &str, style: Style, border_style: Style) {
        if width < 3 { return; }
        let inner = (width as usize) - 3;
        let text_width = text.width();
        let display = if text_width > inner {
            let mut result = String::new();
            let mut w = 0;
            let target = inner.saturating_sub(1);
            for ch in text.chars() {
                let cw = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
                if w + cw > target { break; }
                result.push(ch);
                w += cw;
            }
            result.push('\u{2026}');
            result
        } else {
            text.to_string()
        };
        buf.set_string(x, y, "│", border_style);
        buf.set_string(x + 1, y, format!(" {:<width$} ", display, width = inner), style);
    }

    fn build_title_spans(&self, row_count: usize) -> Line<'a> {
        let mut spans = Vec::new();
        spans.push(Span::styled(format!(" {}", self.title.to_lowercase()), self.theme.title));
        if !self.namespace.is_empty() {
            spans.push(Span::styled("(", self.theme.title));
            spans.push(Span::styled(self.namespace.to_string(), self.theme.title_namespace));
            spans.push(Span::styled(")", self.theme.title));
        }
        spans.push(Span::styled(format!("[{}]", row_count), self.theme.title_counter));
        spans.push(Span::styled(" ", self.theme.title));
        Line::from(spans)
    }

    /// Resolve the content + border styles for a cell at `(row_selected, col_idx)`.
    /// Encapsulates the priority rules:
    ///   Row selected > row marked > col highlighted > row health/normal
    fn cell_styles(&self, base: Style, col_idx: usize, is_row_selected: bool, layout: &ColumnLayout) -> (Style, Style) {
        let content = if col_idx == layout.sel_col && !is_row_selected {
            base.patch(self.theme.col_highlight)
        } else {
            base
        };
        let border = if layout.is_highlighted_border(col_idx) && !is_row_selected {
            self.theme.border.patch(self.theme.col_highlight)
        } else {
            self.theme.border
        };
        (content, border)
    }

    /// Render a row of cells (header or data) using the shared layout.
    fn render_row(
        &self,
        buf: &mut Buffer,
        y: u16,
        cells: &[&str],
        base_style: Style,
        is_row_selected: bool,
        layout: &ColumnLayout,
    ) {
        for (i, &cell) in cells.iter().enumerate() {
            if i >= layout.widths.len() || !layout.is_visible(i) { continue; }
            let (content_style, border_style) = self.cell_styles(base_style, i, is_row_selected, layout);
            Self::render_cell(buf, layout.screen_x(i), y, layout.visible_width(i), cell, content_style, border_style);
        }
        // Trailing │ after last column.
        if let Some(tx) = layout.trailing_border_x() {
            let border = if layout.is_trailing_highlighted() && !is_row_selected {
                self.theme.border.patch(self.theme.col_highlight)
            } else {
                self.theme.border
            };
            buf.set_string(tx, y, "│", border);
        }
    }
}

impl StatefulWidget for ResourceTable<'_> {
    type State = ResourceTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let all_rows: Vec<&Vec<String>> = self.rows.iter().collect();
        state.filtered_count = all_rows.len();

        // Clamp row selection.
        if all_rows.is_empty() {
            state.selected = 0;
            state.offset = 0;
        } else if state.selected >= all_rows.len() {
            state.selected = all_rows.len() - 1;
        }

        // Draw bordered block with title.
        let title_line = self.build_title_spans(all_rows.len());
        let block = Block::bordered()
            .title(title_line)
            .border_style(self.theme.border);
        let inner = block.inner(area);
        block.render(area, buf);
        if inner.height == 0 || inner.width == 0 { return; }

        // Column layout.
        let col_widths = self.compute_col_widths(&all_rows);
        if col_widths.is_empty() { return; }

        // Clamp column selection.
        if state.selected_col >= col_widths.len() {
            state.selected_col = col_widths.len().saturating_sub(1);
        }

        // Build layout and adjust horizontal scroll.
        let mut layout = ColumnLayout::new(col_widths, state.selected_col, state.col_offset, inner.width, inner.x);
        let sel_start = layout.positions[state.selected_col];
        let sel_end = sel_start + layout.widths[state.selected_col];
        if sel_start < state.col_offset {
            state.col_offset = sel_start;
            layout = ColumnLayout::new(layout.widths, state.selected_col, state.col_offset, inner.width, inner.x);
        }
        if sel_end > state.col_offset + inner.width {
            state.col_offset = sel_end.saturating_sub(inner.width);
            layout = ColumnLayout::new(layout.widths, state.selected_col, state.col_offset, inner.width, inner.x);
        }

        // --- Header row ---
        let header_y = inner.y;
        let header_strs: Vec<&str> = self.headers.to_vec();

        // Base header render.
        let header_style = self.theme.header;
        self.render_row(buf, header_y, &header_strs, header_style, false, &layout);

        // Sort indicator overlay (on top of the header cell).
        if let Some(sort_i) = self.sort_col {
            if sort_i < layout.widths.len() && layout.is_visible(sort_i) {
                let arrow = if self.sort_asc { "\u{2191}" } else { "\u{2193}" };
                let vw = layout.visible_width(sort_i);
                let arrow_x = layout.screen_x(sort_i) + vw.saturating_sub(2);
                if arrow_x < inner.x + inner.width {
                    buf.set_string(arrow_x, header_y, arrow, self.theme.sort_indicator);
                }
            }
        }

        // --- Data rows (virtual scroll) ---
        let data_start_y = header_y + 1;
        let visible_height = (inner.y + inner.height).saturating_sub(data_start_y) as usize;
        if visible_height == 0 { return; }

        // Adjust vertical offset.
        if state.selected < state.offset { state.offset = state.selected; }
        if state.selected >= state.offset + visible_height {
            state.offset = state.selected - visible_height + 1;
        }

        let end = (state.offset + visible_height).min(all_rows.len());
        for (vi, row_idx) in (state.offset..end).enumerate() {
            let y = data_start_y + vi as u16;
            if y >= inner.y + inner.height { break; }

            let row = all_rows[row_idx];
            let is_selected = row_idx == state.selected;
            let is_marked = !self.marked.is_empty()
                && self.row_keys.get(row_idx).is_some_and(|k| self.marked.contains(k));
            let is_changed = !self.changed_rows.is_empty()
                && self.row_keys.get(row_idx).is_some_and(|k| self.changed_rows.contains_key(k));

            // Fill selected row background edge-to-edge. Uses `selected`
            // directly — `selected_marked` intentionally has no bg so it
            // inherits this fill via ratatui's `None`-means-don't-touch
            // style patching.
            if is_selected {
                for dx in 0..inner.width {
                    buf.set_string(inner.x + dx, y, " ", self.theme.selected);
                }
            }

            // Cell style priority: selected > marked > changed > health.
            let cell_style = if is_selected && is_marked {
                self.theme.selected_marked
            } else if is_selected {
                self.theme.selected
            } else if is_marked {
                self.theme.marked_row
            } else if is_changed {
                self.theme.delta_changed
            } else {
                use crate::kube::resources::row::RowHealth;
                match self.health_at(row_idx) {
                    RowHealth::Failed => self.theme.status_failed,
                    RowHealth::Pending => self.theme.status_pending,
                    RowHealth::Normal => self.theme.row_normal,
                }
            };

            let cell_strs: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
            self.render_row(buf, y, &cell_strs, cell_style, is_selected, &layout);
        }
    }
}
