
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, StatefulWidget, Widget},
};
use unicode_width::UnicodeWidthStr;

use crate::ui::theme::Theme;

/// State for the ResourceTable widget, tracking selection and scroll offset.
pub struct ResourceTableState {
    /// Currently selected row index (relative to the full dataset, not viewport).
    pub selected: usize,
    /// Scroll offset -- the index of the first visible row.
    pub offset: usize,
    /// Number of rows after filtering (set by the render call).
    pub filtered_count: usize,
}

// `ResourceTableState` is pure data. The authoritative state lives in
// [`crate::app::StatefulTable`] (one per resource); the view function
// builds a `ResourceTableState` via struct literal each draw, so impl
// methods would never be called.

/// A high-performance virtual scrolling table widget.
///
/// Title format: `resource(namespace)[count]`
/// Header row: white bold
/// Selected row: black on aqua
/// Sort indicators: orange arrows
pub struct ResourceTable<'a> {
    headers: Vec<&'a str>,
    rows: &'a [Vec<String>],
    title: &'a str,
    namespace: &'a str,
    sort_col: Option<usize>,
    sort_asc: bool,
    theme: &'a Theme,
    /// Per-visible-position mark bitmap. `marked_visible[i] == true` iff the
    /// row at visible position `i` is marked. Borrowed directly from the
    /// table's cached bitmap — no per-frame allocation, no per-row hashing.
    /// Empty slice means "no marks visible" (the cache hasn't been refreshed
    /// since the last filter change, or there are no marks).
    marked_visible: &'a [bool],
    /// Rows that changed recently: (namespace, name) -> when. Used for delta highlights.
    changed_rows: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    /// Typed `(namespace, name)` keys per visible row, in the same order
    /// as `rows`. Used to look up delta highlights in `changed_rows`. The
    /// widget used to build keys from `row[0]`/`row[1]` but that assumed
    /// namespace=col0/name=col1, which is wrong for cluster-scoped tables
    /// (name=col0) and when the NAMESPACE column is hidden. Taking a
    /// parallel `ObjectKey` slice puts the caller in charge of the typed
    /// lookup.
    row_keys: &'a [crate::kube::protocol::ObjectKey],
    /// Server-computed health per row, used for row coloring. Indexed by
    /// visible row position (same order as `rows`).
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
        Self {
            headers,
            rows,
            title,
            namespace: "",
            sort_col: None,
            sort_asc: true,
            theme,
            marked_visible: &[],
            changed_rows: &EMPTY_MAP,
            row_keys: &[],
            row_health: &[],
        }
    }

    pub fn row_keys(mut self, keys: &'a [crate::kube::protocol::ObjectKey]) -> Self {
        self.row_keys = keys;
        self
    }

    pub fn row_health(mut self, health: &'a [crate::kube::resources::row::RowHealth]) -> Self {
        self.row_health = health;
        self
    }

    /// Get the server-computed health for a visible row index.
    fn health_at(&self, visible_idx: usize) -> crate::kube::resources::row::RowHealth {
        self.row_health.get(visible_idx).copied().unwrap_or_default()
    }

    pub fn marked_visible(mut self, marked: &'a [bool]) -> Self {
        self.marked_visible = marked;
        self
    }

    pub fn sort(mut self, col: Option<usize>, ascending: bool) -> Self {
        self.sort_col = col;
        self.sort_asc = ascending;
        self
    }

    pub fn namespace(mut self, ns: &'a str) -> Self {
        self.namespace = ns;
        self
    }

    pub fn changed_rows(mut self, changed: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>) -> Self {
        self.changed_rows = changed;
        self
    }

    /// Calculate column widths based on header and data content.
    fn compute_col_widths(&self, available_width: u16, rows: &[&Vec<String>]) -> Vec<u16> {
        let num_cols = self.headers.len();
        if num_cols == 0 {
            return Vec::new();
        }

        // Calculate the max width needed for each column
        let mut widths: Vec<u16> = self
            .headers
            .iter()
            .map(|h| h.width() as u16 + 2) // +2 for sort indicator space
            .collect();

        // Sample all rows for accurate width calculation
        for row in rows.iter() {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    let cell_width = cell.width() as u16;
                    widths[i] = widths[i].max(cell_width);
                }
            }
        }

        // Add padding
        for w in &mut widths {
            *w += 2; // 1 space padding each side
        }

        // Fit columns into available width with proportional scaling.
        // Use u32 for intermediate sums to avoid u16 overflow with many/wide columns.
        let total_needed: u32 = widths.iter().map(|w| *w as u32).sum();
        let usable = available_width;
        let usable32 = usable as u32;

        if total_needed <= usable32 {
            // Distribute remaining space evenly across columns. The old
            // proportional approach made wide columns (NAME, LABELS)
            // wastefully wide while narrow columns (READY, AGE) stayed
            // cramped. Even distribution levels the playing field.
            let remaining = usable32 - total_needed;
            if !widths.is_empty() {
                let per_col = remaining / widths.len() as u32;
                let leftover = remaining % widths.len() as u32;
                for (i, w) in widths.iter_mut().enumerate() {
                    let extra = per_col + if (i as u32) < leftover { 1 } else { 0 };
                    *w = (*w as u32 + extra).min(usable32) as u16;
                }
            }
        } else if usable > 0 {
            // Scale down proportionally but give each column at least 4 chars.
            let min_col: u16 = 4;
            let num = num_cols as u16;
            if num == 0 { return widths; }
            if usable <= min_col.saturating_mul(num) {
                for w in &mut widths {
                    *w = usable / num;
                }
            } else {
                let scale = usable as f64 / total_needed as f64;
                for w in &mut widths {
                    *w = ((*w as f64 * scale) as u16).max(min_col);
                }
                // Clamp total to exactly usable — fix both undershoot and overshoot.
                let actual_total: u32 = widths.iter().map(|w| *w as u32).sum();
                if actual_total < usable32 && !widths.is_empty() {
                    widths[0] += (usable32 - actual_total) as u16;
                } else if actual_total > usable32 && !widths.is_empty() {
                    // Shrink columns from the right until we fit.
                    let mut excess = actual_total - usable32;
                    for w in widths.iter_mut().rev() {
                        if excess == 0 { break; }
                        let shrink = excess.min((*w as u32).saturating_sub(min_col as u32));
                        *w -= shrink as u16;
                        excess -= shrink;
                    }
                }
            }
        }

        widths
    }

    /// Whether a column header denotes a numeric-valued column. Numeric
    /// columns are right-aligned for readability.
    fn is_numeric_column(header: &str) -> bool {
        matches!(
            header.trim().to_ascii_uppercase().as_str(),
            "READY" | "UP-TO-DATE" | "AVAILABLE" | "DESIRED" | "CURRENT"
            | "RESTARTS" | "CPU" | "MEM" | "CPU%" | "MEM%"
            | "AGE" | "PORTS" | "PORT(S)" | "COUNT" | "LOCAL" | "REMOTE"
            | "ACTIVE" | "COMPLETIONS" | "PODS" | "RULES" | "SECRETS"
            | "SUBJECTS" | "MIN" | "MAX" | "DATA"
        )
    }

    /// Render a single cell, truncating if necessary. When `right_align` is
    /// true the text is padded on the left so it ends flush with the cell.
    fn render_cell(buf: &mut Buffer, x: u16, y: u16, width: u16, text: &str, style: Style, right_align: bool) {
        if width == 0 {
            return;
        }
        let padded_width = width as usize;
        let text_chars: Vec<char> = text.chars().collect();
        let text_width = text.width();

        let display = if text_width > padded_width.saturating_sub(1) {
            // Truncate with ellipsis
            let mut result = String::new();
            let mut current_width = 0;
            let target = padded_width.saturating_sub(2);
            for ch in &text_chars {
                let ch_width = unicode_width::UnicodeWidthChar::width(*ch).unwrap_or(0);
                if current_width + ch_width > target {
                    break;
                }
                result.push(*ch);
                current_width += ch_width;
            }
            result.push('\u{2026}');
            result
        } else {
            text.to_string()
        };

        let content = if right_align {
            // Right-pad with spaces so the text ends one column before the
            // cell boundary (matches the left-align trailing gap).
            let display_width = display.width();
            let total = padded_width.saturating_sub(1);
            let pad = total.saturating_sub(display_width);
            format!("{} {}", " ".repeat(pad), display)
        } else {
            // Left-align with a leading space.
            format!(" {}", display)
        };
        buf.set_string(x, y, &content, style);
    }

    /// Build the title line as a sequence of styled spans.
    fn build_title_spans(&self, row_count: usize) -> Line<'a> {
        let mut spans = Vec::new();
        // resource_name(namespace)[count]
        spans.push(Span::styled(
            format!(" {}", self.title.to_lowercase()),
            self.theme.title,
        ));
        if !self.namespace.is_empty() {
            spans.push(Span::styled("(", self.theme.title));
            spans.push(Span::styled(
                self.namespace.to_string(),
                self.theme.title_namespace,
            ));
            spans.push(Span::styled(")", self.theme.title));
        }
        spans.push(Span::styled(
            format!("[{}]", row_count),
            self.theme.title_counter,
        ));
        spans.push(Span::styled(" ", self.theme.title));
        Line::from(spans)
    }
}

impl StatefulWidget for ResourceTable<'_> {
    type State = ResourceTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        // The rows are already filtered by StatefulTable -- use them directly.
        let all_rows: Vec<&Vec<String>> = self.rows.iter().collect();

        state.filtered_count = all_rows.len();

        // Clamp selection
        if all_rows.is_empty() {
            state.selected = 0;
            state.offset = 0;
        } else if state.selected >= all_rows.len() {
            state.selected = all_rows.len() - 1;
        }

        // Build title
        let title_line = self.build_title_spans(all_rows.len());

        // Draw bordered block with title
        let block = Block::bordered()
            .title(title_line)
            .border_style(self.theme.border);

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        // Compute column widths
        let col_widths = self.compute_col_widths(inner.width, &all_rows);
        if col_widths.is_empty() {
            return;
        }

        // Render header row: white bold, sort indicators in orange
        // Headers left-align regardless of the underlying column alignment —
        // right-aligned header text next to left-aligned data looks weird.
        let header_y = inner.y;
        let mut x = inner.x;
        for (i, header) in self.headers.iter().enumerate() {
            if i >= col_widths.len() {
                break;
            }
            let w = col_widths[i];
            let is_sort_col = self.sort_col == Some(i);

            if is_sort_col {
                Self::render_cell(buf, x, header_y, w.saturating_sub(2), header, self.theme.header, false);
                let arrow = if self.sort_asc { "\u{2191}" } else { "\u{2193}" };
                let arrow_x = x + w.saturating_sub(2);
                if arrow_x < inner.x + inner.width {
                    buf.set_string(arrow_x, header_y, arrow, self.theme.sort_indicator);
                }
            } else {
                Self::render_cell(buf, x, header_y, w, header, self.theme.header, false);
            }
            x += w;
        }

        // Virtual scrolling: only render visible rows
        let data_start_y = header_y + 1;
        let visible_height = (inner.y + inner.height).saturating_sub(data_start_y) as usize;

        if visible_height == 0 {
            return;
        }

        // Adjust offset to keep selected row visible
        if state.selected < state.offset {
            state.offset = state.selected;
        }
        if state.selected >= state.offset + visible_height {
            state.offset = state.selected - visible_height + 1;
        }

        // Only iterate and render the visible window
        let end = (state.offset + visible_height).min(all_rows.len());
        for (vi, row_idx) in (state.offset..end).enumerate() {
            let y = data_start_y + vi as u16;
            if y >= inner.y + inner.height {
                break;
            }

            let row = all_rows[row_idx];
            let is_selected = row_idx == state.selected;
            // Cache lookup is O(1) and never allocates. Empty slice → false.
            let is_marked = self.marked_visible.get(row_idx).copied().unwrap_or(false);

            // Check if this row has recent changes (delta tracking).
            // The key comes from the typed (namespace, name) pair the
            // caller built from `ResourceRow` — NOT from cell column
            // offsets, which get confused by cluster-scoped tables and
            // hidden-NAMESPACE columns.
            let is_changed = if !self.changed_rows.is_empty() {
                self.row_keys.get(row_idx)
                    .is_some_and(|k| self.changed_rows.contains_key(k))
            } else {
                false
            };

            // Determine row base style. Precedence:
            //   1. selected (highest — user interaction)
            //   2. marked (user mark)
            //   3. changed (recent delta)
            //   4. row health (per-resource diagnosis)
            //   5. normal
            let health = self.health_at(row_idx);
            let row_style = if is_selected {
                self.theme.selected
            } else if is_marked {
                self.theme.marked_row
            } else if is_changed {
                self.theme.delta_changed
            } else {
                use crate::kube::resources::row::RowHealth;
                match health {
                    RowHealth::Failed => self.theme.status_failed,
                    RowHealth::Pending => self.theme.status_pending,
                    RowHealth::Normal => self.theme.row_normal,
                }
            };

            // Fill entire row with background style for selected
            if is_selected {
                for dx in 0..inner.width {
                    buf.set_string(inner.x + dx, y, " ", self.theme.selected);
                }
            }

            // Prepend a mark indicator for marked rows. Drawn even when the
            // row is also selected, so the user gets immediate feedback when
            // toggling Space — without this, the indicator only appears
            // after the cursor moves off the row.
            if is_marked {
                buf.set_string(inner.x, y, "\u{25cf}", self.theme.marked_row);
            }

            // Per-cell style follows row style — every cell uses the
            // typed `row.health` classification computed by the converter
            // (server-side). Previously this loop did its own
            // `match cell.as_str()` over 15 hardcoded Pod status strings,
            // duplicating the classification the server already did.
            let cell_style = if is_selected {
                self.theme.selected
            } else if is_marked {
                self.theme.marked_row
            } else {
                row_style
            };

            let mut cx = inner.x;
            for (col_idx, cell) in row.iter().enumerate() {
                if col_idx >= col_widths.len() {
                    break;
                }
                let w = col_widths[col_idx];
                let right_align = self.headers.get(col_idx)
                    .map(|h| Self::is_numeric_column(h))
                    .unwrap_or(false);
                Self::render_cell(buf, cx, y, w, cell, cell_style, right_align);

                cx += w;
            }
        }
    }
}
