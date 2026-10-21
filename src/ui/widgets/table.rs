
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
    /// The widget's true data-row height, measured from the real `Rect` and
    /// written back so the caller publishes it to the Viewport rather than
    /// re-deriving `area.height - 3`.
    pub visible_height: usize,
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

    /// Rebuild the viewport without recomputing positions/widths.
    fn reposition(&mut self, col_offset: u16, viewport_width: u16) {
        self.viewport_left = col_offset;
        self.viewport_right = col_offset + viewport_width;
    }

    /// How many columns are at least partly on screen, out of how many
    /// there are — `None` when they all are.
    ///
    /// The layout has held both numbers all along; nothing said them. A pod
    /// table with metrics is 200 columns wide, so on a 97-column terminal
    /// you see seven of fourteen and AGE — the LAST column — simply is not
    /// there. It reads as a missing field rather than as one you can scroll
    /// to, and `Ctrl-R` cannot help, because it is a fact about the window
    /// and not about the data.
    fn clipped(&self) -> Option<(usize, usize)> {
        if self.total_width <= self.viewport_right.saturating_sub(self.viewport_left) {
            return None;
        }
        let shown = (0..self.widths.len()).filter(|&i| self.is_visible(i)).count();
        Some((shown, self.widths.len()))
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

    /// Index of the first visible column in the viewport.
    fn first_visible(&self) -> Option<usize> {
        (0..self.widths.len()).find(|&i| self.is_visible(i))
    }
}

pub struct ResourceTable<'a> {
    headers: Vec<&'a str>,
    rows: &'a [Vec<String>],
    /// Pre-computed per-column display widths (from `PreparedView`). Empty ⇒
    /// nothing to render — same guard as the old content-scanning path when
    /// there were no columns.
    col_widths: &'a [u16],
    title: &'a str,
    namespace: &'a str,
    sort_col: Option<usize>,
    /// The visible column whose repeats collapse — the sorted column ONLY if
    /// it declared that it groups. Sorting alone is not a licence to hide a
    /// value: AGE sorted put a ReplicaSet's identically-aged pods adjacent
    /// and blanking them read as missing data.
    group_col: Option<usize>,
    sort_asc: bool,
    theme: &'a Theme,
    marked: &'a std::collections::HashSet<crate::kube::protocol::ObjectKey>,
    changed_rows: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    row_keys: &'a [crate::kube::protocol::ObjectKey],
    row_health: &'a [crate::kube::resources::row::RowHealth],
    /// Per-cell rendering style. `cell_style[row][col]` = `Some(h)` means
    /// that cell has its own coloring; `None` = inherit row style.
    cell_style: &'a [Vec<Option<crate::kube::resources::row::RowHealth>>],
    /// Active search patterns for match highlighting. When non-empty,
    /// matched regions within cells are rendered with `theme.search_match`.
    search_patterns: &'a [crate::util::SearchPattern],
    /// Set when the rows are being painted despite not being live
    /// ([`crate::app::Liveness::warning`]). Rides the TITLE rather than an
    /// extra line: the title is on the border, always on screen, and can't
    /// be scrolled past or pushed off by a full table.
    warning: Option<&'a str>,
}

/// The three per-ROW facts `render_row` needs, as against the per-table
/// `ColumnLayout` beside it. One parameter, because three positional ones
/// read as a tuple of unrelated flags.
#[derive(Clone, Copy)]
struct RowPaint {
    base_style: Style,
    is_row_selected: bool,
    /// The display column this row may leave blank — see
    /// [`ResourceTable::repeated_cell`].
    repeated: Option<usize>,
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
            headers, rows, col_widths: &[], title, namespace: "",
            sort_col: None, group_col: None, sort_asc: true, theme,
            marked: &EMPTY_MARKED,
            changed_rows: &EMPTY_MAP,
            row_keys: &[],
            row_health: &[],
            cell_style: &[],
            search_patterns: &[],
            warning: None,
        }
    }

    pub fn col_widths(mut self, widths: &'a [u16]) -> Self { self.col_widths = widths; self }

    pub fn row_keys(mut self, keys: &'a [crate::kube::protocol::ObjectKey]) -> Self { self.row_keys = keys; self }
    pub fn row_health(mut self, health: &'a [crate::kube::resources::row::RowHealth]) -> Self { self.row_health = health; self }
    pub fn cell_style(mut self, ch: &'a [Vec<Option<crate::kube::resources::row::RowHealth>>]) -> Self { self.cell_style = ch; self }
    pub fn marked(mut self, marked: &'a std::collections::HashSet<crate::kube::protocol::ObjectKey>) -> Self { self.marked = marked; self }
    pub fn sort(mut self, col: Option<usize>, ascending: bool) -> Self { self.sort_col = col; self.sort_asc = ascending; self }
    /// The column whose consecutive equal values collapse, if any declares it.
    pub fn group_col(mut self, col: Option<usize>) -> Self { self.group_col = col; self }
    pub fn namespace(mut self, ns: &'a str) -> Self { self.namespace = ns; self }
    pub fn changed_rows(mut self, changed: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>) -> Self { self.changed_rows = changed; self }
    pub fn search_patterns(mut self, pats: &'a [crate::util::SearchPattern]) -> Self { self.search_patterns = pats; self }
    pub fn warning(mut self, w: Option<&'a str>) -> Self { self.warning = w; self }

    fn health_at(&self, idx: usize) -> crate::kube::resources::row::RowHealth {
        self.row_health.get(idx).copied().unwrap_or_default()
    }

    /// Render a single cell: `│ text  ` (left border + padded content).
    /// The right edge is the next column's left `│` (or the trailing `│`
    /// for the last column). When `match_ranges` is non-empty, matched
    /// character positions are rendered with `match_style` instead of the
    /// base style.
    #[allow(clippy::too_many_arguments)]
    fn render_cell(
        buf: &mut Buffer, x: u16, y: u16, width: u16, text: &str,
        style: Style, border_style: Style,
        match_ranges: &[(usize, usize)], match_style: Style,
        first_col: bool,
    ) {
        if width < 3 { return; }
        let inner = (width as usize) - 3;
        let text_width = text.width();
        let truncated = text_width > inner;

        if first_col {
            buf.set_string(x, y, " ", style);
        } else {
            buf.set_string(x, y, "│", border_style);
        }

        if match_ranges.is_empty() {
            // Fast path: no highlighting — single formatted string.
            let display = if truncated {
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
            buf.set_string(x + 1, y, format!(" {:<width$} ", display, width = inner), style);
        } else {
            // Highlighted path: render character by character, applying
            // match_style to matched regions. Uses a sorted pointer walk
            // over match_ranges so the total cost is O(chars + ranges).
            let target = if truncated { inner.saturating_sub(1) } else { inner };
            let mut sx = x + 2; // past │ and left pad
            let mut dw = 0usize;
            let mut ri = 0; // index into match_ranges

            // Left pad.
            buf.set_string(x + 1, y, " ", style);

            for (byte_pos, ch) in text.char_indices() {
                let cw = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
                if dw + cw > target { break; }

                // Advance past match ranges that end before this byte.
                while ri < match_ranges.len() && match_ranges[ri].1 <= byte_pos {
                    ri += 1;
                }
                let in_match = ri < match_ranges.len()
                    && byte_pos >= match_ranges[ri].0
                    && byte_pos < match_ranges[ri].1;

                let s = if in_match { match_style } else { style };
                let ch_str: String = ch.to_string();
                buf.set_string(sx, y, &ch_str, s);
                sx += cw as u16;
                dw += cw;
            }

            // Ellipsis (if truncated).
            if truncated {
                buf.set_string(sx, y, "\u{2026}", style);
                sx += 1;
                dw += 1;
            }

            // Right padding + trailing pad space to fill the cell.
            let total_pad = inner.saturating_sub(dw) + 1;
            buf.set_string(sx, y, " ".repeat(total_pad), style);
        }
    }

    /// The title, fitted by PRIORITY rather than left to right.
    ///
    /// It is the one line that reports what you cannot otherwise see — how
    /// many rows there are, how many are marked, how many columns are off
    /// screen, and whether any of it is still tracking the cluster — and it
    /// lives in the top border, where ratatui truncates it in silence. A
    /// CRD name plus a namespace plus a batch selection plus the column
    /// count plus a staleness reason passes 100 columns easily, and dropping
    /// from the right would take the liveness warning first: the single
    /// thing here you must not miss.
    ///
    /// So two parts are mandatory — the view's NAME (you have to know where
    /// you are) and the WARNING (you must not trust stale rows as live) —
    /// and the rest are admitted in the order they are worth keeping, while
    /// the budget lasts. They are then emitted in POSITION order, so what
    /// survives reads the same as it always did.
    fn build_title_spans(
        &self,
        row_count: usize,
        shown_rows: std::ops::Range<usize>,
        clipped_cols: Option<(usize, usize)>,
        width: u16,
    ) -> Line<'a> {
        // Mandatory, whatever it costs.
        let name = format!(" {}", self.title);
        let warning = self.warning.map(|w| format!(" {}", w));

        // Optional, in POSITION order.
        let namespace: Option<String> =
            (!self.namespace.is_empty()).then(|| format!("({})", self.namespace));
        // Which rows, not just how many, once there are more than fit.
        //
        // Deliberately NOT a scrollbar, which is what the log, describe and
        // yaml views use: each of those reserves a column for it
        // unconditionally, and this is the one view with no column to spare
        // — a pod table with metrics is already 200 columns against a 97-
        // column terminal. The title costs nothing, is already here, and
        // already reports the other things you cannot see.
        let count = if shown_rows.len() < row_count && !shown_rows.is_empty() {
            format!("[{}-{}/{}]", shown_rows.start + 1, shown_rows.end, row_count)
        } else {
            format!("[{}]", row_count)
        };
        let selection: Option<String> = (!self.marked.is_empty()).then(|| {
            // The count is the STORE-wide marked set (what a batch acts on).
            // When the active filter hides some of it, say so — the title
            // must not imply everything selected is on screen.
            let shown = self.row_keys.iter().filter(|k| self.marked.contains(*k)).count();
            if shown < self.marked.len() {
                format!("[{} selected \u{b7} {} shown]", self.marked.len(), shown)
            } else {
                format!("[{} selected]", self.marked.len())
            }
        });
        let cols: Option<String> =
            clipped_cols.map(|(shown, total)| format!("[{}/{} cols \u{2192}]", shown, total));

        let widths = [
            namespace.as_deref().map_or(0, UnicodeWidthStr::width),
            count.width(),
            selection.as_deref().map_or(0, UnicodeWidthStr::width),
            cols.as_deref().map_or(0, UnicodeWidthStr::width),
        ];
        // What to give up first, read right to left: the column hint goes
        // before the selection detail, which goes before the namespace,
        // which goes before the row count. The column hint is the cheapest
        // loss — the arrows still reach those columns either way.
        const KEEP_ORDER: [usize; 4] = [1, 0, 2, 3];
        let mut budget = usize::from(width)
            .saturating_sub(name.width())
            .saturating_sub(warning.as_deref().map_or(0, UnicodeWidthStr::width))
            // The trailing space that closes the title.
            .saturating_sub(1);
        let mut keep = [false; 4];
        for &i in &KEEP_ORDER {
            if widths[i] > 0 && widths[i] <= budget {
                keep[i] = true;
                budget -= widths[i];
            }
        }

        let mut spans = Vec::new();
        spans.push(Span::styled(name, self.theme.title));
        if keep[0] {
            if let Some(ns) = namespace {
                spans.push(Span::styled(ns, self.theme.title_namespace));
            }
        }
        if keep[1] {
            spans.push(Span::styled(count, self.theme.title_counter));
        }
        if keep[2] {
            if let Some(sel) = selection {
                spans.push(Span::styled(sel, self.theme.title_filter_indicator));
            }
        }
        if keep[3] {
            if let Some(c) = cols {
                spans.push(Span::styled(c, self.theme.title_filter_indicator));
            }
        }
        // Liveness last, so it reads as the final word on everything left
        // of it: these counts describe rows that stopped tracking the
        // cluster. Never silent — if the rows are painted and not live, the
        // title says which.
        if let Some(w) = warning {
            spans.push(Span::styled(w, self.theme.status_failed));
        }
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
    /// The display column whose value this row REPEATS from the row above,
    /// and may therefore leave blank.
    ///
    /// Only ever the sorted column, because that is the only one whose equal
    /// values are guaranteed contiguous — blanking a repeat in an unsorted
    /// column would hide a value whose neighbour above it is a coincidence.
    /// Only ever below the first VISIBLE row, so the top of the screen always
    /// carries the group it is in rather than inheriting a label that scrolled
    /// off. And never a cell the search matched: the highlight exists to show
    /// where the match is, and collapsing the matches after the first would
    /// read as "only one row matched".
    ///
    /// Render-only. Copy and save carry the substance of a row, not a
    /// transcript of the cells it was drawn in, so they still see the value.
    fn repeated_cell(&self, row_idx: usize, first_visible: usize) -> Option<usize> {
        // `group_col`, not `sort_col`. Being the sorted column was the wrong
        // test: it is true of AGE, STATUS and every other per-row fact, and
        // blanking those reads as absent data rather than as grouping. Only
        // a column that DECLARES it groups collapses — see
        // `crate::app::ColumnDef::groups`.
        let col = self.group_col?;
        if row_idx <= first_visible {
            return None;
        }
        let here: &str = self.rows[row_idx].get(col)?.as_ref();
        let above: &str = self.rows[row_idx - 1].get(col)?.as_ref();
        if here != above || here.is_empty() {
            return None;
        }
        self.cell_match_ranges(here).is_empty().then_some(col)
    }

    fn render_row<S: AsRef<str>>(
        &self,
        buf: &mut Buffer,
        y: u16,
        cells: &[S],
        row: RowPaint,
        layout: &ColumnLayout,
    ) {
        let RowPaint { base_style, is_row_selected, repeated } = row;
        let hl = self.theme.search_match;
        let first = layout.first_visible();
        for (i, cell) in cells.iter().enumerate() {
            if i >= layout.widths.len() || !layout.is_visible(i) { continue; }
            let (content_style, border_style) = self.cell_styles(base_style, i, is_row_selected, layout);
            let text = if repeated == Some(i) { "" } else { cell.as_ref() };
            let ranges = self.cell_match_ranges(text);
            Self::render_cell(buf, layout.screen_x(i), y, layout.visible_width(i), text, content_style, border_style, &ranges, hl, first == Some(i));
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

    /// Collect sorted match ranges for a cell's text against all active
    /// search patterns. Returns an empty Vec (no allocation) when there
    /// are no patterns.
    fn cell_match_ranges(&self, text: &str) -> Vec<(usize, usize)> {
        if self.search_patterns.is_empty() { return Vec::new(); }
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for pat in self.search_patterns {
            ranges.extend(pat.find_all(text));
        }
        if ranges.len() > 1 { ranges.sort_unstable(); }
        ranges
    }
}

impl StatefulWidget for ResourceTable<'_> {
    type State = ResourceTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let row_count = self.rows.len();
        state.filtered_count = row_count;

        // Clamp row selection.
        if row_count == 0 {
            state.selected = 0;
            state.offset = 0;
        } else if state.selected >= row_count {
            state.selected = row_count - 1;
        }

        // The block's inner rect does not depend on its title, so it can be
        // measured before the title exists — which it has to be, because the
        // title reports how many COLUMNS fit and only the layout below knows
        // that. The block is still rendered once, titled.
        let block = Block::bordered().border_style(self.theme.border);
        let inner = block.inner(area);
        if inner.height == 0 || inner.width == 0 {
            // Nothing will fit, but the border and title still belong on
            // screen: a degenerate frame must not lose the view's name.
            block.title(self.build_title_spans(row_count, 0..row_count, None, area.width.saturating_sub(2))).render(area, buf);
            return;
        }

        // Report the true data-row height (inner minus the 1 header row) back to
        // the Viewport BEFORE the column early-return below, so paging keeps a
        // real page size even on a (degenerate) zero-column frame.
        state.visible_height = (inner.height as usize).saturating_sub(1);

        // The VERTICAL window, settled here rather than beside the row loop:
        // it depends only on `inner` and the row count, and the title below
        // reports it. Same adjustment, moved up.
        let visible_height = state.visible_height;
        if state.selected < state.offset {
            state.offset = state.selected;
        }
        if visible_height > 0 && state.selected >= state.offset + visible_height {
            state.offset = state.selected - visible_height + 1;
        }
        let shown_rows = state.offset..(state.offset + visible_height).min(row_count);

        // Column layout — widths are pre-computed with the memoized view
        // (see `PreparedView::col_widths`), not re-scanned per frame.
        let col_widths = self.col_widths.to_vec();
        if col_widths.is_empty() {
            block.title(self.build_title_spans(row_count, 0..row_count, None, area.width.saturating_sub(2))).render(area, buf);
            return;
        }

        // Clamp column selection.
        if state.selected_col >= col_widths.len() {
            state.selected_col = col_widths.len().saturating_sub(1);
        }

        // Build layout and adjust horizontal scroll. Positions are computed
        // once; only the viewport shifts when col_offset changes.
        let mut layout = ColumnLayout::new(col_widths, state.selected_col, state.col_offset, inner.width, inner.x);
        let sel_start = layout.positions[state.selected_col];
        let sel_end = sel_start + layout.widths[state.selected_col];
        if sel_start < state.col_offset {
            state.col_offset = sel_start;
            layout.reposition(state.col_offset, inner.width);
        }
        if sel_end > state.col_offset + inner.width {
            // Scroll to a column BOUNDARY, never into the middle of one.
            //
            // `sel_end - inner.width` is the minimum shift that reveals the
            // selected column, but it usually lands inside the first column
            // still on screen — and a partially-scrolled column renders
            // wrong: `screen_x` saturates at the left edge, so that column
            // pins in place at FULL width while every column after it moves,
            // and they overlap. Nothing clips a hidden left-hand prefix.
            //
            // Rather than teach the cell renderer to clip one, keep offsets
            // column-aligned: take the first column start that still leaves
            // the selection visible. Scrolling is then column-granular in
            // both directions, matching the column-granular cursor the
            // keymap already gives (arrows / colLeft / colRight).
            let min_offset = sel_end.saturating_sub(inner.width);
            state.col_offset = layout
                .positions
                .iter()
                .copied()
                .find(|&start| start >= min_offset)
                .unwrap_or(min_offset);
            layout.reposition(state.col_offset, inner.width);
        }

        // NOW the title can say what is off screen — after the horizontal
        // scroll has settled, so the count describes the frame being drawn
        // rather than the previous one.
        block
            .title(self.build_title_spans(
                row_count,
                shown_rows.clone(),
                layout.clipped(),
                area.width.saturating_sub(2),
            ))
            .render(area, buf);

        // --- Header row ---
        let header_y = inner.y;
        let header_style = self.theme.header;
        // The header row repeats nothing, and is never the cursor row.
        self.render_row(
            buf,
            header_y,
            &self.headers,
            RowPaint { base_style: header_style, is_row_selected: false, repeated: None },
            &layout,
        );

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
        if visible_height == 0 { return; }

        for (vi, row_idx) in shown_rows.clone().enumerate() {
            let y = data_start_y + vi as u16;
            if y >= inner.y + inner.height { break; }

            let row = &self.rows[row_idx];
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

            // Row-level base style: selected > marked > changed > row health.
            let row_base = if is_selected && is_marked {
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

            // Per-cell coloring: if this row has cell-level health overrides
            // and the row isn't selected/marked/changed (those take priority
            // over cell coloring), resolve per-cell styles.
            let has_cell_overrides = !is_selected && !is_marked && !is_changed
                && self.cell_style.get(row_idx).is_some_and(|ch| ch.iter().any(|c| c.is_some()));

            // Repeated values of the column you sorted by collapse, so a
            // three-column help sheet stops printing "Everywhere" thirteen
            // times down its left edge. ONE rule, asked by both paths.
            let repeated = self.repeated_cell(row_idx, state.offset);

            if has_cell_overrides {
                let cell_styles = &self.cell_style[row_idx];
                let hl = self.theme.search_match;
                let first = layout.first_visible();
                for (i, cell) in row.iter().enumerate() {
                    if i >= layout.widths.len() || !layout.is_visible(i) { continue; }
                    let style = if let Some(Some(h)) = cell_styles.get(i) {
                        use crate::kube::resources::row::RowHealth;
                        match h {
                            RowHealth::Failed => self.theme.status_failed,
                            RowHealth::Pending => self.theme.status_pending,
                            RowHealth::Normal => row_base,
                        }
                    } else {
                        row_base
                    };
                    let (content_style, border_style) = self.cell_styles(style, i, is_selected, &layout);
                    let text = if repeated == Some(i) { "" } else { cell.as_ref() };
                    let ranges = self.cell_match_ranges(text);
                    Self::render_cell(buf, layout.screen_x(i), y, layout.visible_width(i), text, content_style, border_style, &ranges, hl, first == Some(i));
                }
                // Trailing border.
                if let Some(tx) = layout.trailing_border_x() {
                    buf.set_string(tx, y, "│", self.theme.border);
                }
            } else {
                self.render_row(
                    buf,
                    y,
                    row,
                    RowPaint { base_style: row_base, is_row_selected: is_selected, repeated },
                    &layout,
                );
            }
        }
    }
}
