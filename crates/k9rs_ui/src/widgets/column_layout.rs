//! Where each column sits, and which of them are on screen.
//!
//! Pure geometry: no theme, no rows, no `k9rs_proto`, no ratatui `Buffer`.
//! Split out of `table.rs` because the coupling is one-way and measured —
//! this names NOTHING from the painting half, which names it two dozen
//! times. The painter is about styles and glyphs; this is about `u16`
//! arithmetic, and it is where that arithmetic can be read and tested
//! without building a frame.
//!
//! Three fields are `pub(crate)` (`widths`, `positions`, `sel_col`) because
//! the painter indexes them directly; the other four — the viewport bounds,
//! the total width, the screen origin — stay private and are reached only
//! through the methods. That split is the measured cost of the boundary,
//! counted rather than waved at.

/// Pre-computed layout for all columns: positions, widths, viewport bounds.
/// Built once per render, threaded through every row — no per-cell recomputation.
pub(crate) struct ColumnLayout {
    pub(crate) widths: Vec<u16>,
    /// Pixel x-position of each column's left edge (before viewport offset).
    pub(crate) positions: Vec<u16>,
    /// Pixel x-position one past the last column's right edge.
    total_width: u16,
    /// Viewport bounds (pixel offsets into the virtual column space).
    viewport_left: u16,
    viewport_right: u16,
    /// Which column is selected.
    pub(crate) sel_col: usize,
    /// The screen x-origin (inner.x of the bordered block).
    origin_x: u16,
}

impl ColumnLayout {
    pub(crate) fn new(widths: Vec<u16>, sel_col: usize, col_offset: u16, viewport_width: u16, origin_x: u16) -> Self {
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
            viewport_right: col_offset.saturating_add(viewport_width),
            sel_col,
            origin_x,
        }
    }

    /// Rebuild the viewport without recomputing positions/widths.
    pub(crate) fn reposition(&mut self, col_offset: u16, viewport_width: u16) {
        self.viewport_left = col_offset;
        self.viewport_right = col_offset.saturating_add(viewport_width);
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
    pub(crate) fn clipped(&self) -> Option<(usize, usize)> {
        if self.total_width <= self.viewport_right.saturating_sub(self.viewport_left) {
            return None;
        }
        let shown = (0..self.widths.len()).filter(|&i| self.is_visible(i)).count();
        Some((shown, self.widths.len()))
    }

    /// Is column `i` at least partially visible in the viewport?
    pub(crate) fn is_visible(&self, i: usize) -> bool {
        let start = self.positions[i];
        let end = start.saturating_add(self.widths[i]);
        end > self.viewport_left && start < self.viewport_right
    }

    /// Screen x-position for column `i`'s left edge.
    pub(crate) fn screen_x(&self, i: usize) -> u16 {
        self.origin_x + self.positions[i].saturating_sub(self.viewport_left)
    }

    /// How many pixels of column `i` are visible (may be clipped at right edge).
    pub(crate) fn visible_width(&self, i: usize) -> u16 {
        self.widths[i].min(self.viewport_right.saturating_sub(self.positions[i]))
    }

    /// Whether the column border at `i` should be highlighted. A border is
    /// highlighted if it's the selected column's left edge (i == sel_col)
    /// or the selected column's right edge (i == sel_col + 1, since each
    /// column only draws its LEFT border).
    pub(crate) fn is_highlighted_border(&self, i: usize) -> bool {
        i == self.sel_col || i == self.sel_col + 1
    }

    /// Screen x-position for the trailing │ after the last column.
    /// Returns None if it falls outside the viewport.
    pub(crate) fn trailing_border_x(&self) -> Option<u16> {
        if self.total_width > self.viewport_left {
            let x = self.origin_x + self.total_width.saturating_sub(self.viewport_left);
            if x < self.origin_x.saturating_add(self.viewport_right - self.viewport_left) {
                return Some(x);
            }
        }
        None
    }

    /// Whether the trailing border (after last column) should be highlighted.
    pub(crate) fn is_trailing_highlighted(&self) -> bool {
        self.widths.len().saturating_sub(1) == self.sel_col
    }

    /// Index of the first visible column in the viewport.
    pub(crate) fn first_visible(&self) -> Option<usize> {
        (0..self.widths.len()).find(|&i| self.is_visible(i))
    }
}

#[cfg(test)]
#[path = "../tests/widgets/column_layout.rs"]
mod tests;
