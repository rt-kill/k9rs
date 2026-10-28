//! Column geometry, tested without a frame.
//!
//! This arithmetic had no direct test: it lived inside `table.rs` as a
//! private type, so reaching it meant painting a table and reading pixels
//! back. Splitting it out — the coupling measured one-way, the painter
//! naming it two dozen times and it naming the painter zero — is what makes
//! these assertions possible at all.

use super::super::super::widgets::column_layout::ColumnLayout;

/// Positions accumulate the widths, and the total is one past the last edge.
#[test]
fn positions_accumulate_the_widths() {
    let l = ColumnLayout::new(vec![10, 20, 5], 0, 0, 100, 0);
    assert_eq!(l.positions, vec![0, 10, 30]);
    assert_eq!(l.widths, vec![10, 20, 5]);
    // Everything fits, so nothing is clipped.
    assert_eq!(l.clipped(), None);
}

/// A viewport narrower than the columns reports how many of them are on
/// screen — the fact that made a missing last column read as missing DATA
/// rather than as something you can scroll to.
#[test]
fn a_narrow_viewport_reports_what_it_shows() {
    // Three 10-wide columns in a 15-wide window: the first two are at least
    // partly visible, the third starts at 20 and is not.
    let l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 15, 0);
    assert_eq!(l.clipped(), Some((2, 3)));
    assert!(l.is_visible(0));
    assert!(l.is_visible(1));
    assert!(!l.is_visible(2));
}

/// Scrolled right, the columns behind the offset drop out and the ones ahead
/// come in — and a column straddling the left edge is still "visible".
#[test]
fn scrolling_moves_the_window_not_the_columns() {
    let mut l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 15, 0);
    l.reposition(12, 15);
    assert!(!l.is_visible(0), "column 0 ends at 10, the window starts at 12");
    assert!(l.is_visible(1), "column 1 spans 10..20 and straddles the edge");
    assert!(l.is_visible(2), "column 2 spans 20..30, inside 12..27");
    // Its screen x is relative to the window, not the virtual space, and a
    // column scrolled off the left clamps to the origin rather than wrapping.
    assert_eq!(l.screen_x(1), 0, "10 - 12 saturates to 0");
    assert_eq!(l.screen_x(2), 8, "20 - 12");
}

/// A column clipped by the right edge reports only the part on screen —
/// painting its full width would run past the viewport.
#[test]
fn a_column_clipped_at_the_right_edge_reports_its_visible_part() {
    let l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 15, 0);
    assert_eq!(l.visible_width(0), 10, "wholly inside");
    assert_eq!(l.visible_width(1), 5, "spans 10..20, window ends at 15");
    assert_eq!(l.visible_width(2), 0, "entirely past the edge");
}

/// The selected column's BOTH borders highlight, because each column draws
/// only its own left one.
#[test]
fn the_selected_column_highlights_the_border_on_each_side() {
    let l = ColumnLayout::new(vec![10, 10, 10], 1, 0, 100, 0);
    assert!(!l.is_highlighted_border(0));
    assert!(l.is_highlighted_border(1), "its left edge");
    assert!(l.is_highlighted_border(2), "and column 2's left edge is its right");
    assert!(!l.is_highlighted_border(3));
}

/// The trailing border is drawn only when it lands inside the window.
#[test]
fn the_trailing_border_appears_only_when_it_fits() {
    // 30 columns of content in a 100-wide window: the border sits at 30.
    let l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 100, 0);
    assert_eq!(l.trailing_border_x(), Some(30));

    // The same content in a 15-wide window: the border is off screen.
    let l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 15, 0);
    assert_eq!(l.trailing_border_x(), None);

    // Origin offset rides through — the border is a SCREEN position.
    let l = ColumnLayout::new(vec![10, 10, 10], 0, 0, 100, 7);
    assert_eq!(l.trailing_border_x(), Some(37));
}

/// An empty table has no columns, no total and nothing visible — the
/// degenerate case every indexing accessor here would panic on if it were
/// reached, and `first_visible` is how the painter avoids reaching them.
#[test]
fn an_empty_layout_has_nothing_visible() {
    let l = ColumnLayout::new(Vec::new(), 0, 0, 100, 0);
    assert_eq!(l.first_visible(), None);
    assert_eq!(l.clipped(), None);
    assert_eq!(l.trailing_border_x(), None, "a zero total is not past the left edge");
}

/// Widths that sum past `u16` saturate rather than wrapping — a wrap would
/// put a late column at a tiny position and paint it over an early one.
#[test]
fn accumulated_widths_saturate_rather_than_wrap() {
    let l = ColumnLayout::new(vec![u16::MAX, 10], 0, 0, 100, 0);
    assert_eq!(l.positions, vec![0, u16::MAX], "the second starts at the cap");
    assert!(l.is_visible(0));
    assert!(!l.is_visible(1), "nothing is on screen past the cap");
}
