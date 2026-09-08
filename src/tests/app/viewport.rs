use super::*;

#[test]
fn end_reaches_the_last_row_for_any_extent() {
    // THE bug this type exists to kill: after `end()` + a render that
    // publishes the true (wrap-expanded) extent, the last row is reachable
    // — regardless of how many physical rows the content occupies.
    for (content, view) in [(100usize, 10usize), (7, 20), (1000, 3), (0, 5)] {
        let mut v = Viewport::default();
        v.end();
        v.set_metrics(content, view, /*autoscroll=*/ true);
        assert_eq!(v.offset(), content.saturating_sub(view), "content={content} view={view}");
    }
}

#[test]
fn upward_intent_clears_follow_downward_does_not() {
    let mut v = Viewport::tailing();
    v.set_metrics(100, 10, true);
    assert!(v.following());
    v.line_down(3); // still following
    assert!(v.following());
    v.line_up(1); // history → drop follow
    assert!(!v.following());
    v.end(); // back to follow
    assert!(v.following());
    v.home();
    assert!(!v.following());
    assert_eq!(v.offset(), 0);
}

#[test]
fn paging_uses_published_height_not_a_constant() {
    let mut v = Viewport::default();
    v.set_metrics(200, 25, false); // widget published a 25-row viewport
    v.end();
    v.set_metrics(200, 25, true);
    assert_eq!(v.offset(), 175);
    v.page_up();
    assert_eq!(v.offset(), 150, "one page == the published 25 rows");
    v.page_down();
    assert_eq!(v.offset(), 175);
}

#[test]
fn set_metrics_reclamps_offset_on_shrink() {
    let mut v = Viewport::default();
    v.set_metrics(100, 10, false);
    v.line_down(90); // offset 90 == max
    assert_eq!(v.offset(), 90);
    // Terminal grew / content shrank: the single clamp site fixes offset.
    v.set_metrics(50, 10, false);
    assert_eq!(v.offset(), 40);
}

#[test]
fn reveal_keeps_a_cursor_row_in_view_both_directions() {
    let mut v = Viewport::default();
    v.set_metrics(100, 10, false);
    v.reveal(0);
    assert_eq!(v.offset(), 0);
    v.reveal(9);
    assert_eq!(v.offset(), 0, "row 9 already in [0,10)");
    v.reveal(10);
    assert_eq!(v.offset(), 1, "scroll one to reveal row 10 at the bottom");
    v.reveal(50);
    assert_eq!(v.offset(), 41);
    v.reveal(5);
    assert_eq!(v.offset(), 5, "scroll up so row 5 becomes the top");
}

#[test]
fn center_on_centers_and_clamps() {
    let mut v = Viewport::default();
    v.set_metrics(100, 20, false);
    v.center_on(50);
    assert_eq!(v.offset(), 40, "50 - 20/2");
    v.center_on(5);
    assert_eq!(v.offset(), 0, "clamped at top");
    v.center_on(99);
    assert_eq!(v.offset(), 80, "clamped at max_scroll (100-20)");
}

#[test]
fn tailing_and_seeded_constructors() {
    let mut t = Viewport::tailing();
    t.set_metrics(100, 10, t.following()); // following → snaps to bottom
    assert_eq!(t.offset(), 90);
    let s = Viewport::seeded(40);
    assert_eq!(s.viewport_rows(), 40, "paging works before the first render");
}

#[test]
fn scroll_to_jumps_clamps_and_drops_follow() {
    let mut v = Viewport::tailing();
    v.set_metrics(100, 10, true);
    assert!(v.following());
    v.scroll_to(30);
    assert_eq!(v.offset(), 30);
    assert!(!v.following(), "an explicit jump stops following");
    v.scroll_to(1000);
    assert_eq!(v.offset(), 90, "clamped to max_scroll");
}

#[test]
fn shift_up_tracks_evicted_rows_without_touching_follow() {
    let mut v = Viewport::default();
    v.set_metrics(100, 10, false);
    v.line_down(40);
    v.shift_up(3); // 3 rows evicted off the top
    assert_eq!(v.offset(), 37);
    assert!(!v.following());
}

#[test]
fn apply_render_persists_the_widget_offset() {
    // Cursor tables: the widget computes the revealed offset from the real
    // area; apply_render persists it plus the extent, clamping defensively.
    let mut v = Viewport::seeded(40);
    v.apply_render(5, 100, 20);
    assert_eq!(v.offset(), 5);
    assert_eq!(v.viewport_rows(), 20);
    v.apply_render(999, 100, 20); // a widget offset past the end is clamped
    assert_eq!(v.offset(), 80);
}

#[test]
fn degenerate_extents_are_safe() {
    let mut v = Viewport::default();
    // content smaller than viewport → nowhere to scroll
    v.set_metrics(3, 10, false);
    v.line_down(5);
    assert_eq!(v.offset(), 0);
    v.end();
    v.set_metrics(3, 10, true);
    assert_eq!(v.offset(), 0);
    // zero viewport → no panic, no page step of 0 forever
    v.set_metrics(0, 0, false);
    v.page_down();
    assert_eq!(v.offset(), 0);
}
