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

/// The render's line record survives only while the position it describes
/// does. Every intent goes through `move_to`, so this holds for mutators
/// added later too — the offset cannot be written without clearing it.
#[test]
fn any_move_drops_the_line_anchor() {
    let anchor = LineAnchor { seq: 4200, skip: 2 };
    let seed = || {
        let mut v = Viewport::default();
        v.apply_render_anchored(40, anchor, 100, 10);
        v
    };
    assert_eq!(seed().anchor(), Some(anchor), "the render wrote it");

    type Move = (&'static str, fn(&mut Viewport));
    let moves: [Move; 8] = [
        ("line_up", |v| v.line_up(1)),
        ("line_down", |v| v.line_down(1)),
        ("page_up", |v| v.page_up()),
        ("page_down", |v| v.page_down()),
        ("home", |v| v.home()),
        ("end", |v| v.end()),
        ("scroll_to", |v| v.scroll_to(7)),
        ("center_on", |v| v.center_on(7)),
    ];
    for (name, mv) in moves {
        let mut v = seed();
        mv(&mut v);
        assert_eq!(v.anchor(), None, "{name} moved the window but kept the line record");
    }

    // The two write-backs for surfaces that DON'T anchor publish a bare
    // offset, so they clear it too.
    let mut v = seed();
    v.apply_render(5, 100, 10);
    assert_eq!(v.anchor(), None);
    let mut v = seed();
    v.set_metrics(100, 10, false);
    assert_eq!(v.anchor(), None);

    // Turning follow OFF does not move the window, so the line under the top
    // row is still the line to stay on as the tail grows past it.
    let mut v = Viewport::tailing();
    v.apply_render_anchored(90, anchor, 100, 10);
    v.toggle_follow();
    assert!(!v.following());
    assert_eq!(v.anchor(), Some(anchor), "stopping autoscroll parks where you are");
    // Turning it back on snaps to the bottom — a move, so the record goes.
    v.toggle_follow();
    assert_eq!(v.anchor(), None);
}

/// A restart replaces the content, so the line the anchor names is gone in
/// the only sense that matters: its sequence belongs to the old stream.
#[test]
fn restarted_keeps_the_preference_and_drops_the_position() {
    let mut v = Viewport::tailing();
    v.apply_render_anchored(90, LineAnchor { seq: 12, skip: 1 }, 100, 10);
    let r = v.restarted();
    assert!(r.following(), "the `s` preference is the user's, not the stream's");
    assert_eq!(r.anchor(), None);
    assert_eq!(r.offset(), 0);
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


/// `follow` means exactly one thing: **pinned to the bottom**. Every intent
/// that lands there sets it; every intent that leaves clears it.
///
/// The half that was missing was re-engagement. Scrolling up and then back
/// down to the very last row left `follow` false, so the view sat at the
/// bottom looking like it was tailing while incoming rows quietly pushed it
/// off the end — and the next `s` read as "turn it off" when the user, seeing
/// the bottom of the log, meant "turn it on". That phase error is what made
/// the toggle feel unreliable.
#[test]
fn follow_means_pinned_to_the_bottom_in_both_directions() {
    let mut v = Viewport::tailing();
    v.set_metrics(100, 20, true);
    assert_eq!(v.offset(), 80);

    // Reading history unpins.
    v.line_up(5);
    assert!(!v.following());

    // Returning to the bottom re-pins — THE fix.
    v.line_down(5);
    assert_eq!(v.offset(), 80, "back at the last row");
    assert!(v.following(), "…so it is following again");

    // And it actually tracks: ten more rows, still at the bottom.
    v.set_metrics(110, 20, v.following());
    assert_eq!(v.offset(), 90, "new rows keep the view pinned");

    // Stopping short of the bottom does NOT re-pin.
    v.line_up(30);
    assert!(!v.following());
    v.line_down(10);
    assert!(!v.following(), "still above the last row");
    assert_eq!(v.offset(), 70);
}

/// A range change replaces the stream, not the user's mind.
#[test]
fn restarting_a_stream_keeps_the_follow_preference() {
    // Tailing → still tailing, extent reset.
    let mut following = Viewport::tailing();
    following.set_metrics(500, 20, true);
    let fresh = following.restarted();
    assert!(fresh.following());
    assert_eq!(fresh.offset(), 0, "the old extent is gone with the old store");

    // Explicitly stopped → STAYS stopped. Pressing `s` to stop the scroll and
    // then `2` to change the range used to turn the scroll back on.
    let mut stopped = Viewport::tailing();
    stopped.set_metrics(500, 20, true);
    stopped.toggle_follow();
    assert!(!stopped.following());
    assert!(
        !stopped.restarted().following(),
        "a range change must not undo the toggle",
    );
}

/// `Pan` had no unit tests at all, and shipped with the one bug they would
/// have caught: `right()` clamped straight to the last measured `max`, which
/// is the widest line in the CURRENT window. Pan out over a long line, scroll
/// onto short ones, press the key meaning "further right" — and land at
/// column 0.
#[test]
fn panning_right_never_moves_left() {
    let mut p = Pan::default();
    p.set_extent(300, 80); // max 220; `right()` is one column, vim-style
    for _ in 0..300 {
        p.right();
    }
    assert_eq!(p.offset(), 220, "bounded by the widest line it could see");

    // The window now holds only short lines, so the measured extent collapses.
    p.set_extent(40, 80);
    let parked = p.offset();
    p.right();
    assert_eq!(p.offset(), parked, "‘go right’ must never go left");
    p.left();
    assert!(p.offset() < parked, "…and ‘go left’ still works");
}

/// Under wrap the widget publishes a zero extent, because wrapped content has
/// no off-screen right edge. That must FREEZE the pan, not reset it — the
/// position is restored when wrap goes back off.
#[test]
fn a_wrapped_frame_freezes_the_pan_rather_than_zeroing_it() {
    let mut p = Pan::default();
    p.set_extent(300, 80);
    p.page_right();
    let parked = p.offset();
    assert!(parked > 0);
    p.set_extent(0, 80); // the wrap branch's write-back
    p.page_right();
    assert_eq!(p.offset(), parked, "wrap must not throw the pan away");
}

/// `j` on a log that fits entirely on screen moved nothing and silently
/// turned autoscroll back ON, because `offset == max_scroll()` is trivially
/// true when both are zero — the exact `s`-out-of-phase confusion the
/// re-engagement rule was written to cure.
#[test]
fn a_down_key_that_cannot_move_does_not_re_engage_follow() {
    let mut v = Viewport::default();
    v.set_metrics(5, 40, false); // content smaller than the viewport
    assert!(!v.following());
    v.line_down(1);
    assert!(!v.following(), "nothing moved, so nothing about follow changed");
    v.page_down();
    assert!(!v.following());

    // The real re-engagement still works: scroll up off the bottom, come back.
    let mut v = Viewport::default();
    v.set_metrics(100, 10, false);
    v.line_down(90);
    assert!(v.following(), "landing on the true last row re-pins");
}

/// vim's shape: `h`/`l` a column at a time, `^`/`$` the ends, `H`/`L` a
/// stride in between. The first version made `h`/`l` a quarter-screen,
/// which is a scroll key wearing a cursor key's name.
#[test]
fn the_pan_keys_follow_vims_gears() {
    let mut p = Pan::default();
    p.set_extent(300, 80); // max 220, stride 20
    p.right();
    assert_eq!(p.offset(), 1, "`l` is one column");
    p.left();
    assert_eq!(p.offset(), 0);
    p.page_right();
    assert_eq!(p.offset(), 20, "`L` is the screen-derived stride");
    p.end();
    assert_eq!(p.offset(), 220, "`$` is the far edge of the widest line");
    p.home();
    assert_eq!(p.offset(), 0, "`^` is the left margin");
}

/// `G` works the FIRST time on a view that has not rendered yet.
///
/// `end()` sets `follow` and moves to `max_scroll()` — but on a
/// freshly-loaded content view the extent is still 0, so it moved to the TOP.
/// The content path then called `set_metrics(.., autoscroll: false)`, which
/// ignored the flag, so the position never corrected: the first `G` after a
/// load silently did nothing and only a second press worked.
///
/// The log widget had always resolved follow against the extent it measured
/// that frame. This is the same rule, applied to the surface that had the
/// third argument available and was passing a literal `false`.
#[test]
fn end_on_a_never_rendered_view_lands_at_the_bottom_once_measured() {
    use crate::app::viewport::Viewport;

    let mut v = Viewport::default();
    // Nothing has rendered: the viewport knows no extent at all.
    assert_eq!(v.max_scroll(), 0);

    v.end();
    assert!(v.following(), "`G` states the intent even with nothing measured");
    assert_eq!(v.offset(), 0, "…and can only land at 0, because 0 is all it knows");

    // The frame measures the real content and resolves the intent against it.
    v.set_metrics(500, 20, v.following());
    assert_eq!(v.offset(), 480, "the intent is honoured once the extent exists");

    // And an upward move drops it, so this cannot strand the view down there.
    v.line_up(5);
    assert!(!v.following());
    v.set_metrics(500, 20, v.following());
    assert_eq!(v.offset(), 475, "a deliberate move is kept, not re-pinned");
}
