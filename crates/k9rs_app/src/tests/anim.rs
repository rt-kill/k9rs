use super::*;

#[test]
fn drawing_the_bar_declares_liveness() {
    let a = Anim::default();
    assert!(!a.is_animating(), "idle until a spinner draws");
    let _ = a.bar("Loading...");
    assert!(a.is_animating(), "rendering the bar keeps the animation alive");
}

#[test]
fn a_frame_with_no_spinner_stops_the_animation() {
    let a = Anim::default();
    let _ = a.bar("x");
    a.begin_frame(); // next frame drew no spinner
    assert!(!a.is_animating(), "liveness is per-frame, not sticky");
}

#[test]
fn phase_steps_one_column_per_frame() {
    // Motion is a pure function of the frame counter — no wall clock — so
    // consecutive frames are distinct and it can never alias against the
    // paint schedule.
    let mut a = Anim::default();
    let f0 = a.bar("x");
    a.advance();
    let f1 = a.bar("x");
    a.advance();
    let f2 = a.bar("x");
    assert_ne!(f0, f1, "each animation frame is a distinct phase");
    assert_ne!(f1, f2);
}

#[test]
fn phase_wraps_without_panicking() {
    let mut a = Anim::default();
    for _ in 0..1000 {
        a.advance();
    }
    assert!(a.bar("x").contains("]"), "still renders after wrapping");
}
