use super::*;

const BUDGET: Duration = Duration::from_millis(100);

#[test]
fn idle_is_not_due_and_never_wakes() {
    let r = Repaint::Idle;
    assert!(!r.due(Instant::now()));
    assert_eq!(r.wake_at(), None);
}

#[test]
fn input_paints_immediately() {
    let mut r = Repaint::Idle;
    r.on_input();
    assert!(r.due(Instant::now()));
    // `Now` needs no timer wake — the top-of-loop gate paints it.
    assert_eq!(r.wake_at(), None);
}

#[test]
fn data_coalesces_to_deadline() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_data(t0, BUDGET);
    assert!(!r.due(t0)); // not yet
    assert!(!r.due(t0 + BUDGET / 2)); // still within budget
    assert!(r.due(t0 + BUDGET)); // deadline reached
    assert_eq!(r.wake_at(), Some(t0 + BUDGET));
}

#[test]
fn earliest_deadline_wins() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_data(t0, BUDGET);
    r.on_data(t0 + BUDGET / 2, BUDGET); // a later update…
    // …does not push the deadline out: still first update + budget.
    assert_eq!(r.wake_at(), Some(t0 + BUDGET));
}

#[test]
fn input_beats_pending_coalesce() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_data(t0, BUDGET);
    r.on_input();
    assert_eq!(r, Repaint::Now);
    assert!(r.due(t0)); // instant, no waiting for the deadline
}

#[test]
fn on_data_never_downgrades_now() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_input();
    r.on_data(t0, BUDGET);
    assert_eq!(r, Repaint::Now); // still instant
}

#[test]
fn painted_returns_to_idle() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_data(t0, BUDGET);
    r.painted();
    assert_eq!(r, Repaint::Idle);
    assert!(!r.due(t0 + BUDGET));
    assert_eq!(r.wake_at(), None);
}

#[test]
fn wake_at_matches_deadline() {
    let t0 = Instant::now();
    let mut r = Repaint::Idle;
    r.on_data(t0, BUDGET);
    assert_eq!(r.wake_at(), Some(t0 + BUDGET));
}
