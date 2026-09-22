//! [`GraceGate`] — the one keep-warm gate behind every kept-alive object in
//! the daemon: watchers, shared pollers, exec sources, and a context's local
//! slice.
//!
//! Only `ContextLocals` had these properties before, because only it had
//! bothered to implement a deadline; the other three shared a claim-flag-only
//! `spawn_grace` and so carried the bug pinned below.

use super::*;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn the_window_is_measured_from_the_last_holder_out() {
    // THE first-drop bug. A claim flag alone coalesces correctly — at most one
    // waiter — but it measures the window from whoever dropped FIRST. With two
    // sessions on one watcher, the second to leave lost the claim and inherited
    // the first one's nearly-expired timer, so it could get ~0s of grace: nav
    // away, come straight back, and the "warm" watcher was already gone.
    let gate = Arc::new(GraceGate::new(Duration::from_secs(60)));
    assert!(gate.bump_and_claim(), "the first holder out owns the waiter");

    let waiting = Arc::clone(&gate);
    let waiter = tokio::spawn(async move { waiting.wait_out().await });

    // 50 seconds later, a second holder leaves.
    tokio::time::sleep(Duration::from_secs(50)).await;
    assert!(
        !gate.bump_and_claim(),
        "it must NOT spawn a second waiter — that is what the claim is for",
    );

    // Past t=60, where the first holder's window would have ended.
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert!(
        !waiter.is_finished(),
        "the second holder's full window is still open",
    );

    // And it does end — 60s after the SECOND drop, not the first.
    tokio::time::sleep(Duration::from_secs(50)).await;
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the window has to actually close, or nothing is ever reclaimed")
        .expect("waiter should not panic");
}

#[tokio::test(start_paused = true)]
async fn a_closed_window_can_be_graced_again() {
    // The claim is released before the waiter drops its `Arc`, so a later
    // attach/detach cycle can grace afresh. A gate that stayed claimed would
    // mean the object is never kept warm again — and, worse, never reclaimed,
    // because no future drop would ever spawn the task that releases it.
    let gate = Arc::new(GraceGate::new(Duration::from_millis(100)));
    assert!(gate.bump_and_claim());
    let waiting = Arc::clone(&gate);
    tokio::spawn(async move { waiting.wait_out().await })
        .await
        .expect("waiter should not panic");

    assert!(
        gate.bump_and_claim(),
        "a fresh holder must be able to claim the gate again",
    );
}

#[tokio::test(start_paused = true)]
async fn only_one_waiter_exists_no_matter_how_many_holders_leave() {
    // Coalescing is what keeps context-switch churn from stacking detached
    // timers, each pinning an `Arc` to a full row store.
    let gate = GraceGate::new(Duration::from_secs(60));
    assert!(gate.bump_and_claim(), "first claims");
    for _ in 0..16 {
        assert!(!gate.bump_and_claim(), "every later drop coalesces onto it");
    }
}

/// A gate must be constructible with no runtime in scope.
///
/// Deliberately NOT a `#[tokio::test]`. The gate's clock is
/// `tokio::time::Instant` so a paused-clock test can walk a 30-minute window
/// instantly — but that type is runtime-aware under the `test-util` feature,
/// and `spawn_grace` explicitly handles being reached during process shutdown
/// when no runtime is left. A panic here would turn an orderly teardown into
/// a crash.
#[test]
fn a_gate_can_be_built_and_claimed_with_no_runtime() {
    let gate = GraceGate::new(Duration::from_secs(1));
    assert!(gate.bump_and_claim());
    gate.release();
    assert!(gate.bump_and_claim(), "and re-claimed");
}
