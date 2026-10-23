//! Unit tests for `client_session` (mirrored out of the source file).

// ---------------------------------------------------------------------------
// stream_liveness_tests (mirrored out of client_session.rs)
// ---------------------------------------------------------------------------

use super::*;

#[tokio::test]
async fn is_alive_tracks_the_bridge_task() {
    // A running bridge → alive.
    let running = tokio::spawn(std::future::pending::<()>());
    let live = SubscriptionStream::from_abort_handle(running.abort_handle());
    assert!(live.is_alive());

    // A finished bridge → not alive.
    let done = tokio::spawn(async {});
    let abort = done.abort_handle();
    let _ = done.await; // ensure the task has actually completed
    let dead = SubscriptionStream::from_abort_handle(abort);
    assert!(!dead.is_alive());
}

/// Minting an epoch supersedes whatever was feeding that store, and the
/// predecessor has to be stopped FIRST.
///
/// A stream raises its store's floor when it is minted, and a bridge's
/// internal retries allocate fresh, higher epochs. So a predecessor left
/// running past the mint can raise the floor ABOVE its successor, at
/// which point the successor's own baseline is rejected as stale and the
/// view goes quiet forever. `retire_and_mint` fuses the two steps so the
/// wrong order isn't expressible.
#[tokio::test]
async fn minting_an_epoch_retires_the_stream_it_replaces() {
    let store = crate::store::RowStore::new("pods");

    let old_bridge = tokio::spawn(std::future::pending::<()>());
    let previous = SubscriptionStream::from_abort_handle(old_bridge.abort_handle());
    assert!(previous.is_alive());

    let epoch = Supersedes::Stream(&previous).retire_and_mint(store.as_ref());

    // Aborting is not instantaneous from the aborter's side; joining is
    // how we observe it landed. Bounded, because the predecessor is a
    // `pending()` task — if the abort never happens this must FAIL, not
    // hang the suite.
    let joined = tokio::time::timeout(std::time::Duration::from_secs(2), old_bridge)
        .await
        .expect("the predecessor must be aborted, or it outlives its successor");
    assert!(joined.is_err(), "aborted, not completed");
    assert!(!previous.is_alive());

    // And the floor is this stream's, so anything the predecessor had
    // queued is stale on arrival.
    assert!(
        !store.would_accept_epoch(epoch - 1),
        "a lower epoch must be floored out",
    );
    assert!(store.would_accept_epoch(epoch), "the successor's own epoch stands");
}

/// `Nothing` is a claim, not a default: it must NOT abort anything, or a
/// first subscription would tear down an unrelated live stream.
#[tokio::test]
async fn superseding_nothing_leaves_live_streams_alone() {
    let store = crate::store::RowStore::new("pods");
    let other = tokio::spawn(std::future::pending::<()>());
    let unrelated = SubscriptionStream::from_abort_handle(other.abort_handle());

    let _ = Supersedes::Nothing.retire_and_mint(store.as_ref());

    assert!(unrelated.is_alive(), "an unrelated stream keeps running");
}
