//! The suspension drain: what happens to daemon events while the user is
//! sitting in `$EDITOR`.

use super::*;
use crate::event::AppEvent;

/// Events that arrive while a subprocess is running must be APPLIED, not
/// queued behind a loop that isn't running.
///
/// The link does not pause because a human is thinking. With nobody reading,
/// the client's channel fills, its reader tasks stop reading their substreams,
/// the yamux windows fill, the daemon's writes stall, and its 60s
/// `connection_write_timeout` tears the connection down — a long edit used to
/// come back to a dead session. The flash below is a stand-in for any event:
/// what is being pinned is that the channel is drained at all.
#[tokio::test]
async fn events_arriving_during_a_suspension_are_applied_not_stranded() {
    let mut app = App::new_for_test();
    let (tx, mut rx) = mpsc::channel::<AppEvent>(8);

    // "The subprocess": finishes only once told to, so the drain has to be
    // concurrent rather than after-the-fact.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let sender = tokio::spawn(async move {
        tx.send(AppEvent::NoContextConfigured).await.expect("channel open");
        // Give the drain a chance to consume it before the work completes,
        // so a passing result can't come from a post-hoc drain.
        tokio::task::yield_now().await;
        let _ = done_tx.send(());
    });

    super::draining(&mut app, &mut rx, async {
        let _ = done_rx.await;
    })
    .await;
    sender.await.expect("sender should not panic");

    assert!(
        app.kube.context.is_none(),
        "the NoContextConfigured event reached app state",
    );
    assert!(
        app.ui.flash.is_some(),
        "…and was applied while the work was still running, not dropped",
    );
}

/// A closed channel must not stall the thing being awaited. `select!` disables
/// a branch whose pattern fails to match, and `recv()` on a closed channel
/// yields `None` forever — get this wrong and the editor hangs after a
/// disconnect instead of returning.
#[tokio::test]
async fn a_closed_event_channel_does_not_strand_the_subprocess() {
    let mut app = App::new_for_test();
    let (tx, mut rx) = mpsc::channel::<AppEvent>(1);
    drop(tx);

    let out = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        super::draining(&mut app, &mut rx, async { 42u8 }),
    )
    .await
    .expect("the work must still complete with no daemon behind it");
    assert_eq!(out, 42);
}
