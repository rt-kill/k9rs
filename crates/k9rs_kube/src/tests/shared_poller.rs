use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use k9rs_proto::protocol::{ContextId, ContextName};

fn ctx(n: u64) -> ContextId {
    ContextId::new(ContextName::new(format!("c{n}")).unwrap(), format!("https://{n}"), n)
}

/// A task that lives until aborted — stands in for a never-self-finishing
/// poll loop.
fn forever() -> JoinHandle<()> {
    tokio::spawn(std::future::pending::<()>())
}

#[tokio::test]
async fn same_context_shares_one_poller() {
    let cache: PollerCache<u32> = PollerCache::new();
    let spawns = Arc::new(AtomicUsize::new(0));

    let s = spawns.clone();
    let _sub1 = cache.subscribe(ctx(1), 0, move |_tx| {
        s.fetch_add(1, Ordering::SeqCst);
        forever()
    });
    let s = spawns.clone();
    let _sub2 = cache.subscribe(ctx(1), 0, move |_tx| {
        s.fetch_add(1, Ordering::SeqCst);
        forever()
    });

    // Second subscribe reuses the first poller → its `spawn` never ran.
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn distinct_contexts_get_separate_pollers() {
    let cache: PollerCache<u32> = PollerCache::new();
    let spawns = Arc::new(AtomicUsize::new(0));

    let s = spawns.clone();
    let _a = cache.subscribe(ctx(1), 0, move |_| { s.fetch_add(1, Ordering::SeqCst); forever() });
    let s = spawns.clone();
    let _b = cache.subscribe(ctx(2), 0, move |_| { s.fetch_add(1, Ordering::SeqCst); forever() });

    assert_eq!(spawns.load(Ordering::SeqCst), 2);
}

/// Reusing subscribers ride the *same* poller's snapshot stream. If dedup
/// regressed, `sub2` would back a second poller that never sends 42 and its
/// `changed()` would hang the test — so this also guards the sharing.
#[tokio::test]
async fn reused_subscribers_share_one_stream() {
    let cache: PollerCache<u32> = PollerCache::new();
    let go = Arc::new(tokio::sync::Notify::new());

    let g = go.clone();
    let mut sub1 = cache.subscribe(ctx(1), 0, move |tx| {
        tokio::spawn(async move {
            g.notified().await;
            let _ = tx.send(42);
            std::future::pending::<()>().await;
        })
    });
    // Reuse path: this closure must NOT run (else a second poller is spawned).
    let mut sub2 = cache.subscribe(ctx(1), 0, |_| forever());

    go.notify_one();
    assert!(sub1.changed().await.is_ok());
    assert_eq!(sub1.current(), 42);
    assert!(sub2.changed().await.is_ok());
    assert_eq!(sub2.current(), 42);
}

/// The discovery bridge is changed-first and relies on `watch`'s seed never
/// being observed (the eager one-shot covers t=0). Lock it: a creating
/// receiver's first `changed()` fires on the first *real* publish (7), never
/// on the channel's initial seed (0).
#[tokio::test]
async fn changed_first_never_observes_the_seed() {
    let cache: PollerCache<u32> = PollerCache::new();
    let go = Arc::new(tokio::sync::Notify::new());

    let g = go.clone();
    let mut sub = cache.subscribe(ctx(1), 0, move |tx| {
        tokio::spawn(async move {
            g.notified().await;
            let _ = tx.send(7);
            std::future::pending::<()>().await;
        })
    });

    go.notify_one();
    assert!(sub.changed().await.is_ok());
    assert_eq!(sub.current(), 7, "changed-first must skip the seed and land on the real poll");
}

/// A poll that lands while nobody is subscribed is still published, so a
/// session rejoining inside the keep-warm window gets the latest snapshot.
///
/// `watch::Sender::send` with ZERO receivers returns `Err` and never reaches
/// `send_replace` — so until the poller held a receiver of its own, the
/// entire grace window was a window with no receiver: the task kept polling
/// (keeping it warm being the whole point) and every snapshot was discarded.
/// A rejoining session then got whatever had last been published while
/// somebody happened to be listening.
///
/// The worst case is the one asserted second: metrics has no eager one-shot,
/// and its task sleeps before its first poll, so a subscriber that came and
/// went before that poll left the channel holding its SEED — and the rejoin
/// was served a default snapshot, i.e. blank CPU/MEM, for up to a full poll
/// interval.
#[tokio::test]
async fn a_poll_with_no_subscribers_is_still_published() {
    let cache: PollerCache<u32> = PollerCache::new();
    let go = Arc::new(tokio::sync::Notify::new());

    let g = go.clone();
    let sub = cache.subscribe(ctx(1), 0, move |tx| {
        tokio::spawn(async move {
            g.notified().await;
            // The publish that used to be thrown away: by now the only
            // subscriber is gone and grace is holding the poller.
            let _ = tx.send(99);
            std::future::pending::<()>().await;
        })
    });

    // The last subscriber leaves. Grace keeps the poller (and its task)
    // alive — that is what it is for.
    drop(sub);

    go.notify_one();
    // Let the task's send actually run.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Rejoin inside the window: the same poller, and it must carry the poll
    // that landed while nobody was listening.
    let spawned_again = Arc::new(AtomicUsize::new(0));
    let s = spawned_again.clone();
    let mut rejoined = cache.subscribe(ctx(1), 0, move |_| {
        s.fetch_add(1, Ordering::SeqCst);
        forever()
    });
    assert_eq!(
        spawned_again.load(Ordering::SeqCst),
        0,
        "sanity: grace kept the poller, so this is the REUSE path",
    );
    assert_eq!(
        rejoined.current(),
        99,
        "the poll that landed with no subscribers must still be the current \
         snapshot — a zero-receiver `send` publishes nothing",
    );
}
