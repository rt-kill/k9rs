use super::*;
use std::sync::atomic::AtomicUsize;
use crate::kube::protocol::{ContextId, ContextName};

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
