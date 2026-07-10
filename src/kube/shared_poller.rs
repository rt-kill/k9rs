//! Per-[`ContextId`] shared background pollers (metrics, discovery).
//!
//! Each session used to spawn its own metrics (30s) and discovery (5min) poll
//! loops, so N sessions on the same cluster polled the API server N times for
//! identical data — asymmetric with watcher sharing, which already dedups via
//! [`crate::kube::live_query::WatcherCache`]. This module factors the *same*
//! ownership model (a `DashMap<ContextId, Weak<_>>` cache, an `Arc` keepalive
//! per subscriber, `watch`-channel fan-out, and [`crate::kube::spawn_grace`]
//! reaping on last-drop) into one generic skeleton so a cluster is polled once
//! regardless of how many sessions watch it.
//!
//! It deliberately does NOT generalize `WatcherCache` itself (a kube watch
//! stream is a different producer than an interval poller); it's a parallel,
//! concrete mirror that the two pollers — both born here, both
//! latest-snapshot producers — share via the snapshot type parameter `S`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use dashmap::DashMap;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::kube::protocol::ContextId;

/// A running interval poller publishing snapshots of type `S` on a `watch`
/// channel. The task runs until the [`SharedPoller`] is dropped — i.e. until
/// the last [`PollerSub`] is gone and its grace period elapses — at which point
/// `Drop` aborts it.
pub struct SharedPoller<S: Clone + Send + Sync + 'static> {
    /// Held so new subscribers can `.subscribe()` a fresh receiver; the poll
    /// task publishes through a clone of this.
    snapshot_tx: watch::Sender<S>,
    task: JoinHandle<()>,
    /// Coalesces grace tasks to at most one in flight — same discipline as
    /// [`crate::kube::live_query::LiveQuery`].
    grace_in_flight: AtomicBool,
}

impl<S: Clone + Send + Sync + 'static> Drop for SharedPoller<S> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What a session holds: a `watch` receiver for the shared poller's snapshots
/// plus an `Arc` keepalive that keeps the poller (and its task) alive while any
/// session subscribes.
pub struct PollerSub<S: Clone + Send + Sync + 'static> {
    snapshot_rx: watch::Receiver<S>,
    _keepalive: Arc<SharedPoller<S>>,
}

impl<S: Clone + Send + Sync + 'static> PollerSub<S> {
    /// The latest snapshot (cloned), marking it seen so the next [`changed`]
    /// waits for a genuinely newer one.
    ///
    /// [`changed`]: Self::changed
    pub fn current(&mut self) -> S {
        self.snapshot_rx.borrow_and_update().clone()
    }

    /// Wait for the next published snapshot.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }
}

impl<S: Clone + Send + Sync + 'static> Drop for PollerSub<S> {
    fn drop(&mut self) {
        // Same grace dance as a watcher `Subscription` (see
        // [`crate::kube::spawn_grace`]): keep the poller warm for the grace
        // window after the last session leaves, so a quick reconnect reuses it.
        // The poll task never self-finishes, so `is_finished` is effectively
        // always false here — the skeleton stays uniform with the watcher path.
        crate::kube::spawn_grace(
            self._keepalive.clone(),
            |p| p.task.is_finished(),
            |p| {
                p.grace_in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            },
            |p| p.grace_in_flight.store(false, Ordering::Release),
        );
    }
}

/// Per-process cache of shared pollers of one snapshot type, keyed by
/// [`ContextId`]. Stores `Weak` references so a poller dies (after grace) once
/// no session holds a [`PollerSub`].
pub struct PollerCache<S: Clone + Send + Sync + 'static> {
    entries: DashMap<ContextId, Weak<SharedPoller<S>>>,
}

impl<S: Clone + Send + Sync + 'static> Default for PollerCache<S> {
    fn default() -> Self {
        Self { entries: DashMap::new() }
    }
}

impl<S: Clone + Send + Sync + 'static> PollerCache<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to the shared poller for `ctx`, creating it (via `spawn`) only
    /// if none is live. `spawn` builds the poll task from the snapshot sender;
    /// `initial` seeds the `watch` channel before the first poll publishes
    /// (callers warm it from a cache where one exists). Any session on the same
    /// `ContextId` reuses the running poller — that's the dedup.
    ///
    /// Mirrors [`WatcherCache::subscribe_with`]: reap dead weaks, fast-path
    /// `get`, then an `entry`-locked check-and-insert that resolves races.
    ///
    /// [`WatcherCache::subscribe_with`]: crate::kube::live_query::WatcherCache
    pub fn subscribe<F>(&self, ctx: ContextId, initial: S, spawn: F) -> PollerSub<S>
    where
        F: FnOnce(watch::Sender<S>) -> JoinHandle<()>,
    {
        use dashmap::mapref::entry::Entry;

        self.reap_dead();

        if let Some(weak) = self.entries.get(&ctx) {
            if let Some(arc) = weak.upgrade() {
                // `is_finished` is always false for poll tasks — they never
                // self-terminate — so reuse-vs-recreate turns purely on whether
                // the `Weak` still upgrades. The guard is kept for parity with
                // the watcher path, where a self-finished task must be replaced.
                if !arc.task.is_finished() {
                    return Self::reuse(arc);
                }
            }
        }

        match self.entries.entry(ctx) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    if !arc.task.is_finished() {
                        return Self::reuse(arc);
                    }
                }
                let (poller, rx) = Self::create(initial, spawn);
                e.insert(Arc::downgrade(&poller));
                PollerSub { snapshot_rx: rx, _keepalive: poller }
            }
            Entry::Vacant(e) => {
                let (poller, rx) = Self::create(initial, spawn);
                e.insert(Arc::downgrade(&poller));
                PollerSub { snapshot_rx: rx, _keepalive: poller }
            }
        }
    }

    /// Hand out a fresh subscription backed by an existing live poller.
    fn reuse(arc: Arc<SharedPoller<S>>) -> PollerSub<S> {
        PollerSub { snapshot_rx: arc.snapshot_tx.subscribe(), _keepalive: arc }
    }

    /// Spawn a new poller's task and wrap it in an `Arc`.
    fn create<F>(initial: S, spawn: F) -> (Arc<SharedPoller<S>>, watch::Receiver<S>)
    where
        F: FnOnce(watch::Sender<S>) -> JoinHandle<()>,
    {
        let (snapshot_tx, snapshot_rx) = watch::channel(initial);
        let task = spawn(snapshot_tx.clone());
        let poller = Arc::new(SharedPoller {
            snapshot_tx,
            task,
            grace_in_flight: AtomicBool::new(false),
        });
        (poller, snapshot_rx)
    }

    /// Drop `Weak` slots whose poller is gone, so a long-lived daemon churning
    /// through many clusters doesn't accumulate dead entries.
    fn reap_dead(&self) {
        self.entries.retain(|_, weak| weak.strong_count() > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use crate::kube::protocol::{ContextId, ContextName};

    fn ctx(n: u64) -> ContextId {
        ContextId::new(ContextName::new(format!("c{n}")), format!("https://{n}"), n)
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
}
