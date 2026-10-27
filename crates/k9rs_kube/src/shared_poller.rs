//! Per-[`ContextId`] shared background pollers (metrics, discovery).
//!
//! Each session used to spawn its own metrics (30s) and discovery (5min) poll
//! loops, so N sessions on the same cluster polled the API server N times for
//! identical data — asymmetric with watcher sharing, which already dedups via
//! [`crate::live_query::WatcherCache`]. This module factors the *same*
//! ownership model (a `weak_registry::WeakRegistry`, an `Arc`
//! keepalive per subscriber, `watch`-channel fan-out, and
//! [`crate::grace::spawn_grace`] reaping on last-drop) into one generic
//! skeleton so a cluster is polled once regardless of how many sessions watch
//! it. The registry itself is now shared with `WatcherCache` and the two
//! `local` registries, which is where the `Weak`-map dance lives.
//!
//! It deliberately does NOT generalize `WatcherCache` itself (a kube watch
//! stream is a different producer than an interval poller); it's a parallel,
//! concrete mirror that the two pollers — both born here, both
//! latest-snapshot producers — share via the snapshot type parameter `S`.

use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use k9rs_proto::protocol::ContextId;

/// A running interval poller publishing snapshots of type `S` on a `watch`
/// channel. The task runs until the [`SharedPoller`] is dropped — i.e. until
/// the last [`PollerSub`] is gone and its grace period elapses — at which point
/// `Drop` aborts it.
pub struct SharedPoller<S: Clone + Send + Sync + 'static> {
    /// Held so new subscribers can `.subscribe()` a fresh receiver; the poll
    /// task publishes through a clone of this.
    snapshot_tx: watch::Sender<S>,
    task: JoinHandle<()>,
    /// Keep-warm window after the last `PollerSub` drops — same discipline
    /// as [`crate::live_query::LiveQuery`].
    grace: crate::grace::GraceGate,
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
        // [`crate::grace::spawn_grace`]): keep the poller warm for the grace
        // window after the last session leaves, so a quick reconnect reuses it.
        // The poll task never self-finishes, so `is_finished` is effectively
        // always false here — the skeleton stays uniform with the watcher path.
        crate::grace::spawn_grace(
            self._keepalive.clone(),
            |p| p.task.is_finished(),
            |p| Some(&p.grace),
        );
    }
}

/// Per-process cache of shared pollers of one snapshot type, keyed by
/// [`ContextId`]. Stores `Weak` references so a poller dies (after grace) once
/// no session holds a [`PollerSub`].
pub struct PollerCache<S: Clone + Send + Sync + 'static> {
    entries: crate::weak_registry::WeakRegistry<ContextId, SharedPoller<S>>,
}

impl<S: Clone + Send + Sync + 'static> Default for PollerCache<S> {
    fn default() -> Self {
        Self { entries: crate::weak_registry::WeakRegistry::new() }
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
    /// [`WatcherCache::subscribe_with`]: crate::live_query::WatcherCache
    pub fn subscribe<F>(&self, ctx: ContextId, initial: S, spawn: F) -> PollerSub<S>
    where
        F: FnOnce(watch::Sender<S>) -> JoinHandle<()>,
    {
        // The receiver rides OUT of the closure rather than being re-derived
        // from the poller afterwards, and that is not a style choice.
        // `SharedPoller` has no `_keep_rx` field — its siblings `ExecSource`
        // and `PortForwardSource` both do — so the receiver handed to the
        // first subscriber is the ONLY thing holding this channel open.
        // Measured, not assumed: `watch::Sender::send` with zero receivers
        // returns `Err` and does not publish, every poll task sends with
        // `let _ = tx.send(..)`, and a later `subscribe()` lands on the stale
        // seed. So dropping this receiver to re-subscribe would open a window
        // in which the first real poll is silently thrown away.
        let mut created_rx = None;
        // `is_finished` is always false for poll tasks — they never
        // self-terminate — so reuse-vs-recreate turns purely on whether the
        // `Weak` still upgrades. The predicate is passed anyway, for parity
        // with the watcher path where a self-finished task MUST be replaced:
        // the difference between the two registries is now a declared
        // argument rather than a line one of them happens to omit.
        let (poller, _) = self.entries.get_or_create(
            ctx,
            |arc| !arc.task.is_finished(),
            |_| {
                let (poller, rx) = Self::create(initial, spawn);
                created_rx = Some(rx);
                poller
            },
        );
        match created_rx {
            Some(snapshot_rx) => PollerSub { snapshot_rx, _keepalive: poller },
            None => Self::reuse(poller),
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
            grace: crate::grace::GraceGate::new(crate::grace::watcher_grace()),
        });
        (poller, snapshot_rx)
    }

    /// Live pollers, for `ctl status`. Skips dead `Weak`s without reaping —
    /// reporting must not mutate what it reports on.
    pub fn live_count(&self) -> usize {
        self.entries.live().len()
    }

}

#[cfg(test)]
#[path = "tests/shared_poller.rs"]
mod tests;
