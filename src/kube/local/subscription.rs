//! `LocalSubscription` — a handle to a local resource's snapshot stream.
//!
//! Parallel to [`crate::kube::live_query::Subscription`] and uses the same
//! ownership model: the subscription holds a strong `Arc` to the
//! `LocalResourceSource`, and on `Drop` it spawns a grace-period task that
//! holds the `Arc` for [`GRACE_PERIOD_SECS`](crate::kube::GRACE_PERIOD_SECS)
//! before releasing it.
//!
//! This subscription-level grace matters only for **demand-scoped** sources
//! (exec resources, held `Weak` by their `ContextLocals`): as long as any
//! subscriber or grace task is alive, the `Weak` still upgrades and a
//! re-subscribe reuses the same source. Port-forwards opt out
//! (`try_begin_grace` returns `false`): they're held *strongly* by their
//! `ContextLocals`, so a forward's lifetime is the context's — navigating
//! away from the `:pf` list changes nothing, and teardown happens when the
//! context itself dies (explicit stop aside). The context-level grace lives
//! in [`super::context_locals`], not here.

use tokio::sync::watch;

use crate::kube::protocol::TableBaseline;
use crate::kube::protocol::ResourceId;

use super::SharedLocalSource;

/// A subscription to a local resource source. Unlike the K8s
/// `live_query::Subscription` which carries typed messages to
/// signal "watcher died", local sources are infallible by construction —
/// they always have a current value, and "the source went away" is modeled
/// by dropping the subscription, not by publishing `None`. So the receiver
/// type is `watch::Receiver<TableBaseline>`, no Option.
pub struct LocalSubscription {
    /// The resource id the subscription is for (logging / debugging).
    pub resource: ResourceId,
    /// Receiver for snapshot updates. The source owns the `watch::Sender`.
    pub snapshot_rx: watch::Receiver<TableBaseline>,
    /// Strong handle to the underlying source. Keeps the source alive for
    /// as long as the subscription exists; on `Drop` the grace period
    /// extends that lifetime by [`GRACE_PERIOD_SECS`](crate::kube::GRACE_PERIOD_SECS) more seconds.
    _keepalive: SharedLocalSource,
}

impl LocalSubscription {
    /// Construct a subscription. Takes the source by strong reference so
    /// the caller's `Arc` is preserved (clone it before calling).
    pub fn new(
        resource: ResourceId,
        snapshot_rx: watch::Receiver<TableBaseline>,
        source: SharedLocalSource,
    ) -> Self {
        Self {
            resource,
            snapshot_rx,
            _keepalive: source,
        }
    }

    /// Read the current snapshot without waiting. Always returns a value —
    /// local sources are constructed with an initial snapshot.
    pub fn current(&mut self) -> TableBaseline {
        self.snapshot_rx.borrow_and_update().clone()
    }

    /// Wait for the next snapshot update.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }
}

impl Drop for LocalSubscription {
    fn drop(&mut self) {
        // Shared grace dance (see `crate::kube::spawn_grace`). Local sources
        // are infallible by construction — they never "die" — so the
        // `finished` predicate is always false; claim/reset are the trait's
        // dyn-dispatched `try_begin_grace`/`end_grace` hook (which returns a
        // no-op `false` for port-forwards, held strongly by their
        // ContextLocals and never graced here). Coalescing keeps at most one grace task across
        // context-switch churn that would otherwise stack detached timers,
        // each holding an `Arc<dyn LocalResourceSource>`.
        crate::kube::spawn_grace(
            self._keepalive.clone(),
            |_src| false,
            |src| src.try_begin_grace(),
            |src| src.end_grace(),
        );
    }
}
