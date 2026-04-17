//! `LocalSubscription` — a handle to a local resource's snapshot stream.
//!
//! Parallel to [`crate::kube::live_query::Subscription`] and uses the same
//! ownership model: the subscription holds a strong `Arc` to the
//! `LocalResourceSource`, and on `Drop` it spawns a grace-period task that
//! holds the `Arc` for [`GRACE_PERIOD_SECS`] before releasing it. As long
//! as any subscriber exists (or any grace-period task is alive), the
//! `Weak` ref in [`LocalRegistry`] still upgrades and a re-subscribe within
//! the window reuses the same source.
//!
//! For port-forwards this means: switching context in the TUI starts the
//! grace period for the old context's `PortForwardSource`. Its kubectl
//! subprocesses keep running until either (a) the user re-attaches within
//! the window, in which case the existing PFs are visible again, or (b)
//! the window expires and the source drops, killing the subprocesses via
//! `kill_on_drop`.

use tokio::sync::watch;

use crate::event::ResourceUpdate;
use crate::kube::protocol::ResourceId;

use super::SharedLocalSource;

use crate::kube::GRACE_PERIOD_SECS;

/// A subscription to a local resource source. Unlike the K8s
/// `live_query::Subscription` which carries `Option<ResourceUpdate>` to
/// signal "watcher died", local sources are infallible by construction —
/// they always have a current value, and "the source went away" is modeled
/// by dropping the subscription, not by publishing `None`. So the receiver
/// type is `watch::Receiver<ResourceUpdate>`, no Option.
pub struct LocalSubscription {
    /// The resource id the subscription is for (logging / debugging).
    pub resource: ResourceId,
    /// Receiver for snapshot updates. The source owns the `watch::Sender`.
    pub snapshot_rx: watch::Receiver<ResourceUpdate>,
    /// Strong handle to the underlying source. Keeps the source alive for
    /// as long as the subscription exists; on `Drop` the grace period
    /// extends that lifetime by [`GRACE_PERIOD_SECS`] more seconds.
    _keepalive: SharedLocalSource,
}

impl LocalSubscription {
    /// Construct a subscription. Takes the source by strong reference so
    /// the caller's `Arc` is preserved (clone it before calling).
    pub fn new(
        resource: ResourceId,
        snapshot_rx: watch::Receiver<ResourceUpdate>,
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
    pub fn current(&mut self) -> ResourceUpdate {
        self.snapshot_rx.borrow_and_update().clone()
    }

    /// Wait for the next snapshot update.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }
}

impl Drop for LocalSubscription {
    fn drop(&mut self) {
        // Coalesce: only the first drop in a run wins the `try_begin_grace`
        // CAS and spawns the grace task. Subsequent drops see the slot
        // occupied and just drop their Arc immediately. Without this,
        // rapid context-switch churn used to stack one detached 5-minute
        // task per drop, each holding `Arc<dyn LocalResourceSource>`.
        if !self._keepalive.try_begin_grace() {
            return;
        }
        // Best-effort spawn — if the runtime is shutting down, reset the
        // grace slot and let the Arc drop here; the source dies immediately,
        // which is the right thing on shutdown.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self._keepalive.end_grace();
            return;
        };
        let arc = self._keepalive.clone();
        handle.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
            // Reset the grace slot BEFORE dropping the Arc so a future
            // subscribe-then-drop cycle can spawn a fresh grace task.
            arc.end_grace();
            drop(arc);
        });
    }
}
