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

/// How long to keep a local source alive after the last subscriber drops.
/// Matches the K8s watcher cache so the two systems behave consistently.
const GRACE_PERIOD_SECS: u64 = 300;

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
        // Clone the keepalive `Arc` and hand it to a detached task that
        // holds it for the grace period before releasing. If anything
        // re-subscribes within the window, the registry's `Weak` upgrades
        // to this same `Arc` and the source is reused. After the window,
        // the last `Arc` drops → the source's background tasks die →
        // the registry's `Weak` becomes dead and is recycled on next get.
        let arc = self._keepalive.clone();
        // Best-effort spawn — if the runtime is shutting down, the Arc
        // simply drops here and the source dies immediately, which is the
        // right thing on shutdown.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
                drop(arc);
            });
        }
    }
}
