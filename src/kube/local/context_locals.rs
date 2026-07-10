//! `ContextLocals` — a context's local resources, owned as one unit.
//!
//! Not a context of its own (the name is genitive): this is the *local
//! slice* of a [`ContextId`] — its port-forwards, its exec sources, and
//! any future daemon-side operator — with one lifetime rule at the top:
//!
//! - Every session attached to the context holds a [`ContextKeepalive`]
//!   (a strong `Arc`); the registry holds only a `Weak`.
//! - When the **last** keepalive drops, a grace task keeps the whole slice
//!   alive for [`GRACE_PERIOD_SECS`](crate::kube::GRACE_PERIOD_SECS) —
//!   a session reconnecting within the window upgrades the `Weak` and
//!   recovers everything (running port-forwards included).
//! - When the grace expires, the `Arc` drops and teardown cascades:
//!   port-forward entries drop their
//!   [`OperatorGuard`](super::supervise::OperatorGuard)s, aborting each
//!   supervised loop, whose in-flight attempt drops its `kill_on_drop`
//!   child. Drop *is* the cleanup at every level.
//!
//! Within that bound, two deliberate ownership policies coexist — the
//! strong-vs-`Weak` distinction is the vocabulary for it:
//!
//! - **Port-forwards (strong)**: user-created side effects. They run while
//!   the context is attached (or in grace), regardless of whether anyone
//!   is *viewing* the `:pf` list.
//! - **Exec sources (`Weak`)**: derived views — caches of a command's
//!   output with no user-created state. They exist (and poll) only while
//!   subscribed, with the standard subscription grace; holding them
//!   strongly here would mean running user-configured commands forever
//!   for a context nobody is looking at.
//!
//! # Grace is deadline-based, not first-drop-based
//!
//! The shared [`spawn_grace`](crate::kube::spawn_grace) coalesces on a
//! claim flag, which measures the window from the FIRST dropper — with two
//! sessions, the second to leave could get ~0s of grace. Contexts carry
//! live subprocesses, so this slice uses a movable deadline instead: every
//! keepalive drop bumps `deadline = now + grace`, and the single coalesced
//! task sleeps until the deadline stops moving. The window is always
//! measured from the *last* session out.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::kube::protocol::{ContextId, ResourceId};

use super::exec_source::{ExecSource, ExecSourceConfig};
use super::port_forward::PortForwardSource;
use super::types::LocalResourceKind;
use super::SharedLocalSource;

/// All local resources for one [`ContextId`]. See the module docs for the
/// lifetime model. Held `Weak` by the registry; strong refs are the
/// sessions' [`ContextKeepalive`]s plus at most one in-flight grace task.
pub struct ContextLocals {
    context: ContextId,
    /// Strong: side-effect resources live exactly as long as this slice.
    port_forwards: Arc<PortForwardSource>,
    /// `Weak`: derived views are demand-driven within the context bound.
    /// Same shape the registry itself used before this type existed.
    exec_resources: DashMap<String, Weak<ExecSource>>,
    exec_configs: Arc<Vec<ExecSourceConfig>>,
    /// Grace bookkeeping. `created` anchors the deadline arithmetic so a
    /// monotonic `Instant` fits in an `AtomicU64` of elapsed millis.
    created: Instant,
    grace_deadline_ms: AtomicU64,
    grace_claimed: AtomicBool,
    /// `None` = drop immediately on last detach (`--no-daemon`: the
    /// registry lives inside a single connection, so a post-session grace
    /// window could never be recovered — it would only keep kubectl
    /// children alive pointlessly).
    grace: Option<Duration>,
}

impl ContextLocals {
    pub(super) fn new(
        context: ContextId,
        exec_configs: Arc<Vec<ExecSourceConfig>>,
        grace: Option<Duration>,
    ) -> Arc<Self> {
        let port_forwards = PortForwardSource::for_context(context.name.clone());
        Arc::new(Self {
            context,
            port_forwards,
            exec_resources: DashMap::new(),
            exec_configs,
            created: Instant::now(),
            grace_deadline_ms: AtomicU64::new(0),
            grace_claimed: AtomicBool::new(false),
            grace,
        })
    }

    /// The context this slice belongs to.
    pub fn context(&self) -> &ContextId {
        &self.context
    }

    /// This context's port-forward source.
    pub fn port_forwards(&self) -> Arc<PortForwardSource> {
        Arc::clone(&self.port_forwards)
    }

    /// Subscribe-path lookup: resolve a local [`ResourceId`] to its source.
    /// (Moved verbatim from the old `LocalRegistry::get` — the registry now
    /// resolves contexts, this type resolves resources within one.)
    pub fn get(&self, rid: &ResourceId) -> Option<SharedLocalSource> {
        let ResourceId::Local(kind) = rid else { return None };
        match kind {
            LocalResourceKind::PortForward => {
                Some(self.port_forwards() as SharedLocalSource)
            }
            LocalResourceKind::ExecResource => {
                // Legacy variant — look up by the first exec config if any.
                let name = self.exec_configs.first().map(|c| c.name.clone())?;
                self.exec_source_for(&name).map(|arc| arc as SharedLocalSource)
            }
            LocalResourceKind::Custom(ref name) => {
                self.exec_source_for(name).map(|arc| arc as SharedLocalSource)
            }
        }
    }

    /// Get (or lazily create) the `ExecSource` for a config name. `None`
    /// if no config with that name exists. Demand-driven: the map holds
    /// `Weak`, subscribers hold the strong refs (see module docs).
    fn exec_source_for(&self, name: &str) -> Option<Arc<ExecSource>> {
        let config = self.exec_configs.iter().find(|c| c.name == name)?.clone();

        self.exec_resources.retain(|_, weak| weak.strong_count() > 0);

        if let Some(weak) = self.exec_resources.get(name) {
            if let Some(arc) = weak.upgrade() {
                return Some(arc);
            }
        }
        match self.exec_resources.entry(name.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    Some(arc)
                } else {
                    let arc = ExecSource::for_context(config);
                    e.insert(Arc::downgrade(&arc));
                    Some(arc)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let arc = ExecSource::for_context(config);
                e.insert(Arc::downgrade(&arc));
                Some(arc)
            }
        }
    }

    fn now_ms(&self) -> u64 {
        self.created.elapsed().as_millis() as u64
    }
}

/// A session's strong hold on a context's local resources. Clone freely —
/// each clone is one more attachment; when the last one drops, the grace
/// window (measured from that last drop) begins.
pub struct ContextKeepalive(pub(super) Arc<ContextLocals>);

impl Clone for ContextKeepalive {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl std::ops::Deref for ContextKeepalive {
    type Target = ContextLocals;
    fn deref(&self) -> &ContextLocals {
        &self.0
    }
}

impl Drop for ContextKeepalive {
    fn drop(&mut self) {
        let locals = &self.0;
        let Some(grace) = locals.grace else {
            // --no-daemon: die with the attachment, exactly like today.
            return;
        };
        // Move the deadline out to now + grace. fetch_max (not store) so a
        // racing earlier drop can't pull an already-later deadline back in.
        let deadline = locals.now_ms().saturating_add(grace.as_millis() as u64);
        locals.grace_deadline_ms.fetch_max(deadline, Ordering::AcqRel);

        // Coalesce: at most one grace task per slice. Losers just drop
        // their Arc — the winner's task already watches the (bumped)
        // deadline.
        if locals
            .grace_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // Process shutdown — no runtime to grace on; release the claim
            // and die now (mirrors `spawn_grace`'s fallback arm).
            locals.grace_claimed.store(false, Ordering::Release);
            return;
        };
        let arc = Arc::clone(&self.0);
        handle.spawn(async move {
            loop {
                // Sleep until the deadline stops moving.
                loop {
                    let now = arc.now_ms();
                    let dl = arc.grace_deadline_ms.load(Ordering::Acquire);
                    if now >= dl {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(dl - now)).await;
                }
                arc.grace_claimed.store(false, Ordering::Release);
                // Release-recheck: a keepalive may have bumped the deadline
                // between our last load and the reset above — its claim
                // lost against our still-set flag, so the bump is ours to
                // serve. Re-claim and keep waiting; if someone else claimed
                // in the gap, the bump is theirs.
                let now = arc.now_ms();
                if now >= arc.grace_deadline_ms.load(Ordering::Acquire) {
                    break;
                }
                if arc
                    .grace_claimed
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    break;
                }
            }
            // Task's Arc drops here — if it was the last strong ref, the
            // whole slice tears down (port-forward guards abort, children
            // reaped).
            drop(arc);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::protocol::ContextName;

    fn ctx(n: &str) -> ContextId {
        ContextId::new(ContextName::new(n.to_string()), format!("https://{n}"), 1)
    }

    fn locals(grace: Option<Duration>) -> ContextKeepalive {
        ContextKeepalive(ContextLocals::new(ctx("t"), Arc::new(Vec::new()), grace))
    }

    #[tokio::test]
    async fn no_grace_mode_drops_immediately() {
        let k = locals(None);
        let weak = Arc::downgrade(&k.0);
        drop(k);
        assert!(weak.upgrade().is_none(), "--no-daemon: last detach = teardown");
    }

    #[tokio::test]
    async fn grace_window_allows_recovery_then_expires() {
        let k = locals(Some(Duration::from_millis(60)));
        let weak = Arc::downgrade(&k.0);
        drop(k);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let recovered = weak.upgrade();
        assert!(recovered.is_some(), "within grace the slice is recoverable");
        drop(recovered); // re-drop WITHOUT a keepalive: no new bump
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(weak.upgrade().is_none(), "after grace the slice is gone");
    }

    #[tokio::test]
    async fn deadline_measures_from_last_dropper() {
        // k1 drops at t=0 (deadline 100ms); k2 drops at t=60 (deadline
        // 160ms). At t=120 — past k1's window — the slice must still be
        // alive; at t=240 it must be gone.
        let k1 = locals(Some(Duration::from_millis(100)));
        let k2 = k1.clone();
        let weak = Arc::downgrade(&k1.0);
        drop(k1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(k2);
        tokio::time::sleep(Duration::from_millis(60)).await; // t≈120
        assert!(weak.upgrade().is_none() == false, "second dropper extended the window");
        tokio::time::sleep(Duration::from_millis(120)).await; // t≈240
        assert!(weak.upgrade().is_none(), "extended window expired");
    }

    #[tokio::test]
    async fn reattach_within_grace_keeps_slice_alive_past_expiry() {
        let k = locals(Some(Duration::from_millis(40)));
        let weak = Arc::downgrade(&k.0);
        drop(k);
        // "Reconnect": upgrade within the window and hold a new keepalive.
        let re = ContextKeepalive(weak.upgrade().expect("still in grace"));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(weak.upgrade().is_some(), "held keepalive outlives the old grace window");
        drop(re);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(weak.upgrade().is_none(), "fresh window from the re-drop then expires");
    }
}
