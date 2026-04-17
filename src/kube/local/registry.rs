//! `LocalRegistry` — the daemon-wide directory of *per-context* local
//! resource sources.
//!
//! Lives on `SessionSharedState` next to `WatcherCache` and uses the same
//! `Weak<Arc>` lifetime model:
//!
//! - Sources are constructed **lazily per context** the first time a
//!   session subscribes for that `(context, rid)` pair.
//! - The registry stores only `Weak` refs. The strong `Arc` lives in the
//!   subscriber-side `LocalSubscription` (via the bridge task).
//! - When the last `LocalSubscription` for a `(context, rid)` drops, its
//!   `Drop` impl starts a grace-period timer holding the `Arc`. If a new
//!   subscriber appears within the grace period the `Weak` upgrades and
//!   reuses the same source. After the grace period the inner `Arc` is
//!   released; on the next subscribe the registry constructs a fresh source.
//!
//! Concrete consequence for port-forwards: each context owns its own
//! `PortForwardSource`. Killing a TUI session on context A starts the grace
//! period for A's source — its kubectl subprocesses keep running so the
//! user can re-attach within the window. After the window the source dies,
//! its `kill_on_drop` subprocesses go down with it, and the next subscribe
//! on A creates a fresh source from scratch.

use std::sync::{Arc, Weak};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::kube::protocol::{ContextName, ResourceId};

use super::port_forward::PortForwardSource;
use super::types::LocalResourceKind;
use super::SharedLocalSource;

/// Daemon-wide registry of per-context local resource sources.
pub struct LocalRegistry {
    /// Per-context cache of PortForwardSource. Weak refs only — the strong
    /// `Arc` lives in `LocalSubscription` instances held by the bridge tasks
    /// of currently-subscribed sessions.
    port_forwards: DashMap<ContextName, Weak<PortForwardSource>>,
}

impl Default for LocalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalRegistry {
    pub fn new() -> Self {
        Self {
            port_forwards: DashMap::new(),
        }
    }

    /// Get (or lazily create) the `PortForwardSource` for a context. Mirrors
    /// `WatcherCache::subscribe` — fast path tries to upgrade an existing
    /// `Weak`, slow path uses the `DashMap::entry` API for race-safe insert.
    pub fn port_forwards_for(&self, context: &ContextName) -> Arc<PortForwardSource> {
        // Reap dead Weak entries opportunistically. Without this, every
        // context the daemon has ever served leaves a dead slot behind;
        // `port_forwards` grows monotonically for the daemon's lifetime.
        // Mirrors `WatcherCache::reap_dead`.
        self.port_forwards.retain(|_, weak| weak.strong_count() > 0);

        // Fast path: try existing entry.
        if let Some(weak) = self.port_forwards.get(context) {
            if let Some(arc) = weak.upgrade() {
                return arc;
            }
        }
        // Slow path: atomic check-and-insert.
        match self.port_forwards.entry(context.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    return arc;
                }
                let arc = PortForwardSource::for_context(context.clone());
                e.insert(Arc::downgrade(&arc));
                arc
            }
            Entry::Vacant(e) => {
                let arc = PortForwardSource::for_context(context.clone());
                e.insert(Arc::downgrade(&arc));
                arc
            }
        }
    }

    /// Generic subscribe-path lookup: given `(context, rid)`, return the
    /// matching local source as a trait object. Returns `None` if `rid` is
    /// not a [`ResourceId::Local`] variant.
    ///
    /// New local resource types are wired in by adding a [`LocalResourceKind`]
    /// variant + table entry, then a match arm here. The match is exhaustive,
    /// so the compiler enforces that every kind has a backing source.
    pub fn get(&self, context: &ContextName, rid: &ResourceId) -> Option<SharedLocalSource> {
        let ResourceId::Local(kind) = rid else { return None; };
        match kind {
            LocalResourceKind::PortForward => {
                Some(self.port_forwards_for(context) as SharedLocalSource)
            }
        }
    }
}
