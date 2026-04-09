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

use crate::kube::protocol::ResourceId;

use super::port_forward::PortForwardSource;
use super::SharedLocalSource;

/// Daemon-wide registry of per-context local resource sources.
pub struct LocalRegistry {
    /// Per-context cache of PortForwardSource. Weak refs only — the strong
    /// `Arc` lives in `LocalSubscription` instances held by the bridge tasks
    /// of currently-subscribed sessions.
    port_forwards: DashMap<String, Weak<PortForwardSource>>,
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
    pub fn port_forwards_for(&self, context: &str) -> Arc<PortForwardSource> {
        // Fast path: try existing entry.
        if let Some(weak) = self.port_forwards.get(context) {
            if let Some(arc) = weak.upgrade() {
                return arc;
            }
        }
        // Slow path: atomic check-and-insert.
        match self.port_forwards.entry(context.to_string()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    return arc;
                }
                let arc = PortForwardSource::for_context(context.to_string());
                e.insert(Arc::downgrade(&arc));
                arc
            }
            Entry::Vacant(e) => {
                let arc = PortForwardSource::for_context(context.to_string());
                e.insert(Arc::downgrade(&arc));
                arc
            }
        }
    }

    /// Generic subscribe-path lookup: given `(context, rid)`, return the
    /// matching local source as a trait object. Returns `None` if the rid
    /// doesn't correspond to a registered local type.
    ///
    /// New local resource types are wired in by adding a typed cache field
    /// above and a match arm here.
    pub fn get(&self, context: &str, rid: &ResourceId) -> Option<SharedLocalSource> {
        if !rid.is_local() {
            return None;
        }
        match rid.plural.as_str() {
            "portforwards" => Some(self.port_forwards_for(context) as SharedLocalSource),
            _ => None,
        }
    }
}
