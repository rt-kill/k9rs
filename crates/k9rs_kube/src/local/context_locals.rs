//! `ContextLocals` — a context's local resources, owned as one unit.
//!
//! Not a context of its own (the name is genitive): this is the *local
//! slice* of a [`ContextId`] — its port-forwards, its exec sources, and
//! any future daemon-side operator — with one lifetime rule at the top:
//!
//! - Every session attached to the context holds a [`ContextKeepalive`]
//!   (a strong `Arc`); the registry holds only a `Weak`.
//! - When the **last** keepalive drops, a grace task keeps the whole slice
//!   alive for [`locals_grace`](crate::grace::locals_grace) — a session
//!   reconnecting within the window upgrades the `Weak` and recovers
//!   everything (running port-forwards included).
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
//! # A longer window than a watcher's, for a different reason
//!
//! This slice does NOT share the watcher grace window. A warm watcher costs
//! a row store plus an apiserver watch and buys back a re-LIST; a port-forward
//! costs one `kubectl` child, was created by an explicit user action, and
//! buying it back means the user noticing it died. See
//! `DEFAULT_LOCALS_GRACE_SECS`(crate::grace::DEFAULT_LOCALS_GRACE_SECS).
//!
//! The deadline-based grace this module used to implement by hand — so the
//! second session out doesn't inherit the first one's nearly-expired timer —
//! now lives in [`GraceGate`](crate::grace::GraceGate) and is shared by every
//! kept-alive object in the daemon.

use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::DashMap;

use k9rs_proto::protocol::{ContextId, ResourceId};

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
    /// Keep-alive window after the last session detaches. `None` = drop
    /// immediately (`--no-daemon`: the registry lives inside a single
    /// connection, so a post-session grace window could never be recovered
    /// — it would only keep kubectl children alive pointlessly).
    ///
    /// The deadline-based behaviour this slice used to spell out by hand now
    /// lives in [`crate::grace::GraceGate`] and is shared by every kept-alive object.
    grace: Option<crate::grace::GraceGate>,
}

impl ContextLocals {
    pub(super) fn new(
        context: ContextId,
        exec_configs: Arc<Vec<ExecSourceConfig>>,
        grace: Option<Duration>,
    ) -> Arc<Self> {
        let port_forwards = PortForwardSource::for_context();
        Arc::new(Self {
            context,
            port_forwards,
            exec_resources: DashMap::new(),
            exec_configs,
            grace: grace.map(crate::grace::GraceGate::new),
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
            // The daemon deliberately has no source for contexts. They are
            // the CLIENT's kubeconfig, seeded client-side, and a picker that
            // needed a session could not help the case it exists for — you
            // reach for it precisely when you have no usable context. A
            // subscribe request for one is a client bug, and `None` here
            // makes the daemon say so instead of inventing an answer.
            // Same rule, same reason: keybindings and aliases are facts
            // about the CLIENT — its keymap, its config file, its registry
            // and its command grammar. The daemon has never seen any of it.
            LocalResourceKind::Context
            | LocalResourceKind::Keybinding
            | LocalResourceKind::Alias => None,
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
        // The whole deadline dance now lives in `GraceGate`, shared with
        // watchers, pollers and local sources. `None` (`--no-daemon`) means
        // die with the attachment. When the task's Arc finally drops, if it
        // was the last strong ref the slice tears down: port-forward guards
        // abort, kubectl children are reaped.
        crate::grace::spawn_grace(
            Arc::clone(&self.0),
            |_locals| false,
            |locals| locals.grace.as_ref(),
        );
    }
}

#[cfg(test)]
#[path = "../tests/local/context_locals.rs"]
mod tests;
