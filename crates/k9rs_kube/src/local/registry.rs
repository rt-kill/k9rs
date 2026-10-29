//! `LocalRegistry` — resolves a [`ContextId`] to its [`ContextLocals`]
//! slice (that context's port-forwards, exec sources, and any future local
//! operator).
//!
//! The registry holds each slice **`Weak`**: strong refs are the sessions'
//! [`ContextKeepalive`]s (plus at most one in-flight grace task), so a
//! context's local resources live exactly as long as some session is
//! attached — or its grace window is still open. See
//! [`context_locals`](super::context_locals) for the lifetime model,
//! including why port-forwards are strong *inside* the slice while exec
//! sources stay demand-driven.

use std::sync::Arc;
use std::time::Duration;


use k9rs_proto::protocol::ContextId;

use super::context_locals::{ContextKeepalive, ContextLocals};
use super::exec_source::ExecSourceConfig;

/// Daemon-wide directory of per-context local-resource slices.
pub struct LocalRegistry {
    contexts: crate::weak_registry::WeakRegistry<ContextId, ContextLocals>,
    /// Exec resource configs loaded from daemon config at startup, shared
    /// into every slice.
    exec_configs: Arc<[ExecSourceConfig]>,
    /// Grace window a slice survives after its last keepalive drops.
    /// `None` = tear down immediately (`--no-daemon`, where the registry
    /// lives inside a single connection and recovery is impossible).
    grace: Option<Duration>,
}

impl LocalRegistry {
    pub fn new(exec_configs: Vec<ExecSourceConfig>, grace: Option<Duration>) -> Self {
        // Validated by `ExecSourceConfig::usable`, which the CLIENT's
        // `:`-command lookup reads too — these guards used to be a `filter`
        // closure here, applied by the daemon and by nobody else, so an
        // entry skipped here was still typeable and came back as "Unknown
        // local resource" with no reason.
        let valid_configs = ExecSourceConfig::usable(exec_configs);
        Self {
            contexts: crate::weak_registry::WeakRegistry::new(),
            exec_configs: valid_configs.into(),
            grace,
        }
    }

    /// Every context with a LIVE local slice, for `ctl status`. Skips dead
    /// `Weak`s rather than reaping them — reporting must not mutate what it
    /// reports on.
    pub fn stats(&self) -> Vec<k9rs_proto::protocol::ContextStat> {
        self.contexts
            .live()
            .into_iter()
            // Identity from the SLICE, not the map key. They are the same
            // value by construction (`get_or_create` hands the key to
            // `ContextLocals::new`), and reading the slice is what makes
            // that true rather than assumed — it was also the only reader
            // `ContextLocals::context()` ever had, so the accessor and the
            // field it wraps were both dead.
            .map(|(_key, locals, holders)| {
                let ctx = locals.context();
                k9rs_proto::protocol::ContextStat {
                    context: ctx.name.clone(),
                    server_url: ctx.server_url.clone(),
                    port_forwards: locals.port_forwards().entry_count(),
                    holders,
                }
            })
            .collect()
    }

    /// Attach to a context's local-resource slice, creating it on first
    /// attach. The returned keepalive is the caller's strong hold — clone
    /// it per holder; when the last clone drops, the slice's grace window
    /// begins.
    ///
    /// Upgrade-or-insert runs under the DashMap entry lock (the same slow
    /// path as `WatcherCache::subscribe_with`): two sessions racing a dead
    /// `Weak` must never build two live slices for one `ContextId` — that
    /// would mean two `PortForwardSource`s racing to bind the same local
    /// ports.
    ///
    /// (These two paragraphs were sitting above `stats()`, which is migration
    /// residue, not a claim about `stats` — and left `attach`, the function
    /// they describe, undocumented.)
    pub fn attach(&self, context: &ContextId) -> ContextKeepalive {
        let configs = Arc::clone(&self.exec_configs);
        let grace = self.grace;
        // `|_| true`: a `ContextLocals` cannot die while anything holds it —
        // it owns no task that self-terminates — so upgrading IS liveness.
        let (arc, _) = self.contexts.get_or_create(
            context.clone(),
            |_| true,
            move |ctx| ContextLocals::new(ctx.clone(), configs, grace),
        );
        ContextKeepalive(arc)
    }
}

#[cfg(test)]
#[path = "../tests/local/registry.rs"]
mod tests;
