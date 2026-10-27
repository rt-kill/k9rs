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
        // Validate exec resource configs at startup. A malformed config is
        // skipped (with a warning) rather than aborting the daemon — one bad
        // entry shouldn't take down the others. Each guard rejects a state
        // that would otherwise misrender or misalign columns at view time.
        let mut seen_names = std::collections::HashSet::new();
        let valid_configs: Vec<ExecSourceConfig> = exec_configs.into_iter()
            .filter(|c| {
                if c.name.is_empty() {
                    tracing::warn!("exec resource config has empty name — skipping");
                    return false;
                }
                if !seen_names.insert(c.name.clone()) {
                    tracing::warn!("exec resource config has duplicate name '{}' — skipping", c.name);
                    return false;
                }
                if c.command.is_empty() {
                    tracing::warn!("exec resource config '{}' has empty command — skipping", c.name);
                    return false;
                }
                if c.headers.is_empty() {
                    tracing::warn!("exec resource config '{}' has no headers — skipping", c.name);
                    return false;
                }
                // The converter zips headers with json_field_keys positionally;
                // a length mismatch would silently drop columns or data, so
                // reject it up front instead of misaligning the table.
                if c.headers.len() != c.json_field_keys.len() {
                    tracing::warn!(
                        "exec resource config '{}' has {} headers but {} jsonFieldKeys \
                         — they must match in length and order; skipping",
                        c.name, c.headers.len(), c.json_field_keys.len()
                    );
                    return false;
                }
                true
            })
            .collect();
        Self {
            contexts: crate::weak_registry::WeakRegistry::new(),
            exec_configs: valid_configs.into(),
            grace,
        }
    }

    /// Attach to a context's local-resource slice, creating it on first
    /// attach. The returned keepalive is the caller's strong hold — clone
    /// it per holder; when the last clone drops, the slice's grace window
    /// begins.
    ///
    /// Upgrade-or-insert runs under the DashMap entry lock (the same slow
    /// path as `WatcherCache::subscribe_with`): two sessions racing a dead
    /// `Weak` must never build two live slices for one `ContextId` — that
    /// would mean two `PortForwardSource`s spawning duplicate kubectl
    /// children onto the same local ports.
    /// Every context with a LIVE local slice, for `ctl status`. Skips dead
    /// `Weak`s rather than reaping them — reporting must not mutate what it
    /// reports on.
    pub fn stats(&self) -> Vec<k9rs_proto::protocol::ContextStat> {
        self.contexts
            .live()
            .into_iter()
            .map(|(key, locals, holders)| k9rs_proto::protocol::ContextStat {
                context: key.name.clone(),
                server_url: key.server_url.clone(),
                port_forwards: locals.port_forwards().live_count(),
                holders,
            })
            .collect()
    }

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
