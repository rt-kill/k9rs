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

use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::kube::protocol::ContextId;

use super::context_locals::{ContextKeepalive, ContextLocals};
use super::exec_source::ExecSourceConfig;

/// Daemon-wide directory of per-context local-resource slices.
pub struct LocalRegistry {
    contexts: DashMap<ContextId, Weak<ContextLocals>>,
    /// Exec resource configs loaded from daemon config at startup, shared
    /// into every slice.
    exec_configs: Arc<Vec<ExecSourceConfig>>,
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
            contexts: DashMap::new(),
            exec_configs: Arc::new(valid_configs),
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
    pub fn attach(&self, context: &ContextId) -> ContextKeepalive {
        // Opportunistic sweep so entries for long-expired contexts don't
        // accumulate (bounded by distinct contexts ever visited either way).
        self.contexts.retain(|_, weak| weak.strong_count() > 0);

        if let Some(weak) = self.contexts.get(context) {
            if let Some(arc) = weak.upgrade() {
                return ContextKeepalive(arc);
            }
        }
        match self.contexts.entry(context.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    ContextKeepalive(arc)
                } else {
                    let arc = ContextLocals::new(
                        context.clone(),
                        Arc::clone(&self.exec_configs),
                        self.grace,
                    );
                    e.insert(Arc::downgrade(&arc));
                    ContextKeepalive(arc)
                }
            }
            Entry::Vacant(e) => {
                let arc = ContextLocals::new(
                    context.clone(),
                    Arc::clone(&self.exec_configs),
                    self.grace,
                );
                e.insert(Arc::downgrade(&arc));
                ContextKeepalive(arc)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::protocol::ContextName;

    fn cfg(name: &str, headers: &[&str], keys: &[&str]) -> ExecSourceConfig {
        ExecSourceConfig {
            name: name.to_string(),
            command: "echo".to_string(),
            args: vec![],
            poll_interval_secs: 30,
            headers: headers.iter().map(|s| s.to_string()).collect(),
            json_field_keys: keys.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn cid(n: &str) -> ContextId {
        ContextId::new(ContextName::new(n.to_string()), format!("https://{n}"), 1)
    }

    /// `LocalRegistry::new` keeps only well-formed exec configs: it drops ones
    /// with a header/key length mismatch (would misalign columns), an empty
    /// command or headers, a duplicate name, or an empty name.
    #[test]
    fn new_filters_malformed_exec_configs() {
        let mut nocmd = cfg("nocmd", &["A"], &["a"]);
        nocmd.command = String::new();
        let configs = vec![
            cfg("ok", &["A", "B"], &["a", "b"]),            // valid — kept
            cfg("mismatch", &["A", "B", "C"], &["a", "b"]), // 3 vs 2 → dropped
            cfg("ok", &["A"], &["a"]),                      // duplicate name → dropped
            cfg("", &["A"], &["a"]),                        // empty name → dropped
            nocmd,                                          // empty command → dropped
            cfg("noheaders", &[], &[]),                     // empty headers → dropped
        ];
        let reg = LocalRegistry::new(configs, None);
        assert_eq!(reg.exec_configs.len(), 1);
        assert_eq!(reg.exec_configs[0].name, "ok");
        assert_eq!(reg.exec_configs[0].headers, vec!["A", "B"]);
    }

    /// Two attaches to the same context share ONE slice (and its
    /// port-forward source); a different context gets its own.
    #[tokio::test]
    async fn attach_shares_the_slice_per_context() {
        let reg = LocalRegistry::new(vec![], Some(Duration::from_secs(60)));
        let a = reg.attach(&cid("one"));
        let b = reg.attach(&cid("one"));
        let other = reg.attach(&cid("two"));
        assert!(Arc::ptr_eq(&a.0, &b.0), "same context = same slice");
        assert!(!Arc::ptr_eq(&a.0, &other.0), "different context = different slice");
    }

    /// After the last keepalive drops with no grace, a re-attach builds a
    /// FRESH slice (the old one is gone, its entry swept).
    #[tokio::test]
    async fn reattach_after_teardown_builds_fresh_slice() {
        let reg = LocalRegistry::new(vec![], None); // no grace: dies on drop
        let a = reg.attach(&cid("one"));
        let weak = Arc::downgrade(&a.0);
        drop(a);
        assert!(weak.upgrade().is_none(), "slice tore down with its keepalive");
        let b = reg.attach(&cid("one"));
        assert!(weak.upgrade().is_none() && Arc::strong_count(&b.0) >= 1);
    }
}
