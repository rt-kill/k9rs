//! `LocalRegistry` — the daemon-wide directory of *per-context* local
//! resource sources.
//!
//! Two lifetime models coexist:
//!
//! - **Port forwards** — held strongly by the registry. Port forwards are
//!   user-created side effects with running subprocesses; their lifetime is
//!   "until explicitly stopped or daemon exits". Subscribing/unsubscribing
//!   only affects whether the TUI sees the list, not whether the kubectl
//!   processes run. Port forwards survive context switches and nav pops.
//!
//! - **Exec resources** — `Weak<Arc>` + grace period (same model as the K8s
//!   watcher cache). Demand-driven and safe to recreate on resubscribe.

use std::sync::{Arc, Weak};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::kube::protocol::{ContextName, ResourceId};

use super::exec_source::{ExecSource, ExecSourceConfig};
use super::port_forward::PortForwardSource;
use super::types::LocalResourceKind;
use super::SharedLocalSource;

/// Daemon-wide registry of per-context local resource sources.
pub struct LocalRegistry {
    /// Port forward sources are held **strongly** (not `Weak`). Port forwards
    /// are user-created side effects with running subprocesses — their
    /// lifetime is "until explicitly stopped or daemon exits", not
    /// demand-driven like watchers. This means port forwards survive
    /// context switches, nav-stack pops, and any period without subscribers.
    port_forwards: DashMap<ContextName, Arc<PortForwardSource>>,
    /// Per-(context, config-name) cache of ExecSources. Each config
    /// produces its own resource keyed by `Custom(name)`.
    exec_resources: DashMap<(ContextName, String), Weak<ExecSource>>,
    /// Exec resource configs loaded from daemon config at startup.
    exec_configs: Vec<ExecSourceConfig>,
}

impl LocalRegistry {
    pub fn new(exec_configs: Vec<ExecSourceConfig>) -> Self {
        // Validate exec resource configs at startup.
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
                true
            })
            .collect();
        Self {
            port_forwards: DashMap::new(),
            exec_resources: DashMap::new(),
            exec_configs: valid_configs,
        }
    }

    /// Get (or lazily create) the `PortForwardSource` for a context.
    /// The registry holds a strong Arc, so the source survives even when
    /// no TUI session is viewing the port-forward list.
    pub fn port_forwards_for(&self, context: &ContextName) -> Arc<PortForwardSource> {
        self.port_forwards
            .entry(context.clone())
            .or_insert_with(|| PortForwardSource::for_context(context.clone()))
            .clone()
    }

    /// Get (or lazily create) the `ExecSource` for a specific config name
    /// and context. Returns `None` if no config with that name exists.
    fn exec_source_for(&self, context: &ContextName, name: &str) -> Option<Arc<ExecSource>> {
        let config = self.exec_configs.iter().find(|c| c.name == name)?.clone();
        let key = (context.clone(), name.to_string());

        self.exec_resources.retain(|_, weak| weak.strong_count() > 0);

        if let Some(weak) = self.exec_resources.get(&key) {
            if let Some(arc) = weak.upgrade() {
                return Some(arc);
            }
        }
        match self.exec_resources.entry(key) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    Some(arc)
                } else {
                    let arc = ExecSource::for_context(config);
                    e.insert(Arc::downgrade(&arc));
                    Some(arc)
                }
            }
            Entry::Vacant(e) => {
                let arc = ExecSource::for_context(config);
                e.insert(Arc::downgrade(&arc));
                Some(arc)
            }
        }
    }

    /// Generic subscribe-path lookup: given `(context, rid)`, return the
    /// matching local source as a trait object.
    pub fn get(&self, context: &ContextName, rid: &ResourceId) -> Option<SharedLocalSource> {
        let ResourceId::Local(kind) = rid else { return None; };
        match kind {
            LocalResourceKind::PortForward => {
                Some(self.port_forwards_for(context) as SharedLocalSource)
            }
            LocalResourceKind::ExecResource => {
                // Legacy variant — look up by the first exec config if any.
                let name = self.exec_configs.first().map(|c| c.name.as_str())?;
                self.exec_source_for(context, name).map(|arc| arc as SharedLocalSource)
            }
            LocalResourceKind::Custom(ref name) => {
                self.exec_source_for(context, name).map(|arc| arc as SharedLocalSource)
            }
        }
    }
}
