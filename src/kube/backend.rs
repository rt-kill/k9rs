//! Abstraction over where the TUI reads/writes cached data.
//!
//! `DataBackend::Daemon` — connected to the k9rs daemon via Unix socket.
//! `DataBackend::Local`  — standalone mode: disk cache only, no resource caching.

use super::cache::{self, CachedCrd};
use super::daemon::DaemonClient;

/// Abstraction over the cache backend. The TUI calls methods on this
/// without caring whether a daemon is running or not.
pub enum DataBackend {
    /// Connected to the k9rs daemon.
    Daemon(DaemonClient),
    /// Standalone mode: disk cache for discovery data, no resource caching.
    Local,
}

impl DataBackend {
    /// Try to connect to the daemon. If `no_daemon` is true or the daemon
    /// isn't running, returns `Local`.
    pub async fn connect(no_daemon: bool) -> Self {
        if no_daemon {
            return Self::Local;
        }
        match DaemonClient::connect().await {
            Some(client) => Self::Daemon(client),
            None => Self::Local,
        }
    }

    /// Returns true if connected to the daemon.
    pub fn is_daemon(&self) -> bool {
        matches!(self, Self::Daemon(_))
    }

    /// Load discovery data (namespaces + CRDs) for a context.
    pub async fn get_discovery(
        &mut self,
        cache_key: &str,
    ) -> Option<(Vec<String>, Vec<CachedCrd>)> {
        match self {
            Self::Daemon(dc) => dc.get(cache_key).await,
            Self::Local => {
                let dc = cache::load_cache(cache_key)?;
                Some((dc.namespaces, dc.crds))
            }
        }
    }

    /// Store discovery data.
    pub async fn put_discovery(
        &mut self,
        cache_key: &str,
        namespaces: &[String],
        crds: &[CachedCrd],
    ) {
        match self {
            Self::Daemon(dc) => {
                dc.put(cache_key, namespaces, crds).await;
            }
            Self::Local => {
                let dc = cache::build_cache(cache_key, namespaces, crds);
                cache::save_cache(&dc).await;
            }
        }
    }

    /// Load cached resource list data for instant tab switching.
    pub async fn get_resources(
        &mut self,
        cache_key: &str,
        namespace: &str,
        resource_type: &str,
    ) -> Option<String> {
        match self {
            Self::Daemon(dc) => dc.get_resources(cache_key, namespace, resource_type).await,
            Self::Local => None,
        }
    }

    /// Store resource list data in the cache.
    pub async fn put_resources(
        &mut self,
        cache_key: &str,
        namespace: &str,
        resource_type: &str,
        data: &str,
    ) {
        match self {
            Self::Daemon(dc) => {
                dc.put_resources(cache_key, namespace, resource_type, data).await;
            }
            Self::Local => {} // no-op
        }
    }

    /// Register this TUI as a session with the daemon.
    pub async fn register_session(
        &mut self,
        context: &str,
        namespace: &str,
    ) -> Option<String> {
        match self {
            Self::Daemon(dc) => {
                use crate::kube::protocol::Request;
                let req = Request::RegisterSession {
                    pid: std::process::id(),
                    context: context.to_string(),
                    namespace: namespace.to_string(),
                };
                let resp = dc.request(&req).await?;
                resp.session_id
            }
            Self::Local => None,
        }
    }

    /// Update the daemon about context/namespace changes.
    pub async fn update_session(
        &mut self,
        session_id: &str,
        context: &str,
        namespace: &str,
    ) {
        if let Self::Daemon(dc) = self {
            use crate::kube::protocol::Request;
            let req = Request::UpdateSession {
                session_id: session_id.to_string(),
                context: context.to_string(),
                namespace: namespace.to_string(),
            };
            dc.request(&req).await;
        }
    }

    /// Deregister this TUI session.
    pub async fn deregister_session(&mut self, session_id: &str) {
        if let Self::Daemon(dc) = self {
            use crate::kube::protocol::Request;
            let req = Request::DeregisterSession {
                session_id: session_id.to_string(),
            };
            dc.request(&req).await;
        }
    }
}
