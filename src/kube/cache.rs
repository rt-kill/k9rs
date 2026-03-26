//! Persistent disk cache for CRD names and namespace names.
//!
//! Stores discovery data per-context at `~/.cache/k9rs/{context_name}.json`
//! so that on restart, autocompletion is available instantly without waiting
//! for the API server.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::kube::resources::crds::KubeCrd;
use crate::kube::resources::namespaces::KubeNamespace;

/// Serializable representation of a CRD for the disk cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCrd {
    pub name: String,
    pub kind: String,
    pub plural: String,
    pub group: String,
    pub version: String,
    pub scope: String,
}

/// The on-disk cache format for a single context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryCache {
    pub context: String,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub crds: Vec<CachedCrd>,
    pub updated_at: DateTime<Utc>,
}

/// Returns the cache directory: `~/.cache/k9rs/`
pub fn cache_dir() -> Option<PathBuf> {
    dirs_or_home().map(|d| d.join("k9rs"))
}

/// Get the XDG cache home or fall back to `~/.cache`.
fn dirs_or_home() -> Option<PathBuf> {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache"))
        })
}

/// Build a compound cache key from a context name and cluster name.
///
/// Using just the context name causes collisions when different kubeconfig files
/// (e.g. via KUBECONFIG env var) share context names like "dev" or "default"
/// but point to different clusters. Including the cluster name disambiguates.
pub fn cache_key(context: &str, cluster: &str) -> String {
    if cluster.is_empty() {
        context.to_string()
    } else {
        format!("{}@{}", context, cluster)
    }
}

/// Returns the cache file path for a given context.
pub fn cache_path(context: &str) -> Option<PathBuf> {
    // Sanitize context name for use as a filename (replace / and other unsafe chars)
    let safe_name: String = context
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '@' { c } else { '_' })
        .collect();
    cache_dir().map(|d| d.join(format!("{}.json", safe_name)))
}

/// Load the discovery cache for a context (synchronous, used at startup).
pub fn load_cache(context: &str) -> Option<DiscoveryCache> {
    let path = cache_path(context)?;
    let data = std::fs::read_to_string(&path).ok()?;
    let cache: DiscoveryCache = serde_json::from_str(&data).ok()?;
    if cache.context != context {
        return None;
    }
    debug!("Loaded discovery cache for context '{}' from {:?}", context, path);
    Some(cache)
}

/// Save the discovery cache for a context (async, non-blocking).
pub async fn save_cache(cache: &DiscoveryCache) {
    let Some(path) = cache_path(&cache.context) else {
        return;
    };
    let json = match serde_json::to_string_pretty(cache) {
        Ok(j) => j,
        Err(e) => {
            debug!("Failed to serialize discovery cache: {}", e);
            return;
        }
    };
    // Ensure the cache directory exists
    if let Some(dir) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            debug!("Failed to create cache directory {:?}: {}", dir, e);
            return;
        }
    }
    // Atomic write: write to uniquely-named temp file, then rename.
    // Random suffix prevents corruption when multiple concurrent saves target
    // the same context (e.g., daemon Put handler spawning overlapping tasks).
    let tmp_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), rand::random::<u32>()));
    if let Err(e) = tokio::fs::write(&tmp_path, json.as_bytes()).await {
        debug!("Failed to write cache temp file {:?}: {}", tmp_path, e);
        return;
    }
    if let Err(e) = tokio::fs::rename(&tmp_path, &path).await {
        debug!("Failed to rename cache temp file {:?} -> {:?}: {}", tmp_path, path, e);
        let _ = tokio::fs::remove_file(&tmp_path).await;
    } else {
        debug!("Saved discovery cache for context '{}' to {:?}", cache.context, path);
    }
}

/// Convert cached CRDs back to domain KubeCrd structs.
pub fn cached_crds_to_domain(cached: &[CachedCrd]) -> Vec<KubeCrd> {
    cached
        .iter()
        .map(|c| KubeCrd {
            name: c.name.clone(),
            group: c.group.clone(),
            version: c.version.clone(),
            kind: c.kind.clone(),
            plural: c.plural.clone(),
            scope: c.scope.clone(),
            age: None,
        })
        .collect()
}

/// Convert cached namespace names back to domain KubeNamespace structs.
pub fn cached_namespaces_to_domain(names: &[String]) -> Vec<KubeNamespace> {
    names
        .iter()
        .map(|name| KubeNamespace {
            name: name.clone(),
            status: "Active".to_string(),
            age: None,
            labels: Default::default(),
        })
        .collect()
}

/// Convert domain KubeCrd structs to cacheable form.
pub fn domain_crds_to_cached(crds: &[KubeCrd]) -> Vec<CachedCrd> {
    crds.iter()
        .map(|c| CachedCrd {
            name: c.name.clone(),
            kind: c.kind.clone(),
            plural: c.plural.clone(),
            group: c.group.clone(),
            version: c.version.clone(),
            scope: c.scope.clone(),
        })
        .collect()
}

/// Build a DiscoveryCache from the current app state.
pub fn build_cache(
    context: &str,
    namespaces: &[String],
    crds: &[CachedCrd],
) -> DiscoveryCache {
    DiscoveryCache {
        context: context.to_string(),
        namespaces: namespaces.to_vec(),
        crds: crds.to_vec(),
        updated_at: Utc::now(),
    }
}
