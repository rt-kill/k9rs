//! Wire protocol types for k9rs daemon communication.
//!
//! One unified binary protocol (length-prefixed bincode) for ALL daemon
//! communication — both TUI sessions and management commands (k9rs ctl).
//! No JSON on the wire.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::cache::CachedCrd;
use crate::event::ResourceUpdate;

/// Identity of a Kubernetes resource type, identified by Group/Version/Resource (GVR).
/// This is the unified representation — no distinction between "built-in" and "dynamic".
/// Every K8s resource type (including CRDs) is just a GVR.
///
/// Identity (Hash/Eq) is determined by `(group, version, plural)` only.
/// `kind` and `scope` are properties of the resource, not part of its identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceId {
    /// API group (e.g., "" for core, "apps", "batch", "clickhouse.altinity.com")
    pub group: String,
    /// API version (e.g., "v1", "v1beta1")
    pub version: String,
    /// Kind (e.g., "Pod", "Deployment") — for display, NOT part of identity
    pub kind: String,
    /// Plural name (e.g., "pods", "deployments") — for API URLs, part of identity
    pub plural: String,
    /// Whether this resource is cluster-scoped or namespace-scoped — NOT part of identity
    pub scope: ResourceScope,
}

impl PartialEq for ResourceId {
    fn eq(&self, other: &Self) -> bool {
        self.group == other.group && self.version == other.version && self.plural == other.plural
    }
}

impl Eq for ResourceId {}

impl std::hash::Hash for ResourceId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.group.hash(state);
        self.version.hash(state);
        self.plural.hash(state);
    }
}

impl ResourceId {
    pub fn new(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        plural: impl Into<String>,
        scope: ResourceScope,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            scope,
        }
    }

    /// Look up a built-in resource by any alias (e.g., "po", "deploy", "svc", "namespaces").
    pub fn from_alias(alias: &str) -> Option<Self> {
        crate::kube::resource_types::find_by_alias(alias).map(|meta| Self {
            group: meta.group.to_string(),
            version: meta.version.to_string(),
            kind: meta.kind.to_string(),
            plural: meta.plural.to_string(),
            scope: meta.scope,
        })
    }

    /// Display label (the kind name).
    pub fn display_label(&self) -> &str {
        &self.kind
    }

    /// Short UI label for tab bar/breadcrumbs (e.g., "Deploy", "STS").
    /// Falls back to kind for unknown/CRD resource types.
    pub fn short_label(&self) -> &str {
        crate::kube::resource_types::find_by_plural(&self.plural)
            .map(|m| m.short_label)
            .unwrap_or(&self.kind)
    }

    /// Look up the static metadata for this resource (if it's a known type).
    pub fn meta(&self) -> Option<&'static crate::kube::resource_types::ResourceTypeMeta> {
        crate::kube::resource_types::find_by_plural(&self.plural)
    }

    /// Whether this resource supports viewing logs (resolves to pods).
    pub fn supports_logs(&self) -> bool {
        self.meta().map_or(false, |m| m.supports_logs)
    }

    /// Whether this resource supports shell exec (must be a pod).
    pub fn supports_shell(&self) -> bool {
        self.meta().map_or(false, |m| m.supports_shell)
    }

    /// Whether this resource has a /scale subresource.
    pub fn supports_scale(&self) -> bool {
        self.meta().map_or(false, |m| m.supports_scale)
    }

    /// Whether this resource supports rollout restart.
    pub fn supports_restart(&self) -> bool {
        self.meta().map_or(false, |m| m.supports_restart)
    }

    /// Whether this resource is cluster-scoped.
    pub fn is_cluster_scoped(&self) -> bool {
        self.scope == ResourceScope::Cluster
    }

    /// The plural name used in API URLs and watcher keys.
    pub fn api_plural(&self) -> &str {
        &self.plural
    }
}

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}", self.plural)
        } else {
            write!(f, "{}.{}", self.plural, self.group)
        }
    }
}

/// Whether a Kubernetes resource is cluster-scoped or namespace-scoped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceScope {
    Cluster,
    Namespaced,
}

impl ResourceScope {
    pub fn from_scope_str(s: &str) -> Self {
        match s {
            "Cluster" => ResourceScope::Cluster,
            _ => ResourceScope::Namespaced,
        }
    }
}

/// Namespace selection — either all namespaces or a specific one.
/// Replaces the pattern of using String with magic values "all" and "".
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Namespace {
    /// All namespaces (cross-namespace view).
    All,
    /// A specific namespace.
    Named(String),
}

impl Namespace {
    /// For kube-rs API construction: returns the namespace string or None for all.
    pub fn as_option(&self) -> Option<&str> {
        match self {
            Namespace::All => None,
            Namespace::Named(s) => Some(s),
        }
    }

    /// Display string for UI.
    pub fn display(&self) -> &str {
        match self {
            Namespace::All => "all",
            Namespace::Named(s) => s,
        }
    }

    /// Whether this selects all namespaces.
    pub fn is_all(&self) -> bool {
        matches!(self, Namespace::All)
    }
}

impl From<&str> for Namespace {
    fn from(s: &str) -> Self {
        match s {
            "all" => Namespace::All,
            // Empty string is ambiguous in K8s — treat as "all" for TUI purposes
            // (the TUI's default view is all-namespaces). Callers that mean
            // "use context default" should resolve that before constructing Namespace.
            "" => Namespace::All,
            _ => Namespace::Named(s.to_string()),
        }
    }
}

impl From<String> for Namespace {
    fn from(s: String) -> Self {
        if s == "all" || s.is_empty() {
            Namespace::All
        } else {
            Namespace::Named(s)
        }
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.display())
    }
}

/// Identity of a Kubernetes connection.
///
/// A K8s context binds a cluster (API server) to a user (credentials).
/// The context *name* is a human label — NOT unique across kubeconfig files.
/// The actual identity is `(server_url, auth_identity)`:
/// - `server_url` identifies which cluster (API server endpoint)
/// - `auth_info` identifies which credentials (determines RBAC visibility)
///
/// Two sessions with different credentials on the same cluster MUST NOT
/// share watchers (different RBAC = different visible resources).
///
/// Identity (Hash/Eq) is `(server_url, user)` — the context name is
/// display-only and does NOT affect cache sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextId {
    /// The kubeconfig context name (for display only, NOT part of identity).
    pub name: String,
    /// The API server URL (cluster identity).
    pub server_url: String,
    /// The kubeconfig user name (user identity for cache sharing).
    pub user: String,
}

impl PartialEq for ContextId {
    fn eq(&self, other: &Self) -> bool {
        self.server_url == other.server_url && self.user == other.user
    }
}

impl Eq for ContextId {}

impl std::hash::Hash for ContextId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.server_url.hash(state);
        self.user.hash(state);
    }
}

impl ContextId {
    pub fn new(name: String, server_url: String, user: String) -> Self {
        Self { name, server_url, user }
    }
}

impl std::fmt::Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

/// Reference to a specific Kubernetes object.
/// Replaces scattered `(resource_type, name, namespace)` string tuples.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub resource: ResourceId,
    pub name: String,
    pub namespace: Namespace,
}

impl ObjectRef {
    pub fn new(resource: ResourceId, name: impl Into<String>, namespace: Namespace) -> Self {
        Self {
            resource,
            name: name.into(),
            namespace,
        }
    }

    /// Convenience constructor from a resource type string (alias or plural name).
    /// Uses `ResourceId::from_alias` with a fallback for unknown types.
    pub fn from_parts(resource_type: &str, name: impl Into<String>, namespace: Namespace) -> Self {
        let resource = ResourceId::from_alias(resource_type).unwrap_or_else(|| {
            ResourceId::new("", "", resource_type, resource_type, ResourceScope::Namespaced)
        });
        Self {
            resource,
            name: name.into(),
            namespace,
        }
    }
}

/// Key identifying a Kubernetes object by namespace + name.
/// Used as a map key for metrics, delta tracking, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectKey {
    pub namespace: String,
    pub name: String,
}

impl ObjectKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self { namespace: namespace.into(), name: name.into() }
    }
}

/// CPU and memory usage from the metrics-server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsUsage {
    pub cpu: String,
    pub mem: String,
}

/// Maximum bincode message size: 64 MiB. Protects against corrupted frames.
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// Buffer capacity for BufReader/BufWriter on session connections.
pub const IO_BUFFER_SIZE: usize = 256 * 1024;

/// Buffer size for the in-memory duplex stream (--no-daemon mode).
pub const DUPLEX_BUFFER_SIZE: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Binary framing helpers
// ---------------------------------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Write a bincode-serialized message with a 4-byte big-endian length prefix.
/// Flushes the writer after writing.
pub async fn write_bincode<W, T>(writer: &mut W, msg: &T) -> anyhow::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let bytes = bincode::serialize(msg)?;
    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Write a bincode message without flushing (for batched writes).
pub async fn write_bincode_no_flush<W, T>(writer: &mut W, msg: &T) -> anyhow::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let bytes = bincode::serialize(msg)?;
    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

/// Read a bincode-serialized message from a length-prefixed stream.
pub async fn read_bincode<R, T>(reader: &mut R) -> anyhow::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        anyhow::bail!("Message too large: {} bytes (max {})", len, MAX_MESSAGE_SIZE);
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(bincode::deserialize(&buf)?)
}

// ---------------------------------------------------------------------------
// Daemon status payload
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DaemonStatus {
    pub pid: u32,
    pub uptime_secs: u64,
    pub socket_path: String,
}

// ---------------------------------------------------------------------------
// Unified command type: Client -> Daemon (bincode)
// ---------------------------------------------------------------------------

/// All commands from any client (TUI session or management CLI).
/// The first command on a connection determines the connection type:
/// - `Init` → long-lived TUI session
/// - `Ping`/`Status`/`Shutdown`/`Clear` → one-shot management request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionCommand {
    // --- Session lifecycle ---

    /// Start a TUI session with raw kubeconfig + environment variables.
    Init {
        context: Option<String>,
        namespace: Namespace,
        readonly: bool,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
        cluster_name: String,
        user_name: String,
    },

    // --- Resource subscriptions ---

    Subscribe(ResourceId),
    Unsubscribe(ResourceId),
    SwitchNamespace { namespace: Namespace },
    SwitchContext {
        context: String,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
        cluster_name: String,
        user_name: String,
    },

    // --- Resource operations (target identified by ObjectRef) ---

    Describe(ObjectRef),
    Yaml(ObjectRef),
    Delete(ObjectRef),
    Scale { target: ObjectRef, replicas: u32 },
    Restart(ObjectRef),
    StreamLogs {
        pod: String,
        namespace: Namespace,
        container: String,
        follow: bool,
        tail: Option<u64>,
        since: Option<String>,
        previous: bool,
    },
    Refresh(ResourceId),
    StopLogs,
    GetDiscovery,
    DecodeSecret { name: String, namespace: Namespace },
    TriggerCronJob { name: String, namespace: Namespace },
    SuspendCronJob { name: String, namespace: Namespace, suspend: bool },

    // --- Daemon management (one-shot, no session needed) ---

    Ping,
    Status,
    Shutdown,
    Clear { context: Option<String> },
}

// ---------------------------------------------------------------------------
// Unified event type: Daemon -> Client (bincode)
// ---------------------------------------------------------------------------

/// All events from daemon to any client (TUI session or management CLI).
#[derive(Debug, Serialize, Deserialize)]
pub enum SessionEvent {
    // --- Session events ---

    Ready {
        context: String,
        cluster: String,
        user: String,
        namespaces: Vec<String>,
    },
    Snapshot(ResourceUpdate),
    DescribeResult(String),
    YamlResult(String),
    CommandResult { ok: bool, message: String },
    LogLine(String),
    LogEnd,
    Discovery {
        context: String,
        namespaces: Vec<String>,
        crds: Vec<CachedCrd>,
    },
    ContextSwitched {
        context: String,
        ok: bool,
        message: String,
    },
    SessionError(String),
    /// A subscription failed for a specific resource.
    SubscriptionError {
        resource: ResourceId,
        message: String,
    },
    /// The server resolved an unknown resource to its true identity.
    /// The TUI should update its nav and table keys accordingly.
    ResourceResolved {
        original: ResourceId,
        resolved: ResourceId,
    },
    PodMetrics(HashMap<ObjectKey, MetricsUsage>),
    NodeMetrics(HashMap<String, MetricsUsage>),

    // --- Management responses ---

    DaemonStatus(DaemonStatus),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ResourceUpdate;
    use crate::kube::resources::row::{ResourceRow, ExtraValue, ContainerInfo, OwnerRefInfo};
    use std::collections::BTreeMap;

    #[test]
    fn test_resource_row_bincode_roundtrip() {
        let row = ResourceRow {
            cells: vec!["default".into(), "test-pod".into(), "1/1".into(), "Running".into()],
            name: "test-pod".into(),
            namespace: "default".into(),
            extra: BTreeMap::new(),
        };
        let bytes = bincode::serialize(&row).unwrap();
        let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.name, "test-pod");
        assert_eq!(decoded.cells.len(), 4);
    }

    #[test]
    fn test_resource_row_with_extra_bincode_roundtrip() {
        let mut extra = BTreeMap::new();
        extra.insert("uid".into(), ExtraValue::Str("abc-123".into()));
        extra.insert("labels".into(), ExtraValue::Map({
            let mut m = BTreeMap::new();
            m.insert("app".into(), "test".into());
            m
        }));
        extra.insert("containers".into(), ExtraValue::Containers(vec![
            ContainerInfo {
                name: "main".into(),
                real_name: "main".into(),
                image: "nginx:latest".into(),
                ready: true,
                state: "Running".into(),
                restarts: 0,
                ports: vec![80, 443],
            },
        ]));
        extra.insert("owner_references".into(), ExtraValue::OwnerRefs(vec![
            OwnerRefInfo {
                api_version: "apps/v1".into(),
                kind: "ReplicaSet".into(),
                name: "test-rs".into(),
                uid: "uid-456".into(),
            },
        ]));
        let row = ResourceRow {
            cells: vec!["default".into(), "test-pod".into()],
            name: "test-pod".into(),
            namespace: "default".into(),
            extra,
        };
        let bytes = bincode::serialize(&row).unwrap();
        let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.extra_str("uid"), Some("abc-123"));
        assert_eq!(decoded.extra_containers().unwrap().len(), 1);
        assert_eq!(decoded.extra_owner_refs().unwrap().len(), 1);
    }

    #[test]
    fn test_resource_update_rows_bincode_roundtrip() {
        let rid = ResourceId::new("", "v1", "Pod", "pods", ResourceScope::Namespaced);
        let row = ResourceRow {
            cells: vec!["default".into(), "test".into()],
            name: "test".into(),
            namespace: "default".into(),
            extra: BTreeMap::new(),
        };
        let update = ResourceUpdate::Rows {
            resource: rid.clone(),
            headers: vec!["NAMESPACE".into(), "NAME".into()],
            rows: vec![row],
        };
        let bytes = bincode::serialize(&update).unwrap();
        let decoded: ResourceUpdate = bincode::deserialize(&bytes).unwrap();
        match decoded {
            ResourceUpdate::Rows { resource, headers, rows } => {
                assert_eq!(resource.plural, "pods");
                assert_eq!(headers.len(), 2);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_session_event_snapshot_bincode_roundtrip() {
        let rid = ResourceId::new("apps", "v1", "Deployment", "deployments", ResourceScope::Namespaced);
        let update = ResourceUpdate::Rows {
            resource: rid,
            headers: vec!["NAMESPACE".into(), "NAME".into(), "READY".into()],
            rows: vec![ResourceRow {
                cells: vec!["prod".into(), "web".into(), "3/3".into()],
                name: "web".into(),
                namespace: "prod".into(),
                extra: BTreeMap::new(),
            }],
        };
        let event = SessionEvent::Snapshot(update);
        let bytes = bincode::serialize(&event).unwrap();
        let decoded: SessionEvent = bincode::deserialize(&bytes).unwrap();
        match decoded {
            SessionEvent::Snapshot(ResourceUpdate::Rows { rows, .. }) => {
                assert_eq!(rows[0].name, "web");
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_subscribe_command_bincode_roundtrip() {
        let rid = ResourceId::new("", "v1", "Pod", "pods", ResourceScope::Namespaced);
        let cmd = SessionCommand::Subscribe(rid);
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: SessionCommand = bincode::deserialize(&bytes).unwrap();
        match decoded {
            SessionCommand::Subscribe(r) => assert_eq!(r.plural, "pods"),
            _ => panic!("Wrong command"),
        }
    }
}
