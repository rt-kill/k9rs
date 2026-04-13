//! Wire protocol types for k9rs daemon communication.
//!
//! One unified binary protocol (length-prefixed bincode) for ALL daemon
//! communication — both TUI sessions and management commands (k9rs ctl).
//! No JSON on the wire.

use std::collections::{BTreeMap, HashMap};
use serde::{Deserialize, Serialize};

use super::cache::CachedCrd;

/// Identity of a Kubernetes resource type, identified by Group/Version/Resource (GVR).
/// This is the unified representation — no distinction between "built-in" and "dynamic".
/// Every K8s resource type (including CRDs) is just a GVR.
///
/// Sentinel group for daemon-owned "local" resources (port-forwards, saved
/// queries, etc.). Distinct from every real K8s API group so identity checks
/// are unambiguous. See `crate::kube::local` for the abstraction.
pub const LOCAL_GROUP: &str = "k9rs.local";

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

    /// Look up a built-in resource by any alias (e.g., "po", "deploy", "svc", "namespaces", "pf").
    /// Consults both the K8s RESOURCE_TYPES table and the local LOCAL_RESOURCE_TYPES table.
    pub fn from_alias(alias: &str) -> Option<Self> {
        if let Some(meta) = crate::kube::resource_types::find_by_alias(alias) {
            return Some(Self {
                group: meta.group.to_string(),
                version: meta.version.to_string(),
                kind: meta.kind.to_string(),
                plural: meta.plural.to_string(),
                scope: meta.scope,
            });
        }
        if let Some(local) = crate::kube::local::find_by_alias(alias) {
            return Some(local.to_resource_id());
        }
        None
    }

    /// Display label (the kind name).
    pub fn display_label(&self) -> &str {
        &self.kind
    }

    /// Short UI label for tab bar/breadcrumbs (e.g., "Deploy", "STS", "PF").
    /// Falls back to kind for unknown/CRD resource types.
    pub fn short_label(&self) -> &str {
        if self.is_local() {
            if let Some(m) = crate::kube::local::find_by_plural(&self.plural) {
                return m.short_label;
            }
            return &self.kind;
        }
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

    /// Whether this resource is a daemon-owned local resource (not from K8s API).
    /// Local resources use the sentinel group `"k9rs.local"`.
    pub fn is_local(&self) -> bool {
        self.group == LOCAL_GROUP
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

// ---------------------------------------------------------------------------
// Capabilities — typed declarative manifest of operations a resource supports
// ---------------------------------------------------------------------------
//
// The server sends a `ResourceCapabilities` after a successful subscription.
// It declares *what operations exist on this resource type* and *what input
// shape each operation needs*. The TUI maps user input (keystrokes) to
// `OperationKind`s purely client-side; the server never sees a key.
//
// Wire commands stay strongly typed (`SessionCommand::PortForward { .. }`,
// etc.). The schema describes how to *gather* the values for that command;
// it doesn't dispatch generically.

/// One specific operation the user might want to perform on a row. Each kind
/// corresponds to either a `SessionCommand` variant or a purely client-side
/// action that nonetheless requires server-known facts (like "this row is a
/// pod, you can `kubectl exec` into it").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationKind {
    // Always-on (the server can satisfy these for any K8s row, plus locals
    // that opt in).
    Describe,
    Yaml,
    Delete,

    // Workload operations.
    Restart,
    Scale,
    StreamLogs,
    PreviousLogs,
    PortForward,

    // Pod-specific.
    Shell,
    ShowNode,
    ForceKill,

    // Special-purpose K8s resources.
    DecodeSecret,
    TriggerCronJob,
    ToggleSuspendCronJob,
}

/// A field in an `InputSchema::Form`. Names are stable identifiers the
/// dialog widget keys its values map by; labels are user-facing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormField {
    pub name: String,
    pub label: String,
    pub kind: FieldKind,
}

/// Type-narrowing for a single form field. The dialog widget renders the
/// appropriate input control based on this discriminator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldKind {
    /// Free-form text. `max_len` is advisory; the widget may enforce it.
    Text { max_len: Option<usize> },
    /// Integer with explicit bounds.
    Number { min: i64, max: i64 },
    /// Network port (1..=65535) — convenience for the common case.
    Port,
    /// One of a fixed set of choices. Each entry is `(value, display_label)`.
    Select { options: Vec<(String, String)> },
}

/// A value submitted for a `FormField`. The dialog widget produces these
/// when the user submits; the per-`OperationKind` mapper converts them into
/// the typed `SessionCommand` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    Text(String),
    Number(i64),
    Selection(String),
}

/// What input the operation needs from the user before it can be dispatched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputSchema {
    /// No input — the action runs immediately (modulo `requires_confirm`).
    None,
    /// Render a form with these fields and collect a value for each.
    Form(Vec<FormField>),
}

/// One operation declared on a resource type. The triple `(kind, input,
/// requires_confirm)` is enough for the TUI to gate keys, render any input
/// dialog, and emit the typed wire command on submit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationDescriptor {
    pub kind: OperationKind,
    /// Short user-facing label. The TUI may show it in help text or hint
    /// bars; never used to dispatch.
    pub label: String,
    /// What input the operation needs (None for one-shot actions).
    pub input: InputSchema,
    /// If true, the TUI shows a confirm dialog before dispatching. The
    /// confirm message is built client-side from the kind + row context.
    pub requires_confirm: bool,
}

/// The full set of operations a resource type supports. Sent from the server
/// to the TUI after a successful subscription. Empty `operations` means the
/// resource is read-only.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceCapabilities {
    pub operations: Vec<OperationDescriptor>,
}

impl ResourceCapabilities {
    /// True if any declared operation has the given kind.
    pub fn supports(&self, kind: OperationKind) -> bool {
        self.operations.iter().any(|op| op.kind == kind)
    }

    /// Look up the descriptor for an operation, if declared. Use this when
    /// you need the input schema or the label, not just a boolean.
    pub fn get(&self, kind: OperationKind) -> Option<&OperationDescriptor> {
        self.operations.iter().find(|op| op.kind == kind)
    }
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

/// Filter applied to a resource subscription at the server/watcher level.
/// Labels and fields are pushed to the K8s API (server-side filtering).
/// OwnerUid is applied as a post-filter on the server before sending to the client
/// (K8s API doesn't support filtering by ownerReference).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubscriptionFilter {
    /// Label selector: only return resources matching all key=value pairs.
    Labels(BTreeMap<String, String>),
    /// Field selector string: e.g., "spec.nodeName=node01".
    Field(String),
    /// Owner UID: only return resources owned by this UID (post-filtered server-side).
    OwnerUid(String),
}

impl SubscriptionFilter {
    /// Convert a label map to the K8s API label selector string format.
    pub fn labels_to_selector(labels: &BTreeMap<String, String>) -> String {
        labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",")
    }
}

// ---------------------------------------------------------------------------
// Substream-specific wire types
// ---------------------------------------------------------------------------
//
// With yamux, each subscription and each log view gets its own substream.
// The control substream still uses `SessionCommand`/`SessionEvent` (below)
// for one-shot request/response commands. The types here are the per-substream
// grammars — each substream speaks one of these mini-protocols, never the
// full session enum.

/// First message on any data substream (subscription or log). The daemon
/// reads this to determine what kind of bridge to spawn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubstreamInit {
    /// Subscribe to a resource type's rows.
    Subscribe(SubscriptionInit),
    /// Start streaming logs for a pod/container.
    Log(LogInit),
}

/// Subscribe handshake payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionInit {
    pub resource: ResourceId,
    pub namespace: Namespace,
    pub filter: Option<SubscriptionFilter>,
}

/// Events streamed by the daemon on a **subscription substream**. The
/// substream carries only events for the subscription that opened it; no
/// routing tag is needed because the transport layer (yamux) already
/// isolates the bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamEvent {
    /// Fresh rows for the subscribed resource.
    Snapshot(crate::event::ResourceUpdate),
    /// The subscription failed — no further events will arrive on this
    /// substream. The daemon closes the stream after sending this.
    Error(String),
    /// Server-declared capabilities for the subscribed resource type.
    /// Sent once, right after the initial snapshot.
    Capabilities(ResourceCapabilities),
    /// The server resolved an unknown resource (empty group/version) to
    /// its real identity. The TUI should update its nav/table keys.
    Resolved {
        original: ResourceId,
        resolved: ResourceId,
    },
}

/// First (and only) message the TUI writes to a **log substream**. Each
/// subsequent frame on the substream is a single log line (`String`,
/// bincode-framed). EOF from the daemon = log stream ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogInit {
    pub pod: String,
    pub namespace: Namespace,
    pub container: String,
    pub follow: bool,
    pub tail: Option<u64>,
    pub since: Option<String>,
    pub previous: bool,
}

// ---------------------------------------------------------------------------
// Control-substream wire types (existing SessionCommand / SessionEvent,
// narrowed to one-shot request/response commands + global events)
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

    // --- Session-level commands ---

    SwitchNamespace { namespace: Namespace },

    // --- Resource operations (target identified by ObjectRef) ---

    Describe(ObjectRef),
    Yaml(ObjectRef),
    Delete(ObjectRef),
    Scale { target: ObjectRef, replicas: u32 },
    Restart(ObjectRef),
    // StreamLogs and StopLogs are gone — logs now flow on yamux substreams.
    // The TUI opens a log substream with LogInit, reads lines, drops to stop.
    GetDiscovery,
    /// Apply edited YAML to a resource. The unified edit flow goes
    /// `Yaml(target)` → user edits in `$EDITOR` → `Apply(target, new_yaml)`.
    /// The server routes by `target.resource.is_local()`: K8s resources go
    /// through kube-rs server-side apply; local resources dispatch through
    /// `LocalResourceSource::apply_yaml`. The same wire command works for
    /// both — there is no per-kind branching on the client.
    Apply { target: ObjectRef, yaml: String },
    /// Decode a Secret's data and return it as a describe-style view.
    /// Carries the full `ObjectRef` rather than `(name, namespace)` so the
    /// daemon (and any future filtering layer) gets the typed rid for free.
    DecodeSecret(ObjectRef),
    /// Manually trigger a CronJob (creates a one-shot Job from its template).
    TriggerCronJob(ObjectRef),
    /// Toggle the suspend state of a CronJob. The server reads the current
    /// state from K8s and flips it — the client doesn't need to know.
    /// Toggle a CronJob's `spec.suspend` flag (server reads + flips).
    ToggleSuspendCronJob(ObjectRef),

    // --- Port-forwarding ---

    /// Create a new port-forward. The server delegates to the shared
    /// `PortForwardSource`; state transitions flow through the regular
    /// Subscribe/Snapshot pipeline for the port-forward table.
    PortForward { target: ObjectRef, local_port: u16, container_port: u16 },

    // --- Daemon management (one-shot, no session needed) ---

    Ping,
    Status,
    Shutdown,
    Clear { context: Option<String> },
}

// ---------------------------------------------------------------------------
// Unified event type: Daemon -> Client (bincode)
// ---------------------------------------------------------------------------

/// Events from daemon to TUI on the **control substream**. One-shot
/// responses (CommandResult, DescribeResult, YamlResult) and global
/// events (Discovery, PodMetrics, etc.). Subscription-specific events
/// (Snapshot, Capabilities, Resolved, SubscriptionError) are no longer
/// here — they flow on per-subscription yamux substreams as `StreamEvent`.
#[derive(Debug, Serialize, Deserialize)]
pub enum SessionEvent {
    // --- Session lifecycle ---

    Ready {
        context: String,
        cluster: String,
        user: String,
        namespaces: Vec<String>,
    },
    SessionError(String),

    // --- One-shot command responses ---

    DescribeResult(String),
    YamlResult(String),
    CommandResult { ok: bool, message: String },

    // --- Global events ---

    Discovery {
        context: String,
        namespaces: Vec<String>,
        crds: Vec<CachedCrd>,
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
    use crate::kube::resources::row::ResourceRow;

    #[test]
    fn test_resource_row_bincode_roundtrip() {
        let row = ResourceRow {
            cells: vec!["default".into(), "test-pod".into(), "1/1".into(), "Running".into()],
            name: "test-pod".into(),
            namespace: Some("default".into()),
            containers: Vec::new(),
            owner_refs: Vec::new(),
            pf_ports: Vec::new(),
            node: None,
            crd_info: None,
            drill_target: None,
        };
        let bytes = bincode::serialize(&row).unwrap();
        let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.name, "test-pod");
        assert_eq!(decoded.cells.len(), 4);
    }

    #[test]
    fn test_resource_update_rows_bincode_roundtrip() {
        let rid = ResourceId::new("", "v1", "Pod", "pods", ResourceScope::Namespaced);
        let row = ResourceRow {
            cells: vec!["default".into(), "test".into()],
            name: "test".into(),
            namespace: Some("default".into()),
            containers: Vec::new(),
            owner_refs: Vec::new(),
            pf_ports: Vec::new(),
            node: None,
            crd_info: None,
            drill_target: None,
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
                namespace: Some("prod".into()),
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                crd_info: None,
                drill_target: None,
            }],
        };
        // StreamEvent (substream wire type) bincode roundtrip.
        let stream_event = StreamEvent::Snapshot(update);
        let bytes = bincode::serialize(&stream_event).unwrap();
        let decoded: StreamEvent = bincode::deserialize(&bytes).unwrap();
        match decoded {
            StreamEvent::Snapshot(ResourceUpdate::Rows { rows, .. }) => {
                assert_eq!(rows[0].name, "web");
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_subscription_init_bincode_roundtrip() {
        let rid = ResourceId::new("", "v1", "Pod", "pods", ResourceScope::Namespaced);
        let init = SubscriptionInit { resource: rid, namespace: Namespace::Named("default".into()), filter: None };
        let bytes = bincode::serialize(&init).unwrap();
        let decoded: SubscriptionInit = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.resource.plural, "pods");
    }
}
