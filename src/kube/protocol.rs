//! Wire protocol types for k9rs daemon communication.
//!
//! One unified binary protocol (length-prefixed bincode) for ALL daemon
//! communication — both TUI sessions and management commands (k9rs ctl).
//! No JSON on the wire.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

use crate::kube::local::LocalResourceKind;
use crate::kube::resource_def::BuiltInKind;
use super::cache::CachedCrd;

/// A kubeconfig context name (e.g. "prod-us-west", "minikube"). Typed
/// so you cannot accidentally substitute a namespace, cluster name, user
/// name, or any other free-form string where a context is expected.
///
/// `Arc<str>` inside because context names are read once at kubeconfig
/// load and then cloned into many long-lived places (app state, each
/// session, per-port-forward state, registry keys); ref-counted sharing
/// avoids allocating the same name over and over.
///
/// `#[serde(transparent)]` — wire encoding is byte-identical to a bare
/// `String`, so swapping `context: String` → `context: ContextName` in
/// protocol types is wire-compatible.
///
/// `Borrow<str>` is implemented so `HashMap<ContextName, V>::get(&str)`
/// and `DashMap<ContextName, V>::get(&str)` both work without cloning.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContextName(Arc<str>);

impl ContextName {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for ContextName {
    fn default() -> Self {
        Self(Arc::from(""))
    }
}

impl From<String> for ContextName {
    fn from(s: String) -> Self { Self(Arc::from(s)) }
}

impl From<&str> for ContextName {
    fn from(s: &str) -> Self { Self(Arc::from(s)) }
}

impl std::borrow::Borrow<str> for ContextName {
    fn borrow(&self) -> &str { &self.0 }
}

impl AsRef<str> for ContextName {
    fn as_ref(&self) -> &str { &self.0 }
}

impl std::fmt::Display for ContextName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for ContextName {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

/// Sentinel group for daemon-owned "local" resources (port-forwards, saved
/// queries, etc.). Distinct from every real K8s API group so identity checks
/// are unambiguous. See `crate::kube::local` for the abstraction.
pub const LOCAL_GROUP: &str = "k9rs.local";

/// Identity of a resource type. Three disjoint variants — every callsite
/// can branch on `match` and the compiler enforces exhaustiveness.
///
/// - [`ResourceId::BuiltIn`]: a closed-set K8s resource the daemon ships
///   with (Pod, Deployment, etc.). Carries only the typed [`BuiltInKind`]
///   discriminant; group/version/kind/plural/scope are fetched on demand
///   from the registry's `&'static Gvr`. No allocation.
/// - [`ResourceId::Crd`]: a runtime-discovered CRD. Carries the GVR strings
///   in a [`CrdRef`] because CRDs are not statically known.
/// - [`ResourceId::Local`]: a daemon-owned local resource (port-forwards,
///   etc.). Like built-ins, carries only the typed [`LocalResourceKind`]
///   discriminant.
///
/// Equality / hashing semantics: identity is `(group, version, plural)` for
/// CRDs (matching the previous struct semantics for wire compat), and the
/// typed kind for built-ins and locals. Distinct variants never compare
/// equal, even if a CRD's strings happen to match a built-in.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceId {
    /// Closed-set, statically-known K8s resource. Carries only the typed
    /// kind — the full GVR is looked up from the registry.
    BuiltIn(BuiltInKind),
    /// Runtime-discovered CRD with raw GVR strings.
    Crd(CrdRef),
    /// Daemon-owned local resource (port-forwards, etc.).
    Local(LocalResourceKind),
}

/// GVR payload for CRDs (resources that aren't in the closed [`BuiltInKind`]
/// enum). Identity (Hash/Eq) is `(group, version, plural)` only — `kind`
/// and `scope` are display/metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdRef {
    /// API group (e.g. "clickhouse.altinity.com")
    pub group: String,
    /// API version (e.g. "v1", "v1beta1")
    pub version: String,
    /// K8s kind (e.g. "ClickHouseInstallation") — display, not identity
    pub kind: String,
    /// Plural name used in API URLs — part of identity
    pub plural: String,
    /// Cluster vs namespace scope — display/runtime, not identity
    pub scope: ResourceScope,
}

impl PartialEq for CrdRef {
    fn eq(&self, other: &Self) -> bool {
        self.group == other.group && self.version == other.version && self.plural == other.plural
    }
}

impl Eq for CrdRef {}

impl std::hash::Hash for CrdRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.group.hash(state);
        self.version.hash(state);
        self.plural.hash(state);
    }
}

impl CrdRef {
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

    /// Build a "please resolve me" placeholder CrdRef for cases where the
    /// client only knows the plural / kind and the daemon needs to fill in
    /// group + version via discovery (e.g. user types `:nodeclaims`,
    /// owner-chain drill-down where the owner kind isn't in the registry).
    /// The daemon's `api_resource_for` recognizes this shape via
    /// [`Self::is_unresolved`] and walks discovery.
    pub fn unresolved(plural: impl Into<String>) -> Self {
        let plural = plural.into();
        Self {
            group: String::new(),
            version: String::new(),
            kind: plural.clone(),
            plural,
            scope: ResourceScope::Namespaced,
        }
    }

    /// True if this is an unresolved placeholder produced by
    /// [`Self::unresolved`]. Replaces the open-coded
    /// `crd_ref.group.is_empty() && crd_ref.version.is_empty()` checks
    /// at every dispatch site.
    pub fn is_unresolved(&self) -> bool {
        self.group.is_empty() && self.version.is_empty()
    }
}

impl From<BuiltInKind> for ResourceId {
    fn from(k: BuiltInKind) -> Self { ResourceId::BuiltIn(k) }
}

impl From<LocalResourceKind> for ResourceId {
    fn from(k: LocalResourceKind) -> Self { ResourceId::Local(k) }
}

impl From<CrdRef> for ResourceId {
    fn from(r: CrdRef) -> Self { ResourceId::Crd(r) }
}

/// A borrowed view over a resource's identity metadata. Built once per
/// [`ResourceId::identity`] call, with fields borrowing either `&'static`
/// data (for built-ins and locals — both of which point into `const`
/// tables) or the receiver itself (for CRDs, which own their strings).
///
/// The per-variant dispatch on `ResourceId` lives in [`ResourceId::identity`]
/// — exactly ONE place. Every accessor (`group`, `plural`, `scope`, …)
/// is a one-liner over the returned `IdentityData`, so adding a new
/// accessor means adding a field here and three lines in `identity()`
/// (once), not touching six accessors. Adding a new `ResourceId` variant
/// means one new arm in `identity()`, not six.
pub struct IdentityData<'a> {
    pub group: &'a str,
    pub version: &'a str,
    pub kind_str: &'a str,
    pub plural: &'a str,
    pub short_label: &'a str,
    pub scope: ResourceScope,
}

impl ResourceId {
    /// Construct a CRD-backed ResourceId from raw GVR strings. Used by the
    /// daemon's discovery / resolve paths and by tests.
    pub fn crd(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        plural: impl Into<String>,
        scope: ResourceScope,
    ) -> Self {
        ResourceId::Crd(CrdRef::new(group, version, kind, plural, scope))
    }

    /// Look up a resource by any alias (e.g., "po", "deploy", "svc", "namespaces", "pf").
    /// Consults the trait-based REGISTRY and the local table.
    pub fn from_alias(alias: &str) -> Option<Self> {
        if let Some(def) = crate::kube::resource_defs::REGISTRY.by_alias(alias) {
            return Some(ResourceId::BuiltIn(def.kind()));
        }
        if let Some(kind) = crate::kube::local::find_by_alias(alias) {
            return Some(ResourceId::Local(kind));
        }
        None
    }

    // -- Variant predicates ---------------------------------------------------

    /// True if this is a daemon-owned local resource.
    pub fn is_local(&self) -> bool {
        matches!(self, ResourceId::Local(_))
    }

    /// True if this is a runtime-discovered CRD.
    pub fn is_crd(&self) -> bool {
        matches!(self, ResourceId::Crd(_))
    }

    /// Extract the typed [`BuiltInKind`] if this is a built-in. Useful for
    /// dispatching through the registry without re-parsing strings.
    pub fn built_in_kind(&self) -> Option<BuiltInKind> {
        if let ResourceId::BuiltIn(k) = self { Some(*k) } else { None }
    }

    // -- Identity --------------------------------------------------------

    /// Build a borrowed view over this resource's identity metadata. This
    /// is the single place where `ResourceId`'s variant is resolved to
    /// backing metadata; all six accessors below are one-liners over the
    /// returned view. Adding a new accessor or variant is a localized
    /// edit, not a 6×3 grid refactor.
    pub fn identity(&self) -> IdentityData<'_> {
        match self {
            ResourceId::BuiltIn(k) => {
                let def = crate::kube::resource_defs::REGISTRY.by_kind(*k);
                let g = def.gvr();
                IdentityData {
                    group: g.group,
                    version: g.version,
                    kind_str: g.kind,
                    plural: g.plural,
                    short_label: def.short_label(),
                    scope: g.scope,
                }
            }
            ResourceId::Crd(r) => IdentityData {
                group: &r.group,
                version: &r.version,
                kind_str: &r.kind,
                plural: &r.plural,
                // CRDs have no def-defined short label; fall back to kind.
                short_label: &r.kind,
                scope: r.scope,
            },
            ResourceId::Local(k) => IdentityData {
                group: LOCAL_GROUP,
                version: k.version(),
                kind_str: k.kind_str(),
                plural: k.plural(),
                short_label: k.short_label(),
                scope: k.scope(),
            },
        }
    }

    // -- Accessors (thin wrappers over identity()) ------------------------

    pub fn group(&self) -> &str { self.identity().group }
    pub fn version(&self) -> &str { self.identity().version }

    /// The K8s "kind" string (e.g. "Pod", "Deployment"). Named `kind_str`
    /// to avoid colliding with the typed `BuiltInKind` accessor.
    pub fn kind_str(&self) -> &str { self.identity().kind_str }

    /// The plural name used in API URLs and as the wire-format identifier
    /// (e.g. "pods", "deployments").
    pub fn plural(&self) -> &str { self.identity().plural }

    pub fn scope(&self) -> ResourceScope { self.identity().scope }

    /// Display label (the kind name). Same as `kind_str` — kept for
    /// readability at callsites that mean "show this to the user".
    pub fn display_label(&self) -> &str { self.kind_str() }

    /// Short UI label for tab bar/breadcrumbs (e.g., "Deploy", "STS", "PF").
    pub fn short_label(&self) -> &str { self.identity().short_label }

    /// Whether this resource is cluster-scoped.
    pub fn is_cluster_scoped(&self) -> bool {
        self.scope() == ResourceScope::Cluster
    }

    /// Build the capability manifest for this resource type. The TUI uses
    /// this to gate keys and render action menus. CRDs fall back to the
    /// always-on trio: Describe, YAML, Delete.
    ///
    /// Operations still branch on the variant (not derived from `IdentityData`)
    /// because the computation is fundamentally different per source:
    /// built-ins read flags from the registry's `ResourceDef`, locals call
    /// `LocalResourceKind::operations()`, CRDs get the static trio.
    pub fn capabilities(&self) -> ResourceCapabilities {
        let ops: Vec<OperationKind> = match self {
            ResourceId::BuiltIn(k) => {
                crate::kube::resource_defs::REGISTRY.by_kind(*k).operations()
            }
            ResourceId::Local(k) => k.operations(),
            ResourceId::Crd(_) => {
                vec![OperationKind::Describe, OperationKind::Yaml, OperationKind::Delete]
            }
        };
        ResourceCapabilities { operations: ops }
    }
}

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group = self.group();
        let plural = self.plural();
        if group.is_empty() {
            write!(f, "{}", plural)
        } else {
            write!(f, "{}.{}", plural, group)
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
    /// Parse the K8s API spec.scope string ("Cluster" / "Namespaced") into
    /// the typed enum. Anything other than "Cluster" defaults to
    /// `Namespaced` because that's the K8s default for CRDs that don't
    /// explicitly set a scope. Centralized so the boundary parse lives in
    /// one place — duplicating the match arms across crds.rs and
    /// streaming.rs let one drift away from the other.
    pub fn from_k8s_spec(scope: &str) -> Self {
        match scope {
            "Cluster" => ResourceScope::Cluster,
            _ => ResourceScope::Namespaced,
        }
    }

    /// The canonical K8s spec.scope label for this variant. Used when
    /// rendering the CRD table column.
    pub fn k8s_label(self) -> &'static str {
        match self {
            ResourceScope::Cluster => "Cluster",
            ResourceScope::Namespaced => "Namespaced",
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

    /// Construct from a user command-mode input string. The TUI accepts
    /// `:ns all` and `:ns ""` as "all namespaces"; any other value is a
    /// specific namespace name. This constructor is the single source of
    /// truth for user-input → typed conversion — the previous
    /// `From<&str>` impl baked the same semantic but its name didn't
    /// document the user-input context, leading callsites that handle
    /// row data to reach for it and silently coerce empty strings to
    /// `All` (the audit's "footgun" finding).
    pub fn from_user_command(s: &str) -> Self {
        match s {
            "all" | "" => Namespace::All,
            other => Namespace::Named(other.to_string()),
        }
    }

    /// Construct from a row's namespace field. K8s reports the empty
    /// string for cluster-scoped resources; for namespaced resources it's
    /// always populated. Empty maps to `All` here (so `as_option()` will
    /// return `None` and the kubectl call omits `-n`), but mutating
    /// handlers like force-kill explicitly refuse `Namespace::All` to
    /// guard against data-corruption cases where a namespaced row
    /// somehow lost its namespace string.
    pub fn from_row(s: &str) -> Self {
        if s.is_empty() {
            Namespace::All
        } else {
            Namespace::Named(s.to_string())
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
/// Kubeconfig `(cluster, user)` pair — the two labels the TUI displays
/// alongside the context name in its header / overview, and the server
/// echoes back in [`SessionEvent::Ready`] after the handshake.
///
/// Distinct from [`ContextId`]: this carries the human-readable labels
/// (kubeconfig `clusters:` entry name + `users:` entry name), while
/// [`ContextId`] carries the canonical `(server_url, user)` identity used
/// for cache sharing. Bundling the pair here DRYs up ~8 structs that used
/// to carry `cluster: String, user: String` as adjacent fields — and
/// prevents accidental positional swaps when they ride together.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    pub cluster: String,
    pub user: String,
}

impl ClusterIdentity {
    pub fn new(cluster: String, user: String) -> Self {
        Self { cluster, user }
    }
}

/// Identity (Hash/Eq) is `(server_url, user)` — the context name is
/// display-only and does NOT affect cache sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextId {
    /// The kubeconfig context name (for display only, NOT part of identity).
    pub name: ContextName,
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
    pub fn new(name: ContextName, server_url: String, user: String) -> Self {
        Self { name, server_url, user }
    }
}

impl std::fmt::Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name.as_str())
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

    /// Render this object as the positional argument `kubectl` expects for
    /// subcommands like `port-forward` — `"pods/foo"`, `"services/bar"`, etc.
    /// Uses the plural from the unified `identity()` view so there's no
    /// tripartite match — one lookup, same plural `&'static str` for
    /// built-ins/locals, borrow for CRDs.
    pub fn kubectl_target(&self) -> String {
        format!("{}/{}", self.resource.plural(), self.name)
    }
}

/// Key identifying a Kubernetes object by its namespace + name.
/// Used as a map key for metrics, delta tracking, etc.
///
/// The `namespace` field is a `String` rather than [`Namespace`] because
/// this is a *location* — "which namespace does this object actually live
/// in" — not a *selection* (the all-vs-named distinction [`Namespace`]
/// encodes). Cluster-scoped objects use `""`; namespaced objects use
/// their actual namespace name.
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

/// Typed key for node-name-keyed maps (e.g. node metrics). Prevents
/// accidentally looking up a pod name, resource plural, or any other
/// string where a node name is expected — `NodeMetrics` is declared
/// as `HashMap<NodeName, MetricsUsage>` and nothing but a `NodeName`
/// (or a `&str` via the `Borrow<str>` impl for lookup) can index in.
///
/// `#[serde(transparent)]` keeps the wire encoding byte-identical to
/// the pre-newtype `HashMap<String, MetricsUsage>` shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeName(String);

impl NodeName {
    pub fn as_str(&self) -> &str { &self.0 }
}

impl From<String> for NodeName {
    fn from(s: String) -> Self { Self(s) }
}

impl From<&str> for NodeName {
    fn from(s: &str) -> Self { Self(s.to_owned()) }
}

impl std::borrow::Borrow<str> for NodeName {
    fn borrow(&self) -> &str { &self.0 }
}

impl std::fmt::Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// CPU and memory usage from the metrics-server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsUsage {
    pub cpu: String,
    pub mem: String,
    /// Raw CPU usage in millicores (for percentage computations).
    #[serde(default)]
    pub cpu_milli: u64,
    /// Raw memory usage in bytes (for percentage computations).
    #[serde(default)]
    pub mem_bytes: u64,
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

    // Node-specific.
    NodeShell,

    // Special-purpose K8s resources.
    DecodeSecret,
    TriggerCronJob,
    ToggleSuspendCronJob,
}

/// Single source of truth for client-side form-field name strings.
/// Form construction (`session_handlers::build_*_form`) and submit
/// dispatch (`session_commands::handle_form_submit`) both reach for these
/// — drift becomes a compile error rather than a silent "field not found"
/// lookup miss at submit time. Not part of the wire protocol; the daemon
/// never sees field names.
pub mod form_field_name {
    pub const REPLICAS: &str = "replicas";
    pub const CONTAINER_PORT: &str = "container_port";
    pub const LOCAL_PORT: &str = "local_port";
}

/// The full set of operations a resource type supports. Sent from the server
/// to the TUI after a successful subscription. Empty `operations` means the
/// resource is read-only.
///
/// We send the bare list of [`OperationKind`] discriminants over the wire —
/// labels, input schemas, and confirm-gates are all derived client-side
/// from the discriminant (the TUI knows what "Scale" means; the daemon
/// doesn't need to spell it out).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceCapabilities {
    pub operations: Vec<OperationKind>,
}

impl ResourceCapabilities {
    /// True if the given operation is declared.
    pub fn supports(&self, kind: OperationKind) -> bool {
        self.operations.contains(&kind)
    }
}

/// Maximum bincode message size on the daemon socket. The largest
/// legitimate message is a `ResourceUpdate::Rows` snapshot for a busy
/// cluster — even 5000 pods × 10 containers each lands well under 8 MiB.
/// The cap is set to 16 MiB to give comfortable headroom while still
/// rejecting outrageous allocations from a corrupted frame.
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

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
    /// Open an interactive exec session into a pod container.
    Exec(ExecInit),
}

/// Subscribe handshake payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionInit {
    pub resource: ResourceId,
    pub namespace: Namespace,
    pub filter: Option<SubscriptionFilter>,
    /// If true, invalidate any cached watcher and start a fresh LIST.
    /// Used by Ctrl-R refresh to guarantee fresh data from the API server.
    #[serde(default)]
    pub force: bool,
}

/// Events streamed by the daemon on a **subscription substream**. The
/// substream carries only events for the subscription that opened it; no
/// routing tag is needed because the transport layer (yamux) already
/// isolates the bytes.
///
/// Capabilities used to be sent as a dedicated `Capabilities(ResourceCapabilities)`
/// variant right after the initial snapshot. They're gone now —
/// `ResourceId::capabilities()` computes the same manifest on both sides
/// from the typed kind, and the wire round-trip + client-side cache was
/// pure duplication that introduced a three-map rekey bug every time a
/// CRD resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamEvent {
    /// Fresh rows for the subscribed resource.
    Snapshot(crate::event::ResourceUpdate),
    /// The subscription failed — no further events will arrive on this
    /// substream. The daemon closes the stream after sending this.
    Error(String),
    /// The server resolved an unknown resource (empty group/version) to
    /// its real identity. The TUI should update its nav/table keys.
    Resolved {
        original: ResourceId,
        resolved: ResourceId,
    },
}

/// Which container(s) a log stream subscribes to. Replaces the previous
/// magic-string `"all"` sentinel — the closed enum makes the daemon's
/// kubectl-arg construction exhaustive and protects against typos.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogContainer {
    /// Stream every container in the pod (`kubectl logs --all-containers`).
    All,
    /// Stream a specific container (`kubectl logs -c <name>`).
    Named(String),
    /// Let kubectl pick the default container (omit `-c`).
    Default,
}

/// Handshake for an exec substream. The daemon spawns `kubectl` with the
/// provided args in a PTY and bridges terminal bytes over yamux. Covers
/// both `kubectl exec` (pod shell) and `kubectl debug` (node shell).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecInit {
    /// Full kubectl argument list (everything after `kubectl`).
    /// The daemon prepends `--context` from the session's active context.
    pub kubectl_args: Vec<String>,
    pub term_width: u16,
    pub term_height: u16,
}

/// Frames exchanged on an exec substream after the handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecFrame {
    /// Raw terminal bytes (bidirectional).
    Data(Vec<u8>),
    /// Terminal resize event (TUI → daemon only).
    Resize { width: u16, height: u16 },
}

/// First (and only) message the TUI writes to a **log substream**. Each
/// subsequent frame on the substream is a single log line (`String`,
/// bincode-framed). EOF from the daemon = log stream ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogInit {
    pub pod: String,
    pub namespace: Namespace,
    pub container: LogContainer,
    pub follow: bool,
    pub tail: Option<u64>,
    pub since: Option<String>,
    pub previous: bool,
}

// ---------------------------------------------------------------------------
// Control-substream wire types (existing SessionCommand / SessionEvent,
// narrowed to one-shot request/response commands + global events)
// ---------------------------------------------------------------------------

/// Protocol version for the TUI↔daemon wire format. Bump this whenever
/// `ResourceRow`, `SessionCommand`, `SessionEvent`, or any serialized type
/// changes in a bincode-incompatible way (new fields, reordering, etc.).
/// The daemon rejects Init commands with a mismatched version so stale
/// daemons fail fast instead of producing silent data corruption.
pub const PROTOCOL_VERSION: u32 = 1;

/// All commands from any client (TUI session or management CLI).
/// The first command on a connection determines the connection type:
/// - `Init` → long-lived TUI session
/// - `Ping`/`Status`/`Shutdown`/`Clear` → one-shot management request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionCommand {
    // --- Session lifecycle ---

    /// Start a TUI session with raw kubeconfig + environment variables.
    Init {
        /// Protocol version — daemon rejects mismatches to prevent silent
        /// data corruption from stale binaries.
        #[serde(default)]
        protocol_version: u32,
        context: Option<ContextName>,
        namespace: Namespace,
        readonly: bool,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
        identity: ClusterIdentity,
    },

    // --- Resource operations (target identified by ObjectRef) ---
    //
    // (There used to be a `SwitchNamespace` session-level command here;
    // it was deleted because every subscription carries its own namespace
    // in `SubscriptionInit`, so the server never needed session-level
    // namespace state.)

    Describe(ObjectRef),
    Yaml(ObjectRef),
    Delete(ObjectRef),
    /// Force-delete a pod immediately (`grace_period_seconds: 0`,
    /// background propagation). Pod-only — the daemon refuses anything
    /// else. Replaces the old client-side `kubectl delete --force`
    /// shell-out so the daemon can enforce RBAC, run without a `kubectl`
    /// binary on the client host, and surface structured errors.
    ForceKill(ObjectRef),
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
    Clear { context: Option<ContextName> },
}

impl SessionCommand {
    /// True if this command mutates cluster or daemon state and must be
    /// refused when the session is read-only. Exhaustive match so that
    /// adding a new command variant is a compile error in this method —
    /// the classification can't drift from the wire type, and no handler
    /// has to remember to call a readonly gate.
    ///
    /// `DecodeSecret` is NOT classified as mutating: it reads an existing
    /// Secret (same auth as a regular `Describe`) and renders the decoded
    /// values. Feel free to treat it as sensitive at the UI layer, but the
    /// server-side readonly gate protects against *writes*, and decoding
    /// is a read.
    pub fn is_mutating(&self) -> bool {
        match self {
            // Lifecycle / reads
            SessionCommand::Init { .. }
            | SessionCommand::Describe(_)
            | SessionCommand::Yaml(_)
            | SessionCommand::DecodeSecret(_)
            | SessionCommand::GetDiscovery
            | SessionCommand::Ping
            | SessionCommand::Status => false,

            // Cluster/daemon mutations
            SessionCommand::Delete(_)
            | SessionCommand::ForceKill(_)
            | SessionCommand::Apply { .. }
            | SessionCommand::Scale { .. }
            | SessionCommand::Restart(_)
            | SessionCommand::TriggerCronJob(_)
            | SessionCommand::ToggleSuspendCronJob(_)
            | SessionCommand::PortForward { .. } => true,

            // Management (one-shot, don't arrive on session connections —
            // dispatched by the daemon's accept loop, not `handle_command`).
            SessionCommand::Shutdown | SessionCommand::Clear { .. } => true,
        }
    }
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
        context: ContextName,
        identity: ClusterIdentity,
        namespaces: Vec<String>,
    },
    SessionError(String),

    // --- One-shot command responses ---
    //
    // Each response carries the originating `ObjectRef` so the TUI can
    // gate apply on a target match. Rapid navigation (A→B) used to let
    // A's slower fetch arrive while the route was already B, writing A's
    // YAML to B's temp file in the edit flow. Same shape as the LogLine
    // generation fix but uses the typed target for correlation.

    DescribeResult { target: ObjectRef, content: String },
    YamlResult { target: ObjectRef, content: String },
    /// Result of a mutating operation. The message is carried in the Ok/Err
    /// variant directly so the receiver can't access the message without
    /// branching on success/failure — the old `{ ok: bool, message: String }`
    /// shape let callers read the message with unchecked ok, trusting
    /// convention instead of types.
    CommandResult(Result<String, String>),

    // --- Global events ---

    Discovery {
        context: ContextName,
        namespaces: Vec<String>,
        crds: Vec<CachedCrd>,
    },
    PodMetrics(HashMap<ObjectKey, MetricsUsage>),
    NodeMetrics(HashMap<NodeName, MetricsUsage>),

    // --- Management responses ---

    DaemonStatus(DaemonStatus),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ResourceUpdate;
    use crate::kube::resources::row::{ResourceRow, RowHealth};

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
            health: RowHealth::Normal,
            crd_info: None,
            drill_target: None,
            ..Default::default()
        };
        let bytes = bincode::serialize(&row).unwrap();
        let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.name, "test-pod");
        assert_eq!(decoded.cells.len(), 4);
        assert_eq!(decoded.cpu_request, None);
        assert_eq!(decoded.cpu_limit, None);
        assert_eq!(decoded.mem_request, None);
        assert_eq!(decoded.mem_limit, None);
    }

    /// Verify that resource-request/limit fields survive a bincode
    /// roundtrip. Bincode is positional and ignores `#[serde(default)]`
    /// / `skip_serializing_if` — these fields are always on the wire.
    #[test]
    fn test_resource_row_bincode_roundtrip_with_metrics_fields() {
        let row = ResourceRow {
            cells: vec!["default".into(), "busy-pod".into()],
            name: "busy-pod".into(),
            namespace: Some("default".into()),
            cpu_request: Some(500),
            cpu_limit: Some(1000),
            mem_request: Some(128 * 1024 * 1024),
            mem_limit: Some(256 * 1024 * 1024),
            ..Default::default()
        };
        let bytes = bincode::serialize(&row).unwrap();
        let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.cpu_request, Some(500));
        assert_eq!(decoded.cpu_limit, Some(1000));
        assert_eq!(decoded.mem_request, Some(128 * 1024 * 1024));
        assert_eq!(decoded.mem_limit, Some(256 * 1024 * 1024));
    }

    #[test]
    fn test_resource_update_rows_bincode_roundtrip() {
        let rid = ResourceId::BuiltIn(BuiltInKind::Pod);
        let row = ResourceRow {
            cells: vec!["default".into(), "test".into()],
            name: "test".into(),
            namespace: Some("default".into()),
            containers: Vec::new(),
            owner_refs: Vec::new(),
            pf_ports: Vec::new(),
            node: None,
            health: RowHealth::Normal,
            crd_info: None,
            drill_target: None,
            ..Default::default()
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
                assert_eq!(resource, ResourceId::BuiltIn(BuiltInKind::Pod));
                assert_eq!(resource.plural(), "pods");
                assert_eq!(headers.len(), 2);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_session_event_snapshot_bincode_roundtrip() {
        let rid = ResourceId::BuiltIn(BuiltInKind::Deployment);
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
                health: RowHealth::Normal,
                crd_info: None,
                drill_target: None,
                ..Default::default()
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
        let rid = ResourceId::BuiltIn(BuiltInKind::Pod);
        let init = SubscriptionInit { resource: rid, namespace: Namespace::Named("default".into()), filter: None, force: false };
        let bytes = bincode::serialize(&init).unwrap();
        let decoded: SubscriptionInit = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.resource, ResourceId::BuiltIn(BuiltInKind::Pod));
        assert_eq!(decoded.resource.plural(), "pods");
    }

    #[test]
    fn test_resource_id_crd_roundtrip() {
        let rid = ResourceId::crd(
            "clickhouse.altinity.com", "v1", "ClickHouseInstallation",
            "clickhouseinstallations", ResourceScope::Namespaced,
        );
        let bytes = bincode::serialize(&rid).unwrap();
        let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, rid);
        assert_eq!(decoded.plural(), "clickhouseinstallations");
        assert!(decoded.is_crd());
    }

    #[test]
    fn test_resource_id_local_roundtrip() {
        let rid = ResourceId::Local(LocalResourceKind::PortForward);
        let bytes = bincode::serialize(&rid).unwrap();
        let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, rid);
        assert!(decoded.is_local());
        assert_eq!(decoded.plural(), "portforwards");
    }

    #[test]
    fn test_resource_id_distinct_variants_never_equal() {
        let built_in = ResourceId::BuiltIn(BuiltInKind::Pod);
        let crd = ResourceId::crd("", "v1", "Pod", "pods", ResourceScope::Namespaced);
        // Even though the CRD's strings happen to match Pod's GVR, the
        // tagged variants are distinct types — they must never compare equal.
        assert_ne!(built_in, crd);
    }
}
