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
/// - [`ResourceId::Crd`]: a runtime-discovered CRD with fully resolved GVR.
///   Carries the GVR strings in a [`CrdRef`] because CRDs are not statically
///   known.
/// - [`ResourceId::CrdUnresolved`]: a CRD referenced by plural name only
///   (e.g. user typed `:nodeclaims`). The daemon resolves this to `Crd` via
///   API discovery. Compile-time distinct from `Crd` so code that expects
///   a resolved GVR cannot accidentally receive a placeholder.
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
    /// Runtime-discovered CRD with raw GVR strings. Always fully resolved
    /// (group, version, kind, plural, scope all populated).
    Crd(CrdRef),
    /// Placeholder for a CRD the user referenced by plural name (e.g.
    /// `:nodeclaims`) before discovery has filled in the full GVR. The
    /// daemon resolves this to `Crd(CrdRef)` via API discovery and sends
    /// a `StreamEvent::Resolved` back to the TUI. Compile-time distinct
    /// from `Crd` so code that expects a resolved GVR cannot accidentally
    /// receive an unresolved placeholder.
    CrdUnresolved(String),
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
/// data (for built-ins — which point into `const` tables), the enum
/// variant's owned data (for locals), or the receiver itself (for CRDs,
/// which own their strings).
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

    /// True if this is a CRD (resolved or unresolved).
    pub fn is_crd(&self) -> bool {
        matches!(self, ResourceId::Crd(_) | ResourceId::CrdUnresolved(_))
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
            ResourceId::CrdUnresolved(plural) => IdentityData {
                group: "",
                version: "",
                kind_str: plural,
                plural,
                short_label: plural,
                scope: ResourceScope::Namespaced,
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
            ResourceId::Crd(_) | ResourceId::CrdUnresolved(_) => {
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
/// [`ContextId`] carries the canonical `(server_url, credential-fingerprint)`
/// identity used for cache sharing. Bundling the pair here DRYs up ~8 structs that used
/// to carry `cluster: String, user: String` as adjacent fields — and
/// prevents accidental positional swaps when they ride together.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    pub cluster: String,
    pub user: String,
    pub k8s_version: String,
}

impl ClusterIdentity {
    pub fn new(cluster: String, user: String) -> Self {
        Self { cluster, user, k8s_version: String::new() }
    }
}

/// Identity (Hash/Eq) is `(server_url, cred_fingerprint)` — the context name
/// is display-only and does NOT affect cache sharing.
///
/// **Process-internal only.** `cred_fingerprint` is a `DefaultHasher` digest
/// whose value is not portable across binaries / Rust versions, so a
/// `ContextId` must never be persisted or sent on the wire — it is purely a
/// daemon-side cache key. Hence, deliberately, no `Serialize`/`Deserialize`.
#[derive(Debug, Clone)]
pub struct ContextId {
    /// The kubeconfig context name (for display only, NOT part of identity).
    pub name: ContextName,
    /// The API server URL (cluster identity).
    pub server_url: String,
    /// A stable hash of the *actual* credential material this connection
    /// authenticates with (token / client cert / exec spec + its injected
    /// env / impersonation / …), NOT the kubeconfig user *name*. Two configs
    /// whose `users:` entries happen to share a name on one cluster but carry
    /// different credentials hash differently and so never share a watcher —
    /// the "different credentials MUST NOT share" rule above, made real.
    /// Computed daemon-side from the resolved `AuthInfo`
    /// (`server_session::client_build::fingerprint_auth`).
    pub cred_fingerprint: u64,
}

impl PartialEq for ContextId {
    fn eq(&self, other: &Self) -> bool {
        self.server_url == other.server_url && self.cred_fingerprint == other.cred_fingerprint
    }
}

impl Eq for ContextId {}

impl std::hash::Hash for ContextId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.server_url.hash(state);
        self.cred_fingerprint.hash(state);
    }
}

impl ContextId {
    pub fn new(name: ContextName, server_url: String, cred_fingerprint: u64) -> Self {
        Self { name, server_url, cred_fingerprint }
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    /// User-defined operation from overlay config.
    Custom(String),
}

// ---------------------------------------------------------------------------
// Form schemas — declarative field definitions for operation dialogs
// ---------------------------------------------------------------------------

/// Declarative schema for the form dialog an operation needs.
/// Per-operation, not per-resource — Scale always needs a Replicas field.
pub struct FormSchema {
    pub title_template: &'static str,
    pub fields: &'static [FormFieldSchema],
}

pub struct FormFieldSchema {
    pub name: &'static str,
    pub label: &'static str,
    pub kind: FormFieldSchemaKind,
}

pub enum FormFieldSchemaKind {
    /// Integer input with bounds. Default value extracted from a column.
    Number { min: i64, max: i64, default_column: Option<&'static str> },
    /// Port number input.
    Port,
    /// Select populated from row data (e.g., container ports). Falls back
    /// to a simple input if no options available.
    DynamicSelect { fallback: DynamicSelectFallback },
}

pub enum DynamicSelectFallback {
    Port,
}

static SCALE_SCHEMA: FormSchema = FormSchema {
    title_template: "Scale: {{kind}}/{{name}}",
    fields: &[FormFieldSchema {
        name: "replicas",
        label: "Replicas",
        kind: FormFieldSchemaKind::Number {
            min: 0, max: 1_000_000,
            default_column: Some("READY"),
        },
    }],
};

static PORT_FORWARD_SCHEMA: FormSchema = FormSchema {
    title_template: "Port forward: {{kind}}/{{name}}",
    fields: &[
        FormFieldSchema {
            name: "container_port",
            label: "Container port",
            kind: FormFieldSchemaKind::DynamicSelect {
                fallback: DynamicSelectFallback::Port,
            },
        },
        FormFieldSchema {
            name: "local_port",
            label: "Local port",
            kind: FormFieldSchemaKind::Port,
        },
    ],
};

impl OperationKind {
    /// The form schema for this operation, if it needs user input.
    pub fn form_schema(&self) -> Option<&'static FormSchema> {
        match self {
            OperationKind::Scale => Some(&SCALE_SCHEMA),
            OperationKind::PortForward => Some(&PORT_FORWARD_SCHEMA),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Exec templates — declarative kubectl argument construction
// ---------------------------------------------------------------------------

/// Template for building kubectl exec arguments.
pub struct ExecTemplate {
    pub args: &'static [ExecArg],
    pub title_template: &'static str,
}

pub enum ExecArg {
    Literal(&'static str),
    Placeholder(ExecPlaceholder),
    /// Literal + placeholder pair, included only if placeholder is non-empty.
    ConditionalPair(&'static str, ExecPlaceholder),
}

#[derive(Debug, Clone, Copy)]
pub enum ExecPlaceholder {
    Namespace,
    PodName,
    Container,
    NodeName,
    NodeDebugPodName,
}

static SHELL_TEMPLATE: ExecTemplate = ExecTemplate {
    args: &[
        ExecArg::Literal("exec"), ExecArg::Literal("-it"),
        ExecArg::Literal("-n"), ExecArg::Placeholder(ExecPlaceholder::Namespace),
        ExecArg::Placeholder(ExecPlaceholder::PodName),
        ExecArg::ConditionalPair("-c", ExecPlaceholder::Container),
        // k9s pattern: start sh, try to upgrade to bash, fall back to sh.
        // Works on any container that has at least sh.
        ExecArg::Literal("--"), ExecArg::Literal("sh"), ExecArg::Literal("-c"),
        ExecArg::Literal("command -v bash >/dev/null && exec bash || exec sh"),
    ],
    title_template: "{{pod}}/{{container}}",
};

static NODE_SHELL_TEMPLATE: ExecTemplate = ExecTemplate {
    args: &[
        ExecArg::Literal("debug"),
        ExecArg::Placeholder(ExecPlaceholder::NodeName),
        ExecArg::Literal("-it"),
        ExecArg::Literal("--profile=general"),
        ExecArg::Literal("--image=busybox"),
        ExecArg::ConditionalPair("--pod-name", ExecPlaceholder::NodeDebugPodName),
    ],
    title_template: "node/{{node}}",
};

impl OperationKind {
    /// The exec template for this operation, if it launches an interactive session.
    pub fn exec_template(&self) -> Option<&'static ExecTemplate> {
        match self {
            OperationKind::Shell => Some(&SHELL_TEMPLATE),
            OperationKind::NodeShell => Some(&NODE_SHELL_TEMPLATE),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Operation descriptors — unified metadata per operation
// ---------------------------------------------------------------------------

/// Per-operation metadata: key binding, label, form schema, exec template.
pub struct OperationDescriptor {
    pub label: &'static str,
    pub default_key: Option<char>,
}

impl OperationKind {
    pub fn descriptor(&self) -> OperationDescriptor {
        match self {
            OperationKind::Describe => OperationDescriptor { label: "Describe", default_key: Some('d') },
            OperationKind::Yaml => OperationDescriptor { label: "YAML", default_key: Some('y') },
            OperationKind::Delete => OperationDescriptor { label: "Delete", default_key: None }, // Ctrl-D
            OperationKind::Restart => OperationDescriptor { label: "Restart", default_key: Some('r') },
            OperationKind::Scale => OperationDescriptor { label: "Scale", default_key: Some('s') },
            OperationKind::StreamLogs => OperationDescriptor { label: "Logs", default_key: Some('L') },
            OperationKind::PreviousLogs => OperationDescriptor { label: "Previous logs", default_key: Some('p') },
            OperationKind::PortForward => OperationDescriptor { label: "Port forward", default_key: None }, // Shift-F hardcoded
            OperationKind::Shell => OperationDescriptor { label: "Shell", default_key: Some('s') },
            OperationKind::ShowNode => OperationDescriptor { label: "Show node", default_key: Some('o') },
            OperationKind::ForceKill => OperationDescriptor { label: "Force kill", default_key: None }, // Ctrl-K
            OperationKind::NodeShell => OperationDescriptor { label: "Node shell", default_key: Some('s') },
            OperationKind::DecodeSecret => OperationDescriptor { label: "Decode", default_key: Some('x') },
            OperationKind::TriggerCronJob => OperationDescriptor { label: "Trigger", default_key: Some('t') },
            OperationKind::ToggleSuspendCronJob => OperationDescriptor { label: "Toggle suspend", default_key: Some('s') },
            OperationKind::Custom(ref _name) => OperationDescriptor { label: "Custom", default_key: None },
        }
    }

    /// Map this operation to its corresponding Action variant.
    pub fn to_action(&self) -> crate::app::actions::Action {
        use crate::app::actions::Action;
        match self {
            OperationKind::Describe => Action::Describe,
            OperationKind::Yaml => Action::Yaml,
            OperationKind::Delete => Action::Delete,
            OperationKind::Restart => Action::Restart,
            OperationKind::Scale => Action::Scale,
            OperationKind::StreamLogs => Action::Logs,
            OperationKind::PreviousLogs => Action::PreviousLogs,
            OperationKind::PortForward => Action::PortForward,
            OperationKind::Shell => Action::Shell,
            OperationKind::ShowNode => Action::ShowNode,
            OperationKind::ForceKill => Action::ForceKill,
            OperationKind::NodeShell => Action::NodeShell,
            OperationKind::DecodeSecret => Action::DecodeSecret,
            OperationKind::TriggerCronJob => Action::TriggerCronJob,
            OperationKind::ToggleSuspendCronJob => Action::SuspendCronJob,
            OperationKind::Custom(ref name) => Action::OverlayCapability(name.clone()),
        }
    }
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
/// legitimate message is a `TableBaseline` for a busy
/// cluster. 64 MiB gives headroom for very large clusters while still
/// rejecting outrageous allocations from a corrupted frame.
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
/// Stream contract: a subscription stream is one `Baseline`, then zero or
/// more `Delta`s; `Baseline` may recur (join, watcher recovery, fanout
/// lag, force-refresh) and REPLACES all prior state. No sequence numbers:
/// the substream is ordered and reliable with a single writer, so the only
/// possible loss is daemon-internal fanout lag — repaired by an in-band
/// `Baseline` on the same stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamEvent {
    /// Full replacing state (headers ride only here).
    Baseline(TableBaseline),
    /// Incremental changes since the previous frame. The resource id is
    /// implied by the substream (one subscription per substream).
    Delta(TableDelta),
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
/// Exchanged in the raw [`write_handshake`]/[`read_handshake`] preamble
/// BEFORE any framed message, so a version mismatch can never mis-parse an
/// enum — stale peers fail fast with a readable error.
pub const PROTOCOL_VERSION: u32 = 8;

/// Handshake magic — "K9RS" as a big-endian u32. Doubles as a poison
/// length: a pre-8 daemon reads these 4 bytes as a frame-length prefix of
/// ~1.26 GB, trips `MAX_MESSAGE_SIZE`, and bails cleanly instead of
/// mis-parsing; a v8 peer reading a pre-8 peer's first frame sees a small
/// length where the magic should be and reports the mismatch.
pub const PROTOCOL_MAGIC: u32 = 0x4B39_5253;

/// Write the 8-byte connection preamble: MAGIC then PROTOCOL_VERSION, both
/// big-endian (matching the hand-rolled frame-length convention). The
/// client writes first; the server replies with its own preamble.
pub async fn write_handshake<W: AsyncWriteExt + Unpin>(writer: &mut W) -> anyhow::Result<()> {
    writer.write_all(&PROTOCOL_MAGIC.to_be_bytes()).await?;
    writer.write_all(&PROTOCOL_VERSION.to_be_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Read and validate the peer's 8-byte preamble. Errors distinguish a
/// pre-handshake peer (no magic) from a version mismatch, so the caller
/// can surface an actionable message ("restart the daemon: pkill k9rs").
pub async fn read_handshake<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<()> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;
    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if magic != PROTOCOL_MAGIC {
        anyhow::bail!(
            "incompatible peer (no protocol handshake — pre-v8 binary?). \
             Restart the daemon: pkill k9rs"
        );
    }
    if version != PROTOCOL_VERSION {
        anyhow::bail!(
            "protocol version mismatch: peer speaks v{version}, this binary speaks v{}. \
             Restart the daemon: pkill k9rs",
            PROTOCOL_VERSION
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Subscription stream payloads (protocol 8)
// ---------------------------------------------------------------------------

/// Full authoritative state of one subscribed resource. Reset semantics:
/// the receiver REPLACES everything it holds for this stream. Row order is
/// unspecified — the client owns sorting. Headers ride only here (a
/// header-changing event on the daemon always produces a fresh baseline).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBaseline {
    pub resource: ResourceId,
    pub headers: Vec<String>,
    pub rows: Vec<crate::kube::resources::row::ResourceRow>,
}

/// One flush window's changes. Daemon-enforced invariant: at most one
/// change per `ObjectKey` per batch (last-writer-wins coalescing at
/// origination), so application is order-independent within a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDelta {
    pub changes: Vec<RowChange>,
}

/// A single row mutation. Application MUST be idempotent: `Upsert` is
/// insert-or-replace, `Remove` of an absent key is a no-op (the OwnerUid
/// post-filter statelessly translates filtered-out upserts into removes).
// Variant size asymmetry (full row vs key) is inherent to the wire shape;
// boxing the row would add a per-change allocation on the hot flush path
// for a transient batch value. Deliberate.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowChange {
    /// Insert-or-replace. Identity derives from the row itself
    /// (`ObjectKey::new(row.namespace, row.name)`) — no second key field.
    Upsert(crate::kube::resources::row::ResourceRow),
    /// Remove by identity.
    Remove(ObjectKey),
}

impl RowChange {
    /// The identity this change targets.
    pub fn key(&self) -> ObjectKey {
        match self {
            RowChange::Upsert(row) => ObjectKey::new(
                row.namespace.clone().unwrap_or_default(),
                row.name.clone(),
            ),
            RowChange::Remove(key) => key.clone(),
        }
    }
}

/// All commands from any client (TUI session or management CLI).
/// The first command on a connection determines the connection type:
/// - `Init` → long-lived TUI session
/// - `Ping`/`Status`/`Shutdown`/`Clear` → one-shot management request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionCommand {
    // --- Session lifecycle ---

    /// Start a TUI session with raw kubeconfig + environment variables.
    /// (Versioning lives in the connection handshake — see
    /// [`write_handshake`] — not in this message.)
    Init {
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

/// One line of a describe view, with its structural role tagged by the
/// *producer* so the UI renders by role instead of re-deriving structure from
/// the text. The daemon already owns presentation here — `text` is the
/// fully-formatted line (alignment included), exactly what used to travel as a
/// flat `String`; the only thing added is `kind`. The flat projection used for
/// search / scroll / clipboard / the TTL cache is just the `text`s joined with
/// newlines (see [`describe_lines_text`]).
///
/// Why: `format_describe` knows each line's role as it writes it, but the old
/// wire shipped a bare `String` and the UI guessed the role back via
/// `ends_with(':')` / `find(':')` — fragile (a field *value* ending in `:`
/// rendered as a section header) and a separation-of-concerns leak.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeLine {
    pub text: String,
    pub kind: DescribeLineKind,
}

/// The structural role of a [`DescribeLine`], chosen by the producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DescribeLineKind {
    /// A section header (e.g. `Spec:`, `Status:`). The whole line is emphasized.
    Section,
    /// A `key: value` field. `key_end` is the byte index in `text` of the
    /// structural colon: the renderer styles `[indent..key_end]` as the key and
    /// `[key_end..]` (colon onward) as the value, preserving leading indent.
    Field { key_end: usize },
    /// Plain text with no key/section structure (blank lines, list items,
    /// free-form continuation values).
    Plain,
}

impl DescribeLine {
    /// A section-header line.
    pub fn section(text: impl Into<String>) -> Self {
        Self { text: text.into(), kind: DescribeLineKind::Section }
    }
    /// A plain line (no structure).
    pub fn plain(text: impl Into<String>) -> Self {
        Self { text: text.into(), kind: DescribeLineKind::Plain }
    }
    /// A `key: value` field whose structural colon sits at byte `key_end`.
    pub fn field(text: impl Into<String>, key_end: usize) -> Self {
        Self { text: text.into(), kind: DescribeLineKind::Field { key_end } }
    }
}

/// Flatten typed describe lines to the plain text used for search, scroll,
/// clipboard, and the TTL cache. Inverse of splitting text into lines.
pub fn describe_lines_text(lines: &[DescribeLine]) -> String {
    lines.iter().map(|l| l.text.as_str()).collect::<Vec<_>>().join("\n")
}

/// A single streamed log line, split at the daemon. `content` is the log text;
/// `container` names the source container *only* when the line came from a
/// multiplexed `--all-containers` stream — there the daemon strips kubectl's
/// `[pod/container]` prefix and tags it here. Single-container streams leave
/// `container` `None`: there's one source, already named in the view header.
///
/// Why typed: kubectl `--all-containers` auto-enables `--prefix`, emitting
/// `[pod/container] body` on every line. The old wire shipped that raw `String`
/// and the UI re-derived the container with a first-word heuristic that rejected
/// the real `/`-bearing prefix outright — so the prefix leaked into the rendered
/// body and never colored. The daemon knows the source, so it tags it once here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub container: Option<String>,
    pub content: String,
}

impl LogLine {
    /// A line from a single-source stream — no per-line container tag.
    pub fn untagged(content: impl Into<String>) -> Self {
        Self { container: None, content: content.into() }
    }

    /// Flat text form for clipboard / file export: a tagged line regains a
    /// `[container] ` prefix so its source stays visible outside the colored
    /// UI; an untagged line is just its content, borrowed (no allocation).
    pub fn flat_text(&self) -> std::borrow::Cow<'_, str> {
        match &self.container {
            Some(c) => std::borrow::Cow::Owned(format!("[{}] {}", c, self.content)),
            None => std::borrow::Cow::Borrowed(&self.content),
        }
    }
}

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

    DescribeResult { target: ObjectRef, lines: Vec<DescribeLine> },
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
    use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

    #[test]
    fn test_resource_row_bincode_roundtrip() {
        let row = ResourceRow {
            cells: vec![
                CellValue::Text("default".into()),
                CellValue::Text("test-pod".into()),
                CellValue::Text("1/1".into()),
                CellValue::Text("Running".into()),
            ],
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
            cells: vec![
                CellValue::Text("default".into()),
                CellValue::Text("busy-pod".into()),
            ],
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
            cells: vec![
                CellValue::Text("default".into()),
                CellValue::Text("test".into()),
            ],
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
        let baseline = TableBaseline {
            resource: rid.clone(),
            headers: vec!["NAMESPACE".into(), "NAME".into()],
            rows: vec![row],
        };
        let bytes = bincode::serialize(&baseline).unwrap();
        let decoded: TableBaseline = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.resource, ResourceId::BuiltIn(BuiltInKind::Pod));
        assert_eq!(decoded.resource.plural(), "pods");
        assert_eq!(decoded.headers.len(), 2);
        assert_eq!(decoded.rows.len(), 1);
    }

    #[test]
    fn test_stream_event_baseline_delta_roundtrip() {
        let rid = ResourceId::BuiltIn(BuiltInKind::Deployment);
        let row = ResourceRow {
            cells: vec![
                CellValue::Text("prod".into()),
                CellValue::Text("web".into()),
                CellValue::Text("3/3".into()),
            ],
            name: "web".into(),
            namespace: Some("prod".into()),
            ..Default::default()
        };
        let baseline = StreamEvent::Baseline(TableBaseline {
            resource: rid,
            headers: vec!["NAMESPACE".into(), "NAME".into(), "READY".into()],
            rows: vec![row.clone()],
        });
        let bytes = bincode::serialize(&baseline).unwrap();
        match bincode::deserialize::<StreamEvent>(&bytes).unwrap() {
            StreamEvent::Baseline(b) => assert_eq!(b.rows[0].name, "web"),
            _ => panic!("Wrong event type"),
        }
        let delta = StreamEvent::Delta(TableDelta {
            changes: vec![
                RowChange::Upsert(row),
                RowChange::Remove(ObjectKey::new("prod".to_string(), "old".to_string())),
            ],
        });
        let bytes = bincode::serialize(&delta).unwrap();
        match bincode::deserialize::<StreamEvent>(&bytes).unwrap() {
            StreamEvent::Delta(d) => {
                assert_eq!(d.changes.len(), 2);
                assert_eq!(d.changes[1].key(), ObjectKey::new("prod".to_string(), "old".to_string()));
            }
            _ => panic!("Wrong event type"),
        }
    }

    /// Envelope wire-tag stability (bincode u32 LE declaration-index) for
    /// the enums the wire depends on. RULE: any change to these goldens is
    /// a protocol break — bump PROTOCOL_VERSION in the same change.
    /// Appending variants is safe; reordering or mid-enum inserts are not.
    #[test]
    fn envelope_wire_tags_are_stable() {
        // SessionCommand: pin the head (Init MUST stay tag 0 — the daemon
        // dispatches on the first frame) and the ops that follow.
        let init = SessionCommand::Init {
            context: None,
            namespace: Namespace::All,
            readonly: false,
            kubeconfig_yaml: String::new(),
            env_vars: std::collections::HashMap::new(),
            identity: ClusterIdentity::default(),
        };
        assert_eq!(&bincode::serialize(&init).unwrap()[..4], 0u32.to_le_bytes());
        let obj = ObjectRef {
            resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            namespace: Namespace::All,
            name: String::new(),
        };
        assert_eq!(&bincode::serialize(&SessionCommand::Describe(obj.clone())).unwrap()[..4], 1u32.to_le_bytes());
        assert_eq!(&bincode::serialize(&SessionCommand::Yaml(obj.clone())).unwrap()[..4], 2u32.to_le_bytes());
        assert_eq!(&bincode::serialize(&SessionCommand::Delete(obj)).unwrap()[..4], 3u32.to_le_bytes());

        // SessionEvent: head pins.
        let ready = SessionEvent::Ready {
            context: ContextName::new(String::new()),
            identity: ClusterIdentity::default(),
            namespaces: vec![],
        };
        assert_eq!(&bincode::serialize(&ready).unwrap()[..4], 0u32.to_le_bytes());
        assert_eq!(&bincode::serialize(&SessionEvent::SessionError(String::new())).unwrap()[..4], 1u32.to_le_bytes());

        // SubstreamInit: complete (3 variants).
        let sub = SubstreamInit::Subscribe(SubscriptionInit {
            resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            namespace: Namespace::All,
            filter: None,
            force: false,
        });
        assert_eq!(&bincode::serialize(&sub).unwrap()[..4], 0u32.to_le_bytes());

        // OperationKind: complete, in declaration order — capabilities
        // ride the typed enum on both sides.
        use OperationKind::*;
        for (i, op) in [Describe, Yaml, Delete, Restart, Scale, StreamLogs,
                        PreviousLogs, PortForward, Shell, ShowNode, ForceKill,
                        NodeShell, DecodeSecret, TriggerCronJob,
                        ToggleSuspendCronJob]
            .iter().enumerate()
        {
            assert_eq!(
                &bincode::serialize(op).unwrap()[..4],
                (i as u32).to_le_bytes(),
                "OperationKind tag drift at {op:?}",
            );
        }
    }

    /// Wire-tag stability for the proto-8 stream enums (bincode u32 LE
    /// declaration-index). Appending is safe; reorder/insert is a break.
    #[test]
    fn stream_wire_tags_are_stable() {
        assert_eq!(PROTOCOL_VERSION, 8);
        let b = bincode::serialize(&StreamEvent::Error("x".into())).unwrap();
        assert_eq!(&b[..4], 2u32.to_le_bytes()); // Baseline=0, Delta=1, Error=2
        let r = bincode::serialize(&RowChange::Remove(ObjectKey::new(String::new(), String::new()))).unwrap();
        assert_eq!(&r[..4], 1u32.to_le_bytes()); // Upsert=0, Remove=1
        // Handshake preamble bytes are pinned: magic then version, BE.
        assert_eq!(PROTOCOL_MAGIC.to_be_bytes(), [0x4B, 0x39, 0x52, 0x53]);
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

    #[test]
    fn all_builtin_ops_have_descriptors() {
        use super::OperationKind::*;
        let ops = [Describe, Yaml, Delete, Restart, Scale, StreamLogs,
                   PreviousLogs, PortForward, Shell, ShowNode, ForceKill,
                   NodeShell, DecodeSecret, TriggerCronJob, ToggleSuspendCronJob];
        for op in ops {
            assert!(!op.descriptor().label.is_empty(), "{:?} has no label", op);
        }
    }

    #[test]
    fn custom_operation_descriptor() {
        let op = OperationKind::Custom("my-op".into());
        let desc = op.descriptor();
        assert_eq!(desc.default_key, None);
        // to_action should produce OverlayCapability
        let action = op.to_action();
        assert!(matches!(action, crate::app::actions::Action::OverlayCapability(ref s) if s == "my-op"));
    }

    #[test]
    fn custom_operation_has_no_schema_or_template() {
        let op = OperationKind::Custom("test".into());
        assert!(op.form_schema().is_none());
        assert!(op.exec_template().is_none());
    }

    #[test]
    fn custom_operation_bincode_roundtrip() {
        let op = OperationKind::Custom("my-custom-op".into());
        let bytes = bincode::serialize(&op).unwrap();
        let decoded: OperationKind = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
    }

    #[test]
    fn custom_local_kind_bincode_roundtrip() {
        let rid = ResourceId::Local(crate::kube::local::LocalResourceKind::Custom("my-resource".into()));
        let bytes = bincode::serialize(&rid).unwrap();
        let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, rid);
    }
}

/// Golden byte-layout tests for the positionally-encoded wire types.
///
/// The roundtrip tests above serialize-then-deserialize with the SAME code, so
/// they pass even if a `CellValue` variant is reordered or a `ResourceRow`
/// field is inserted/removed — a daemon and a TUI built from different revisions
/// would then silently misread each other's frames (bincode is positional;
/// nothing self-describing is on the wire, only the `PROTOCOL_VERSION` gate).
///
/// These tests freeze the exact bytes. If one fails you changed an on-wire
/// shape: that is a breaking change — bump [`PROTOCOL_VERSION`], update the
/// golden here, and ship daemon + TUI together.
#[cfg(test)]
mod wire_layout_golden {
    use super::{DescribeLine, DescribeLineKind, LogLine};
    use crate::kube::resources::row::{CellValue, QuantityUnit, ResourceRow, RowHealth};

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn cell_tag(v: &CellValue) -> u32 {
        let b = bincode::serialize(v).unwrap();
        u32::from_le_bytes([b[0], b[1], b[2], b[3]])
    }

    #[test]
    fn cellvalue_variant_tags_are_positional() {
        // bincode encodes the enum discriminant as a u32 LE tag = declaration
        // index. This order IS the wire contract: reordering or inserting a
        // variant silently remaps every cell. (The golden below pins the full
        // byte layout; this documents the tag↔variant map explicitly.)
        assert_eq!(cell_tag(&CellValue::Text(String::new())), 0);
        assert_eq!(cell_tag(&CellValue::Ratio { num: 0, denom: 0 }), 1);
        assert_eq!(cell_tag(&CellValue::Quantity { value: 0, unit: QuantityUnit::Millicores }), 2);
        assert_eq!(cell_tag(&CellValue::Age(None)), 3);
        assert_eq!(cell_tag(&CellValue::Count(0)), 4);
        assert_eq!(cell_tag(&CellValue::Bool(false)), 5);
        assert_eq!(cell_tag(&CellValue::List(Vec::new())), 6);
        assert_eq!(cell_tag(&CellValue::Status { text: String::new(), health: RowHealth::Normal }), 7);
        assert_eq!(cell_tag(&CellValue::Percentage(None)), 8);
        assert_eq!(cell_tag(&CellValue::Placeholder), 9);
    }

    #[test]
    fn cellvalue_wire_layout_golden() {
        // One of every variant, with distinctive payloads — pins both the
        // variant order AND each variant's field layout.
        let sample = vec![
            CellValue::Text("t".into()),
            CellValue::Ratio { num: 1, denom: 2 },
            CellValue::Quantity { value: 3, unit: QuantityUnit::Bytes },
            CellValue::Age(Some(4)),
            CellValue::Count(5),
            CellValue::Bool(true),
            CellValue::List(vec!["a".into()]),
            CellValue::Status { text: "s".into(), health: RowHealth::Failed },
            CellValue::Percentage(Some(6)),
            CellValue::Placeholder,
        ];
        assert_eq!(
            hex(&bincode::serialize(&sample).unwrap()),
            "0a00000000000000000000000100000000000000740100000001000000020000000200000003000000000000000100000003000000010400000000000000040000000500000000000000050000000106000000010000000000000001000000000000006107000000010000000000000073020000000800000001060000000000000009000000",
        );
    }

    #[test]
    fn resourcerow_wire_layout_golden() {
        // Pins ResourceRow field order/presence (incl. the metrics tail that
        // has `#[serde(default)]` but is always positionally on the wire).
        let row = ResourceRow {
            cells: vec![CellValue::Text("c".into()), CellValue::Count(9)],
            name: "row".into(),
            namespace: Some("ns".into()),
            drill_target: None,
            containers: Vec::new(),
            owner_refs: Vec::new(),
            pf_ports: vec![80, 443],
            crd_info: None,
            node: None,
            health: RowHealth::Pending,
            cpu_request: Some(7),
            cpu_limit: None,
            mem_request: None,
            mem_limit: None,
        };
        assert_eq!(
            hex(&bincode::serialize(&row).unwrap()),
            "0200000000000000000000000100000000000000630400000009000000000000000300000000000000726f770102000000000000006e73000000000000000000000000000000000002000000000000005000bb01000001000000010700000000000000000000",
        );
    }

    #[test]
    fn describeline_wire_layout_golden() {
        // The describe wire type added in PROTOCOL_VERSION 6.
        let lines = vec![
            DescribeLine { text: "Name: x".into(), kind: DescribeLineKind::Field { key_end: 4 } },
            DescribeLine { text: "Spec:".into(), kind: DescribeLineKind::Section },
            DescribeLine { text: String::new(), kind: DescribeLineKind::Plain },
        ];
        assert_eq!(
            hex(&bincode::serialize(&lines).unwrap()),
            "030000000000000007000000000000004e616d653a20780100000004000000000000000500000000000000537065633a00000000000000000000000002000000",
        );
    }

    #[test]
    fn logline_wire_layout_golden() {
        // The log-stream wire type added in PROTOCOL_VERSION 7. Field order is
        // `container` (Option — 1-byte bincode tag, then the String if Some)
        // then `content`. The All-containers path rides the tagged shape; every
        // single-source line rides the untagged (None) shape.
        let tagged = LogLine { container: Some("c".into()), content: "hi".into() };
        assert_eq!(
            hex(&bincode::serialize(&tagged).unwrap()),
            "0101000000000000006302000000000000006869",
        );
        let untagged = LogLine::untagged("hi");
        assert_eq!(
            hex(&bincode::serialize(&untagged).unwrap()),
            "0002000000000000006869",
        );
    }

    #[test]
    fn logline_flat_text() {
        // Untagged: bare content (borrowed, no prefix). Tagged: the source
        // container regains a `[container] ` prefix for clipboard / file export.
        assert_eq!(LogLine::untagged("hi").flat_text().as_ref(), "hi");
        assert_eq!(
            LogLine { container: Some("web".into()), content: "msg".into() }.flat_text().as_ref(),
            "[web] msg",
        );
    }
}
