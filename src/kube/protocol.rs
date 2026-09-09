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
/// A context name is NON-EMPTY by construction — see [`ContextName::new`].
/// There is deliberately no `Default`, no infallible `From<&str>`, and no
/// `is_empty()`: absence is [`Option::None`], never a name that happens to
/// be blank.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ContextName(Arc<str>);

/// A context name was empty. Its own type (rather than a bare string) so
/// the boundary conversions can be `TryFrom` without inventing an error at
/// each site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyContextName;

impl std::fmt::Display for EmptyContextName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("context name is empty")
    }
}

impl std::error::Error for EmptyContextName {}

impl ContextName {
    /// A context name, or `None` if the string is empty.
    ///
    /// Empty is not a name — and it is exactly what "no context" looks like
    /// coming from outside: `kubectl config unset current-context` writes
    /// `current-context: ""` rather than removing the key, so kube-rs hands
    /// back `Some("")`. Absence has to become `None` at the ONE place a name
    /// enters the system; otherwise every consumer must remember to re-check,
    /// which is how a "no current-context in kubeconfig" guard came to sit
    /// directly next to the value that sails straight past it and ask the
    /// daemon for a context named `""`.
    pub fn new(s: impl Into<Arc<str>>) -> Option<Self> {
        let s = s.into();
        (!s.is_empty()).then_some(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ContextName {
    type Error = EmptyContextName;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s).ok_or(EmptyContextName)
    }
}

impl TryFrom<&str> for ContextName {
    type Error = EmptyContextName;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s).ok_or(EmptyContextName)
    }
}

/// Hand-written so the invariant holds for names arriving off the WIRE too,
/// not just ones built in-process. Fail-closed: a peer that sends `""` gets
/// a decode error rather than a session pointed at a context that cannot
/// exist. `Serialize` stays derived + transparent, so the encoding is still
/// byte-identical to a bare `String`.
impl<'de> Deserialize<'de> for ContextName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::new(s).ok_or_else(|| serde::de::Error::custom(EmptyContextName))
    }
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
    type Err = EmptyContextName;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
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
/// Non-empty by construction, for the same reason [`ContextName`] is: this
/// is a HASH-MAP KEY (node metrics are stored by it), so an empty one would
/// silently collide with every other empty one rather than failing loudly.
/// No infallible `From` and no `Default` — a name that can be blank isn't a
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct NodeName(String);

/// A node name was empty. Its own type so boundary conversions can be
/// `TryFrom` without inventing an error at each site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyNodeName;

impl std::fmt::Display for EmptyNodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("node name is empty")
    }
}

impl std::error::Error for EmptyNodeName {}

impl NodeName {
    /// A node name, or `None` for the empty string.
    pub fn new(s: impl Into<String>) -> Option<Self> {
        let s = s.into();
        (!s.is_empty()).then_some(Self(s))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl TryFrom<String> for NodeName {
    type Error = EmptyNodeName;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s).ok_or(EmptyNodeName)
    }
}

impl TryFrom<&str> for NodeName {
    type Error = EmptyNodeName;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s).ok_or(EmptyNodeName)
    }
}

/// Fail-closed off the WIRE too — a peer sending `""` gets a decode error
/// rather than a map key that swallows every other nameless node.
/// `Serialize` stays derived + transparent, so the encoding is unchanged.
impl<'de> Deserialize<'de> for NodeName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::new(s).ok_or_else(|| serde::de::Error::custom(EmptyNodeName))
    }
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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

    /// The edit flow's server-side apply. NOT user-invocable from a
    /// capability list (the edit overlay drives it) — the variant exists so
    /// `SessionEvent::OpResult` can name its operation: without it, an
    /// apply result and e.g. a concurrent batch-restart result for the
    /// same target were indistinguishable and could claim each other.
    /// Appended last (wire-tag discipline).
    Apply,
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

impl FormSchema {
    /// The `Scale` dialog — referenced directly by the scale action, which
    /// knows statically that it has one.
    pub const SCALE: FormSchema = FormSchema {
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

    /// The `PortForward` dialog — likewise statically known.
    pub const PORT_FORWARD: FormSchema = FormSchema {
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
}

impl OperationKind {
    /// The form schema for this operation, if it needs user input.
    /// EXHAUSTIVE, no wildcard — same discipline as
    /// [`OperationKind::batch_support`]. A `_ => None` arm meant a new
    /// operation silently got no form dialog instead of forcing the author
    /// to decide, which is the whole reason these manifests are written as
    /// closed matches.
    ///
    /// Call sites that already KNOW the kind should use [`FormSchema::SCALE`]
    /// / [`FormSchema::PORT_FORWARD`] directly rather than asking here and
    /// unwrapping — the guarantee is theirs to have statically.
    pub fn form_schema(&self) -> Option<&'static FormSchema> {
        match self {
            OperationKind::Scale => Some(&FormSchema::SCALE),
            OperationKind::PortForward => Some(&FormSchema::PORT_FORWARD),
            OperationKind::Describe
            | OperationKind::Yaml
            | OperationKind::Delete
            | OperationKind::Restart
            | OperationKind::StreamLogs
            | OperationKind::PreviousLogs
            | OperationKind::Shell
            | OperationKind::ShowNode
            | OperationKind::ForceKill
            | OperationKind::NodeShell
            | OperationKind::DecodeSecret
            | OperationKind::TriggerCronJob
            | OperationKind::ToggleSuspendCronJob
            | OperationKind::Apply
            | OperationKind::Custom(_) => None,
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
            OperationKind::StreamLogs => OperationDescriptor { label: "Logs", default_key: Some('l') }, // k9s parity; rebindable via `keys.logs`
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
            // Never appears in a capability list; label used only if a
            // result flash ever needs to name the operation.
            OperationKind::Apply => OperationDescriptor { label: "Apply", default_key: None },
        }
    }

    // NOTE: the OperationKind → client `Action` mapping deliberately does
    // NOT live here — it's `impl From<&OperationKind> for Action` in
    // `app::actions`, so this wire module never depends on client types.

    /// Batch stance: whether this operation may act on the MARKED SET
    /// (select mode) or is inherently single-target. Static manifest like
    /// [`Self::form_schema`] / [`Self::exec_template`] — NOT a wire type.
    /// EXHAUSTIVE by design: a new operation (including overlay-defined
    /// `Custom` ones) must declare its stance here or the build breaks; a
    /// `_` wildcard would silently classify future operations.
    pub fn batch_support(&self) -> BatchSupport {
        match self {
            OperationKind::Delete | OperationKind::Restart | OperationKind::ForceKill => {
                BatchSupport::PerItem
            }
            OperationKind::Describe
            | OperationKind::Yaml
            | OperationKind::Scale
            | OperationKind::StreamLogs
            | OperationKind::PreviousLogs
            | OperationKind::PortForward
            | OperationKind::Shell
            | OperationKind::ShowNode
            | OperationKind::NodeShell
            | OperationKind::DecodeSecret
            | OperationKind::TriggerCronJob
            | OperationKind::ToggleSuspendCronJob
            | OperationKind::Custom(_)
            | OperationKind::Apply => BatchSupport::SingleOnly,
        }
    }
}

/// Batch stance of an operation (see [`OperationKind::batch_support`]).
/// `PerItem` = the operation is dispatched once per marked row (each item
/// is an independent server command); everything else is single-target
/// and its keybinding is dead in select mode. An `Aggregate` stance
/// (one operation consuming the whole set, e.g. merged multi-pod logs)
/// is deliberately absent until a real consumer exists — adding it later
/// is free precisely because this is not a wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSupport {
    PerItem,
    SingleOnly,
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

/// Enforce the frame-size cap on the WRITE side too. Without this, an
/// oversized frame (e.g. a `TableBaseline` for a pathologically large
/// cluster — ~100k rows crosses 64 MiB) is serialized and streamed in
/// full, only to be rejected by the peer's read-side cap AFTER the bytes
/// are on the wire — which, mid-session, becomes an infinite
/// serialize→peer-RST→re-attach churn loop. Guarding here turns it into a
/// clean, caught error the daemon can surface as `StreamEvent::Error`.
/// Also closes the `as u32` truncation footgun (a >4 GiB frame would wrap
/// and corrupt framing).
fn check_frame_size(len: usize) -> anyhow::Result<u32> {
    if len > MAX_MESSAGE_SIZE as usize {
        anyhow::bail!(
            "message too large to send: {} bytes (cap {})",
            len,
            MAX_MESSAGE_SIZE
        );
    }
    Ok(len as u32)
}

/// Whether `msg` would exceed the frame cap on the wire, WITHOUT
/// serializing it (bincode computes the size from the value). Lets a
/// caller substitute a small typed error for an undeliverable payload
/// before paying to serialize/stream it.
pub fn frame_exceeds_cap<T: Serialize>(msg: &T) -> bool {
    bincode::serialized_size(msg).map(|n| n > MAX_MESSAGE_SIZE as u64).unwrap_or(true)
}

/// Write a bincode-serialized message with a 4-byte big-endian length prefix.
/// Flushes the writer after writing.
pub async fn write_bincode<W, T>(writer: &mut W, msg: &T) -> anyhow::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let bytes = bincode::serialize(msg)?;
    let len = check_frame_size(bytes.len())?;
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
    let len = check_frame_size(bytes.len())?;
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
    /// The daemon's WATCH stopped feeding this subscription: the cluster
    /// hop is broken (watch stream erroring and relisting, or the watcher
    /// died and the bridge is backing off before it respawns one). Rows
    /// already delivered remain the last known truth, but nothing new is
    /// arriving.
    ///
    /// Deliberately NOT [`StreamEvent::Error`]: that one is terminal and
    /// closes the substream. This is a recoverable degradation, always
    /// followed by [`StreamEvent::Live`], a `Delta`, or a fresh `Baseline`.
    /// Without it a cluster-side outage is invisible to the TUI — the
    /// watcher retries internally for two minutes before it even reports
    /// death, and the bridge then retries forever, all while the client
    /// paints its frozen rows as current.
    Stale(String),
    /// The watch recovered with no row change to report. `Baseline` and
    /// `Delta` say the same thing implicitly (data is flowing again); this
    /// variant exists for the QUIET recovery, where nothing changed while
    /// the watch was down so no data event would otherwise be sent — and
    /// the client would stay marked stale over rows that are live.
    Live,
}

/// Borrowed mirror of [`StreamEvent`] for ZERO-COPY serialization on the
/// hot delta path. A per-subscription bridge that doesn't rewrite the
/// delta (the common non-`OwnerUid` case) would otherwise clone the whole
/// batch just to wrap it in an owned `StreamEvent::Delta`. bincode tags
/// are positional, so serializing `StreamEventRef::Delta(&d)` produces
/// bytes identical to `StreamEvent::Delta(d)` — the variant order MUST
/// match, which `stream_event_ref_tags_match` pins.
#[derive(Serialize)]
pub enum StreamEventRef<'a> {
    Baseline(&'a TableBaseline),
    Delta(&'a TableDelta),
    Error(&'a str),
    Resolved { original: &'a ResourceId, resolved: &'a ResourceId },
    Stale(&'a str),
    Live,
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
///
/// v9: `SessionEvent::OpResult` — mutating-operation results carry their
/// originating `ObjectRef` (per-item batch correlation; target-gated edit
/// apply). `CommandResult` narrowed to management acknowledgments.
///
/// v10: `OpResult` carries its `OperationKind` (op+target correlation —
/// target alone let an edit-apply and a batch op on the same object claim
/// each other's results); `OperationKind::Apply` appended;
/// `ContainerInfo.status: String` split into typed `state: ContainerState`
/// plus open `reason: Option<String>`; the MANAGEMENT connection path now
/// exchanges this same preamble (it was the one unversioned door).
///
/// v11: `StreamEvent::Stale`/`Live` — the daemon↔cluster hop reports its own
/// health. Before this the only cluster-side signal was the terminal
/// `Error`, sent solely when a subscription failed BEFORE its first
/// baseline; a watch that broke after delivering data was silently retried
/// (2 minutes inside the watcher, then forever in the bridge) while the TUI
/// painted the frozen rows as live.
pub const PROTOCOL_VERSION: u32 = 11;

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
// Subscription stream payloads (see PROTOCOL_VERSION)
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
///
/// The connection type is decided by the `CONN_TYPE` byte the daemon reads
/// before any framed message (see `daemon.rs`) — NOT by which command
/// arrives first, as this said until 2026-09-09. `Init` opens a long-lived
/// TUI session; `Ping`/`Status`/`Shutdown`/`Clear` are one-shot management
/// requests. Both doors exchange the version handshake first.
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
    /// Acknowledgment of a MANAGEMENT command (ping/shutdown/stats) — the
    /// commands with no resource target. Results of mutating operations
    /// on a resource ride [`Self::OpResult`] instead, which carries the
    /// target. The message lives inside the Ok/Err variant so the
    /// receiver can't read it without branching on success/failure.
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

    /// Result of a mutating operation on a specific target (v9; `op` added
    /// in v10). The target rides the result so the client can correlate
    /// per-item outcomes of a batch — aggregate one summary flash, unmark
    /// rows per-success, retain marks on failures — and gate the edit-apply
    /// flow. `op` names WHICH operation produced the result: target alone
    /// was ambiguous (an edit-apply and a batch-restart on the same object
    /// could claim each other's outcomes). Same correlation shape as
    /// `DescribeResult`/`YamlResult`. Appended after `DaemonStatus` so
    /// every pre-existing tag keeps its position.
    OpResult { op: OperationKind, target: ObjectRef, result: Result<String, String> },
}

#[cfg(test)]
#[path = "../tests/kube/protocol.rs"]
mod tests;

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
#[path = "../tests/kube/protocol_golden.rs"]
mod wire_layout_golden;
