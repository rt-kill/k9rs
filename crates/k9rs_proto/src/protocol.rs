//! Wire protocol types for k9rs daemon communication.
//!
//! One unified binary protocol (length-prefixed bincode) for ALL daemon
//! communication — both TUI sessions and management commands (k9rs ctl).
//! No JSON on the wire.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

use crate::local_types::LocalResourceKind;
use crate::resource_def::BuiltInKind;
use crate::crd_cache::CachedCrd;

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
/// are unambiguous. See the local-resource module for the abstraction.
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
    /// Consults the definition table (`BuiltInKind::def`) and the local one.
    pub fn from_alias(alias: &str) -> Option<Self> {
        if let Some(def) = crate::resource_def::def_by_alias(alias) {
            return Some(ResourceId::BuiltIn(def.kind()));
        }
        if let Some(kind) = crate::local_types::find_by_alias(alias) {
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
                let def = k.def();
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
                k.def().operations()
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
///
/// `Named("")` is NOT a valid value: the empty selection is [`Namespace::All`],
/// which is what both constructors here map an empty string to. It could
/// still arrive off the WIRE, where the derived `Deserialize` accepted it —
/// and that mattered, because the daemon's guard on a mutating operation
/// refuses `All` and so sailed straight past `Named("")`, after which
/// [`Namespace::as_option`] answers `Some("")` and that goes into the
/// request path. Decoding now refuses it; see `NamespaceWire` below.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "NamespaceWire")]
pub enum Namespace {
    /// All namespaces (cross-namespace view).
    All,
    /// A specific namespace. Never empty — see the type's own doc.
    Named(String),
}

/// The wire form of [`Namespace`]: the same two variants in the same order,
/// so the bincode tags and payloads are byte-identical, existing only so
/// that decoding can REFUSE `Named("")` instead of admitting a namespace
/// selection that names nothing.
///
/// A separate type rather than a hand-written `Deserialize` for `Namespace`,
/// because hand-writing one for an enum is where a wire layout quietly
/// changes. `Serialize` stays derived on `Namespace` itself.
#[derive(Deserialize)]
enum NamespaceWire {
    All,
    Named(String),
}

impl TryFrom<NamespaceWire> for Namespace {
    type Error = String;

    fn try_from(wire: NamespaceWire) -> Result<Self, Self::Error> {
        match wire {
            NamespaceWire::All => Ok(Namespace::All),
            NamespaceWire::Named(s) if s.is_empty() => Err(
                "namespace selection Named(\"\") names nothing; the empty \
                 selection is Namespace::All"
                    .to_string(),
            ),
            NamespaceWire::Named(s) => Ok(Namespace::Named(s)),
        }
    }
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
/// Every field is INDEPENDENTLY unknown, and unknown is `None` — never a
/// string that happens to be blank. Same rule as [`ContextName`], for the
/// same reason: all three arrive from outside as possibly-empty strings
/// (kube-rs types `Context::user` as `Option<String>`, a context missing from
/// the kubeconfig yields nothing at all, and `k8s_version` is not known until
/// the daemon has asked the apiserver), and a blank that survives the
/// boundary makes every reader responsible for re-checking it. Six readers
/// did; the contexts table forgot, and rendered an empty cell where the
/// header showed `n/a`.
///
/// Fields are private so the only way to read one is through an accessor
/// that hands back `Option<&str>` — there is no blank string to mistake for
/// a value. `Default` is all-unknown, which is a real state (before the
/// handshake lands) rather than a placeholder.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    cluster: Option<String>,
    user: Option<String>,
    k8s_version: Option<String>,
}

impl ClusterIdentity {
    /// The kubeconfig `(cluster, user)` labels for a context. Blank collapses
    /// to unknown HERE — the one boundary where these enter the process.
    pub fn new(cluster: String, user: String) -> Self {
        Self {
            cluster: non_empty(cluster),
            user: non_empty(user),
            k8s_version: None,
        }
    }

    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    pub fn k8s_version(&self) -> Option<&str> {
        self.k8s_version.as_deref()
    }

    /// Record the apiserver version, once the daemon has one. Blank collapses
    /// to unknown for the same reason as the constructor: a version string the
    /// apiserver didn't give us is not a version.
    pub fn set_k8s_version(&mut self, version: String) {
        self.k8s_version = non_empty(version);
    }
}

/// `Some(s)` unless `s` is empty. The one-line rule behind every
/// blank-is-absence collapse in this module.
fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
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

/// Declare [`OperationKind`] and generate `BUILT_IN` from the same list.
///
/// `BUILT_IN` is "every operation the app itself defines", and it used to be
/// a second hand-written array beside the enum. Nothing asserted it was
/// complete — and nothing could, because iterating a list can only check
/// the entries it has. A variant missing from it still compiled arms in
/// `descriptor`, `config_key`, `op_override`, `batch_support`,
/// `structural_chord` and `From<&OperationKind> for Action`, so it looked
/// thoroughly handled while escaping every check that derives its universe
/// from `BUILT_IN`: the reserved-chord check and the duplicate-chord check
/// in `KeysConfig::validate` (whose stated purpose is "a startup error, not
/// a mystery"), the stale guard, and the batch-support manifest.
///
/// Why a token muncher rather than two groups: the declaration ORDER is the
/// wire's tag order, and `Custom` sits between `ToggleSuspendCronJob` and
/// `Apply`. Emitting the unit variants and then the payload one would swap
/// two tags, which is a protocol break for a cosmetic gain. So the entries
/// are consumed one at a time, in order, and only the unit ones are
/// collected into the list.
///
/// Same intent as `built_in_kinds!` and `view_classes!`; this is the third
/// list in this codebase that Rust cannot enumerate for itself.
macro_rules! operation_kinds {
    ($(#[$m:meta])* pub enum $name:ident { $($entries:tt)* }) => {
        operation_kinds!(@collect $name [$(#[$m])*] [] [] $($entries)*);
    };

    // A variant WITH a payload: declared, but not a member of `BUILT_IN` —
    // it is overlay-declared and unbounded, so there is no list to put it in.
    // Matched before the unit arm because it is the more specific shape.
    (@collect $name:ident [$($m:tt)*] [$($decl:tt)*] [$($list:tt)*]
        $(#[$vm:meta])* $v:ident ( $t:ty ) , $($rest:tt)*
    ) => {
        operation_kinds!(@collect $name [$($m)*]
            [$($decl)* $(#[$vm])* $v($t),]
            [$($list)*]
            $($rest)*);
    };

    // A unit variant: declared AND listed.
    (@collect $name:ident [$($m:tt)*] [$($decl:tt)*] [$($list:tt)*]
        $(#[$vm:meta])* $v:ident , $($rest:tt)*
    ) => {
        operation_kinds!(@collect $name [$($m)*]
            [$($decl)* $(#[$vm])* $v,]
            [$($list)* $name::$v,]
            $($rest)*);
    };

    (@collect $name:ident [$($m:tt)*] [$($decl:tt)*] [$($list:tt)*]) => {
        $($m)*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name { $($decl)* }

        // The enum takes its allow from the invocation's attributes; this
        // block cannot, so the allow lives here. It is inert for the real
        // `OperationKind` (`pub` in a `pub` module with cross-crate
        // callers) and load-bearing for the toy enum the macro's own test
        // declares inside a test module.
        #[allow(unreachable_pub)]
        impl $name {
            /// Every operation the app itself defines, in wire-tag order.
            /// Generated from the enum's own declaration, so it cannot be
            /// missing one. `Custom` is absent because it carries a payload
            /// — see the macro's doc.
            pub const BUILT_IN: &'static [$name] = &[$($list)*];
        }
    };
}

operation_kinds! {
    /// One specific operation the user might want to perform on a row. Each kind
    /// corresponds to either a `SessionCommand` variant or a purely client-side
    /// action that nonetheless requires server-known facts (like "this row is a
    /// pod, you can `kubectl exec` into it").
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
        name: form_field_name::REPLICAS,
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
            name: form_field_name::CONTAINER_PORT,
            label: "Container port",
            kind: FormFieldSchemaKind::DynamicSelect {
                fallback: DynamicSelectFallback::Port,
            },
        },
        FormFieldSchema {
            name: form_field_name::LOCAL_PORT,
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
    /// THE op→schema pairing. Call sites used to name both halves — a
    /// `&FormSchema::SCALE` beside an `OperationKind::Scale` — on this doc's
    /// own advice, which left the pairing restated per site and this match
    /// with no caller but a test. `build_form_for` asks here now.
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
        // `ConditionalPair`, not a bare `Literal` beside a `Placeholder`.
        // It is the one flag/value pair in either template that was not
        // using the variant built for exactly this, and it went wrong two
        // different ways:
        //
        //   - with a POD target, `resolve_placeholder` answers
        //     `Some(namespace)` even when the namespace is empty, so the
        //     invocation became `exec -it -n "" <pod> -- sh …` — an empty
        //     `--namespace` passed explicitly rather than omitted.
        //   - with a MISMATCHED target (a `Node` under the shell template)
        //     it answers `None`, `build_exec_init` skips the placeholder and
        //     keeps the literal, and the dangling `-n` then takes the next
        //     argument as its value and shifts everything after it.
        //
        // The second needs an op paired with the wrong target variant, which
        // nothing constructs today — which is why it is worth a type and a
        // test rather than a reading.
        ExecArg::ConditionalPair("-n", ExecPlaceholder::Namespace),
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
            // EXHAUSTIVE, like its sibling manifests. `form_schema` eighty
            // lines above argues this case verbatim — "a `_ => None` arm
            // meant a new operation silently got no form dialog instead of
            // forcing the author to decide" — and then this one kept the
            // wildcard it was arguing against.
            OperationKind::Describe
            | OperationKind::Yaml
            | OperationKind::Delete
            | OperationKind::Restart
            | OperationKind::Scale
            | OperationKind::StreamLogs
            | OperationKind::PreviousLogs
            | OperationKind::PortForward
            | OperationKind::ShowNode
            | OperationKind::ForceKill
            | OperationKind::DecodeSecret
            | OperationKind::TriggerCronJob
            | OperationKind::ToggleSuspendCronJob
            | OperationKind::Custom(_)
            | OperationKind::Apply => None,
        }
    }

    /// The `keys.*` config field name that overrides this operation's chord,
    /// or `None` for the ones that aren't user-bindable.
    ///
    /// Exhaustive, and the source of `KeysConfig::entries` — which was a
    /// hand-written array parallel to both the struct fields and
    /// `op_override`, so a binding added to those two and missed here
    /// escaped the reserved-chord check AND the duplicate check, in the one
    /// function whose stated purpose is "a startup error, not a mystery".
    pub fn config_key(&self) -> Option<&'static str> {
        match self {
            OperationKind::Describe => Some("describe"),
            OperationKind::Yaml => Some("yaml"),
            OperationKind::StreamLogs => Some("logs"),
            OperationKind::PreviousLogs => Some("previousLogs"),
            OperationKind::Shell => Some("shell"),
            OperationKind::Restart => Some("restart"),
            OperationKind::Scale => Some("scale"),
            OperationKind::Delete => Some("delete"),
            OperationKind::ForceKill => Some("forceKill"),
            OperationKind::PortForward => Some("portForward"),
            OperationKind::ShowNode => Some("showNode"),
            OperationKind::NodeShell => Some("nodeShell"),
            OperationKind::DecodeSecret => Some("decodeSecret"),
            OperationKind::TriggerCronJob => Some("triggerCronJob"),
            OperationKind::ToggleSuspendCronJob => Some("toggleSuspendCronJob"),
            OperationKind::Custom(_) | OperationKind::Apply => None,
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
    // `app::actions`.
    //
    // That used to end "…so this wire module never depends on client types",
    // which was false sixteen lines below: `structural_chord` returns
    // `app::KeyCombo`. One dependency was moved out and a different one added
    // underneath the claim. The honest statement is that this module has
    // exactly ONE client dependency and it is the next function down;
    // `KeyCombo` is a two-field POD and belongs below this module, which is
    // what would make the claim true rather than aspirational.

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
            // One stream over the whole marked set, merged by the daemon —
            // NOT one log view per pod. `PreviousLogs` stays single-only:
            // the crashed-container-before-this-one question is about one
            // container, and merging N of them answers nothing.
            OperationKind::StreamLogs => BatchSupport::Aggregate,
            OperationKind::Describe
            | OperationKind::Yaml
            | OperationKind::Scale
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSupport {
    /// Dispatched once per marked row — each item is an independent server
    /// command with its own `OpResult`.
    PerItem,
    /// ONE operation consuming the whole set: merged multi-pod logs, where
    /// N streams into N views would defeat the point of asking.
    Aggregate,
    /// Inherently single-target; the keybinding is dead in select mode.
    SingleOnly,
}

/// Single source of truth for client-side form-field name strings.
///
/// The declarative schemas above (`SCALE_FORM`, `PORT_FORWARD_FORM`) and the
/// submit dispatcher (`k9rs_app::form`'s `build_command`) both spell their
/// field names with these, so renaming one is a compile error rather than a
/// silent "field not found" at the moment the user presses OK.
///
/// That was prose until 2026-10-27: the schemas held bare literals, so the
/// two sides agreed by eye. Not part of the wire protocol; the daemon never
/// sees field names.
pub mod form_field_name {
    pub const REPLICAS: &str = "replicas";
    pub const CONTAINER_PORT: &str = "container_port";
    pub const LOCAL_PORT: &str = "local_port";
}

// The schemas above now spell their `name` with these constants, which is
// what makes the shared-source claim true. They were bare literals —
// `name: "container_port"` — against a doc saying "drift becomes a compile
// error rather than a silent 'field not found' lookup miss at submit time".
// Renaming a schema field compiled fine and `build_command` answered
// `Err("Missing field: container_port")` when the user pressed OK, which is
// exactly the outcome the doc called impossible.

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
///
/// `pub` because it has a RELATIONSHIP that only holds if both ends can see
/// it: `k9rs_mux::MAX_STREAM_WINDOW` must be at least this, or a legitimate
/// message of exactly this size stalls behind flow control instead of
/// moving. Both are 64 MiB today and neither imports the other —
/// `k9rs_mux` has no protocol dependency on purpose, which is the point of
/// its being a separate crate — so the relationship is asserted from
/// `k9rs_daemon`, which depends on both
/// (`the_mux_window_admits_a_max_size_message`).
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// Buffer capacity for BufReader/BufWriter on session connections.
pub const IO_BUFFER_SIZE: usize = 256 * 1024;

/// Buffer size for the in-memory duplex stream (--no-daemon mode).
pub const DUPLEX_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// How long a freshly accepted connection has to say what it is.
///
/// Every door into the daemon begins with a fixed exchange the peer must
/// start: the connection-type byte, then the version preamble, then (for a
/// session) the control substream. All three were unbounded waits, so a
/// peer that connected and then said nothing held a task and a file
/// descriptor for as long as the daemon lived — no error, no log line, and
/// nothing to see but the fd count. It is the same shape as a half-open
/// socket, and the fix is the same: the handshake is the one part of the
/// conversation whose timing we get to insist on, because nothing about it
/// waits on a cluster.
///
/// Deliberately generous. This is not a latency budget — a handshake is a
/// few bytes over a Unix socket on the same machine — it is the line
/// between "slow" and "never", and it has to sit far enough above a
/// loaded machine's scheduling jitter that no real client can trip it.
///
/// It bounds only the handshake. Once a session is established its
/// substreams are as long-lived as the user's views, and a `logs -f`
/// stream is idle by design.
pub const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
    /// What the daemon is actually holding open on the user's behalf.
    ///
    /// `ctl status` used to answer only "is it up, and since when" — which
    /// says nothing about the two things that go wrong in a long-lived
    /// daemon: watchers that should have been reclaimed and aren't, and
    /// port-forwards the user has lost track of. Both are `Weak`-cached, so
    /// the counts below are also the shape of a leak if one exists.
    pub caches: CacheStats,
}

/// A snapshot of the daemon's `Weak`-keyed caches.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct CacheStats {
    /// One per live watcher — i.e. per distinct
    /// `(context, namespace, resource, filter)`.
    pub watchers: Vec<WatcherStat>,
    /// One per context with a live local slice.
    pub contexts: Vec<ContextStat>,
    /// Contexts with cached discovery (namespaces + CRD definitions).
    pub discovery_entries: usize,
    pub metrics_pollers: usize,
    pub discovery_pollers: usize,
}

/// One live watcher, as `ctl status` reports it.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WatcherStat {
    /// The context name that created the watcher's cache key. Watchers are
    /// keyed by `ContextId`, whose identity excludes this name, so it is a
    /// label for humans — not the sharing key.
    pub context: ContextName,
    pub namespace: String,
    pub resource: String,
    pub filter: Option<String>,
    /// Strong refs to the watcher: live `Subscription`s plus, if the last one
    /// has left, the in-flight grace task. A watcher lingering at `1` with no
    /// open view is the grace window doing its job — at `1` for minutes is
    /// not.
    pub holders: usize,
}

/// One context's local slice.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContextStat {
    pub context: ContextName,
    pub server_url: String,
    /// Forwards currently running for this context. These outlive any view
    /// of them, so this is the only place they are countable.
    pub port_forwards: usize,
    /// Sessions attached, plus an in-flight grace task if the last one left.
    pub holders: usize,
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
    /// Only rows that NAME this object — the reverse lookup behind `U`.
    /// Post-filtered server-side for the same reason as `OwnerUid`: the K8s
    /// API cannot select on a spec's references, so the filter runs against
    /// the converted row's [`crate::row::ResourceRow::references`].
    References { kind: crate::resource_def::BuiltInKind, name: String },
}

impl SubscriptionFilter {
    /// Convert a label map to the K8s API label selector string format.
    pub fn labels_to_selector(labels: &BTreeMap<String, String>) -> String {
        labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",")
    }

    /// Whether this filter is applied by the DAEMON, against each converted
    /// row, rather than pushed into the watch's selector.
    ///
    /// Exhaustive, and here rather than at the asking end, for the same
    /// reason [`SessionCommand::is_mutating`] is: a new variant must declare
    /// which it is or it does not compile. The daemon used to answer this
    /// with a `matches!` over the two post-filtered variants, which is a
    /// wildcard in disguise — a new variant that the apiserver cannot
    /// select on would have answered `false`, and `false` here means two
    /// things at once: split the watcher by this filter, and do not filter
    /// the rows. So the new filter would both cost its own apiserver watch
    /// AND show every row, which is the "an ignored filter shows MORE rows
    /// than asked for" failure that the row-filter's own doc claims to have
    /// made impossible.
    pub fn is_post_filter(&self) -> bool {
        match self {
            // The apiserver has no selector for an ownerReference, nor for a
            // spec's references.
            Self::OwnerUid(_) | Self::References { .. } => true,
            Self::Labels(_) | Self::Field(_) => false,
        }
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
    /// Stream exactly these containers — a marked selection inside a
    /// container view. Distinct from [`Self::All`]: `--all-containers` is one
    /// kubectl invocation that names its own sources, while this is one
    /// stream PER container, merged like a pod fan-out.
    ///
    /// APPENDED (v14), not inserted, so the three older tags keep their
    /// wire values.
    Set(Vec<String>),
}

impl LogContainer {
    /// Short user-facing label for headers and filenames. Lives here, next
    /// to the variants, so the UI never re-derives a name for the sentinel
    /// cases from a magic string.
    pub fn label(&self) -> std::borrow::Cow<'_, str> {
        match self {
            LogContainer::All => "all".into(),
            LogContainer::Named(n) => n.as_str().into(),
            LogContainer::Set(names) => format!("{} containers", names.len()).into(),
            LogContainer::Default => "(default)".into(),
        }
    }
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
    pub target: LogTarget,
    pub namespace: Namespace,
    pub container: LogContainer,
    pub follow: bool,
    pub tail: Option<u64>,
    /// How far back to reach, or `None` for "as far as there is" — see
    /// [`LogSince`]. `None` with `tail: None` is the whole history, which is
    /// a state a duration cannot express and the reason this is not just a
    /// number.
    pub since: Option<LogSince>,
    pub previous: bool,
}

/// Declare [`LogSince`], its complete `ALL`, and both projections from ONE
/// list of `Variant => ("label", seconds)`.
///
/// The same shape as [`crate::resource_def::BuiltInKind`]'s declaration and
/// `ViewClass`'s, and for the same reason: Rust cannot enumerate an enum's
/// variants, so a hand-written "every window" list is a second copy by
/// construction. Here it buys something extra — a window's LABEL and its
/// DURATION are written on one line, side by side, so the two cannot
/// disagree. That disagreement is the whole subject of this type: the field
/// used to be a string, and a label that said one thing while the stream
/// showed another was expressible.
macro_rules! log_windows {
    ($(#[$m:meta])* pub enum $name:ident {
        $($variant:ident => ($label:literal, $secs:expr)),* $(,)?
    }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name { $($variant),* }

        impl $name {
            /// Every window, generated from the declaration, in the order the
            /// number keys bind them.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// What the user sees, and what `kubectl --since` would accept
            /// verbatim.
            pub fn label(self) -> &'static str {
                match self { $($name::$variant => $label),* }
            }

            /// The window as a count of seconds — the one grammar both
            /// backends share, and what each is given rather than a string
            /// to re-parse.
            pub fn seconds(self) -> u32 {
                match self { $($name::$variant => $secs),* }
            }
        }
    };
}

log_windows! {
    /// How far back a log stream reaches: one of the windows the UI offers.
    ///
    /// A closed set, on the wire, because this field has TWO readers with
    /// different parsers. It used to be an `Option<String>`, passed raw to
    /// `kubectl --since` on one path and run through the daemon's own
    /// `parse_since_to_seconds` on the other — and that parser DROPPED the value
    /// when it did not understand it, leaving the API path with no window at all.
    /// So `since: "1h30m"` meant ninety minutes through kubectl and the entire
    /// history through the apiserver, with nothing on screen to say which
    /// backend had served (the API is tried first, kubectl is the fallback).
    /// The same `LogInit` meaning two different things depending on who read it
    /// is exactly what the namespace field's doc two screens up says the wire
    /// type must not be able to express.
    ///
    /// It was dormant only by luck: the producers were six string literals, all
    /// of them inside the parser's grammar. The grammar lived in no type, so
    /// nothing kept the seventh literal honest.
    ///
    /// Each variant carries its own label, so the hint bar's text and the window
    /// it asks for cannot disagree either.
    pub enum LogSince {
        OneMinute => ("1m", 60),
        FiveMinutes => ("5m", 5 * 60),
        FifteenMinutes => ("15m", 15 * 60),
        ThirtyMinutes => ("30m", 30 * 60),
        OneHour => ("1h", 60 * 60),
        OneDay => ("24h", 24 * 60 * 60),
    }
}

// ---------------------------------------------------------------------------
// Control-substream wire types (existing SessionCommand / SessionEvent,
// narrowed to one-shot request/response commands + global events)
// ---------------------------------------------------------------------------

/// Wire-format revision — a MARKER FOR HUMANS AND TESTS, read by nothing at
/// runtime.
///
/// It used to be the handshake's identity, and that was the debt: a number
/// you had to remember to bump, whose failure mode when you forgot was
/// SILENT. bincode is positional, so a new client against an old daemon does
/// not error — it decodes bytes into the wrong variant. A discipline standing
/// in for a mechanism, which is the same shape as a doc claiming a guarantee
/// nothing enforces.
///
/// The handshake now exchanges [`build_id`], which is derived and therefore
/// cannot be forgotten. This number stays for the two jobs it is actually
/// good at, neither of which is gating a connection:
///
/// 1. The history below — why the wire changed, in order. That is
///    documentation, and deriving an identity does not write it for you.
/// 2. A deliberate-break tripwire the golden tests hang off
///    (`stream_wire_tags_are_stable` asserts this value, so a conscious wire
///    break forces a re-read of the pinned bincode tags). A derived
///    fingerprint cannot do this — it changes when a comment changes, so
///    nothing can assert on it.
///
/// Forgetting to bump it now fails a test instead of corrupting a decode.
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
///
/// v12: two changes, one break.
/// - `ClusterIdentity`'s three fields become `Option<String>` — blank was
///   doing double duty as "the apiserver/kubeconfig didn't tell us", the same
///   shape that let `current-context: ""` reach the daemon as a real name
///   (see [`ContextName`]). `Option` is a discriminant byte on the wire, so
///   this alone is a break even though nothing was added or reordered.
/// - `DaemonStatus` gains [`CacheStats`], so `ctl status` can report what the
///   daemon is actually holding open (watchers, contexts, port-forwards)
///   rather than just pid and uptime.
///
/// v13: `LogInit.pod: String` becomes [`LogTarget`] — one pod, an explicit
/// list, or a label selector the DAEMON resolves. The old field was a
/// `kubectl`-shaped string that happened to accept `deployments/foo`, which
/// kubectl then resolved to a single arbitrary pod; a workload's logs are
/// every replica's logs, and that can't be said in one `String`.
///
/// v14: both of a log stream's axes become sets, so that a MARKED SELECTION
/// of anything can be one merged stream.
/// - `LogTarget::Many(Vec<String>)` → `Union { targets, label }`.
///   A selection is heterogeneous — two deployments and a bare pod are two
///   `Selector`s and a `One` — which a flat list of pod names could not say,
///   so batch logs refused every non-pod row. (Shipped recursive; made flat
///   over [`LogLeaf`] later, after the recursion proved to be an unbounded
///   decode and nothing had ever nested a `Union`.)
/// - `LogContainer` gains `Set(Vec<String>)`: the marked containers of one
///   pod, streamed one apiece and merged.
///
/// v15: the reverse lookup stops being a guess.
/// - `ResourceRow` gains `references: Vec<ResourceRefInfo>` — the objects a
///   pod's spec NAMES (claims, secrets, configmaps, its service account),
///   extracted at conversion like every other typed field.
/// - `SubscriptionFilter::References { kind, name }` filters on them,
///   post-applied server-side for the same reason `OwnerUid` is: no field
///   selector can select on a spec. `U` used to grep the pod list for the
///   resource's NAME and call the result "pods referencing X".
/// - `K8sFieldSelector::InvolvedObject` (appended) carries the `E` drill —
///   the events ABOUT one object.
///
/// v16: `LogLine` gains `timestamp: Option<String>`. The daemon now passes
/// `--timestamps` / `timestamps: true` and splits the kubelet's stamp off
/// itself, because the renderer's heuristic was scoring application text and
/// deleting it from the body when timestamps were toggled off.
///
/// v17: [`LogTarget`] loses its recursion. `Union`'s members become
/// [`LogLeaf`], a three-variant enum that cannot hold a union, so
/// `Union { targets: [Union { .. }] }` is no longer a sentence the wire can
/// say. Nothing had ever built one; what the recursion did build was an
/// unbounded decode — ~20 bytes per level under a 64 MiB
/// [`MAX_MESSAGE_SIZE`], so 200 KB from any client overflowed the stack and
/// aborted the daemon with every session on it. Tags move: `LogTarget` is
/// `Leaf=0, Union=1` and `LogLeaf` is `One=0, Selector=1, Owner=2`.
/// v18: [`LogInit::since`] becomes `Option<`[`LogSince`]`>`, a closed set,
/// instead of `Option<String>`. The field had two readers with two parsers:
/// `kubectl --since` took the string verbatim, while the apiserver path ran
/// the daemon's own parser and DROPPED the window when it failed — so one
/// `LogInit` could mean "the last ninety minutes" or "everything" depending
/// on which backend served it, invisibly. Both paths now derive from
/// [`LogSince::seconds`].
pub const WIRE_FORMAT_REVISION: u32 = 18;

/// This build's identity — what the handshake actually compares.
///
/// The daemon and the TUI are the SAME binary (`k9rs daemon` is a
/// subcommand), so skew here never comes from distribution; it comes from
/// PROCESS LIFETIME. A daemon started at 12:41 is still serving when you
/// rebuild at 14:34, and that is the only way the two ends can disagree.
/// So the honest question is not "which protocol version do you speak" but
/// "are you running my executable", and that is answerable without anyone
/// maintaining anything.
///
/// Derived from the running executable's identity on disk — device, inode,
/// size and mtime. Cheap (one `stat`, cached), and it moves whenever the
/// binary does. When cargo replaces the file, a running daemon keeps the old
/// inode open while a fresh client stats the new one, so the two compute
/// different ids and the mismatch is caught in the preamble.
///
/// Hashing is safe here precisely because both ends are one build: the value
/// never has to be stable across rustc versions or machines, only within a
/// single executable's own lifetime.
///
/// Falls back to [`WIRE_FORMAT_REVISION`] if the executable cannot be
/// stat'ed, which keeps the handshake meaningful rather than making every
/// peer look identical.
pub fn build_id() -> u64 {
    static ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *ID.get_or_init(|| {
        use std::hash::{Hash, Hasher};
        use std::os::unix::fs::MetadataExt;
        let Ok(meta) = std::env::current_exe().and_then(std::fs::metadata) else {
            return u64::from(WIRE_FORMAT_REVISION);
        };
        let mut h = std::collections::hash_map::DefaultHasher::new();
        meta.dev().hash(&mut h);
        meta.ino().hash(&mut h);
        meta.size().hash(&mut h);
        meta.mtime().hash(&mut h);
        meta.mtime_nsec().hash(&mut h);
        h.finish()
    })
}

/// Handshake magic — "K9RS" as a big-endian u32. Doubles as a poison
/// length: a pre-8 daemon reads these 4 bytes as a frame-length prefix of
/// ~1.26 GB, trips [`MAX_MESSAGE_SIZE`], and bails cleanly instead of
/// mis-parsing; a v8 peer reading a pre-8 peer's first frame sees a small
/// length where the magic should be and reports the mismatch.
pub const PROTOCOL_MAGIC: u32 = 0x4B39_5253;

/// Write the 12-byte connection preamble: MAGIC then [`build_id`], both
/// big-endian (matching the hand-rolled frame-length convention). The
/// client writes first; the server replies with its own preamble.
pub async fn write_handshake<W: AsyncWriteExt + Unpin>(writer: &mut W) -> anyhow::Result<()> {
    writer.write_all(&PROTOCOL_MAGIC.to_be_bytes()).await?;
    writer.write_all(&build_id().to_be_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Read and validate the peer's 12-byte preamble. Errors distinguish a peer
/// that speaks no preamble at all from one that is simply a different build,
/// so the caller can surface an actionable message.
pub async fn read_handshake<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<()> {
    let mut buf = [0u8; 12];
    reader.read_exact(&mut buf).await?;
    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let peer = u64::from_be_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    if magic != PROTOCOL_MAGIC {
        anyhow::bail!(
            "incompatible peer (no protocol handshake). Restart the daemon: pkill k9rs"
        );
    }
    if peer != build_id() {
        // Not "which version" — the two ends are the same binary, so a
        // mismatch means one of them is a stale PROCESS, not an old release.
        // Two build ids have no ordering, so there is nothing useful to say
        // about which is newer; say what to do instead.
        //
        // Restarting is the USER'S call. A self-exit on binary change lived
        // here briefly and was wrong: the daemon holds port-forwards, shells
        // and log streams, so a rebuild would have torn down live work that
        // had nothing to do with the rebuild.
        anyhow::bail!(
            "the running daemon is a different build of k9rs than this client \
             (it was started from an older binary). Restart it when convenient: \
             pkill k9rs"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Subscription stream payloads (see WIRE_FORMAT_REVISION)
// ---------------------------------------------------------------------------

/// Full authoritative state of one subscribed resource. Reset semantics:
/// the receiver REPLACES everything it holds for this stream. Row order is
/// unspecified — the client owns sorting. Headers ride only here (a
/// header-changing event on the daemon always produces a fresh baseline).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBaseline {
    pub resource: ResourceId,
    pub headers: Vec<String>,
    pub rows: Vec<crate::row::ResourceRow>,
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
    Upsert(crate::row::ResourceRow),
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
    /// Fetch the API surface (CRDs, namespaces) now.
    ///
    /// THE CLIENT NEVER SENDS THIS, and the sender was deleted 2026-10-14 as
    /// dead code: the daemon fetches discovery eagerly when a session opens
    /// (`run_session_inner`) and refreshes it on `discoveryRefreshSecs`, so
    /// there has never been a moment the client had to ask. The variant and
    /// its handler stay because removing a variant renumbers the ones after
    /// it — a protocol break, and a `pkill`, to delete something harmless.
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
    /// The variant's NAME, with nothing from its payload.
    ///
    /// Exists so an error path can say which command arrived without
    /// `{:?}`-ing the value: `Init` carries `kubeconfig_yaml` (inline
    /// `client-key-data` and all) plus every forwarded env var, and the
    /// management door used to Debug-format whatever it got straight back to
    /// the peer. Exhaustive, so a new command cannot be reported as some
    /// other command's name.
    pub fn kind_name(&self) -> &'static str {
        match self {
            SessionCommand::Init { .. } => "Init",
            SessionCommand::Describe(_) => "Describe",
            SessionCommand::Yaml(_) => "Yaml",
            SessionCommand::Delete(_) => "Delete",
            SessionCommand::ForceKill(_) => "ForceKill",
            SessionCommand::Scale { .. } => "Scale",
            SessionCommand::Restart(_) => "Restart",
            SessionCommand::GetDiscovery => "GetDiscovery",
            SessionCommand::Apply { .. } => "Apply",
            SessionCommand::DecodeSecret(_) => "DecodeSecret",
            SessionCommand::TriggerCronJob(_) => "TriggerCronJob",
            SessionCommand::ToggleSuspendCronJob(_) => "ToggleSuspendCronJob",
            SessionCommand::PortForward { .. } => "PortForward",
            SessionCommand::Ping => "Ping",
            SessionCommand::Status => "Status",
            SessionCommand::Shutdown => "Shutdown",
            SessionCommand::Clear { .. } => "Clear",
        }
    }

    /// One line safe to write to a log file: the command's kind and its
    /// SUBJECT, never its payload.
    ///
    /// An allowlist by inclusion — every arm names the fields it prints and
    /// `..`-ignores the rest, so a new field is invisible here by default
    /// and a new variant is a compile error. The daemon's session loop used
    /// to do the opposite: special-case `Init` and `Apply` out and `{:?}`
    /// everything else. That is an allowlist by EXCLUSION, where the next
    /// variant carrying a body leaks by default, into a log file that
    /// outlives the session and may be world-readable under a lax umask.
    /// Today `Init` carries `kubeconfig_yaml` with inline `client-key-data`
    /// plus the session's whole forwarded env, and `Apply` carries — for a
    /// Secret — the decoded YAML. (The management door's post-mortem is the
    /// same shape: a `{:?}` echoed a misdirected `Init` straight back to
    /// the peer. It answers with [`Self::kind_name`], which is the kind
    /// alone because the peer is being told what IT sent.)
    ///
    /// A resource's plural/name and a port number are not secrets, and they
    /// are what makes a log worth reading.
    pub fn log_summary(&self) -> String {
        let kind = self.kind_name();
        match self {
            // Nothing beyond the name is safe or useful.
            Self::Init { .. } | Self::GetDiscovery | Self::Ping | Self::Status
            | Self::Shutdown => kind.to_string(),
            Self::Clear { context } => match context {
                Some(c) => format!("{kind} {}", c.as_str()),
                None => format!("{kind} (all contexts)"),
            },
            Self::Describe(target)
            | Self::Yaml(target)
            | Self::Delete(target)
            | Self::ForceKill(target)
            | Self::Restart(target)
            | Self::DecodeSecret(target)
            | Self::TriggerCronJob(target)
            | Self::ToggleSuspendCronJob(target)
            | Self::Apply { target, .. } => format!("{kind} {}", target.kubectl_target()),
            Self::Scale { target, replicas } => {
                format!("{kind} {} to {replicas}", target.kubectl_target())
            }
            Self::PortForward { target, local_port, container_port } => format!(
                "{kind} {} {local_port}:{container_port}",
                target.kubectl_target(),
            ),
        }
    }

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
/// `source` names where the line came from, and is `Some` exactly when the
/// stream has more than one source to distinguish:
///
/// - `--all-containers` on one pod → the container name (the daemon strips
///   kubectl's own `[pod/container] ` prefix and tags it here),
/// - a [`LogTarget`] fan-out → the pod name,
/// - both at once → `pod/container`.
///
/// A single-source stream leaves it `None`: there is one source and the view
/// header already names it.
///
/// Why typed: kubectl `--all-containers` auto-enables `--prefix`, emitting
/// `[pod/container] body` on every line. The old wire shipped that raw `String`
/// and the UI re-derived the container with a first-word heuristic that rejected
/// the real `/`-bearing prefix outright — so the prefix leaked into the rendered
/// body and never colored. The daemon knows the source, so it tags it once here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub source: Option<String>,
    /// The RFC3339 stamp the kubelet recorded, when the daemon ASKED for it
    /// (it always does — see `stream_logs_via_api`). Split off here for the
    /// same reason `source` is: the renderer used to sniff one out of
    /// `content` with a "starts with four digits and a dash" heuristic, which
    /// scored the application's own `[2026-09-17 10:00:00] …` and then, with
    /// timestamps toggled OFF, rendered the line without it. A field the
    /// producer fills cannot delete the consumer's text.
    pub timestamp: Option<String>,
    pub content: String,
}

/// The parts of a log line a view can independently show, hide, search and
/// export.
///
/// CLOSED on purpose. Every question that used to be asked of a line by
/// pattern-matching on which `Option` was populated — is this shown, is this
/// searchable, how is it coloured — is asked of a role instead, once. Those
/// answers used to be spelled out separately in the measurer and the
/// renderer, which is why `show_source` appears as the identical
/// `then_some(..).flatten()` in both, with a test pinning that the two copies
/// agree.
///
/// The roles are declared here because they ARE the wire fields. The
/// PROJECTION of a line into them lives with the stored line
/// (`app::store::StoredLine::segments`), because the body it yields is the
/// derived visible text and only the client has that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The daemon's tag for where the line came from. Only present when more
    /// than one source feeds the stream.
    Source,
    /// The kubelet's ingestion stamp, split into a typed field by the daemon.
    Timestamp,
    /// What the workload actually wrote — the only part carrying its own ANSI.
    Body,
}

impl Role {
    /// Whether a grep runs over this part. The body and the source tag are
    /// content; the timestamp is chrome, and matching it would make `/2026`
    /// select every line. Previously an unstated choice duplicated in
    /// `LogFilter::drain_from_parent` and the draft derive.
    pub fn searchable(self) -> bool {
        matches!(self, Role::Source | Role::Body)
    }

    /// The hideable roles, in SHED ORDER — first to go is the one whose
    /// columns buy the least.
    ///
    /// The timestamp leads because it is 31 columns duplicating a stamp the
    /// service almost always prints itself; the source tag is 22 columns of
    /// information nothing else carries, and only exists when more than one
    /// pod is feeding the stream. `Body` is absent because it is never
    /// hidden: shedding chrome trades width for context, it does not remove
    /// what the workload said.
    pub const SHED_ORDER: [Role; 2] = [Role::Timestamp, Role::Source];

    /// Visible at this shed level. One key cycles the level, so adding a
    /// role to `SHED_ORDER` extends the cycle — no new binding, no new
    /// boolean, and no unreachable combination of them.
    pub fn shown(self, shed: usize) -> bool {
        match Self::SHED_ORDER.iter().position(|r| *r == self) {
            Some(i) => i >= shed,
            None => true,
        }
    }

    /// How many levels the cycle has: every role shed one at a time, plus
    /// the state where all of them are showing.
    pub const fn levels() -> usize {
        Self::SHED_ORDER.len() + 1
    }
}

impl LogLine {
    /// A line from a single-source stream — no per-line tag, and no stamp
    /// (this is k9rs speaking, not the workload).
    pub fn untagged(content: impl Into<String>) -> Self {
        Self { source: None, timestamp: None, content: content.into() }
    }

    /// Split kubelet's `--timestamps` prefix off a raw line. Safe only
    /// because we request it; see [`Self::timestamp`].
    pub fn split_timestamp(line: &str) -> (Option<String>, &str) {
        // `2026-09-17T10:00:00.123456789Z rest…` — one token, then a space.
        let Some(sp) = line.find(' ') else { return (None, line) };
        let (head, rest) = line.split_at(sp);
        let plausible = head.len() >= 20
            && head.ends_with('Z')
            && head.as_bytes()[..4].iter().all(u8::is_ascii_digit)
            && head.as_bytes()[4] == b'-';
        if plausible {
            (Some(head.to_string()), &rest[1..])
        } else {
            (None, line)
        }
    }

}

/// What a log stream is OVER.
///
/// Replaces a bare `pod: String`. That field was always a kubectl-style ref
/// rather than strictly a pod name — a deployment sent `deployments/my-app`,
/// which kubectl resolves to ONE of its pods — so "show me this workload's
/// logs" silently meant "show me one arbitrary replica's logs".
///
/// Fan-out lives on the DAEMON side and merges into the single existing
/// substream, so the client keeps one `LogStream` over one `LineStore` under
/// one epoch. That matters: the epoch floor is what rejects a superseded
/// stream's stragglers, and it assumes one writer per store — N client-side
/// substreams sharing a store would floor each other out.
/// ONE log source, resolvable on its own.
///
/// Split out of [`LogTarget`] so that a union of unions is UNWRITABLE. The
/// nesting bought nothing: a selection is heterogeneous because its members
/// have different *shapes*, which a `Vec` of this three-variant enum already
/// expresses — `Union` was never built holding another `Union`, by any path.
/// What the recursion did buy was an unbounded decode: ~20 bytes of wire per
/// level against a 64 MiB [`MAX_MESSAGE_SIZE`], so 200 KB from any client
/// overflowed the stack and **aborted the whole daemon**, every other
/// session with it. A depth counter would have been a policy to tune and
/// forget; a non-recursive type cannot be made to recurse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLeaf {
    /// One pod, or a kubectl-style ref that resolves to one. Lines are
    /// untagged, exactly as before.
    One(String),
    /// Every pod matching this label selector — a workload. Resolved by the
    /// daemon at stream time, so a replica that appears later is included on
    /// the next restart rather than being invisible forever.
    Selector { labels: BTreeMap<String, String>, label: String },
    /// Every pod owned by this object UID — a job or replicaset, which
    /// identify their pods by `ownerReference` rather than by a selector
    /// (a job's `controller-uid` label is an implementation detail, and a
    /// replicaset shares its deployment's labels with its siblings).
    Owner { uid: String, label: String },
}

impl LogLeaf {
    /// A short human label for the view header. The indirect shapes carry
    /// their own — a header reading `app=api,tier=web` is the selector, not
    /// the thing you drilled into.
    pub fn display(&self) -> String {
        match self {
            LogLeaf::One(name) => name.clone(),
            LogLeaf::Selector { label, .. } | LogLeaf::Owner { label, .. } => label.clone(),
        }
    }

    /// Whether this source can produce lines from more than one pod.
    pub fn is_fanout(&self) -> bool {
        match self {
            LogLeaf::One(_) => false,
            LogLeaf::Selector { .. } | LogLeaf::Owner { .. } => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogTarget {
    /// One source — `Ctrl-L` on a row.
    Leaf(LogLeaf),
    /// Everything these sources resolve to, merged — a marked selection.
    ///
    /// Heterogeneous: marking two deployments and a bare pod is a `Selector`,
    /// a `Selector` and a `One`, and the daemon resolving each in turn is the
    /// only place that can be decided (the client may never have loaded any
    /// of their pods). The daemon dedupes, so marking a deployment AND one of
    /// its pods streams that pod once. Flat by type — see [`LogLeaf`].
    Union { targets: Vec<LogLeaf>, label: String },
}

impl LogTarget {
    /// One pod or kubectl-style ref, the common case — spelled short because
    /// `LogTarget::Leaf(LogLeaf::One(..))` at a dozen call sites reads as
    /// though the nesting mattered.
    pub fn one(name: impl Into<String>) -> Self {
        LogTarget::Leaf(LogLeaf::One(name.into()))
    }

    /// A short human label for the view header ("3 pods", "deploy/api").
    pub fn display(&self) -> String {
        match self {
            LogTarget::Leaf(leaf) => leaf.display(),
            LogTarget::Union { label, .. } => label.clone(),
        }
    }

    /// Whether this target can produce lines from more than one pod.
    ///
    /// A *claim about the target*, not the tagging rule — the daemon tags by
    /// how many streams it actually opened, because a selector matching one
    /// replica has one source no matter what its shape promised.
    pub fn is_fanout(&self) -> bool {
        match self {
            LogTarget::Leaf(leaf) => leaf.is_fanout(),
            LogTarget::Union { targets, .. } => {
                targets.len() > 1 || targets.first().is_some_and(LogLeaf::is_fanout)
            }
        }
    }
}

impl From<LogLeaf> for LogTarget {
    fn from(leaf: LogLeaf) -> Self {
        LogTarget::Leaf(leaf)
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
#[path = "tests/protocol.rs"]
mod tests;

/// Golden byte-layout tests for the positionally-encoded wire types.
///
/// The roundtrip tests above serialize-then-deserialize with the SAME code, so
/// they pass even if a `CellValue` variant is reordered or a `ResourceRow`
/// field is inserted/removed — a daemon and a TUI built from different revisions
/// would then silently misread each other's frames (bincode is positional;
/// nothing self-describing is on the wire — the only thing standing between
/// that and a corrupt decode is the [`build_id`] preamble, which refuses the
/// connection outright).
///
/// These tests freeze the exact bytes. If one fails you changed an on-wire
/// shape: that is a breaking change — bump [`WIRE_FORMAT_REVISION`] (the
/// marker, so this test's tripwire fires), update the golden here, and ship
/// daemon + TUI together. The runtime refusal is automatic; the bump is for
/// the humans and the tripwire.
#[cfg(test)]
#[path = "tests/protocol_golden.rs"]
mod wire_layout_golden;
