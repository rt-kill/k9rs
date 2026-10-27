//! Core trait hierarchy for the resource type system.
//!
//! Every built-in resource implements [`ResourceDef`] for identity and
//! metadata. Each def declares its supported [`OperationKind`] set via
//! [`ResourceDef::operations`]; capability gating uses that set directly.
//!
//! The `BuiltInKind` enum and `Gvr` struct are used on BOTH client and
//! server side — `ResourceId::BuiltIn(kind)` carries them over the wire.
//! Capability gating happens server-side; the client receives
//! [`crate::protocol::ResourceCapabilities`] and [`crate::row::DrillTarget`] and doesn't reach into
//! the trait hierarchy.

use serde::{Deserialize, Serialize};

use crate::protocol::{OperationKind, ResourceId, ResourceScope};


/// The flavor of metrics-server overlay a resource receives. Different
/// kinds are merged from different APIs into different columns, so the
/// session event loop dispatches on this enum instead of string-matching
/// on the resource plural.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsKind {
    /// Pod-level CPU/MEM from the metrics-server pod API.
    Pod,
    /// Node-level CPU%/MEM% from the metrics-server node API.
    Node,
}

// ---------------------------------------------------------------------------
// Column metadata — typed replacement for the EXTRA_COLUMNS string table
// ---------------------------------------------------------------------------

/// Which metrics overlay column this is, if any. Used by
/// `apply_pod_metrics` / `apply_node_metrics` to find the column index
/// by typed tag instead of by header string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricsColumn {
    Cpu,
    Mem,
    /// Allocatable CPU/MEM columns. NOT metrics-server data — filled at row
    /// build from `node.status.allocatable` — but tagged so the metrics overlay
    /// can locate them and read their `Quantity` value to compute the percent
    /// columns (`usage * 100 / allocatable`).
    CpuAlloc,
    MemAlloc,
    CpuPercent,
    MemPercent,
    CpuPercentRequest,
    CpuPercentLimit,
    MemPercentRequest,
    MemPercentLimit,
}

/// Display level for table columns. A column is visible when the user's
/// current display level >= the column's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ColumnLevel {
    Default = 0,
    Extra = 1,
}

impl ColumnLevel {
    pub fn next(self) -> Self {
        match self {
            ColumnLevel::Default => ColumnLevel::Extra,
            ColumnLevel::Extra => ColumnLevel::Default,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ColumnLevel::Default => "default",
            ColumnLevel::Extra => "extra",
        }
    }
}

/// Metadata for a single **explicitly-tagged** table column: its `&'static`
/// header plus display level and optional metrics overlay. Defs that need
/// non-default levels or a metrics overlay (pods, nodes, services, events)
/// return these from `column_defs()`, tagging columns via the `const fn`
/// constructors. Columns a def does NOT tag have their level inferred from the
/// header by [`ColumnDef::infer`] — the same classification that used to live
/// in the disconnected `EXTRA_COLUMNS` / `column_sort_kind_for_header` tables
/// in `app/mod.rs`.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDef {
    pub header: &'static str,
    pub level: ColumnLevel,
    pub metrics: Option<MetricsColumn>,
    /// Per-column width ceiling. `None` = fall back to the global
    /// `ui.maxColumnWidth`, which is a last resort, not a policy: one number
    /// for every column of every resource means a column whose values are
    /// long-but-uninteresting (an EKS cluster ARN, a full image ref) eats the
    /// width that the columns you are actually reading needed. Declared HERE,
    /// next to the column it describes, so it travels with the column instead
    /// of living in a table someone has to remember to update.
    pub max_width: Option<u16>,
    /// Whether consecutive EQUAL values in this column collapse to one, when
    /// it is the column being sorted on.
    ///
    /// OPT-IN, and declared next to the column, because the two cases look
    /// identical from the data and read in opposite ways. Help's `SECTION` is
    /// a category you grouped BY, so thirteen rows repeating "Everywhere"
    /// down the left edge is noise. A resource's `AGE` is a per-row fact you
    /// scan, and a ReplicaSet's pods are all created in the same second — so
    /// collapsing it blanked eighteen rows and read as MISSING DATA. (It was
    /// reported that way: "is the age not properly populated for every single
    /// row?") Inferring from "is it sorted" got that backwards for every
    /// resource column, which is why the column now says.
    pub groups: bool,
}

impl ColumnDef {
    pub const fn new(header: &'static str) -> Self {
        Self { header, level: ColumnLevel::Default, metrics: None, max_width: None, groups: false }
    }
    pub const fn extra(header: &'static str) -> Self {
        Self { header, level: ColumnLevel::Extra, metrics: None, max_width: None, groups: false }
    }
    /// Cap this column's width regardless of how long its values run.
    pub const fn max_width(self, w: u16) -> Self {
        Self { max_width: Some(w), ..self }
    }

    /// Collapse consecutive equal values when sorted on this column — for a
    /// column that is a CATEGORY, not a per-row measurement. See `groups`.
    pub const fn groups(self) -> Self {
        Self { groups: true, ..self }
    }
    // (`age`/`extra_age` ctors removed — they were byte-identical to
    // `new`/`extra`. Age columns are rendered from `CellValue::Age` in the
    // row's cells, not from any `ColumnDef` flag, so the level is all that
    // mattered; `ColumnSortKind` that once distinguished them is long gone.)
    pub const fn with_metrics(self, m: MetricsColumn) -> Self {
        Self { metrics: Some(m), ..self }
    }

    /// Classify a column's display level from its header string — the DEFAULT
    /// rule for any column a def doesn't explicitly tag, using the same rules
    /// that lived in the disconnected `EXTRA_COLUMNS` / `column_sort_kind`
    /// tables. Returns only the [`ColumnLevel`]: inference never produces a
    /// metrics overlay, and the header belongs to the caller (it owns the
    /// runtime `String`), so a bare level is the whole result — no
    /// `&'static`-vs-runtime lifetime mismatch to paper over with an empty
    /// header. Defs needing non-inferable metadata tag columns explicitly via
    /// `column_defs()` and the `const fn` constructors above.
    pub fn infer(header: &str) -> ColumnLevel {
        let upper = header.to_ascii_uppercase();
        let is_extra = matches!(
            upper.as_str(),
            "LABELS" | "CONTAINERS" | "IMAGES" | "SELECTOR" | "QOS"
            | "SERVICE-ACCOUNT" | "READINESS GATES" | "LAST RESTART"
            | "NODE SELECTOR" | "INTERNAL-IP" | "EXTERNAL-IP" | "ARCH"
            | "TAINTS" | "CPU" | "MEM" | "CPU%" | "MEM%"
            // NOTE: "MESSAGE" is intentionally absent — EventDef tags it Default
            // explicitly, so leaving it out of inference keeps the two
            // classifications from disagreeing for any other MESSAGE column.
        );
        if is_extra { ColumnLevel::Extra } else { ColumnLevel::Default }
    }
}

// ---------------------------------------------------------------------------
// BuiltInKind — closed enum of every built-in K8s resource the daemon serves
// ---------------------------------------------------------------------------

/// Declare `BuiltInKind` and its complete `ALL` from ONE list.
///
/// Rust cannot enumerate an enum's variants, so any "every kind" list is a
/// second copy by construction — and a hand-maintained `BuiltInKind::ALL`
/// previously existed, was found to be exactly that, and was deleted. This
/// generates the list from the declaration instead, so the copy cannot
/// disagree: adding a variant adds it to `ALL`, and `ALL` is what the
/// registration guard walks.
macro_rules! built_in_kinds {
    ($(#[$m:meta])* pub enum $name:ident { $($variant:ident => $def:path),* $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name { $($variant),* }

        impl $name {
            /// Every variant, generated from the enum's own declaration.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// This kind as a [`ResourceId`].
            ///
            /// Symmetric with
            /// [`LocalResourceKind::to_resource_id`](crate::local_types::LocalResourceKind::to_resource_id),
            /// which had it and this did not — so the TUI grew a
            /// `nav::rid(kind)` helper three crates up, in the navigation
            /// module, duplicating the `From` impl below. A missing method on
            /// one of two sibling enums is how a constructor ends up filed
            /// under something unrelated.
            pub fn to_resource_id(self) -> ResourceId {
                ResourceId::from(self)
            }

            /// This kind's definition — identity, aliases, columns, operations.
            ///
            /// Generated from the SAME declaration as the enum and `ALL`, so
            /// the mapping cannot be partial: a new variant without a def is a
            /// compile error in the generated match, not a gap some test has
            /// to notice. That is what the registry could not offer — it was
            /// built by 34 hand-written `register_*` calls, and a variant
            /// nobody registered panicked in `by_kind` under a user's cursor
            /// until `every_built_in_kind_is_registered` was added to catch it.
            /// Here there is nothing left to catch.
            ///
            /// Returns `&'static` because every def is a unit struct, so the
            /// reference is const-promoted — no registry, no `LazyLock`, no
            /// allocation, and reachable without naming kube-rs.
            pub fn def(self) -> &'static dyn ResourceDef {
                match self { $($name::$variant => &$def),* }
            }
        }
    };
}
built_in_kinds! {
    /// Closed enum of every built-in resource type the daemon knows about.
    ///
    /// The canonical typed identity for built-ins — [`ResourceId::BuiltIn`]
    /// wraps it, and [`BuiltInKind::def`] maps it to its definition.
    ///
    /// Declared through `built_in_kinds!` so that [`BuiltInKind::ALL`] is
    /// generated FROM this list and cannot omit a variant. That is what makes
    /// `every_built_in_kind_is_registered` a real guarantee: the registry's
    /// `registry_consistency` test walks the LIVE registry, so it can only ever
    /// see kinds that were registered — a variant nobody called `register_*` for
    /// was invisible to it and panicked in `by_kind` the first time a user
    /// navigated there, in a TUI.
    ///
    /// The doc here used to claim "adding a variant forces an update to every
    /// exhaustive match downstream". There were NO exhaustive matches over this
    /// enum, so nothing was forced and the claim was simply untrue; the rest of
    /// it leaned on "caught during any test that exercises the new variant",
    /// which required somebody to write that test.
    ///
    /// WIRE-ORDERED: the enum derives `Serialize`/`Deserialize` and rides
    /// `ResourceId` on the wire, so variants are APPENDED, never reordered.
    pub enum BuiltInKind {
    // Workloads
    Pod => pods::PodDef,
    Deployment => deployments::DeploymentDef,
    StatefulSet => statefulsets::StatefulSetDef,
    DaemonSet => daemonsets::DaemonSetDef,
    ReplicaSet => replicasets::ReplicaSetDef,
    Job => jobs::JobDef,
    CronJob => cronjobs::CronJobDef,

    // Core / config / network
    Service => services::ServiceDef,
    ConfigMap => configmaps::ConfigMapDef,
    Secret => secrets::SecretDef,
    ServiceAccount => serviceaccounts::ServiceAccountDef,
    Ingress => ingresses::IngressDef,
    NetworkPolicy => networkpolicies::NetworkPolicyDef,
    Hpa => hpa::HpaDef,
    Endpoints => endpoints::EndpointsDef,
    EndpointSlice => endpointslices::EndpointSliceDef,
    LimitRange => limitranges::LimitRangeDef,
    ResourceQuota => resourcequotas::ResourceQuotaDef,
    PodDisruptionBudget => pdb::PodDisruptionBudgetDef,
    Event => events::EventDef,
    PersistentVolumeClaim => pvcs::PvcDef,
    Lease => leases::LeaseDef,

    // Cluster
    Namespace => namespaces::NamespaceDef,
    Node => nodes::NodeDef,
    PersistentVolume => pvs::PvDef,
    StorageClass => storageclasses::StorageClassDef,
    PriorityClass => priorityclasses::PriorityClassDef,
    Role => roles::RoleDef,
    ClusterRole => clusterroles::ClusterRoleDef,
    RoleBinding => rolebindings::RoleBindingDef,
    ClusterRoleBinding => clusterrolebindings::ClusterRoleBindingDef,
    ValidatingWebhookConfiguration => webhooks::ValidatingWebhookDef,
    MutatingWebhookConfiguration => webhooks::MutatingWebhookDef,
    CustomResourceDefinition => crds::CrdDef,
}
}

// ---------------------------------------------------------------------------
// The definition table — metadata lookup without the watcher registry
// ---------------------------------------------------------------------------

/// Every built-in definition, in declaration order.
///
/// `BuiltInKind::def` makes the per-kind lookup free, so this is the whole
/// table: no registry object, no registration calls, and nothing here names
/// kube-rs. The `ResourceRegistry` still exists, but only for what genuinely
/// needs the k8s types — spawning a typed watcher.
pub fn all_defs() -> impl Iterator<Item = &'static dyn ResourceDef> {
    BuiltInKind::ALL.iter().map(|k| k.def())
}

/// Lowercased alias → kind, and plural → kind.
///
/// Indexed rather than scanned, because a scan over a `HashMap`'s values
/// resolved the first alias collision differently on each launch. Built once;
/// a duplicate alias or plural PANICS here, which is where the registry used
/// to reject it at registration time — the check moved with the data rather
/// than being dropped along the way.
/// The two string indexes over the definition table. Named rather than a
/// tuple so the two maps cannot be confused at the use site.
struct DefIndex {
    /// Lowercased alias → kind.
    by_alias: std::collections::HashMap<String, BuiltInKind>,
    /// Plural name → kind.
    by_plural: std::collections::HashMap<&'static str, BuiltInKind>,
}

static INDEX: std::sync::LazyLock<DefIndex> = std::sync::LazyLock::new(|| {
    let mut aliases = std::collections::HashMap::new();
    let mut plurals = std::collections::HashMap::new();
    for &kind in BuiltInKind::ALL {
        let def = kind.def();
        for alias in def.aliases() {
            let lower = alias.to_lowercase();
            if let Some(owner) = aliases.insert(lower.clone(), kind) {
                panic!("alias {lower:?} claimed by both {owner:?} and {kind:?}");
            }
        }
        if let Some(owner) = plurals.insert(def.gvr().plural, kind) {
            panic!("plural {:?} claimed by both {owner:?} and {kind:?}", def.gvr().plural);
        }
    }
    DefIndex { by_alias: aliases, by_plural: plurals }
});

/// Look up a definition by any of its aliases (case-insensitive).
pub fn def_by_alias(alias: &str) -> Option<&'static dyn ResourceDef> {
    INDEX.by_alias.get(&alias.to_lowercase()).map(|k| k.def())
}

/// Look up a definition by its plural name.
pub fn def_by_plural(plural: &str) -> Option<&'static dyn ResourceDef> {
    INDEX.by_plural.get(plural).map(|k| k.def())
}

// `BuiltInKind::ALL` used to live here as a hand-maintained `&[BuiltInKind]`
// slice, duplicating the enum's variants. It was deleted because:
//
// 1. Nothing in production code iterated it — only one test drift guard.
// 2. The drift guard it fed walked the slice, so a variant missing from
//    the slice was silently missing from test coverage — weaker than the
//    `registry_consistency` test which walks the live registry.
// 3. Iterating every kind is rare, and when needed, the registry itself
//    is the authoritative source.
//
// Use `crate::resource_def::all_defs().map(|d| d.kind())` when
// you need every kind — that's the real source of truth.

// ---------------------------------------------------------------------------
// Gvr — heap-free identity payload for built-ins
// ---------------------------------------------------------------------------

/// Group/Version/Resource identity payload. Built-in defs return a
/// `&'static Gvr` from [`ResourceDef::gvr`] — the data lives in a `const`
/// inside each impl, so the registry never copies strings.
///
/// CRDs use the same shape but with `String` fields (see
/// [`crate::protocol::CrdRef`]).
#[derive(Debug, Clone, Copy)]
pub struct Gvr {
    /// API group (e.g. "" for core, "apps", "batch").
    pub group: &'static str,
    /// API version (e.g. "v1", "v1beta1").
    pub version: &'static str,
    /// K8s-style kind (e.g. "Pod", "Deployment") — for display.
    pub kind: &'static str,
    /// Plural name (e.g. "pods", "deployments") — used in API URLs.
    pub plural: &'static str,
    /// Cluster- or namespace-scoped.
    pub scope: ResourceScope,
}

// ---------------------------------------------------------------------------
// ResourceDef — identity, metadata, operation set
// ---------------------------------------------------------------------------

/// The identity, metadata, and operation set of a resource type.
///
/// Object-safe so it can be stored as `dyn ResourceDef` in the registry.
pub trait ResourceDef: Send + Sync + 'static {
    /// Typed kind discriminant — the closed enum variant this def belongs
    /// to. The mapping back ([`BuiltInKind::def`]) is GENERATED from the
    /// enum's declaration, so the 1:1 correspondence is a compile error to
    /// break rather than something a test has to notice.
    fn kind(&self) -> BuiltInKind;

    /// Identity payload (group/version/kind/plural/scope) as a heap-free
    /// `&'static Gvr`. Built-in defs return a reference to a `const` inside
    /// the function body so the data has `'static` lifetime without any
    /// per-call allocation.
    fn gvr(&self) -> &'static Gvr;

    /// The full [`ResourceId`] — typed enum wrapping [`BuiltInKind`].
    /// Provided as a default that wraps `kind()`; defs never override.
    fn resource_id(&self) -> ResourceId {
        ResourceId::BuiltIn(self.kind())
    }

    /// Short aliases for command-mode lookup (e.g. `&["po", "pod", "pods"]`).
    fn aliases(&self) -> &[&str];

    /// Short UI label for tabs/breadcrumbs (e.g. "Deploy", "STS").
    fn short_label(&self) -> &str;

    /// The column headers for this resource's table view. The `cells` vec
    /// in [`crate::row::ResourceRow`] is built in this
    /// order, and the table widget renders headers and cells in lockstep.
    /// (We never wired up the K8s Table API for server-provided columns
    /// — this *is* the column layout.)
    fn default_headers(&self) -> Vec<String>;

    /// Explicitly-tagged column metadata (header + level + metrics overlay),
    /// positional with `default_headers()`. The default is EMPTY — a resource
    /// that tags nothing has each column's level inferred from its header on
    /// demand (see `app::column_level_for`), so there is no fake `&'static`
    /// header to invent for the runtime header strings. Defs that need
    /// non-default levels or a metrics overlay (pods, nodes, services, events)
    /// override this and tag columns via the `const fn` constructors — same
    /// optional-override shape as `is_core` / `metrics_kind` below.
    fn column_defs(&self) -> Vec<ColumnDef> {
        Vec::new()
    }

    /// Whether this resource is auto-subscribed on connection (e.g. namespaces
    /// for the namespace picker, nodes for drill-down). Override for the few
    /// resources that need permanent daemon-side watchers.
    fn is_core(&self) -> bool { false }

    /// Kind of metrics-server overlay this resource receives, if any.
    /// Used by the session event loop to dispatch the correct overlay
    /// application (pods merge CPU/MEM, nodes merge CPU%/MEM%). `None`
    /// for everything else.
    fn metrics_kind(&self) -> Option<MetricsKind> { None }

    /// Operations this resource supports. Each resource def overrides this
    /// to declare its own operations. Every resource gets Describe/Yaml/Delete
    /// by default.
    fn operations(&self) -> Vec<OperationKind> {
        vec![OperationKind::Describe, OperationKind::Yaml, OperationKind::Delete]
    }

}

// ---------------------------------------------------------------------------
// Capability flags
//
// Capabilities used to be empty marker traits (`Loggable`, `Scaleable`, etc.)
// opt'd into via `Option<&dyn Trait>` downcasts on `ResourceDef`. Since the
// markers had no methods, the trait-object indirection was pure ceremony —
// collapsed to `is_*` boolean overrides on `ResourceDef`. The per-operation
// bodies (scale patch, restart annotation, decode secret, etc.) live as
// free functions in the daemon's `ops` module.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/resource_def.rs"]
mod tests;

pub mod namespaces;
pub mod clusterrolebindings;
pub mod clusterroles;
pub mod configmaps;
pub mod crds;
pub mod cronjobs;
pub mod daemonsets;
pub mod deployments;
pub mod endpoints;
pub mod endpointslices;
pub mod events;
pub mod hpa;
pub mod ingresses;
pub mod jobs;
pub mod leases;
pub mod limitranges;
pub mod networkpolicies;
pub mod nodes;
pub mod pdb;
pub mod pods;
pub mod priorityclasses;
pub mod pvcs;
pub mod pvs;
pub mod replicasets;
pub mod resourcequotas;
pub mod rolebindings;
pub mod roles;
pub mod secrets;
pub mod serviceaccounts;
pub mod services;
pub mod statefulsets;
pub mod storageclasses;
pub mod webhooks;
