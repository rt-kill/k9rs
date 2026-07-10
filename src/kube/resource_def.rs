//! Core trait hierarchy for the resource type system.
//!
//! Every built-in resource implements [`ResourceDef`] for identity and
//! metadata. Each def declares its supported [`OperationKind`] set via
//! [`ResourceDef::operations`]; capability gating uses that set directly.
//!
//! The `BuiltInKind` enum and `Gvr` struct are used on BOTH client and
//! server side — `ResourceId::BuiltIn(kind)` carries them over the wire.
//! Capability gating happens server-side; the client receives
//! [`ResourceCapabilities`] and [`DrillTarget`] and doesn't reach into
//! the trait hierarchy.

use serde::{Deserialize, Serialize};

use crate::kube::protocol::{OperationKind, ResourceId, ResourceScope};
use crate::kube::resources::row::ResourceRow;


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
}

impl ColumnDef {
    pub const fn new(header: &'static str) -> Self {
        Self { header, level: ColumnLevel::Default, metrics: None }
    }
    pub const fn extra(header: &'static str) -> Self {
        Self { header, level: ColumnLevel::Extra, metrics: None }
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

/// Closed enum of every built-in resource type the daemon knows about. The
/// canonical typed identity for built-ins — [`ResourceId::BuiltIn`] wraps it,
/// and [`ResourceRegistry`](crate::kube::resource_defs::ResourceRegistry)
/// indexes defs by it.
///
/// The enum is closed at the type level: adding a variant forces an update
/// to every exhaustive match downstream, and the registry's
/// `registry_consistency` test walks the live registry (not a parallel
/// slice), so any def whose `kind()` disagrees with its registration key
/// fails at test time. Adding a variant without backing it with a
/// `ResourceDef` impl surfaces as a runtime panic on first use, which is
/// caught during any test that exercises the new variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltInKind {
    // Workloads
    Pod,
    Deployment,
    StatefulSet,
    DaemonSet,
    ReplicaSet,
    Job,
    CronJob,

    // Core / config / network
    Service,
    ConfigMap,
    Secret,
    ServiceAccount,
    Ingress,
    NetworkPolicy,
    Hpa,
    Endpoints,
    EndpointSlice,
    LimitRange,
    ResourceQuota,
    PodDisruptionBudget,
    Event,
    PersistentVolumeClaim,
    Lease,

    // Cluster
    Namespace,
    Node,
    PersistentVolume,
    StorageClass,
    PriorityClass,
    Role,
    ClusterRole,
    RoleBinding,
    ClusterRoleBinding,
    ValidatingWebhookConfiguration,
    MutatingWebhookConfiguration,
    CustomResourceDefinition,
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
// Use `crate::kube::resource_defs::REGISTRY.all().map(|d| d.kind())` when
// you need every kind — that's the real source of truth.

// ---------------------------------------------------------------------------
// Gvr — heap-free identity payload for built-ins
// ---------------------------------------------------------------------------

/// Group/Version/Resource identity payload. Built-in defs return a
/// `&'static Gvr` from [`ResourceDef::gvr`] — the data lives in a `const`
/// inside each impl, so the registry never copies strings.
///
/// CRDs use the same shape but with `String` fields (see
/// [`crate::kube::protocol::CrdRef`]).
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
    /// Typed kind discriminant — the closed enum variant that identifies
    /// this def inside [`crate::kube::resource_defs::ResourceRegistry`]. The
    /// registry's `kind_table_complete` test enforces a 1:1 mapping between
    /// `BuiltInKind` variants and registered defs.
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
    /// in [`crate::kube::resources::row::ResourceRow`] is built in this
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
// free functions in `crate::kube::server_session::ops`.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Typed converter trait
// ---------------------------------------------------------------------------

/// Converts a strongly-typed K8s API object into a [`ResourceRow`].
///
/// NOT object-safe (generic over K), so it lives on the concrete ZST,
/// not on `dyn ResourceDef`. The existing `*_to_row` free functions are
/// the implementations — `ConvertToRow` just delegates to them.
pub trait ConvertToRow<K> {
    fn convert(obj: K) -> ResourceRow;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::kube::resource_defs::REGISTRY;
    use crate::kube::protocol::OperationKind;

    /// Every resource must include the base operations (Describe, Yaml, Delete).
    #[test]
    fn all_resources_have_base_operations() {
        for def in REGISTRY.all() {
            let ops = def.operations();
            for base in [OperationKind::Describe, OperationKind::Yaml, OperationKind::Delete] {
                assert!(
                    ops.contains(&base),
                    "def `{}` missing base operation {:?}",
                    def.gvr().kind, base,
                );
            }
        }
    }

    /// bincode encodes a fieldless enum as its u32 declaration-index (LE), so
    /// these tags are the on-wire identity of every `ResourceId::BuiltIn`.
    /// Appending a variant is safe; reordering or inserting one silently remaps
    /// every existing row on the wire. `ordered` MUST be in declaration order —
    /// the test asserts each variant serializes to its position, so a reorder
    /// fails here and an append trips the count assert (extend the slice and
    /// bump `PROTOCOL_VERSION`).
    #[test]
    fn builtin_kind_wire_tags_are_stable() {
        use super::BuiltInKind::*;
        let ordered = [
            Pod, Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob,
            Service, ConfigMap, Secret, ServiceAccount, Ingress, NetworkPolicy,
            Hpa, Endpoints, EndpointSlice, LimitRange, ResourceQuota,
            PodDisruptionBudget, Event, PersistentVolumeClaim, Lease,
            Namespace, Node, PersistentVolume, StorageClass, PriorityClass,
            Role, ClusterRole, RoleBinding, ClusterRoleBinding,
            ValidatingWebhookConfiguration, MutatingWebhookConfiguration,
            CustomResourceDefinition,
        ];
        assert_eq!(ordered.len(), 34, "BuiltInKind variant count changed");
        for (i, kind) in ordered.iter().enumerate() {
            assert_eq!(
                bincode::serialize(kind).expect("serialize"),
                (i as u32).to_le_bytes(),
                "{:?}: wire tag drifted from {}", kind, i,
            );
        }
    }
}
