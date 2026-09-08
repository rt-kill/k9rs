use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::util::{format_age_secs, format_cpu, format_mem};

/// Row health indicator, computed server-side by each converter.
/// The client reads this for row coloring — no resource-type-specific
/// knowledge needed on the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum RowHealth {
    /// Healthy / running / ready.
    #[default]
    Normal,
    /// In-progress / starting / pending.
    Pending,
    /// Error / degraded / not-ready.
    Failed,
}

// ---------------------------------------------------------------------------
// CellValue — typed cell representation
// ---------------------------------------------------------------------------

/// Unit of a quantity cell, used to select the right formatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuantityUnit {
    Millicores,
    Bytes,
}

/// A typed cell value. Variant order is **load-bearing** for bincode
/// (positional encoding) — do NOT reorder or insert between existing
/// variants after Phase 1 is merged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CellValue {
    Text(String),
    Ratio { num: u32, denom: u32 },
    Quantity { value: u64, unit: QuantityUnit },
    Age(Option<i64>),
    Count(i64),
    Bool(bool),
    List(Vec<String>),
    Status { text: String, health: RowHealth },
    Percentage(Option<u64>),
    Placeholder,
}

impl CellValue {
    /// Build a `List` from a pre-joined comma-separated string.
    /// Returns an empty list if the input is empty.
    pub fn from_comma_str(s: &str) -> Self {
        if s.is_empty() {
            CellValue::List(vec![])
        } else {
            CellValue::List(s.split(',').filter(|p| !p.is_empty()).map(String::from).collect())
        }
    }

    /// The raw numeric value if this is a [`CellValue::Quantity`] (millicores or
    /// bytes), else `None`. Used by the metrics overlay to read a node's
    /// allocatable CPU/MEM and compute the percent columns.
    pub fn quantity_value(&self) -> Option<u64> {
        match self {
            CellValue::Quantity { value, .. } => Some(*value),
            _ => None,
        }
    }
}

// ---- Display ----------------------------------------------------------------

impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CellValue::Text(s) => f.write_str(s),
            CellValue::Ratio { num, denom } => write!(f, "{}/{}", num, denom),
            CellValue::Quantity { value, unit } => match unit {
                QuantityUnit::Millicores => {
                    let formatted = format_cpu(&format!("{}m", value));
                    f.write_str(&formatted)
                }
                QuantityUnit::Bytes => {
                    let formatted = format_mem(&value.to_string());
                    f.write_str(&formatted)
                }
            },
            CellValue::Age(Some(epoch_secs)) => {
                let elapsed = Utc::now().timestamp() - epoch_secs;
                let formatted = format_age_secs(elapsed);
                f.write_str(&formatted)
            }
            CellValue::Age(None) => f.write_str("<unknown>"),
            CellValue::Count(n) => write!(f, "{}", n),
            CellValue::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            CellValue::List(items) => f.write_str(&items.join(",")),
            CellValue::Status { text, .. } => f.write_str(text),
            CellValue::Percentage(Some(v)) => write!(f, "{}%", v),
            CellValue::Percentage(None) => f.write_str("n/a"),
            CellValue::Placeholder => f.write_str("n/a"),
        }
    }
}

// ---- Ordering ---------------------------------------------------------------

/// Returns a discriminant index for cross-variant comparison.
/// Mirrors the declaration order in the enum.
fn cell_discriminant(v: &CellValue) -> u8 {
    match v {
        CellValue::Text(_) => 0,
        CellValue::Ratio { .. } => 1,
        CellValue::Quantity { .. } => 2,
        CellValue::Age(_) => 3,
        CellValue::Count(_) => 4,
        CellValue::Bool(_) => 5,
        CellValue::List(_) => 6,
        CellValue::Status { .. } => 7,
        CellValue::Percentage(_) => 8,
        CellValue::Placeholder => 9,
    }
}

impl Ord for CellValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (CellValue::Text(a), CellValue::Text(b)) => a.cmp(b),

            (CellValue::Ratio { num: an, denom: ad }, CellValue::Ratio { num: bn, denom: bd }) => {
                let fa = *an as f64 / (*ad).max(1) as f64;
                let fb = *bn as f64 / (*bd).max(1) as f64;
                fa.partial_cmp(&fb).unwrap_or(an.cmp(bn))
            }

            (CellValue::Quantity { value: a, .. }, CellValue::Quantity { value: b, .. }) => {
                a.cmp(b)
            }

            // Age: Some sorts before None. Larger epoch (more recent) sorts later.
            (CellValue::Age(a), CellValue::Age(b)) => match (a, b) {
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },

            (CellValue::Count(a), CellValue::Count(b)) => a.cmp(b),

            (CellValue::Bool(a), CellValue::Bool(b)) => a.cmp(b),

            (CellValue::List(a), CellValue::List(b)) => {
                a.len().cmp(&b.len()).then_with(|| {
                    let ja = a.join(",");
                    let jb = b.join(",");
                    ja.cmp(&jb)
                })
            }

            (CellValue::Status { text: a, .. }, CellValue::Status { text: b, .. }) => a.cmp(b),

            // Percentage: Some sorts before None.
            (CellValue::Percentage(a), CellValue::Percentage(b)) => match (a, b) {
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },

            (CellValue::Placeholder, CellValue::Placeholder) => Ordering::Equal,

            // Cross-variant: compare by discriminant (deterministic but meaningless).
            _ => cell_discriminant(self).cmp(&cell_discriminant(other)),
        }
    }
}

impl PartialOrd for CellValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A single resource row in the unified table model.
/// Replaces all 28 typed Kube* structs with a generic representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRow {
    /// Typed cell values, in header order. Sort, filter, and render all
    /// operate on these directly via `CellValue::cmp()` and
    /// `CellValue::to_string()`.
    pub cells: Vec<CellValue>,
    /// Resource name (cached for O(1) access in sorts/filters).
    pub name: String,
    /// Resource namespace. `None` for cluster-scoped resources.
    pub namespace: Option<String>,
    /// What happens when the user presses Enter on this row.
    /// Set by the converter (server-side); the client reads this blindly
    /// to construct the appropriate nav action — no K8s knowledge needed.
    /// `None` means describe-on-Enter.
    pub drill_target: Option<DrillTarget>,
    /// Container metadata (pods only). Used by the client to render container
    /// selectors and by the server for owner-chain port resolution.
    pub containers: Vec<ContainerInfo>,
    /// Owner references (server-side for OwnerUid post-filtering).
    pub owner_refs: Vec<OwnerRefInfo>,
    /// Port-forward metadata: suggested local/remote ports for this resource.
    /// Used by the client to populate the port-forward dialog.
    pub pf_ports: Vec<u16>,
    /// CRD definition metadata (only set on rows in the `crds` table).
    /// Used by the client for command completion and autocomplete.
    pub crd_info: Option<CrdRowInfo>,
    /// Node name. `Some(name)` for pods scheduled to a node, `None` for
    /// every other resource type AND for unscheduled pods. The client uses
    /// this for `ShowNode` navigation; non-pod rows skip the action because
    /// the field is `None` rather than the empty string.
    pub node: Option<String>,
    /// Server-computed health for row coloring. The client reads this
    /// directly instead of parsing cells per resource type.
    #[serde(default)]
    pub health: RowHealth,
    /// Summed CPU request across all containers, in millicores (pods only).
    /// Used by the metrics overlay to compute %CPU/R.
    ///
    /// NOTE: no `skip_serializing_if` — bincode is positional, so skipping a
    /// field shifts every subsequent field and deserialization reads garbage.
    /// The `#[serde(default)]` is harmless (bincode ignores it) but left for
    /// forward-compat with JSON snapshots.
    #[serde(default)]
    pub cpu_request: Option<u64>,
    /// Summed CPU limit across all containers, in millicores (pods only).
    #[serde(default)]
    pub cpu_limit: Option<u64>,
    /// Summed memory request across all containers, in bytes (pods only).
    #[serde(default)]
    pub mem_request: Option<u64>,
    /// Summed memory limit across all containers, in bytes (pods only).
    #[serde(default)]
    pub mem_limit: Option<u64>,
}

/// CRD definition metadata (for rows in the `crds` table). Type alias over
/// [`crate::kube::protocol::CrdRef`] — the wire shape is identical, and
/// using one type means converters/consumers can hand the value straight
/// to anything that takes a `CrdRef` (e.g. `ResourceId::Crd(CrdRef)` for
/// nav drill-downs).
pub type CrdRowInfo = crate::kube::protocol::CrdRef;

/// Container info for pods — used by shell, logs, port-forward.
///
/// Only the fields the client actually reads ride the wire. The server
/// uses additional intermediate values (image, ready, state, restarts,
/// container-level ports) at construction time but distills them into
/// the parent row's cells / `pf_ports` / `health` before serializing.
///
/// `kind` distinguishes init from regular containers as a typed enum;
/// the UI renders the `init:` prefix from the discriminant rather than
/// from a string-prefix encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    /// Container name as `kubectl exec/logs` expects it. The UI
    /// renders it with an `init:` prefix when `kind == Init`, and
    /// passes it verbatim into `LogContainer::Named` on shell/log.
    pub name: String,
    /// Init vs regular. Typed so the UI doesn't string-parse `name`.
    #[serde(default)]
    pub kind: ContainerKind,
    /// Container image (e.g., "nginx:1.25").
    #[serde(default)]
    pub image: String,
    /// The kubelet's container STATE — the closed axis, typed (v10; was a
    /// single stringly field that mixed this with the open-ended reason).
    #[serde(default)]
    pub state: ContainerState,
    /// The kubelet's REASON for a waiting/terminated state, when it gave
    /// one — an inherently OPEN set ("CrashLoopBackOff", "Completed",
    /// "OOMKilled", "ImagePullBackOff", …), so it stays a string by
    /// design. Display falls back to the state's name (`status_label`).
    #[serde(default)]
    pub reason: Option<String>,
    /// Whether the container is ready.
    #[serde(default)]
    pub ready: bool,
    /// Cumulative restart count.
    #[serde(default)]
    pub restart_count: i32,
}

impl ContainerInfo {
    /// Display name for the container list: regular containers show their bare
    /// name; init containers get an `init:` prefix so the user can tell them
    /// apart. Derived from the typed `kind` discriminant — the prefix is never
    /// carried in `name` (which stays exactly what `kubectl exec/logs` expects).
    /// Single source of truth for the derived-view rows (`project_containers`)
    /// and the container-select dialog.
    pub fn display_name(&self) -> String {
        match self.kind {
            ContainerKind::Init => format!("init:{}", self.name),
            ContainerKind::Regular => self.name.clone(),
        }
    }

    /// The status text shown in tables: the kubelet's reason when it gave
    /// one, else the state's name — byte-for-byte what the old stringly
    /// `status` field carried.
    pub fn status_label(&self) -> &str {
        self.reason.as_deref().unwrap_or(self.state.label())
    }
}

/// The closed container-state axis as the kubelet reports it. `Unknown`
/// covers a status entry with no state block at all (unscheduled /
/// API-partial) — an honest fourth case, not a fallback bucket for
/// unrecognized strings (the open-ended part lives in
/// [`ContainerInfo::reason`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerState {
    Running,
    Waiting,
    Terminated,
    #[default]
    Unknown,
}

impl ContainerState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::Waiting => "Waiting",
            Self::Terminated => "Terminated",
            Self::Unknown => "Unknown",
        }
    }
}

/// Init vs regular container. Defaults to `Regular` for forward compat
/// with snapshots that pre-date this field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerKind {
    #[default]
    Regular,
    Init,
}

/// Owner reference info for pods — used by owner chain drill-down.
/// Only the fields the client reads (kind/name/uid for breadcrumbs and
/// chain matching) ride the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRefInfo {
    pub kind: String,
    pub name: String,
    pub uid: String,
}

/// Typed K8s field selector. Each variant corresponds to a specific K8s
/// field selector path; the variant data is the value to match against.
/// New field paths get new variants — the wire-format string and
/// breadcrumb are derived from the variant.
///
/// WIRE-FROZEN: rides inside [`DrillTarget::PodsByField`] on `ResourceRow`
/// (proto 8). Variant order and fields are part of the bincode layout —
/// append-only, never reorder. Pinned by `drill_wire_tags_are_stable`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum K8sFieldSelector {
    /// `metadata.name=<name>` — exact match on the resource name.
    /// Universally supported by the K8s API for all resource types.
    MetadataName(String),
    /// `spec.nodeName=<name>` — pods scheduled on a specific node.
    SpecNodeName(String),
    /// `status.phase=<phase>` — pods in a specific lifecycle phase.
    StatusPhase(String),
}

impl K8sFieldSelector {
    /// Render the selector in the format K8s expects on the wire
    /// (`field.path=value`), suitable for `SubscriptionFilter::Field`.
    pub fn to_wire(&self) -> String {
        match self {
            Self::MetadataName(v) => format!("metadata.name={}", v),
            Self::SpecNodeName(v) => format!("spec.nodeName={}", v),
            Self::StatusPhase(v) => format!("status.phase={}", v),
        }
    }

    /// Short user-facing label for the breadcrumb.
    pub fn breadcrumb(&self) -> String {
        match self {
            Self::MetadataName(v) => format!("name={}", v),
            Self::SpecNodeName(v) => format!("node={}", v),
            Self::StatusPhase(v) => format!("phase={}", v),
        }
    }
}

/// A client-side derived view type (containers of a pod, ...). The DATA
/// enum lives here because it rides the wire inside
/// [`DrillTarget::Derived`]; the BEHAVIOR (labels, projection, operations)
/// lives in [`crate::app::view`], next to the projection registry it
/// dispatches into.
///
/// WIRE-FROZEN: variant order is part of the bincode layout — append-only,
/// never reorder. Pinned by `drill_wire_tags_are_stable`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DerivedViewKind {
    /// Containers of a pod — projected from `ResourceRow.containers`.
    Containers,
}

/// What happens when the user presses Enter on a row. Set by the converter
/// (server-side) so the client doesn't need K8s knowledge to drill down.
///
/// The client reads this blindly and constructs the appropriate nav action.
///
/// WIRE-FROZEN: variant order and fields are part of the bincode layout —
/// append-only, never reorder. Pinned by `drill_wire_tags_are_stable`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrillTarget {
    /// Enter a namespace: make it the active scope and drill into the pods
    /// running in it (what k9s does on Enter over a namespace row). Typed as
    /// `Namespace` because this is a *selection* — the user picking a scope —
    /// not a location string. Only namespace rows produce this variant.
    PodsInNamespace(crate::kube::protocol::Namespace),
    /// Push a CRD-instance view onto the nav stack. Wraps a [`crate::kube::protocol::CrdRef`]
    /// so the drill handler can build a `ResourceId::Crd(...)` directly
    /// without re-marshaling fields.
    BrowseCrd(crate::kube::protocol::CrdRef),
    /// Drill down to pods filtered by label selector (deploy/sts/ds/svc/job).
    PodsByLabels {
        labels: BTreeMap<String, String>,
        /// Display label for the breadcrumb (e.g., "deploy/my-app").
        breadcrumb: String,
    },
    /// Drill down to pods filtered by ownerReference UID (replicaset/job).
    PodsByOwner {
        uid: String,
        /// Parent kind, typed. Producers have a [`BuiltInKind`] in hand
        /// already; stringifying and re-parsing on the client was extra
        /// motion. Breadcrumb display fetches the human string via
        /// [`crate::kube::resource_defs::REGISTRY`].
        kind: crate::kube::resource_def::BuiltInKind,
        name: String,
    },
    /// Drill down to pods filtered by a typed K8s field selector.
    /// Replaces the older `PodsByField { field: String, value: String }`
    /// shape — the typed enum carries the field path so producers can't
    /// fat-finger `"spec.nodeName"`.
    PodsByField(K8sFieldSelector),
    /// Drill down to pods by name prefix (fallback when no selector exists).
    PodsByNameGrep(String),
    /// Drill down to jobs owned by a parent resource (via ownerReference
    /// UID). Produced server-side by the cronjobs converter today (so
    /// `kind` is `CronJob`), but typed so it doesn't assume the parent
    /// kind at the client.
    JobsByOwner {
        uid: String,
        kind: crate::kube::resource_def::BuiltInKind,
        name: String,
    },
    /// Drill into a client-side derived view projected from this row's
    /// data. The `DerivedViewKind` determines what to project (containers,
    /// volumes, etc.); the row carries the data to project from.
    /// Adding a new derived view type never touches DrillTarget — the
    /// taxonomy lives on [`DerivedViewKind`].
    Derived(DerivedViewKind),
    /// Switch the session to this kubeconfig context. Carried by rows of
    /// the client-seeded `contexts` list, which is why Enter over a context
    /// needs no special case in the key handler: the ROW says what Enter
    /// does, exactly as it does for a namespace or a CRD.
    SwitchContext(crate::kube::protocol::ContextName),
}

impl super::KubeResource for ResourceRow {
    fn cells(&self) -> &[CellValue] {
        &self.cells
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        self.namespace.as_deref().unwrap_or("")
    }
}

impl ResourceRow {
    /// Mutate a cell in-place (e.g., for metrics overlay). No-op if `col`
    /// is out of bounds.
    pub fn set_cell(&mut self, col: usize, value: CellValue) {
        if col < self.cells.len() {
            self.cells[col] = value;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../../tests/kube/resources/row.rs"]
mod tests;
