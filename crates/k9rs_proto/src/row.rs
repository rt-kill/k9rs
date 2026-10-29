use std::collections::BTreeMap;
use crate::protocol::OperationKind;
use std::cmp::Ordering;
use std::fmt;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use k9rs_util::units::{format_age_secs, format_cpu, format_mem};

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

/// Utilization percentages at or above which a cell colours itself.
///
/// A default, not a policy: a per-column `coloring` overlay rule still wins
/// (see `app::store::derive_view`). What this removes is the state where a
/// utilization column is born INVISIBLE — colouring used to exist only in
/// hand-written overlay YAML keyed by header STRING, so `CPU%` instead of
/// `%CPU` silently coloured nothing, and every new percent column started
/// out the same way.
pub const UTILIZATION_WARN_PCT: u64 = 75;
pub const UTILIZATION_CRIT_PCT: u64 = 90;

impl CellValue {
    /// Threshold health for a utilization percentage, or `None` for any
    /// other cell (and for a percentage with nothing to report).
    ///
    /// Lives on the TYPE rather than in a table of header names, for the
    /// same reason [`crate::protocol::LogContainer::label`] does: the
    /// cell already knows it is a percentage and already holds the number.
    /// The old path stringified that number, matched a header to decide
    /// whether to care, then parsed it back out of the rendered `"85%"`.
    pub fn utilization_health(&self) -> Option<RowHealth> {
        match self {
            CellValue::Percentage(Some(v)) if *v >= UTILIZATION_CRIT_PCT => Some(RowHealth::Failed),
            CellValue::Percentage(Some(v)) if *v >= UTILIZATION_WARN_PCT => Some(RowHealth::Pending),
            _ => None,
        }
    }

    /// A quantity cell, or [`CellValue::Placeholder`] when there is no
    /// value — the ONE answer to "this row has no number here".
    ///
    /// The choice was made at each call site, and a pods row made it three
    /// ways: `CPU`/`MEM` used `Placeholder`, the `%` columns
    /// `Percentage(None)`, and `CPU/R`/`CPU/L`/`MEM/R`/`MEM/L`
    /// `Text(String::new())`. `cell_discriminant` puts `Text` at 0 and
    /// `Placeholder` at 9, and `Percentage(None)` sorts after `Some` — so in
    /// ONE row, sorting by `CPU/R` put the no-request pods FIRST, by `CPU`
    /// LAST, and by `%CPU/R` LAST. Three directions for one meaning.
    ///
    /// `Placeholder` is the answer because it is the only variant that MEANS
    /// absent: it renders `n/a` (the word the header already uses) and sorts
    /// past every real value, so "no limit set" collects at one end instead
    /// of masquerading as the smallest number.
    pub fn quantity_or_absent(value: Option<u64>, unit: QuantityUnit) -> Self {
        match value {
            Some(value) => CellValue::Quantity { value, unit },
            None => CellValue::Placeholder,
        }
    }

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
    /// Objects this row NAMES (see [`ResourceRefInfo`]) — populated by the
    /// pods converter from the spec, empty for every other kind. Feeds the
    /// `U` reverse lookup as a server-side post-filter, the same shape
    /// `SubscriptionFilter::OwnerUid` already uses.
    #[serde(default)]
    pub references: Vec<ResourceRefInfo>,
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
/// [`crate::protocol::CrdRef`] — the wire shape is identical, and
/// using one type means converters/consumers can hand the value straight
/// to anything that takes a `CrdRef` (e.g. `ResourceId::Crd(CrdRef)` for
/// nav drill-downs).
pub type CrdRowInfo = crate::protocol::CrdRef;

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
    /// The DAEMON's verdict, from the same `classify_pod_reason` that decides
    /// the pod row — so the container row and its pod can no longer disagree.
    ///
    /// The client used to re-derive this from `ready`/`state`/`restart_count`,
    /// ignoring `reason`. An `ImagePullBackOff` with zero restarts came out
    /// Failed (red) on the pod row and Pending (yellow) on the container row
    /// one Enter down, with the cell TEXT reading "ImagePullBackOff" in both —
    /// text and colour contradicting each other, which is exactly what the
    /// typed classifier was introduced to end.
    #[serde(default)]
    pub health: RowHealth,
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

/// An object this row REFERENCES — a volume's claim, a mounted secret, a
/// service account. The outward edges, where [`OwnerRefInfo`] is the upward
/// one.
///
/// Extracted by the converter at the API boundary, like every other typed
/// field on a row, so the reverse lookup ("what uses this PVC") is a filter
/// over data rather than a guess. It used to be a grep of the pod list for
/// the resource's NAME, which found `data-postgres-0` → `postgres-0` by
/// luck of naming convention and missed a secret mounted by `api-7d9f`
/// entirely — with no indication, because "no pods" looks like an answer.
///
/// `kind` is typed: only the kinds a pod can actually name are extractable,
/// so a reference to something unrepresentable can't be constructed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceRefInfo {
    pub kind: crate::resource_def::BuiltInKind,
    pub name: String,
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
    /// `involvedObject.{kind,name,namespace}` — the events ABOUT one object.
    ///
    /// All three parts, because a Service and a Deployment can share a name
    /// and so can two objects in different namespaces; the whole point of
    /// this drill is that it answers for one object and not something that
    /// resembles it. `namespace` is empty for cluster-scoped subjects, whose
    /// events land in whatever namespace the reporting component chose — so
    /// the drill scopes the LIST to all namespaces and lets the selector do
    /// the narrowing.
    InvolvedObject { kind: String, name: String, namespace: String },
}

impl K8sFieldSelector {
    /// Render the selector in the format K8s expects on the wire
    /// (`field.path=value`, comma-separated for AND), suitable for
    /// `SubscriptionFilter::Field`.
    pub fn to_wire(&self) -> String {
        match self {
            Self::MetadataName(v) => format!("metadata.name={}", v),
            Self::SpecNodeName(v) => format!("spec.nodeName={}", v),
            Self::StatusPhase(v) => format!("status.phase={}", v),
            Self::InvolvedObject { kind, name, namespace } => {
                let mut s = format!("involvedObject.kind={kind},involvedObject.name={name}");
                if !namespace.is_empty() {
                    s.push_str(&format!(",involvedObject.namespace={namespace}"));
                }
                s
            }
        }
    }

    /// Short user-facing label for the breadcrumb.
    pub fn breadcrumb(&self) -> String {
        match self {
            Self::MetadataName(v) => format!("name={}", v),
            Self::SpecNodeName(v) => format!("node={}", v),
            Self::StatusPhase(v) => format!("phase={}", v),
            Self::InvolvedObject { kind, name, .. } => {
                format!("{}/{}", kind.to_lowercase(), name)
            }
        }
    }
}

/// A client-side derived view type (containers of a pod, ...). The DATA
/// enum lives here because it rides the wire inside
/// [`DrillTarget::Derived`]; the BEHAVIOR (labels, projection, operations)
/// lives in the client's view-projection registry, next to the projections it
/// dispatches into.
///
/// WIRE-FROZEN: variant order is part of the bincode layout — append-only,
/// never reorder. Pinned by `drill_wire_tags_are_stable`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DerivedViewKind {
    /// Containers of a pod — projected from `ResourceRow.containers`.
    Containers,
}

// The metadata half of `DerivedViewKind`, beside the enum.
//
// It lived as `impl DerivedViewKind` in `app/view.rs`, reached through a
// `pub use` of this very type — so once this file became its own crate that
// impl was an inherent impl on a foreign type, which Rust forbids. It was the
// only such hazard in the tree, and it is invisible to a module-level
// dependency count. `project` did NOT come along: it calls into the client's
// projection functions, so it is a free function up there now.
impl DerivedViewKind {
    /// Short label for breadcrumbs and tab bar.
    pub fn short_label(&self) -> &'static str {
        match self {
            Self::Containers => "Containers",
        }
    }

    /// Plural name (for overlay lookup, display).
    pub fn plural(&self) -> &'static str {
        match self {
            Self::Containers => "containers",
        }
    }

    /// Whether this view is cluster-scoped.
    pub fn is_cluster_scoped(&self) -> bool {
        match self {
            Self::Containers => false,
        }
    }

    /// Operations available on rows in this view.
    pub fn operations(&self) -> Vec<OperationKind> {
        match self {
            Self::Containers => vec![
                OperationKind::StreamLogs,
                OperationKind::PreviousLogs,
                OperationKind::Shell,
                OperationKind::Describe,
                // The container's pod has a node, and the projection carries
                // it onto every row (see `project_containers`), so `o` works
                // here for the same reason it works on the pod itself.
                OperationKind::ShowNode,
            ],
        }
    }

    /// Default column headers for this view's table.
    pub fn default_headers(&self) -> Vec<String> {
        match self {
            Self::Containers => {
                ["NAME", "IMAGE", "STATUS", "READY", "RESTARTS"]
                    .into_iter().map(String::from).collect()
            }
        }
    }
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
    PodsInNamespace(crate::protocol::Namespace),
    /// Push a CRD-instance view onto the nav stack. Wraps a [`crate::protocol::CrdRef`]
    /// so the drill handler can build a `ResourceId::Crd(...)` directly
    /// without re-marshaling fields.
    BrowseCrd(crate::protocol::CrdRef),
    /// Drill down to pods filtered by label selector (deploy/sts/ds/svc/job).
    PodsByLabels {
        labels: BTreeMap<String, String>,
        /// Display label for the breadcrumb (e.g., "deploy/my-app").
        breadcrumb: String,
    },
    /// Drill down to pods filtered by ownerReference UID (replicaset/job).
    PodsByOwner {
        uid: String,
        /// Parent kind, typed. Producers have a [`crate::resource_def::BuiltInKind`] in hand
        /// already; stringifying and re-parsing on the client was extra
        /// motion. Breadcrumb display fetches the human string via
        /// the definition table (`BuiltInKind::def`).
        kind: crate::resource_def::BuiltInKind,
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
        kind: crate::resource_def::BuiltInKind,
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
    SwitchContext(crate::protocol::ContextName),
    /// Open one SECTION of the `?` sheet, named here.
    ///
    /// `?` lists its sections now and Enter drills into one, rather than
    /// showing all 61 rows flat. The section name rides the row for the same
    /// reason [`Self::SwitchContext`] carries its context: the alternative
    /// is a branch in the key handler that knows help is special, which is
    /// precisely the bespoke-view shape the nav model exists to avoid.
    ///
    /// APPENDED. Decode-safe for a peer that predates it only because no
    /// server-side converter emits it — which is a GREP, not a type fact:
    /// `DrillTarget` lives in this crate and `k9rs_kube`/`k9rs_daemon`
    /// depend on it, so one of them could construct this variant and
    /// nothing here would stop it. Measured 2026-10-29: zero occurrences
    /// outside `k9rs_app`, `k9rs_session` and this file. If that ever
    /// changes, tag 9 reaches an older peer and fails to decode — which is
    /// what the revision bump exists to make loud.
    ///
    /// (Written first as "nothing ever puts tag 9 on the wire", which is a
    /// guarantee this enum does not offer.)
    HelpSection(String),
}

/// The row contract the derivation machinery reads through: cells, name,
/// namespace. The only implementor is [`ResourceRow`], immediately below —
/// the trait exists so the client's `derive_view` and the sort/filter helpers
/// do not have to name the concrete row type.
///
/// It lived in `resources::mod` beside the k8s-openapi helpers, which put a
/// trait and its single implementor in different files for no reason and left
/// a standing hazard: separate them across a crate line and the `impl` below
/// becomes an orphan. Beside each other, it is a local-trait impl wherever
/// they end up.
pub trait KubeResource: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The typed cell values for this row, in header order.
    fn cells(&self) -> &[CellValue];
    /// Resource name (cached for O(1) access in sorts/filters).
    fn name(&self) -> &str;
    /// Resource namespace, or `""` for cluster-scoped rows.
    fn namespace(&self) -> &str;
}

impl KubeResource for ResourceRow {
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
    /// Mutate a cell in-place. No-op if `col` is out of bounds.
    ///
    /// Test-only. The doc used to offer "e.g., for metrics overlay" as its
    /// purpose, and the metrics overlay does not use it — it goes through
    /// the lens at derive time, so no row is mutated in place at all.
    /// Production rows are built whole by their converters.
    #[cfg(test)]
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
#[path = "tests/row.rs"]
mod tests;
