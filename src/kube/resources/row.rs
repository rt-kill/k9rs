use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Row health indicator, computed server-side by each converter.
/// The client reads this for row coloring — no resource-type-specific
/// knowledge needed on the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RowHealth {
    /// Healthy / running / ready.
    #[default]
    Normal,
    /// In-progress / starting / pending.
    Pending,
    /// Error / degraded / not-ready.
    Failed,
}

/// A single resource row in the unified table model.
/// Replaces all 28 typed Kube* structs with a generic representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRow {
    /// Display columns, matching the header order.
    pub cells: Vec<String>,
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

/// What happens when the user presses Enter on a row. Set by the converter
/// (server-side) so the client doesn't need K8s knowledge to drill down.
///
/// The client reads this blindly and constructs the appropriate nav action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrillTarget {
    /// Switch to a different namespace (used by namespace rows). Typed
    /// as `Namespace` because this is a *selection* (the user picking a
    /// scope), not a location string.
    SwitchNamespace(crate::kube::protocol::Namespace),
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
    PodsByField(crate::app::nav::K8sFieldSelector),
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
}

impl super::KubeResource for ResourceRow {
    fn cells(&self) -> &[String] {
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
    /// Mutate a cell in-place (e.g., for metrics overlay).
    pub fn set_cell(&mut self, col: usize, value: String) {
        if col < self.cells.len() {
            self.cells[col] = value;
        }
    }
}
