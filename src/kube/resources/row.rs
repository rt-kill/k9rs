use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A single resource row in the unified table model.
/// Replaces all 28 typed Kube* structs with a generic representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRow {
    /// Display columns, matching the header order.
    pub cells: Vec<String>,
    /// Resource name (cached for O(1) access in sorts/filters).
    pub name: String,
    /// Resource namespace (empty string for cluster-scoped resources).
    pub namespace: String,
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
}

/// CRD definition metadata (for rows in the `crds` table).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdRowInfo {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub scope: crate::kube::protocol::ResourceScope,
}

/// Container info for pods — used by shell, logs, port-forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    pub name: String,
    pub real_name: String,
    pub image: String,
    pub ready: bool,
    pub state: String,
    pub restarts: i32,
    pub ports: Vec<u16>,
}

/// Owner reference info for pods — used by owner chain drill-down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRefInfo {
    pub api_version: String,
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
    /// Switch to a different namespace (used by namespace rows).
    SwitchNamespace(String),
    /// Push a CRD-instance view onto the nav stack.
    BrowseCrd {
        group: String,
        version: String,
        kind: String,
        plural: String,
        scope: crate::kube::protocol::ResourceScope,
    },
    /// Drill down to pods filtered by label selector (deploy/sts/ds/svc/job).
    PodsByLabels {
        labels: BTreeMap<String, String>,
        /// Display label for the breadcrumb (e.g., "deploy/my-app").
        breadcrumb: String,
    },
    /// Drill down to pods filtered by ownerReference UID (replicaset/job).
    PodsByOwner {
        uid: String,
        kind: String,
        name: String,
    },
    /// Drill down to pods filtered by field selector (e.g., spec.nodeName=X).
    PodsByField {
        field: String,
        value: String,
        breadcrumb: String,
    },
    /// Drill down to pods by name prefix (fallback when no selector exists).
    PodsByNameGrep(String),
}

impl super::KubeResource for ResourceRow {
    fn cells(&self) -> &[String] {
        &self.cells
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
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
