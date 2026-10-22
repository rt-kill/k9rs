use k8s_openapi::api::rbac::v1::ClusterRole;

use super::ConvertToRow;
use crate::kube::resource_def::clusterroles::ClusterRoleDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ClusterRoleDef
// ---------------------------------------------------------------------------


impl ConvertToRow<ClusterRole> for ClusterRoleDef {
    fn convert(cr: ClusterRole) -> ResourceRow {
        let meta = CommonMeta::from_k8s(cr.metadata);
        let rules_count = cr.rules.map(|r| r.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Count(rules_count as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}
