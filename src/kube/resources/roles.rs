use k8s_openapi::api::rbac::v1::Role;

use super::ConvertToRow;
use crate::kube::resource_def::roles::RoleDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// RoleDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Role> for RoleDef {
    fn convert(role: Role) -> ResourceRow {
        let meta = CommonMeta::from_k8s(role.metadata);
        let rules_count = role.rules.map(|r| r.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Count(rules_count as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
