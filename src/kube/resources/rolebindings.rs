use k8s_openapi::api::rbac::v1::RoleBinding;

use super::ConvertToRow;
use crate::kube::resource_def::rolebindings::RoleBindingDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// RoleBindingDef
// ---------------------------------------------------------------------------


impl ConvertToRow<RoleBinding> for RoleBindingDef {
    fn convert(rb: RoleBinding) -> ResourceRow {
        let meta = CommonMeta::from_k8s(rb.metadata);
        let role_ref = format!("{}/{}", rb.role_ref.kind, rb.role_ref.name);
        let subjects = rb.subjects
            .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(role_ref),
            CellValue::Text(subjects),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
