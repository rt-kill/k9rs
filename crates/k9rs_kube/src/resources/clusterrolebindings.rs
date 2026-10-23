use k8s_openapi::api::rbac::v1::ClusterRoleBinding;

use super::ConvertToRow;
use k9rs_proto::resource_def::clusterrolebindings::ClusterRoleBindingDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ClusterRoleBindingDef
// ---------------------------------------------------------------------------


impl ConvertToRow<ClusterRoleBinding> for ClusterRoleBindingDef {
    fn convert(crb: ClusterRoleBinding) -> ResourceRow {
        let meta = CommonMeta::from_k8s(crb.metadata);
        let role_ref = format!("{}/{}", crb.role_ref.kind, crb.role_ref.name);
        let subjects = crb.subjects
            .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Text(role_ref),
            CellValue::Text(subjects),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}
