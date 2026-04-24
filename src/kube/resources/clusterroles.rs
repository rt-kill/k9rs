use k8s_openapi::api::rbac::v1::ClusterRole;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ClusterRoleDef
// ---------------------------------------------------------------------------

pub struct ClusterRoleDef;

impl ResourceDef for ClusterRoleDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ClusterRole }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRole",
            plural: "clusterroles", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cr", "clusterrole", "clusterroles"] }
    fn short_label(&self) -> &str { "CRoles" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "RULES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

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
