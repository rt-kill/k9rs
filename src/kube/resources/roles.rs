use k8s_openapi::api::rbac::v1::Role;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// RoleDef
// ---------------------------------------------------------------------------

pub struct RoleDef;

impl ResourceDef for RoleDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Role }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "Role",
            plural: "roles", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["role", "roles"] }
    fn short_label(&self) -> &str { "Roles" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "RULES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

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
