use k8s_openapi::api::rbac::v1::Role;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

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
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                rules_count.to_string(), crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
