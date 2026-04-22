use k8s_openapi::api::rbac::v1::ClusterRole;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

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
        ResourceRow {
            cells: vec![
                meta.name.clone(),
                rules_count.to_string(),
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: None,
            ..Default::default()
        }
    }
}
