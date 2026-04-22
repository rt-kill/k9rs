use k8s_openapi::api::rbac::v1::ClusterRoleBinding;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// ClusterRoleBindingDef
// ---------------------------------------------------------------------------

pub struct ClusterRoleBindingDef;

impl ResourceDef for ClusterRoleBindingDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ClusterRoleBinding }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRoleBinding",
            plural: "clusterrolebindings", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["crb", "clusterrolebinding", "clusterrolebindings"] }
    fn short_label(&self) -> &str { "CRB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "ROLE", "SUBJECTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ClusterRoleBinding> for ClusterRoleBindingDef {
    fn convert(crb: ClusterRoleBinding) -> ResourceRow {
        let meta = CommonMeta::from_k8s(crb.metadata);
        let role_ref = format!("{}/{}", crb.role_ref.kind, crb.role_ref.name);
        let subjects = crb.subjects
            .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        ResourceRow {
            cells: vec![
                meta.name.clone(),
                role_ref, subjects,
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: None,
            ..Default::default()
        }
    }
}
