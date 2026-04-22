use k8s_openapi::api::rbac::v1::RoleBinding;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// RoleBindingDef
// ---------------------------------------------------------------------------

pub struct RoleBindingDef;

impl ResourceDef for RoleBindingDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::RoleBinding }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "RoleBinding",
            plural: "rolebindings", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["rb", "rolebinding", "rolebindings"] }
    fn short_label(&self) -> &str { "RB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ROLE", "SUBJECTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<RoleBinding> for RoleBindingDef {
    fn convert(rb: RoleBinding) -> ResourceRow {
        let meta = CommonMeta::from_k8s(rb.metadata);
        let role_ref = format!("{}/{}", rb.role_ref.kind, rb.role_ref.name);
        let subjects = rb.subjects
            .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                role_ref, subjects, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
