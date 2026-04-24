use k8s_openapi::api::rbac::v1::RoleBinding;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

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
