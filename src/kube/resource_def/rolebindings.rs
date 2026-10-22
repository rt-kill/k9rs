//! `RoleBindingDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::rolebindings`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

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
