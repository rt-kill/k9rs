//! `ClusterRoleBindingDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::clusterrolebindings`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

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
