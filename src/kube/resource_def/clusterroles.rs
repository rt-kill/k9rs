//! `ClusterRoleDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::clusterroles`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

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
