//! `RoleDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::roles`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

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
