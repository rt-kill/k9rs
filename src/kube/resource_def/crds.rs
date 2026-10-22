//! `CrdDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::crds`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct CrdDef;

impl ResourceDef for CrdDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CustomResourceDefinition }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apiextensions.k8s.io", version: "v1", kind: "CustomResourceDefinition",
            plural: "customresourcedefinitions", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["crd", "crds", "customresourcedefinition", "customresourcedefinitions"] }
    fn short_label(&self) -> &str { "CRDs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "GROUP", "VERSION", "KIND", "SCOPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
