//! `ConfigMapDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::configmaps`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct ConfigMapDef;

impl ResourceDef for ConfigMapDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ConfigMap }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ConfigMap",
            plural: "configmaps", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cm", "configmap", "configmaps"] }
    fn short_label(&self) -> &str { "CM" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
