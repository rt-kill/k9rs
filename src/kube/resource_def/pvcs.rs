//! `PvcDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::pvcs`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct PvcDef;

impl ResourceDef for PvcDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PersistentVolumeClaim }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "PersistentVolumeClaim",
            plural: "persistentvolumeclaims", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pvc", "persistentvolumeclaim", "persistentvolumeclaims", "pvcs"] }
    fn short_label(&self) -> &str { "PVC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "STATUS", "VOLUME", "CAPACITY", "ACCESS MODES",
         "STORAGECLASS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
