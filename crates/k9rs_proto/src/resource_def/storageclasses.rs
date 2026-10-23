//! `StorageClassDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::storageclasses`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct StorageClassDef;

impl ResourceDef for StorageClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StorageClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "storage.k8s.io", version: "v1", kind: "StorageClass",
            plural: "storageclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sc", "storageclass", "storageclasses"] }
    fn short_label(&self) -> &str { "SC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "PROVISIONER", "RECLAIM POLICY", "VOLUME BINDING MODE",
         "EXPANSION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
