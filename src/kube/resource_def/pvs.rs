//! `PvDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::pvs`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct PvDef;

impl ResourceDef for PvDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PersistentVolume }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "PersistentVolume",
            plural: "persistentvolumes", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pv", "persistentvolume", "persistentvolumes", "pvs"] }
    fn short_label(&self) -> &str { "PV" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "CAPACITY", "ACCESS MODES", "RECLAIM POLICY", "STATUS",
         "CLAIM", "STORAGECLASS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
