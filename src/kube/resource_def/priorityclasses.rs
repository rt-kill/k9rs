//! `PriorityClassDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::priorityclasses`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct PriorityClassDef;

impl ResourceDef for PriorityClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PriorityClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "scheduling.k8s.io", version: "v1", kind: "PriorityClass",
            plural: "priorityclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pc", "priorityclass", "priorityclasses"] }
    fn short_label(&self) -> &str { "PC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "VALUE", "GLOBAL-DEFAULT", "PREEMPTION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
