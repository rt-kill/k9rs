//! `ReplicaSetDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::replicasets`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::{OperationKind, ResourceScope};

pub struct ReplicaSetDef;

impl ResourceDef for ReplicaSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ReplicaSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "ReplicaSet",
            plural: "replicasets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["rs", "replicaset", "replicasets"] }
    fn short_label(&self) -> &str { "RS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Scale, PortForward]
    }
}
