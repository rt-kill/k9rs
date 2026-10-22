//! `StatefulSetDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::statefulsets`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct StatefulSetDef;

impl ResourceDef for StatefulSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StatefulSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "StatefulSet",
            plural: "statefulsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sts", "statefulset", "statefulsets"] }
    fn short_label(&self) -> &str { "STS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "SERVICE", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Scale, Restart, PortForward]
    }
}
