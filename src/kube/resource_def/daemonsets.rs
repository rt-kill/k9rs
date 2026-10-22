//! `DaemonSetDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::daemonsets`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct DaemonSetDef;

impl ResourceDef for DaemonSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::DaemonSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "DaemonSet",
            plural: "daemonsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ds", "daemonset", "daemonsets"] }
    fn short_label(&self) -> &str { "DS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY", "UP-TO-DATE",
         "AVAILABLE", "NODE SELECTOR", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Restart, PortForward]
    }
}
