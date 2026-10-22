//! `JobDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::jobs`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct JobDef;

impl ResourceDef for JobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Job }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "Job",
            plural: "jobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["job", "jobs"] }
    fn short_label(&self) -> &str { "Jobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "COMPLETIONS", "DURATION", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, PortForward]
    }
}
