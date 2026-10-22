//! `DeploymentDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::deployments`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct DeploymentDef;

impl ResourceDef for DeploymentDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Deployment }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "Deployment",
            plural: "deployments", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["dp", "deploy", "deployment", "deployments"] }
    fn short_label(&self) -> &str { "Deploy" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "UP-TO-DATE", "AVAILABLE",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Scale, Restart, PortForward]
    }
}
