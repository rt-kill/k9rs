//! `EndpointsDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::endpoints`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct EndpointsDef;

impl ResourceDef for EndpointsDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Endpoints }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Endpoints",
            plural: "endpoints", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ep", "endpoints"] }
    fn short_label(&self) -> &str { "EP" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ENDPOINTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
