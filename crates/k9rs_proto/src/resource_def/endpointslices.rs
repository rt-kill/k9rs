//! `EndpointSliceDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::endpointslices`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct EndpointSliceDef;

impl ResourceDef for EndpointSliceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::EndpointSlice }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "discovery.k8s.io", version: "v1", kind: "EndpointSlice",
            plural: "endpointslices", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["eps", "endpointslice", "endpointslices"] }
    fn short_label(&self) -> &str { "EPS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ADDRESSTYPE", "ENDPOINTS", "PORTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
