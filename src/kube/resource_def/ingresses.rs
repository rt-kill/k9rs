//! `IngressDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::ingresses`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct IngressDef;

impl ResourceDef for IngressDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Ingress }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "networking.k8s.io", version: "v1", kind: "Ingress",
            plural: "ingresses", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ing", "ingress", "ingresses"] }
    fn short_label(&self) -> &str { "Ing" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "CLASS", "HOSTS", "ADDRESS", "PORTS", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
