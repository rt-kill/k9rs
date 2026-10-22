//! `HpaDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::hpa`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct HpaDef;

impl ResourceDef for HpaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Hpa }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "autoscaling", version: "v2", kind: "HorizontalPodAutoscaler",
            plural: "horizontalpodautoscalers", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["hpa", "horizontalpodautoscaler", "horizontalpodautoscalers"] }
    fn short_label(&self) -> &str { "HPA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "REFERENCE", "MIN", "MAX", "CURRENT", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
