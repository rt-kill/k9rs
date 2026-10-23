//! `LimitRangeDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::limitranges`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct LimitRangeDef;

impl ResourceDef for LimitRangeDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::LimitRange }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "LimitRange",
            plural: "limitranges", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["limits", "limitrange", "limitranges"] }
    fn short_label(&self) -> &str { "Limits" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
