//! `ResourceQuotaDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::resourcequotas`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct ResourceQuotaDef;

impl ResourceDef for ResourceQuotaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ResourceQuota }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ResourceQuota",
            plural: "resourcequotas", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["quota", "resourcequota", "resourcequotas"] }
    fn short_label(&self) -> &str { "Quota" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "HARD", "USED", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
