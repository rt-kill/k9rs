//! `ServiceAccountDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::serviceaccounts`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct ServiceAccountDef;

impl ResourceDef for ServiceAccountDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ServiceAccount }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ServiceAccount",
            plural: "serviceaccounts", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sa", "serviceaccount", "serviceaccounts"] }
    fn short_label(&self) -> &str { "SA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SECRETS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
