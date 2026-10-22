//! `LeaseDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::leases`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct LeaseDef;

impl ResourceDef for LeaseDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Lease }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "coordination.k8s.io", version: "v1", kind: "Lease",
            plural: "leases", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["lease", "leases"] }
    fn short_label(&self) -> &str { "LEASE" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "HOLDER", "DURATION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
