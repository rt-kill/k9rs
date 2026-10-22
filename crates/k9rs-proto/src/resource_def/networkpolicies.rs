//! `NetworkPolicyDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::networkpolicies`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

pub struct NetworkPolicyDef;

impl ResourceDef for NetworkPolicyDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::NetworkPolicy }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "networking.k8s.io", version: "v1", kind: "NetworkPolicy",
            plural: "networkpolicies", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["np", "networkpolicy", "networkpolicies"] }
    fn short_label(&self) -> &str { "NetPol" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "POD-SELECTOR", "POLICY TYPES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
