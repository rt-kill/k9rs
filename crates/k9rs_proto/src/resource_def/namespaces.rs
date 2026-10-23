//! `NamespaceDef` — identity, aliases, columns and operations.
//!
//! The METADATA half of `resources::namespaces`. Split out because
//! `impl ResourceDef` is pure data while `impl ConvertToRow<K>` needs the
//! k8s-openapi type: the pure/impure seam runs THROUGH each resource file,
//! not between directories. Keeping the two halves in one file is what makes
//! the whole definition table transitively depend on kube-rs, and that is
//! what stops the wire module from being extractable.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::{ResourceScope};

pub struct NamespaceDef;

impl ResourceDef for NamespaceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Namespace }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Namespace",
            plural: "namespaces", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ns", "namespace", "namespaces"] }
    fn short_label(&self) -> &str { "NS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "STATUS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn is_core(&self) -> bool { true }
}
