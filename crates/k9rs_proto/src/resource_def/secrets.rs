//! `SecretDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::secrets`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::{OperationKind, ResourceScope};

pub struct SecretDef;

impl ResourceDef for SecretDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Secret }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Secret",
            plural: "secrets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sec", "secret", "secrets"] }
    fn short_label(&self) -> &str { "Secrets" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, DecodeSecret]
    }
}
