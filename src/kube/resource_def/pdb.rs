//! `PodDisruptionBudgetDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::pdb`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

pub struct PodDisruptionBudgetDef;

impl ResourceDef for PodDisruptionBudgetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PodDisruptionBudget }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "policy", version: "v1", kind: "PodDisruptionBudget",
            plural: "poddisruptionbudgets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pdb", "poddisruptionbudget", "poddisruptionbudgets"] }
    fn short_label(&self) -> &str { "PDB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "MIN AVAILABLE", "MAX UNAVAILABLE",
         "ALLOWED DISRUPTIONS", "CURRENT", "DESIRED", "EXPECTED", "AGE"]
            .into_iter().map(String::from).collect()
    }
}
