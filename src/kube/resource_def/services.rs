//! `ServiceDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::services`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, ColumnDef, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct ServiceDef;

impl ResourceDef for ServiceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Service }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Service",
            plural: "services", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["svc", "service", "services"] }
    fn short_label(&self) -> &str { "Svc" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "CLUSTER-IP", "EXTERNAL-IP", "SELECTOR",
         "PORT(S)", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        vec![
            C::new("NAMESPACE"), C::new("NAME"), C::new("TYPE"),
            C::new("CLUSTER-IP"), C::new("EXTERNAL-IP"),
            C::extra("SELECTOR"), C::new("PORT(S)"),
            C::extra("LABELS"), C::new("AGE"),
        ]
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, PortForward]
    }
}
