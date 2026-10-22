//! `PodDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::pods`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{MetricsKind, BuiltInKind, ColumnDef, Gvr, MetricsColumn, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct PodDef;

impl ResourceDef for PodDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Pod }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Pod",
            plural: "pods", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["po", "pod", "pods"] }
    fn short_label(&self) -> &str { "Pods" }
    fn default_headers(&self) -> Vec<String> {
        // Order must match column_defs() and the cells vec in convert().
        ["NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "LAST RESTART",
         "CPU", "%CPU/R", "%CPU/L",
         "MEM", "%MEM/R", "%MEM/L",
         "CPU/R", "CPU/L", "MEM/R", "MEM/L",
         "IP", "NODE", "NOMINATED NODE",
         "QOS", "SERVICE-ACCOUNT",
         "READINESS GATES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn metrics_kind(&self) -> Option<MetricsKind> { Some(MetricsKind::Pod) }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        use MetricsColumn::*;
        // Column levels match k9s: percentage metrics are default-visible,
        // absolute request/limit and lifecycle details are extra/wide.
        vec![
            C::new("NAMESPACE"), C::new("NAME"), C::new("READY"), C::new("STATUS"),
            C::new("RESTARTS"), C::extra("LAST RESTART"),
            C::new("CPU").with_metrics(Cpu),
            C::new("%CPU/R").with_metrics(CpuPercentRequest),
            C::new("%CPU/L").with_metrics(CpuPercentLimit),
            C::new("MEM").with_metrics(Mem),
            C::new("%MEM/R").with_metrics(MemPercentRequest),
            C::new("%MEM/L").with_metrics(MemPercentLimit),
            C::extra("CPU/R"), C::extra("CPU/L"), C::extra("MEM/R"), C::extra("MEM/L"),
            C::new("IP"), C::new("NODE"), C::extra("NOMINATED NODE"),
            C::extra("QOS"),
            C::extra("SERVICE-ACCOUNT"), C::extra("READINESS GATES"),
            C::extra("LABELS"), C::new("AGE"),
        ]
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Shell, ShowNode, PortForward, ForceKill]
    }
}
