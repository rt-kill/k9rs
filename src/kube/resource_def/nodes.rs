//! `NodeDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::nodes`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{MetricsKind, BuiltInKind, ColumnDef, Gvr, MetricsColumn, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct NodeDef;

impl ResourceDef for NodeDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Node }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Node",
            plural: "nodes", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["no", "node", "nodes"] }
    fn short_label(&self) -> &str { "Nodes" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "STATUS", "ROLES", "INSTANCE-TYPE", "TAINTS", "VERSION", "PODS/A",
         "CPU", "CPU/A", "%CPU", "MEM", "MEM/A", "%MEM",
         "GPU/A", "GPU/C", "SH-GPU/A", "SH-GPU/C",
         "OS-IMAGE", "KERNEL", "INTERNAL-IP", "EXTERNAL-IP", "ARCH", "LABELS",
         "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn is_core(&self) -> bool { true }
    fn metrics_kind(&self) -> Option<MetricsKind> { Some(MetricsKind::Node) }
    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, NodeShell]
    }
    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        use MetricsColumn::*;
        vec![
            C::new("NAME"), C::new("STATUS"), C::new("ROLES"),
            C::new("INSTANCE-TYPE"),
            C::new("TAINTS"), C::new("VERSION"), C::new("PODS/A"),
            C::new("CPU").with_metrics(Cpu),
            C::new("CPU/A").with_metrics(CpuAlloc),
            C::new("%CPU").with_metrics(CpuPercent),
            C::new("MEM").with_metrics(Mem),
            C::new("MEM/A").with_metrics(MemAlloc),
            C::new("%MEM").with_metrics(MemPercent),
            C::new("GPU/A"), C::new("GPU/C"),
            C::new("SH-GPU/A"), C::new("SH-GPU/C"),
            C::extra("OS-IMAGE"), C::extra("KERNEL"),
            C::extra("INTERNAL-IP"), C::extra("EXTERNAL-IP"),
            C::extra("ARCH"), C::extra("LABELS"),
            C::new("AGE"),
        ]
    }
}
