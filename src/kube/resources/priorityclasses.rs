use k8s_openapi::api::scheduling::v1::PriorityClass;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// PriorityClassDef
// ---------------------------------------------------------------------------

pub struct PriorityClassDef;

impl ResourceDef for PriorityClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PriorityClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "scheduling.k8s.io", version: "v1", kind: "PriorityClass",
            plural: "priorityclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pc", "priorityclass", "priorityclasses"] }
    fn short_label(&self) -> &str { "PC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "VALUE", "GLOBAL-DEFAULT", "PREEMPTION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PriorityClass> for PriorityClassDef {
    fn convert(pc: PriorityClass) -> ResourceRow {
        let meta = CommonMeta::from_k8s(pc.metadata);
        let _value = pc.value.to_string();
        let _global_default = if pc.global_default.unwrap_or(false) { "true" } else { "false" };
        let preemption = pc.preemption_policy.unwrap_or_else(|| "PreemptLowerPriority".to_string());
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Count(pc.value as i64),
            CellValue::Bool(pc.global_default.unwrap_or(false)),
            CellValue::Text(preemption),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}
