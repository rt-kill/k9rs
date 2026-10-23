use k8s_openapi::api::scheduling::v1::PriorityClass;

use super::ConvertToRow;
use k9rs_proto::resource_def::priorityclasses::PriorityClassDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// PriorityClassDef
// ---------------------------------------------------------------------------


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
