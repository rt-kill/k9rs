use k8s_openapi::api::core::v1::LimitRange;

use super::ConvertToRow;
use k9rs_proto::resource_def::limitranges::LimitRangeDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// LimitRangeDef
// ---------------------------------------------------------------------------


impl ConvertToRow<LimitRange> for LimitRangeDef {
    fn convert(lr: LimitRange) -> ResourceRow {
        let meta = CommonMeta::from_k8s(lr.metadata);
        let types = lr.spec
            .and_then(|spec| {
                let items: Vec<String> = spec.limits.iter().map(|item| item.type_.clone()).collect();
                if items.is_empty() { None } else { Some(items.join(", ")) }
            })
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(types),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
