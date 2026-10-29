use k8s_openapi::api::core::v1::ConfigMap;

use super::ConvertToRow;
use k9rs_proto::resource_def::configmaps::ConfigMapDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ConfigMapDef
// ---------------------------------------------------------------------------


impl ConvertToRow<ConfigMap> for ConfigMapDef {
    fn convert(cm: ConfigMap) -> ResourceRow {
        let meta = CommonMeta::from_k8s(cm.metadata);
        let data_count = cm.data.map(|d| d.len()).unwrap_or(0)
            + cm.binary_data.map(|d| d.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Count(data_count as i64),
            meta.labels_cell(),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
