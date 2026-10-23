use k8s_openapi::api::core::v1::Secret;

use super::ConvertToRow;
use k9rs_proto::resource_def::secrets::SecretDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// SecretDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Secret> for SecretDef {
    fn convert(secret: Secret) -> ResourceRow {
        let meta = CommonMeta::from_k8s(secret.metadata);
        let secret_type = secret.type_.unwrap_or_else(|| "Opaque".to_string());
        let data_count = secret.data.map(|d| d.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(secret_type),
            CellValue::Count(data_count as i64),
            CellValue::Text(meta.labels_str),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
