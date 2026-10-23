use k8s_openapi::api::storage::v1::StorageClass;

use super::ConvertToRow;
use k9rs_proto::resource_def::storageclasses::StorageClassDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// StorageClassDef
// ---------------------------------------------------------------------------


impl ConvertToRow<StorageClass> for StorageClassDef {
    fn convert(sc: StorageClass) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sc.metadata);
        let provisioner = sc.provisioner;
        let reclaim_policy = sc.reclaim_policy.unwrap_or_else(|| "Delete".to_string());
        let volume_binding_mode = sc.volume_binding_mode.unwrap_or_else(|| "Immediate".to_string());
        let allow_expansion = sc.allow_volume_expansion.unwrap_or(false);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Text(provisioner),
            CellValue::Text(reclaim_policy),
            CellValue::Text(volume_binding_mode),
            CellValue::Bool(allow_expansion),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}
