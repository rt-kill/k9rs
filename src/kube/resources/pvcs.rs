use k8s_openapi::api::core::v1::PersistentVolumeClaim;

use super::ConvertToRow;
use crate::kube::resource_def::pvcs::PvcDef;

use crate::kube::resources::k8s_const::*;
use crate::kube::resources::{CommonMeta, access_mode_short};
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// PvcDef
// ---------------------------------------------------------------------------


impl ConvertToRow<PersistentVolumeClaim> for PvcDef {
    fn convert(pvc: PersistentVolumeClaim) -> ResourceRow {
        let meta = CommonMeta::from_k8s(pvc.metadata);
        let spec = pvc.spec.unwrap_or_default();
        let status_obj = pvc.status.unwrap_or_default();
        let status = status_obj.phase.unwrap_or_else(|| PHASE_PENDING.to_string());
        let volume = spec.volume_name.unwrap_or_default();
        let capacity = status_obj.capacity.as_ref()
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();
        let access_modes = status_obj.access_modes.as_ref()
            .or(spec.access_modes.as_ref())
            .map(|modes| modes.iter().map(|m| access_mode_short(m)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let storage_class = spec.storage_class_name.unwrap_or_default();
        let health = match status.as_str() {
            PHASE_BOUND => RowHealth::Normal,
            PHASE_PENDING => RowHealth::Pending,
            _ => RowHealth::Failed,
        };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Status { text: status, health },
            CellValue::Text(volume),
            CellValue::Text(capacity),
            CellValue::Text(access_modes),
            CellValue::Text(storage_class),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            health,
            cells,
            ..Default::default()
        }
    }
}
