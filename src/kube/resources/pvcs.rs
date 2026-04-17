use k8s_openapi::api::core::v1::PersistentVolumeClaim;

use crate::kube::resources::{CommonMeta, access_mode_short};
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s PersistentVolumeClaim into a generic ResourceRow.
pub(crate) fn pvc_to_row(pvc: PersistentVolumeClaim) -> ResourceRow {
    let meta = CommonMeta::from_k8s(pvc.metadata);
    let spec = pvc.spec.unwrap_or_default();
    let status_obj = pvc.status.unwrap_or_default();
    let status = status_obj.phase.unwrap_or_else(|| "Pending".to_string());
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
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            status, volume, capacity, access_modes, storage_class,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
