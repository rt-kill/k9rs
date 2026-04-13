use k8s_openapi::api::core::v1::PersistentVolumeClaim;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s PersistentVolumeClaim into a generic ResourceRow.
pub(crate) fn pvc_to_row(pvc: PersistentVolumeClaim) -> ResourceRow {
    use crate::kube::resources::access_mode_short;
    let metadata = pvc.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
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
        cells: vec![ns.clone(), name.clone(), status, volume, capacity, access_modes, storage_class, crate::util::format_age(age)],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
