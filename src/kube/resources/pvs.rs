use k8s_openapi::api::core::v1::PersistentVolume;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::k8s_const::*;
use crate::kube::resources::{CommonMeta, access_mode_short};
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// PvDef
// ---------------------------------------------------------------------------

pub struct PvDef;

impl ResourceDef for PvDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PersistentVolume }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "PersistentVolume",
            plural: "persistentvolumes", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pv", "persistentvolume", "persistentvolumes", "pvs"] }
    fn short_label(&self) -> &str { "PV" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "CAPACITY", "ACCESS MODES", "RECLAIM POLICY", "STATUS",
         "CLAIM", "STORAGECLASS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PersistentVolume> for PvDef {
    fn convert(pv: PersistentVolume) -> ResourceRow {
        let meta = CommonMeta::from_k8s(pv.metadata);
        let spec = pv.spec.unwrap_or_default();
        let capacity = spec.capacity.as_ref()
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();
        let access_modes = spec.access_modes.as_ref()
            .map(|modes| modes.iter().map(|m| access_mode_short(m)).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let reclaim_policy = spec.persistent_volume_reclaim_policy.unwrap_or_else(|| "Retain".to_string());
        let status = pv.status.and_then(|s| s.phase).unwrap_or_else(|| PHASE_AVAILABLE.to_string());
        let claim = spec.claim_ref
            .map(|cr| {
                let cns = cr.namespace.unwrap_or_default();
                let cn = cr.name.unwrap_or_default();
                if cns.is_empty() { cn } else { format!("{}/{}", cns, cn) }
            })
            .unwrap_or_default();
        let storage_class = spec.storage_class_name.unwrap_or_default();
        let health = match status.as_str() {
            PHASE_BOUND => RowHealth::Normal,
            PHASE_AVAILABLE | "Released" => RowHealth::Pending,
            _ => RowHealth::Failed,
        };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Text(capacity),
            CellValue::Text(access_modes),
            CellValue::Text(reclaim_policy),
            CellValue::Status { text: status, health },
            CellValue::Text(claim),
            CellValue::Text(storage_class),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            health,
            cells,
            ..Default::default()
        }
    }
}
