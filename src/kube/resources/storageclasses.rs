use k8s_openapi::api::storage::v1::StorageClass;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// StorageClassDef
// ---------------------------------------------------------------------------

pub struct StorageClassDef;

impl ResourceDef for StorageClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StorageClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "storage.k8s.io", version: "v1", kind: "StorageClass",
            plural: "storageclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sc", "storageclass", "storageclasses"] }
    fn short_label(&self) -> &str { "SC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "PROVISIONER", "RECLAIM POLICY", "VOLUME BINDING MODE",
         "EXPANSION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<StorageClass> for StorageClassDef {
    fn convert(sc: StorageClass) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sc.metadata);
        let provisioner = sc.provisioner;
        let reclaim_policy = sc.reclaim_policy.unwrap_or_else(|| "Delete".to_string());
        let volume_binding_mode = sc.volume_binding_mode.unwrap_or_else(|| "Immediate".to_string());
        let allow_expansion = if sc.allow_volume_expansion.unwrap_or(false) { "true" } else { "false" };
        ResourceRow {
            cells: vec![
                meta.name.clone(),
                provisioner, reclaim_policy, volume_binding_mode,
                allow_expansion.to_string(),
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: None,
            ..Default::default()
        }
    }
}
