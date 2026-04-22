use k8s_openapi::api::core::v1::ConfigMap;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// ConfigMapDef
// ---------------------------------------------------------------------------

pub struct ConfigMapDef;

impl ResourceDef for ConfigMapDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ConfigMap }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ConfigMap",
            plural: "configmaps", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cm", "configmap", "configmaps"] }
    fn short_label(&self) -> &str { "CM" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ConfigMap> for ConfigMapDef {
    fn convert(cm: ConfigMap) -> ResourceRow {
        let meta = CommonMeta::from_k8s(cm.metadata);
        let data_count = cm.data.map(|d| d.len()).unwrap_or(0)
            + cm.binary_data.map(|d| d.len()).unwrap_or(0);
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                data_count.to_string(),
                meta.labels_str, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
