use k8s_openapi::api::core::v1::ResourceQuota;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ResourceQuotaDef
// ---------------------------------------------------------------------------

pub struct ResourceQuotaDef;

impl ResourceDef for ResourceQuotaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ResourceQuota }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ResourceQuota",
            plural: "resourcequotas", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["quota", "resourcequota", "resourcequotas"] }
    fn short_label(&self) -> &str { "Quota" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "HARD", "USED", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ResourceQuota> for ResourceQuotaDef {
    fn convert(rq: ResourceQuota) -> ResourceRow {
        let meta = CommonMeta::from_k8s(rq.metadata);
        let format_qmap = |map: &Option<std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| -> String {
            match map {
                Some(m) if !m.is_empty() => m.iter().map(|(k, v)| format!("{}: {}", k, v.0)).collect::<Vec<_>>().join(", "),
                _ => String::new(),
            }
        };
        let hard = rq.spec.as_ref().map(|s| format_qmap(&s.hard)).unwrap_or_default();
        let used = rq.status.as_ref().map(|s| format_qmap(&s.used)).unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(hard),
            CellValue::Text(used),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
