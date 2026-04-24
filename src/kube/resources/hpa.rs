use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// HpaDef
// ---------------------------------------------------------------------------

pub struct HpaDef;

impl ResourceDef for HpaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Hpa }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "autoscaling", version: "v2", kind: "HorizontalPodAutoscaler",
            plural: "horizontalpodautoscalers", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["hpa", "horizontalpodautoscaler", "horizontalpodautoscalers"] }
    fn short_label(&self) -> &str { "HPA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "REFERENCE", "MIN", "MAX", "CURRENT", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<HorizontalPodAutoscaler> for HpaDef {
    fn convert(hpa: HorizontalPodAutoscaler) -> ResourceRow {
        let meta = CommonMeta::from_k8s(hpa.metadata);
        let spec = hpa.spec.unwrap_or_default();
        let reference = format!("{}/{}", spec.scale_target_ref.kind, spec.scale_target_ref.name);
        let min_replicas = spec.min_replicas.map(|v| v.to_string()).unwrap_or_else(|| "<unset>".to_string());
        let _max_replicas = spec.max_replicas.to_string();
        let status = hpa.status.unwrap_or_default();
        let current_replicas = status.current_replicas.unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(reference),
            CellValue::Text(min_replicas),
            CellValue::Count(spec.max_replicas as i64),
            CellValue::Count(current_replicas as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
