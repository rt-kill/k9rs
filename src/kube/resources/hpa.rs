use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

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
        let max_replicas = spec.max_replicas.to_string();
        let status = hpa.status.unwrap_or_default();
        let current_replicas = status.current_replicas.map(|v| v.to_string()).unwrap_or_else(|| "0".to_string());
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                reference, min_replicas, max_replicas, current_replicas,
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
