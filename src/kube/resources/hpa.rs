use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s HorizontalPodAutoscaler into a generic ResourceRow.
pub(crate) fn hpa_to_row(hpa: HorizontalPodAutoscaler) -> ResourceRow {
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
