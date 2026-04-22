use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

fn int_or_string_display(v: &IntOrString) -> String {
    match v {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

/// Convert a k8s PodDisruptionBudget into a generic ResourceRow.
pub(crate) fn pdb_to_row(pdb: PodDisruptionBudget) -> ResourceRow {
    let meta = CommonMeta::from_k8s(pdb.metadata);
    let spec = pdb.spec;
    let min_available = spec.as_ref()
        .and_then(|s| s.min_available.as_ref())
        .map(int_or_string_display)
        .unwrap_or_else(|| "N/A".to_string());
    let max_unavailable = spec.as_ref()
        .and_then(|s| s.max_unavailable.as_ref())
        .map(int_or_string_display)
        .unwrap_or_else(|| "N/A".to_string());
    let status = pdb.status.as_ref();
    let allowed_disruptions = status
        .map(|s| s.disruptions_allowed.to_string())
        .unwrap_or_else(|| "0".to_string());
    let current_healthy = status
        .map(|s| s.current_healthy.to_string())
        .unwrap_or_else(|| "0".to_string());
    let desired_healthy = status
        .map(|s| s.desired_healthy.to_string())
        .unwrap_or_else(|| "0".to_string());
    let expected_pods = status
        .map(|s| s.expected_pods.to_string())
        .unwrap_or_else(|| "0".to_string());
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            min_available, max_unavailable, allowed_disruptions,
            current_healthy, desired_healthy, expected_pods,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
