use k8s_openapi::api::policy::v1::PodDisruptionBudget;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s PodDisruptionBudget into a generic ResourceRow.
pub(crate) fn pdb_to_row(pdb: PodDisruptionBudget) -> ResourceRow {
    let meta = CommonMeta::from_k8s(pdb.metadata);
    let spec = pdb.spec;
    let min_available = spec.as_ref()
        .and_then(|s| s.min_available.as_ref())
        .map(|v| match v {
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => i.to_string(),
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => s.clone(),
        })
        .unwrap_or_else(|| "N/A".to_string());
    let max_unavailable = spec.as_ref()
        .and_then(|s| s.max_unavailable.as_ref())
        .map(|v| match v {
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => i.to_string(),
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => s.clone(),
        })
        .unwrap_or_else(|| "N/A".to_string());
    let allowed_disruptions = pdb.status.as_ref()
        .map(|s| s.disruptions_allowed.to_string())
        .unwrap_or_else(|| "0".to_string());
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            min_available, max_unavailable, allowed_disruptions,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
