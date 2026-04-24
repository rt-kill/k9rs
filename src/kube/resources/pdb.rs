use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

fn int_or_string_display(v: &IntOrString) -> String {
    match v {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

// ---------------------------------------------------------------------------
// PodDisruptionBudgetDef
// ---------------------------------------------------------------------------

pub struct PodDisruptionBudgetDef;

impl ResourceDef for PodDisruptionBudgetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PodDisruptionBudget }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "policy", version: "v1", kind: "PodDisruptionBudget",
            plural: "poddisruptionbudgets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pdb", "poddisruptionbudget", "poddisruptionbudgets"] }
    fn short_label(&self) -> &str { "PDB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "MIN AVAILABLE", "MAX UNAVAILABLE",
         "ALLOWED DISRUPTIONS", "CURRENT", "DESIRED", "EXPECTED", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PodDisruptionBudget> for PodDisruptionBudgetDef {
    fn convert(pdb: PodDisruptionBudget) -> ResourceRow {
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
            .map(|s| s.expected_pods)
            .unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(min_available),
            CellValue::Text(max_unavailable),
            CellValue::Count(allowed_disruptions.parse::<i64>().unwrap_or(0)),
            CellValue::Count(current_healthy.parse::<i64>().unwrap_or(0)),
            CellValue::Count(desired_healthy.parse::<i64>().unwrap_or(0)),
            CellValue::Count(expected_pods as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
