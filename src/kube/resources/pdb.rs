use k8s_openapi::api::policy::v1::PodDisruptionBudget;

use super::ConvertToRow;
use crate::kube::resource_def::pdb::PodDisruptionBudgetDef;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

fn int_or_string_display(v: &IntOrString) -> String {
    match v {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

// ---------------------------------------------------------------------------
// PodDisruptionBudgetDef
// ---------------------------------------------------------------------------


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
        let allowed = allowed_disruptions.parse::<i64>().unwrap_or(0);
        let current = current_healthy.parse::<i64>().unwrap_or(0);
        let desired = desired_healthy.parse::<i64>().unwrap_or(0);

        // `currentHealthy < desiredHealthy` is the breach: the budget is not
        // being met and a disruption would take you further below it.
        //
        // NOT `allowed == 0`. That was the first cut, and it painted the
        // healthy steady state red: a 1-replica Deployment with
        // `minAvailable: 1` sits at `disruptionsAllowed = 0` forever — it is
        // satisfied and has no slack, which is the normal condition of every
        // singleton and every `maxUnavailable: 0` budget. It also made this
        // arm unreachable, because a real breach forces `allowed` to 0 too,
        // so both states were Failed and nothing was ever Pending.
        let health = if current < desired {
            RowHealth::Failed
        } else if allowed == 0 && expected_pods > 0 {
            // Met, but nothing can be evicted — a drain will block here.
            RowHealth::Pending
        } else {
            RowHealth::Normal
        };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(min_available),
            CellValue::Text(max_unavailable),
            // `Count`, not `Status`: wrapping a number in `Status` sorts the
            // column as TEXT (`row.rs`'s `Status` arm compares strings), so
            // `O` on it gave 9, 8, 7, 3, 20, 12, 10, 1. The row health is the
            // verdict; the number stays a number.
            CellValue::Count(allowed),
            CellValue::Count(current),
            CellValue::Count(desired),
            CellValue::Count(expected_pods as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            health,
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "../../tests/kube/resources/pdb.rs"]
mod tests;
