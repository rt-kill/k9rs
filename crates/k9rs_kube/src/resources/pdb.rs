use k8s_openapi::api::policy::v1::PodDisruptionBudget;

use super::ConvertToRow;
use k9rs_proto::resource_def::pdb::PodDisruptionBudgetDef;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

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
        // A PDB sets exactly one of these two, so the other really is
        // absent. `Placeholder` rather than `Text("N/A")`: `Placeholder`
        // renders `n/a` — the word the header and every other absent cell
        // already use — so the same concept was spelled two ways differing
        // only in case, and `Text` also sorts absent BEFORE every real
        // value instead of past it.
        let cell = |v: Option<&k8s_openapi::apimachinery::pkg::util::intstr::IntOrString>| {
            match v {
                Some(v) => CellValue::Text(int_or_string_display(v)),
                None => CellValue::Placeholder,
            }
        };
        let min_available = cell(spec.as_ref().and_then(|s| s.min_available.as_ref()));
        let max_unavailable = cell(spec.as_ref().and_then(|s| s.max_unavailable.as_ref()));
        // Straight to `i64`. These were built as `String` and parsed back
        // three lines later, and the strings were never displayed — the
        // cells are `Count`. Residue of the `Status`→`Count` fix below.
        let status = pdb.status.as_ref();
        let allowed = status.map(|s| i64::from(s.disruptions_allowed)).unwrap_or(0);
        let current = status.map(|s| i64::from(s.current_healthy)).unwrap_or(0);
        let desired = status.map(|s| i64::from(s.desired_healthy)).unwrap_or(0);
        let expected_pods = status.map(|s| s.expected_pods).unwrap_or(0);

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
            min_available,
            max_unavailable,
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
#[path = "../tests/resources/pdb.rs"]
mod tests;
