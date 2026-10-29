
use k8s_openapi::api::apps::v1::Deployment;

use super::ConvertToRow;
use k9rs_proto::resource_def::deployments::DeploymentDef;

use crate::resources::{AtZero, CommonMeta, WorkloadContainers, WorkloadTally};
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// DeploymentDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------


impl ConvertToRow<Deployment> for DeploymentDef {
    fn convert(dep: Deployment) -> ResourceRow {
        let meta = CommonMeta::from_k8s(dep.metadata);

        let spec = dep.spec.unwrap_or_default();
        let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
        let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());

        let desired = spec.replicas.unwrap_or(0);
        let status = dep.status.unwrap_or_default();
        let up_to_date = status.updated_replicas.unwrap_or(0);
        let available = status.available_replicas.unwrap_or(0);
        // READY is `readyReplicas`, which is what `kubectl get deploy` shows
        // and what the column means. It used to read `availableReplicas` —
        // a pod is Ready when its probe passes and Available only after
        // `minReadySeconds` on top, so the two agree at the default of 0 and
        // diverge exactly when a rollout is in the window where you are
        // watching this column. It also made READY and AVAILABLE the same
        // number by construction, so one of the two columns said nothing.
        let ready = status.ready_replicas.unwrap_or(0);

        // Drill-down: deployment -> pods by selector labels.
        let drill_target = if !selector_labels.is_empty() {
            Some(DrillTarget::PodsByLabels {
                labels: selector_labels,
                breadcrumb: format!("deploy/{}", meta.name),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        // Red follows the column the user is READING: driving it from
        // `available` while READY shows `ready` would paint a row red next
        // to a `3/3`, and the colour and the number beside it have to agree
        // about what they mean. `status.replicas` — non-terminated pods
        // across every revision — is read for the verdict only, and is the
        // field that says "more pods than we are scaled to" during a surge.
        // At one to three replicas it is the *only* field that says a
        // rollout is happening; see `WorkloadTally::rollout_in_flight`.
        let health = WorkloadTally {
            desired,
            ready,
            total: status.replicas.unwrap_or(0),
            up_to_date: Some(up_to_date),
            at_zero: AtZero::Parked,
        }.health();

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Ratio { num: ready as u32, denom: desired as u32 },
            CellValue::Count(up_to_date as i64),
            CellValue::Count(available as i64),
            CellValue::from_comma_str(&containers.names),
            CellValue::from_comma_str(&containers.images),
            meta.labels_cell(),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];
        ResourceRow {
            cells,
            name: meta.name,
            namespace: Some(meta.namespace),
            pf_ports: containers.tcp_ports,
            health,
            drill_target,
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "../tests/resources/deployments.rs"]
mod tests;
