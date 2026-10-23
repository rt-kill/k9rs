
use k8s_openapi::api::apps::v1::StatefulSet;

use super::ConvertToRow;
use k9rs_proto::resource_def::statefulsets::StatefulSetDef;

use crate::resources::{AtZero, CommonMeta, WorkloadContainers, WorkloadTally};
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// StatefulSetDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------


impl ConvertToRow<StatefulSet> for StatefulSetDef {
    fn convert(sts: StatefulSet) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sts.metadata);

        let spec = sts.spec.unwrap_or_default();
        let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
        let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());
        let service_name = spec.service_name.clone();

        let desired = spec.replicas.unwrap_or(0);
        let status = sts.status.unwrap_or_default();
        let ready_replicas = status.ready_replicas.unwrap_or(0);
        let drill_target = if !selector_labels.is_empty() {
            Some(DrillTarget::PodsByLabels {
                labels: selector_labels,
                breadcrumb: format!("sts/{}", meta.name),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        // `updatedReplicas` is deliberately not fed in: the StatefulSet table
        // shows no UP-TO-DATE column (neither does k9s's, nor `kubectl get
        // sts`), so it could only produce a yellow with nothing in the row to
        // explain it. It costs nothing to leave out — an STS rolling update
        // recreates pods in place one at a time, so its rollout already shows
        // as a READY dip, which is red by clause 2.
        let health = WorkloadTally {
            desired,
            ready: ready_replicas,
            total: status.replicas,
            up_to_date: None,
            at_zero: AtZero::Parked,
        }.health();

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Ratio { num: ready_replicas as u32, denom: desired as u32 },
            CellValue::Text(service_name),
            CellValue::from_comma_str(&containers.names),
            CellValue::from_comma_str(&containers.images),
            CellValue::from_comma_str(&meta.labels_str),
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
