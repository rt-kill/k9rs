
use k8s_openapi::api::apps::v1::DaemonSet;

use super::ConvertToRow;
use k9rs_proto::resource_def::daemonsets::DaemonSetDef;

use crate::resources::{AtZero, CommonMeta, WorkloadContainers, WorkloadTally};
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// DaemonSetDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------


impl ConvertToRow<DaemonSet> for DaemonSetDef {
    fn convert(ds: DaemonSet) -> ResourceRow {
        let meta = CommonMeta::from_k8s(ds.metadata);

        let selector_labels = ds.spec.as_ref()
            .and_then(|s| s.selector.match_labels.clone())
            .unwrap_or_default();
        let pod_spec = ds.spec.as_ref().and_then(|s| s.template.spec.as_ref());
        let containers = WorkloadContainers::from_pod_spec(pod_spec);

        let node_selector = pod_spec
            .and_then(|ps| ps.node_selector.as_ref())
            .map(|ns| {
                let pairs: Vec<String> = ns.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                if pairs.is_empty() { "<none>".to_string() } else { pairs.join(",") }
            })
            .unwrap_or_else(|| "<none>".to_string());

        let status = ds.status.unwrap_or_default();
        let desired = status.desired_number_scheduled;
        let current = status.current_number_scheduled;
        let ready = status.number_ready;
        let up_to_date = status.updated_number_scheduled.unwrap_or(0);
        let available = status.number_available.unwrap_or(0);

        let drill_target = if !selector_labels.is_empty() {
            Some(DrillTarget::PodsByLabels {
                labels: selector_labels,
                breadcrumb: format!("ds/{}", meta.name),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        // A DaemonSet's "scale target" is a node count, so `total` is the
        // pods actually scheduled. Unlike a Deployment, its default strategy
        // is `maxSurge: 0` / `maxUnavailable: 1` — it takes a pod away before
        // adding one, so its rollouts were always visible as a READY dip.
        // That asymmetry, not a wrong field, is why a Deployment rollout used
        // to pass silently and this one did not.
        let health = WorkloadTally {
            desired,
            ready,
            total: current,
            up_to_date: Some(up_to_date),
            at_zero: AtZero::Parked,
        }.health();

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Count(desired as i64),
            CellValue::Count(current as i64),
            CellValue::Count(ready as i64),
            CellValue::Count(up_to_date as i64),
            CellValue::Count(available as i64),
            CellValue::Text(node_selector),
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
