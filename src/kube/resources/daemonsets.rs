
use k8s_openapi::api::apps::v1::DaemonSet;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// DaemonSetDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct DaemonSetDef;

impl ResourceDef for DaemonSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::DaemonSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "DaemonSet",
            plural: "daemonsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ds", "daemonset", "daemonsets"] }
    fn short_label(&self) -> &str { "DS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY", "UP-TO-DATE",
         "AVAILABLE", "NODE SELECTOR", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Restart, PortForward]
    }
}

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

        let health = if desired == 0 { RowHealth::Pending }
            else if ready < desired { RowHealth::Failed }
            else { RowHealth::Normal };

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
