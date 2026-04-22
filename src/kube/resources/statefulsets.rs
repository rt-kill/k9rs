
use k8s_openapi::api::apps::v1::StatefulSet;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// StatefulSetDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct StatefulSetDef;

impl ResourceDef for StatefulSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StatefulSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "StatefulSet",
            plural: "statefulsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sts", "statefulset", "statefulsets"] }
    fn short_label(&self) -> &str { "STS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "SERVICE", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Scale, Restart, PortForward]
    }
}

impl ConvertToRow<StatefulSet> for StatefulSetDef {
    fn convert(sts: StatefulSet) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sts.metadata);

        let spec = sts.spec.unwrap_or_default();
        let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
        let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());
        let service_name = spec.service_name.clone();

        let desired = spec.replicas.unwrap_or(0);
        let ready_replicas = sts.status.and_then(|s| s.ready_replicas).unwrap_or(0);
        let ready = format!("{}/{}", ready_replicas, desired);

        let drill_target = if !selector_labels.is_empty() {
            Some(DrillTarget::PodsByLabels {
                labels: selector_labels,
                breadcrumb: format!("sts/{}", meta.name),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        let health = if desired == 0 { RowHealth::Pending }
            else if ready_replicas < desired { RowHealth::Failed }
            else { RowHealth::Normal };

        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(), ready,
                service_name, containers.names, containers.images,
                meta.labels_str, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            pf_ports: containers.tcp_ports,
            health,
            drill_target,
            ..Default::default()
        }
    }
}
