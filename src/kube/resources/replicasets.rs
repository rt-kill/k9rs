
use k8s_openapi::api::apps::v1::ReplicaSet;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// ReplicaSetDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct ReplicaSetDef;

impl ResourceDef for ReplicaSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ReplicaSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "ReplicaSet",
            plural: "replicasets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["rs", "replicaset", "replicasets"] }
    fn short_label(&self) -> &str { "RS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Scale, PortForward]
    }
}

impl ConvertToRow<ReplicaSet> for ReplicaSetDef {
    fn convert(rs: ReplicaSet) -> ResourceRow {
        let meta = CommonMeta::from_k8s(rs.metadata);

        let spec = rs.spec.unwrap_or_default();
        let desired = spec.replicas.unwrap_or(0);
        let containers = WorkloadContainers::from_pod_spec(spec.template.as_ref().and_then(|t| t.spec.as_ref()));

        let status = rs.status.unwrap_or_default();
        let current = status.replicas;
        let ready = status.ready_replicas.unwrap_or(0);

        let drill_target = if !meta.uid.is_empty() {
            Some(DrillTarget::PodsByOwner {
                uid: meta.uid.clone(),
                kind: BuiltInKind::ReplicaSet,
                name: meta.name.clone(),
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
