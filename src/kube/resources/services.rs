use k8s_openapi::api::core::v1::Service;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// ServiceDef
// ---------------------------------------------------------------------------

pub struct ServiceDef;

impl ResourceDef for ServiceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Service }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Service",
            plural: "services", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["svc", "service", "services"] }
    fn short_label(&self) -> &str { "Svc" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "CLUSTER-IP", "EXTERNAL-IP", "SELECTOR",
         "PORT(S)", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        vec![
            C::new("NAMESPACE"), C::new("NAME"), C::new("TYPE"),
            C::new("CLUSTER-IP"), C::new("EXTERNAL-IP"),
            C::extra("SELECTOR"), C::new("PORT(S)"),
            C::extra("LABELS"), C::age("AGE"),
        ]
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, PortForward]
    }
}

impl ConvertToRow<Service> for ServiceDef {
    fn convert(svc: Service) -> ResourceRow {
        let meta = CommonMeta::from_k8s(svc.metadata);

        let spec = svc.spec.unwrap_or_default();
        let selector = spec.selector.clone().unwrap_or_default();
        let service_type = spec.type_.unwrap_or_else(|| "ClusterIP".to_string());
        let cluster_ip = spec.cluster_ip.unwrap_or_else(|| "<none>".to_string());

        let external_ip = {
            let mut ips: Vec<String> = Vec::new();
            if let Some(ext_ips) = &spec.external_ips {
                ips.extend(ext_ips.clone());
            }
            if let Some(ref status) = svc.status {
                if let Some(ref lb) = status.load_balancer {
                    if let Some(ref ingress) = lb.ingress {
                        for ing in ingress {
                            if let Some(ref ip) = ing.ip {
                                ips.push(ip.clone());
                            } else if let Some(ref hostname) = ing.hostname {
                                ips.push(hostname.clone());
                            }
                        }
                    }
                }
            }
            if ips.is_empty() { "<none>".to_string() } else { ips.join(",") }
        };

        let port_list: Vec<u16> = spec.ports.as_ref().unwrap_or(&vec![]).iter()
            .map(|p| p.port as u16)
            .collect();

        let ports_str = spec.ports.as_ref().unwrap_or(&vec![]).iter()
            .map(|p| {
                let port = p.port;
                let protocol = p.protocol.as_deref().unwrap_or("TCP");
                if let Some(node_port) = p.node_port {
                    format!("{}:{}/{}", port, node_port, protocol)
                } else {
                    format!("{}/{}", port, protocol)
                }
            })
            .collect::<Vec<_>>()
            .join(",");

        let selector_str = {
            let pairs: Vec<String> = selector.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            if pairs.is_empty() { "<none>".to_string() } else { pairs.join(",") }
        };

        let drill_target = if !selector.is_empty() {
            Some(DrillTarget::PodsByLabels {
                labels: selector,
                breadcrumb: format!("svc/{}", meta.name),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(service_type),
            CellValue::Text(cluster_ip),
            CellValue::Text(external_ip),
            CellValue::Text(selector_str),
            CellValue::Text(ports_str),
            CellValue::Text(meta.labels_str),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            pf_ports: port_list,
            drill_target,
            cells,
            ..Default::default()
        }
    }
}
