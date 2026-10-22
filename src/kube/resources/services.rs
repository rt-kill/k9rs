use k8s_openapi::api::core::v1::Service;

use super::ConvertToRow;
use crate::kube::resource_def::services::ServiceDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// ServiceDef
// ---------------------------------------------------------------------------


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

        // LoadBalancer services with no external IP are pending.
        use crate::kube::resources::row::RowHealth;
        let health = if service_type == "LoadBalancer" && (external_ip == "<none>" || external_ip.is_empty()) {
            RowHealth::Pending
        } else {
            RowHealth::Normal
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
            health,
            ..Default::default()
        }
    }
}
