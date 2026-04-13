
use k8s_openapi::api::core::v1::Service;

use crate::kube::resources::row::{DrillTarget, ResourceRow};

/// Convert a k8s Service into a generic ResourceRow.
pub(crate) fn service_to_row(svc: Service) -> ResourceRow {
    let metadata = svc.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

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
            breadcrumb: format!("svc/{}", name),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(name.clone()))
    };

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), service_type, cluster_ip, external_ip,
            selector_str, ports_str, labels_str, crate::util::format_age(age),
        ],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: port_list,
        node: None,
        crd_info: None,
        drill_target,
    }
}
