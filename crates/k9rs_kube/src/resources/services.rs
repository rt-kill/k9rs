use k8s_openapi::api::core::v1::Service;

use super::ConvertToRow;
use k9rs_proto::resource_def::services::ServiceDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow};

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

        // The addresses reachable from outside the cluster, kept as a LIST.
        //
        // It used to flatten straight to the displayed string, and then the
        // health check 45 lines below re-tested that string against its own
        // `"<none>"` sentinel — with a second `is_empty()` arm for a case
        // the sentinel had already covered. "Has no external address" is one
        // question; it is asked of the list now, and `"<none>"` is a
        // rendering of the empty list rather than a value anything decides
        // on. Empty strings are dropped on the way in, which is what made
        // that second arm look reachable.
        let external_ips: Vec<String> = {
            let mut ips: Vec<String> = Vec::new();
            if let Some(ext_ips) = &spec.external_ips {
                ips.extend(ext_ips.iter().cloned());
            }
            let ingress = svc
                .status
                .as_ref()
                .and_then(|s| s.load_balancer.as_ref())
                .and_then(|lb| lb.ingress.as_deref())
                .unwrap_or(&[]);
            for ing in ingress {
                // A LoadBalancer is addressed by IP or by hostname; an
                // ingress entry carrying neither names nothing.
                if let Some(addr) = ing.ip.as_deref().or(ing.hostname.as_deref()) {
                    ips.push(addr.to_string());
                }
            }
            ips.retain(|ip| !ip.is_empty());
            ips
        };

        // TCP only, for the reason `resources::forwardable_ports` gives: the
        // forward rides a websocket tunnel, so offering a UDP port means a
        // forward that loops `Reconnecting`. A Service's ports are its own
        // (not a PodSpec's), so this is the third shape of the same
        // question rather than a call — but the RULE is one, and it was
        // unfiltered here.
        // One walk's worth of ports, bound once: the forwardable subset and
        // the displayed string are two questions about the same list, and
        // asking `spec.ports` twice let them be asked of different lists.
        let ports = spec.ports.as_deref().unwrap_or(&[]);

        let port_list: Vec<u16> = ports
            .iter()
            .filter(|p| p.protocol.as_deref() != Some("UDP"))
            .map(|p| p.port as u16)
            .collect();

        let ports_str = ports.iter()
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

        // A LoadBalancer with no external address yet is still being
        // provisioned. Asked of the list, not of the rendered cell.
        use k9rs_proto::row::RowHealth;
        let health = if service_type == "LoadBalancer" && external_ips.is_empty() {
            RowHealth::Pending
        } else {
            RowHealth::Normal
        };

        let external_ip = if external_ips.is_empty() {
            "<none>".to_string()
        } else {
            external_ips.join(",")
        };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(service_type),
            CellValue::Text(cluster_ip),
            CellValue::Text(external_ip),
            CellValue::Text(selector_str),
            CellValue::Text(ports_str),
            meta.labels_cell(),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];
        ResourceRow {
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

#[cfg(test)]
#[path = "../tests/resources/services.rs"]
mod tests;
