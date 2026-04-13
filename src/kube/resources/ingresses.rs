use k8s_openapi::api::networking::v1::Ingress;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Ingress into a generic ResourceRow.
pub(crate) fn ingress_to_row(ing: Ingress) -> ResourceRow {
    let metadata = ing.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);
    let spec = ing.spec.unwrap_or_default();
    let class = spec.ingress_class_name.unwrap_or_else(|| "<none>".to_string());
    let hosts = spec.rules.as_ref()
        .map(|rules| rules.iter().filter_map(|r| r.host.clone()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let hosts = if hosts.is_empty() { "*".to_string() } else { hosts };
    let address = ing.status
        .and_then(|s| s.load_balancer)
        .and_then(|lb| lb.ingress)
        .map(|ingresses| ingresses.iter().filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone())).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let has_tls = spec.tls.map(|t| !t.is_empty()).unwrap_or(false);
    let ports = if has_tls { "80, 443".to_string() } else { "80".to_string() };
    ResourceRow {
        cells: vec![ns.clone(), name.clone(), class, hosts, address, ports, labels_str, crate::util::format_age(age)],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
