use k8s_openapi::api::networking::v1::Ingress;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s Ingress into a generic ResourceRow.
pub(crate) fn ingress_to_row(ing: Ingress) -> ResourceRow {
    let meta = CommonMeta::from_k8s(ing.metadata);
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
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            class, hosts, address, ports,
            meta.labels_str, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
