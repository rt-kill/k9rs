use k8s_openapi::api::networking::v1::NetworkPolicy;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s NetworkPolicy into a generic ResourceRow.
pub(crate) fn network_policy_to_row(np: NetworkPolicy) -> ResourceRow {
    let metadata = np.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let (pod_selector, policy_types) = np.spec
        .map(|s| {
            let labels = s.pod_selector.match_labels.unwrap_or_default();
            let sel = if labels.is_empty() {
                "<all>".to_string()
            } else {
                labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",")
            };
            let types = s.policy_types.map(|t| t.join(",")).unwrap_or_default();
            (sel, types)
        })
        .unwrap_or_else(|| ("<none>".to_string(), String::new()));
    ResourceRow {
        cells: vec![ns.clone(), name.clone(), pod_selector, policy_types, crate::util::format_age(age)],
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
