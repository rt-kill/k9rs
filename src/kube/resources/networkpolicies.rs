use k8s_openapi::api::networking::v1::NetworkPolicy;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s NetworkPolicy into a generic ResourceRow.
pub(crate) fn network_policy_to_row(np: NetworkPolicy) -> ResourceRow {
    let meta = CommonMeta::from_k8s(np.metadata);
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
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            pod_selector, policy_types,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
