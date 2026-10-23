use k8s_openapi::api::networking::v1::NetworkPolicy;

use super::ConvertToRow;
use k9rs_proto::resource_def::networkpolicies::NetworkPolicyDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// NetworkPolicyDef
// ---------------------------------------------------------------------------


impl ConvertToRow<NetworkPolicy> for NetworkPolicyDef {
    fn convert(np: NetworkPolicy) -> ResourceRow {
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
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(pod_selector),
            CellValue::Text(policy_types),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
