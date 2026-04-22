use k8s_openapi::api::networking::v1::NetworkPolicy;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// NetworkPolicyDef
// ---------------------------------------------------------------------------

pub struct NetworkPolicyDef;

impl ResourceDef for NetworkPolicyDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::NetworkPolicy }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "networking.k8s.io", version: "v1", kind: "NetworkPolicy",
            plural: "networkpolicies", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["np", "networkpolicy", "networkpolicies"] }
    fn short_label(&self) -> &str { "NetPol" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "POD-SELECTOR", "POLICY TYPES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

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
}
