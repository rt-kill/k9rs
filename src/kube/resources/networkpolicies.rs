use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::networking::v1::NetworkPolicy;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeNetworkPolicy {
    pub namespace: String,
    pub name: String,
    pub pod_selector: String,
    pub policy_types: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeNetworkPolicy {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "POD-SELECTOR", "POLICY TYPES", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.pod_selector),
            Cow::Borrowed(&self.policy_types),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "networkpolicy"
    }
}

impl From<NetworkPolicy> for KubeNetworkPolicy {
    fn from(np: NetworkPolicy) -> Self {
        let metadata = np.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let (pod_selector, policy_types) = np
            .spec
            .map(|s| {
                let labels = s.pod_selector.match_labels.unwrap_or_default();
                let sel = if labels.is_empty() {
                    "<all>".to_string()
                } else {
                    labels
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<_>>()
                        .join(",")
                };

                let types = s
                    .policy_types
                    .map(|t| t.join(","))
                    .unwrap_or_default();

                (sel, types)
            })
            .unwrap_or_else(|| ("<none>".to_string(), String::new()));

        KubeNetworkPolicy {
            namespace,
            name,
            pod_selector,
            policy_types,
            age,
        }
    }
}
