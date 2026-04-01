use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::Deployment;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeDeployment {
    pub namespace: String,
    pub name: String,
    pub ready: String,
    pub up_to_date: i32,
    pub available: i32,
    pub strategy: String,
    pub age: Option<DateTime<Utc>>,
    pub labels: BTreeMap<String, String>,
    pub images: Vec<String>,
    /// From spec.selector.matchLabels — used for drill-down to pods.
    #[serde(default)]
    pub selector_labels: BTreeMap<String, String>,
}

impl KubeResource for KubeDeployment {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "READY",
            "UP-TO-DATE",
            "AVAILABLE",
            "STRATEGY",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.ready),
            Cow::Owned(self.up_to_date.to_string()),
            Cow::Owned(self.available.to_string()),
            Cow::Borrowed(&self.strategy),
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
        "deployment"
    }
}

impl From<Deployment> for KubeDeployment {
    fn from(dep: Deployment) -> Self {
        let metadata = dep.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let labels = metadata.labels.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = dep.spec.unwrap_or_default();
        let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
        let desired = spec.replicas.unwrap_or(0);

        let strategy = spec
            .strategy
            .and_then(|s| s.type_)
            .unwrap_or_else(|| "RollingUpdate".to_string());

        let status = dep.status.unwrap_or_default();
        let ready_replicas = status.ready_replicas.unwrap_or(0);
        let up_to_date = status.updated_replicas.unwrap_or(0);
        let available = status.available_replicas.unwrap_or(0);

        let ready = format!("{}/{}", ready_replicas, desired);

        let images: Vec<String> = spec
            .template
            .spec
            .map(|ps| {
                ps.containers
                    .iter()
                    .filter_map(|c| c.image.clone())
                    .collect()
            })
            .unwrap_or_default();

        KubeDeployment {
            namespace,
            name,
            ready,
            up_to_date,
            available,
            strategy,
            age,
            labels,
            images,
            selector_labels,
        }
    }
}
