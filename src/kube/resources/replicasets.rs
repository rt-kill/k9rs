use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::ReplicaSet;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeReplicaSet {
    pub namespace: String,
    pub name: String,
    pub desired: i32,
    pub current: i32,
    pub ready: i32,
    pub age: Option<DateTime<Utc>>,
    #[serde(default)]
    pub selector_labels: BTreeMap<String, String>,
}

impl KubeResource for KubeReplicaSet {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Owned(self.desired.to_string()),
            Cow::Owned(self.current.to_string()),
            Cow::Owned(self.ready.to_string()),
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
        "replicaset"
    }
}

impl From<ReplicaSet> for KubeReplicaSet {
    fn from(rs: ReplicaSet) -> Self {
        let metadata = rs.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let selector_labels = rs.spec
            .as_ref()
            .and_then(|s| s.selector.match_labels.clone())
            .unwrap_or_default();
        let desired = rs.spec.and_then(|s| s.replicas).unwrap_or(0);
        let status = rs.status.unwrap_or_default();
        let current = status.replicas;
        let ready = status.ready_replicas.unwrap_or(0);

        KubeReplicaSet {
            namespace,
            name,
            desired,
            current,
            ready,
            age,
            selector_labels,
        }
    }
}
