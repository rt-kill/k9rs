use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::StatefulSet;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeStatefulSet {
    pub namespace: String,
    pub name: String,
    pub ready: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeStatefulSet {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "READY", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.ready),
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
        "statefulset"
    }
}

impl From<StatefulSet> for KubeStatefulSet {
    fn from(sts: StatefulSet) -> Self {
        let metadata = sts.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let desired = sts.spec.and_then(|s| s.replicas).unwrap_or(0);
        let ready_replicas = sts.status.and_then(|s| s.ready_replicas).unwrap_or(0);

        let ready = format!("{}/{}", ready_replicas, desired);

        KubeStatefulSet {
            namespace,
            name,
            ready,
            age,
        }
    }
}
