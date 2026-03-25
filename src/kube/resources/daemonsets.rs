use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::DaemonSet;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeDaemonSet {
    pub namespace: String,
    pub name: String,
    pub desired: i32,
    pub current: i32,
    pub ready: i32,
    pub up_to_date: i32,
    pub available: i32,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeDaemonSet {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "DESIRED",
            "CURRENT",
            "READY",
            "UP-TO-DATE",
            "AVAILABLE",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Owned(self.desired.to_string()),
            Cow::Owned(self.current.to_string()),
            Cow::Owned(self.ready.to_string()),
            Cow::Owned(self.up_to_date.to_string()),
            Cow::Owned(self.available.to_string()),
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
        "daemonset"
    }
}

impl From<DaemonSet> for KubeDaemonSet {
    fn from(ds: DaemonSet) -> Self {
        let metadata = ds.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let status = ds.status.unwrap_or_default();
        let desired = status.desired_number_scheduled;
        let current = status.current_number_scheduled;
        let ready = status.number_ready;
        let up_to_date = status.updated_number_scheduled.unwrap_or(0);
        let available = status.number_available.unwrap_or(0);

        KubeDaemonSet {
            namespace,
            name,
            desired,
            current,
            ready,
            up_to_date,
            available,
            age,
        }
    }
}
