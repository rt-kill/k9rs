use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Namespace;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeNamespace {
    pub name: String,
    pub status: String,
    pub age: Option<DateTime<Utc>>,
    pub labels: BTreeMap<String, String>,
}

impl KubeResource for KubeNamespace {
    fn headers() -> &'static [&'static str] {
        &["NAME", "STATUS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.status),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "namespace"
    }
}

impl From<Namespace> for KubeNamespace {
    fn from(ns: Namespace) -> Self {
        let metadata = ns.metadata;
        let name = metadata.name.unwrap_or_default();
        let labels = metadata.labels.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let status = ns
            .status
            .and_then(|s| s.phase)
            .unwrap_or_else(|| "Active".to_string());

        KubeNamespace {
            name,
            status,
            age,
            labels,
        }
    }
}
