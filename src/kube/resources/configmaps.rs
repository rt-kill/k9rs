use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ConfigMap;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeConfigMap {
    pub namespace: String,
    pub name: String,
    pub data_count: usize,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeConfigMap {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "DATA", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Owned(self.data_count.to_string()),
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
        "configmap"
    }
}

impl From<ConfigMap> for KubeConfigMap {
    fn from(cm: ConfigMap) -> Self {
        let metadata = cm.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let data_count = cm.data.map(|d| d.len()).unwrap_or(0)
            + cm.binary_data.map(|d| d.len()).unwrap_or(0);

        KubeConfigMap {
            namespace,
            name,
            data_count,
            age,
        }
    }
}
