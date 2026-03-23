use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ResourceQuota;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeResourceQuota {
    pub namespace: String,
    pub name: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeResourceQuota {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
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
        "resourcequota"
    }
}

impl From<ResourceQuota> for KubeResourceQuota {
    fn from(rq: ResourceQuota) -> Self {
        let metadata = rq.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        KubeResourceQuota {
            namespace,
            name,
            age,
        }
    }
}
