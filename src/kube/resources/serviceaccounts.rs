use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ServiceAccount;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeServiceAccount {
    pub namespace: String,
    pub name: String,
    pub secrets: usize,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeServiceAccount {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "SECRETS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Owned(self.secrets.to_string()),
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
        "serviceaccount"
    }
}

impl From<ServiceAccount> for KubeServiceAccount {
    fn from(sa: ServiceAccount) -> Self {
        let metadata = sa.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let secrets = sa.secrets.map(|s| s.len()).unwrap_or(0);

        KubeServiceAccount {
            namespace,
            name,
            secrets,
            age,
        }
    }
}
