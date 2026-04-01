use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Secret;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeSecret {
    pub namespace: String,
    pub name: String,
    pub secret_type: String,
    pub data_count: usize,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeSecret {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "TYPE", "DATA", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.secret_type),
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
        "secret"
    }
}

impl From<Secret> for KubeSecret {
    fn from(secret: Secret) -> Self {
        let metadata = secret.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let secret_type = secret.type_.unwrap_or_else(|| "Opaque".to_string());
        let data_count = secret.data.map(|d| d.len()).unwrap_or(0);

        KubeSecret {
            namespace,
            name,
            secret_type,
            data_count,
            age,
        }
    }
}
