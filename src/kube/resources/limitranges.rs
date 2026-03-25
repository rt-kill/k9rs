use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::LimitRange;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeLimitRange {
    pub namespace: String,
    pub name: String,
    pub types: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeLimitRange {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "TYPE", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.types),
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
        "limitrange"
    }
}

impl From<LimitRange> for KubeLimitRange {
    fn from(lr: LimitRange) -> Self {
        let metadata = lr.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let types = lr
            .spec
            .and_then(|spec| {
                let items: Vec<String> = spec
                    .limits
                    .iter()
                    .map(|item| item.type_.clone())
                    .collect();
                if items.is_empty() {
                    None
                } else {
                    Some(items.join(", "))
                }
            })
            .unwrap_or_default();

        KubeLimitRange {
            namespace,
            name,
            types,
            age,
        }
    }
}
