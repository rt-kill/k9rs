use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ResourceQuota;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeResourceQuota {
    pub namespace: String,
    pub name: String,
    pub hard: String,
    pub used: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeResourceQuota {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "HARD", "USED", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.hard),
            Cow::Borrowed(&self.used),
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

fn format_quantity_map(map: &Option<std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>) -> String {
    match map {
        Some(m) if !m.is_empty() => {
            m.iter()
                .map(|(k, v)| format!("{}: {}", k, v.0))
                .collect::<Vec<_>>()
                .join(", ")
        }
        _ => String::new(),
    }
}

impl From<ResourceQuota> for KubeResourceQuota {
    fn from(rq: ResourceQuota) -> Self {
        let metadata = rq.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let hard = rq.spec.as_ref().and_then(|s| {
            let formatted = format_quantity_map(&s.hard);
            if formatted.is_empty() { None } else { Some(formatted) }
        }).unwrap_or_default();

        let used = rq.status.as_ref().and_then(|s| {
            let formatted = format_quantity_map(&s.used);
            if formatted.is_empty() { None } else { Some(formatted) }
        }).unwrap_or_default();

        KubeResourceQuota {
            namespace,
            name,
            hard,
            used,
            age,
        }
    }
}
