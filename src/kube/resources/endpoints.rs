use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Endpoints;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeEndpoints {
    pub namespace: String,
    pub name: String,
    pub endpoints: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeEndpoints {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "ENDPOINTS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.endpoints),
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
        "endpoints"
    }
}

impl From<Endpoints> for KubeEndpoints {
    fn from(ep: Endpoints) -> Self {
        let metadata = ep.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let endpoints = ep
            .subsets
            .unwrap_or_default()
            .iter()
            .flat_map(|subset| {
                let addresses = subset.addresses.as_deref().unwrap_or_default();
                let ports = subset.ports.as_deref().unwrap_or_default();
                addresses.iter().flat_map(move |addr| {
                    let ip = addr.ip.clone();
                    if ports.is_empty() {
                        vec![ip.clone()]
                    } else {
                        ports
                            .iter()
                            .map(|p| format!("{}:{}", ip, p.port))
                            .collect()
                    }
                })
            })
            .collect::<Vec<_>>()
            .join(",");

        let endpoints = if endpoints.is_empty() {
            "<none>".to_string()
        } else {
            endpoints
        };

        KubeEndpoints {
            namespace,
            name,
            endpoints,
            age,
        }
    }
}
