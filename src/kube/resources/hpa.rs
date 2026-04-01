use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeHpa {
    pub namespace: String,
    pub name: String,
    pub reference: String,
    pub min_replicas: String,
    pub max_replicas: String,
    pub current_replicas: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeHpa {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "REFERENCE",
            "MIN",
            "MAX",
            "CURRENT",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.reference),
            Cow::Borrowed(&self.min_replicas),
            Cow::Borrowed(&self.max_replicas),
            Cow::Borrowed(&self.current_replicas),
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
        "horizontalpodautoscaler"
    }
}

impl From<HorizontalPodAutoscaler> for KubeHpa {
    fn from(hpa: HorizontalPodAutoscaler) -> Self {
        let metadata = hpa.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = hpa.spec.unwrap_or_default();

        let reference = {
            let r = &spec.scale_target_ref;
            format!("{}/{}", r.kind, r.name)
        };

        let min_replicas = spec
            .min_replicas
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unset>".to_string());

        let max_replicas = spec.max_replicas.to_string();

        let status = hpa.status.unwrap_or_default();
        let current_replicas = status
            .current_replicas
            .map(|v| v.to_string())
            .unwrap_or_else(|| "0".to_string());

        KubeHpa {
            namespace,
            name,
            reference,
            min_replicas,
            max_replicas,
            current_replicas,
            age,
        }
    }
}
