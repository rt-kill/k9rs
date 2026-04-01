use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubePdb {
    pub namespace: String,
    pub name: String,
    pub min_available: String,
    pub max_unavailable: String,
    pub allowed_disruptions: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubePdb {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "MIN AVAILABLE",
            "MAX UNAVAILABLE",
            "ALLOWED DISRUPTIONS",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.min_available),
            Cow::Borrowed(&self.max_unavailable),
            Cow::Borrowed(&self.allowed_disruptions),
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
        "poddisruptionbudget"
    }
}

impl From<PodDisruptionBudget> for KubePdb {
    fn from(pdb: PodDisruptionBudget) -> Self {
        let metadata = pdb.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = pdb.spec;

        let min_available = spec
            .as_ref()
            .and_then(|s| s.min_available.as_ref())
            .map(|v| match v {
                k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => {
                    i.to_string()
                }
                k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => {
                    s.clone()
                }
            })
            .unwrap_or_else(|| "N/A".to_string());

        let max_unavailable = spec
            .as_ref()
            .and_then(|s| s.max_unavailable.as_ref())
            .map(|v| match v {
                k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => {
                    i.to_string()
                }
                k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => {
                    s.clone()
                }
            })
            .unwrap_or_else(|| "N/A".to_string());

        let allowed_disruptions = pdb
            .status
            .as_ref()
            .map(|s| s.disruptions_allowed.to_string())
            .unwrap_or_else(|| "0".to_string());

        KubePdb {
            namespace,
            name,
            min_available,
            max_unavailable,
            allowed_disruptions,
            age,
        }
    }
}
