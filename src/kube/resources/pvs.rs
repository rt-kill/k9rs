use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::PersistentVolume;

use super::{access_mode_short, KubeResource};

#[derive(Debug, Clone)]
pub struct KubePv {
    pub name: String,
    pub capacity: String,
    pub access_modes: String,
    pub reclaim_policy: String,
    pub status: String,
    pub claim: String,
    pub storage_class: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubePv {
    fn headers() -> &'static [&'static str] {
        &[
            "NAME",
            "CAPACITY",
            "ACCESS MODES",
            "RECLAIM POLICY",
            "STATUS",
            "CLAIM",
            "STORAGECLASS",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.capacity),
            Cow::Borrowed(&self.access_modes),
            Cow::Borrowed(&self.reclaim_policy),
            Cow::Borrowed(&self.status),
            Cow::Borrowed(&self.claim),
            Cow::Borrowed(&self.storage_class),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "pv"
    }
}

impl From<PersistentVolume> for KubePv {
    fn from(pv: PersistentVolume) -> Self {
        let metadata = pv.metadata;
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = pv.spec.unwrap_or_default();

        let capacity = spec
            .capacity
            .as_ref()
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();

        let access_modes = spec
            .access_modes
            .as_ref()
            .map(|modes| {
                modes
                    .iter()
                    .map(|m| access_mode_short(m))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        let reclaim_policy = spec
            .persistent_volume_reclaim_policy
            .unwrap_or_else(|| "Retain".to_string());

        let status = pv
            .status
            .and_then(|s| s.phase)
            .unwrap_or_else(|| "Available".to_string());

        let claim = spec
            .claim_ref
            .map(|cr| {
                let ns = cr.namespace.unwrap_or_default();
                let n = cr.name.unwrap_or_default();
                if ns.is_empty() {
                    n
                } else {
                    format!("{}/{}", ns, n)
                }
            })
            .unwrap_or_default();

        let storage_class = spec.storage_class_name.unwrap_or_default();

        KubePv {
            name,
            capacity,
            access_modes,
            reclaim_policy,
            status,
            claim,
            storage_class,
            age,
        }
    }
}
