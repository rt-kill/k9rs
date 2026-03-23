use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::PersistentVolumeClaim;

use super::{access_mode_short, KubeResource};

#[derive(Debug, Clone)]
pub struct KubePvc {
    pub namespace: String,
    pub name: String,
    pub status: String,
    pub volume: String,
    pub capacity: String,
    pub access_modes: String,
    pub storage_class: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubePvc {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "STATUS",
            "VOLUME",
            "CAPACITY",
            "ACCESS MODES",
            "STORAGECLASS",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.status),
            Cow::Borrowed(&self.volume),
            Cow::Borrowed(&self.capacity),
            Cow::Borrowed(&self.access_modes),
            Cow::Borrowed(&self.storage_class),
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
        "pvc"
    }
}

impl From<PersistentVolumeClaim> for KubePvc {
    fn from(pvc: PersistentVolumeClaim) -> Self {
        let metadata = pvc.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = pvc.spec.unwrap_or_default();
        let status_obj = pvc.status.unwrap_or_default();

        let status = status_obj
            .phase
            .unwrap_or_else(|| "Pending".to_string());

        let volume = spec.volume_name.unwrap_or_default();

        let capacity = status_obj
            .capacity
            .as_ref()
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();

        let access_modes = status_obj
            .access_modes
            .as_ref()
            .or(spec.access_modes.as_ref())
            .map(|modes| {
                modes
                    .iter()
                    .map(|m| access_mode_short(m))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        let storage_class = spec.storage_class_name.unwrap_or_default();

        KubePvc {
            namespace,
            name,
            status,
            volume,
            capacity,
            access_modes,
            storage_class,
            age,
        }
    }
}
