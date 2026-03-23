use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::storage::v1::StorageClass;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeStorageClass {
    pub name: String,
    pub provisioner: String,
    pub reclaim_policy: String,
    pub volume_binding_mode: String,
    pub allow_expansion: bool,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeStorageClass {
    fn headers() -> &'static [&'static str] {
        &[
            "NAME",
            "PROVISIONER",
            "RECLAIM POLICY",
            "VOLUME BINDING MODE",
            "EXPANSION",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.provisioner),
            Cow::Borrowed(&self.reclaim_policy),
            Cow::Borrowed(&self.volume_binding_mode),
            Cow::Borrowed(if self.allow_expansion {
                "true"
            } else {
                "false"
            }),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "storageclass"
    }
}

impl From<StorageClass> for KubeStorageClass {
    fn from(sc: StorageClass) -> Self {
        let metadata = sc.metadata;
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let provisioner = sc.provisioner;
        let reclaim_policy = sc
            .reclaim_policy
            .unwrap_or_else(|| "Delete".to_string());
        let volume_binding_mode = sc
            .volume_binding_mode
            .unwrap_or_else(|| "Immediate".to_string());
        let allow_expansion = sc.allow_volume_expansion.unwrap_or(false);

        KubeStorageClass {
            name,
            provisioner,
            reclaim_policy,
            volume_binding_mode,
            allow_expansion,
            age,
        }
    }
}
