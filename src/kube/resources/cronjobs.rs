use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::CronJob;

use crate::util::format_age;
use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeCronJob {
    pub namespace: String,
    pub name: String,
    pub schedule: String,
    pub suspend: bool,
    pub active: i32,
    pub last_schedule: Option<DateTime<Utc>>,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeCronJob {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "SCHEDULE",
            "SUSPEND",
            "ACTIVE",
            "LAST SCHEDULE",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.schedule),
            Cow::Owned(self.suspend.to_string()),
            Cow::Owned(self.active.to_string()),
            Cow::Owned(format_age(self.last_schedule)),
            Cow::Owned(format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "cronjob"
    }
}

impl From<CronJob> for KubeCronJob {
    fn from(cj: CronJob) -> Self {
        let metadata = cj.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = cj.spec.unwrap_or_default();
        let schedule = spec.schedule;
        let suspend = spec.suspend.unwrap_or(false);

        let status = cj.status.unwrap_or_default();
        let active = status
            .active
            .as_ref()
            .map(|a| a.len() as i32)
            .unwrap_or(0);

        let last_schedule = status.last_schedule_time.map(|t| t.0);

        KubeCronJob {
            namespace,
            name,
            schedule,
            suspend,
            active,
            last_schedule,
            age,
        }
    }
}
