use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeJob {
    pub namespace: String,
    pub name: String,
    pub completions: String,
    pub duration: String,
    pub status: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeJob {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "COMPLETIONS", "DURATION", "STATUS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.completions),
            Cow::Borrowed(&self.duration),
            Cow::Borrowed(&self.status),
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
        "job"
    }
}

impl From<Job> for KubeJob {
    fn from(job: Job) -> Self {
        let metadata = job.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = job.spec.unwrap_or_default();
        let desired_completions = spec.completions.unwrap_or(1);

        let status_obj = job.status.unwrap_or_default();
        let succeeded = status_obj.succeeded.unwrap_or(0);
        let completions = format!("{}/{}", succeeded, desired_completions);

        // Determine job status
        let status = if status_obj.succeeded.unwrap_or(0) >= desired_completions {
            "Complete".to_string()
        } else if status_obj.failed.unwrap_or(0) > 0 {
            "Failed".to_string()
        } else if status_obj.active.unwrap_or(0) > 0 {
            "Running".to_string()
        } else {
            "Pending".to_string()
        };

        // Calculate duration from start_time to completion_time (or now)
        let duration = match status_obj.start_time {
            Some(ref start) => {
                let end = status_obj
                    .completion_time
                    .as_ref()
                    .map(|t| t.0)
                    .unwrap_or_else(chrono::Utc::now);
                let dur = end.signed_duration_since(start.0);
                let total_secs = dur.num_seconds().max(0);
                let hours = total_secs / 3600;
                let minutes = (total_secs % 3600) / 60;
                let seconds = total_secs % 60;
                if hours > 0 {
                    format!("{}h{}m{}s", hours, minutes, seconds)
                } else if minutes > 0 {
                    format!("{}m{}s", minutes, seconds)
                } else {
                    format!("{}s", seconds)
                }
            }
            None => String::new(),
        };

        KubeJob {
            namespace,
            name,
            completions,
            duration,
            status,
            age,
        }
    }
}
