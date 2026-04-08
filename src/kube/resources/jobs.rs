use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;

use crate::kube::resources::row::{ExtraValue, ResourceRow};

/// Convert a k8s Job into a generic ResourceRow.
pub(crate) fn job_to_row(job: Job) -> ResourceRow {
    let metadata = job.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let uid = metadata.uid.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let spec = job.spec.unwrap_or_default();
    let selector_labels = spec.selector.as_ref()
        .and_then(|s| s.match_labels.clone())
        .unwrap_or_default();
    let desired_completions = spec.completions.unwrap_or(1);

    let containers = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let images = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.image.clone().unwrap_or_default()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();

    let status_obj = job.status.unwrap_or_default();
    let succeeded = status_obj.succeeded.unwrap_or(0);
    let completions = format!("{}/{}", succeeded, desired_completions);

    let duration = match status_obj.start_time {
        Some(ref start) => {
            let end = status_obj.completion_time.as_ref().map(|t| t.0).unwrap_or_else(chrono::Utc::now);
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

    let mut extra = BTreeMap::new();
    extra.insert("selector_labels".into(), ExtraValue::Map(selector_labels));
    extra.insert("uid".into(), ExtraValue::Str(uid));

    ResourceRow {
        cells: vec![ns.clone(), name.clone(), completions, duration, containers, images, labels_str, crate::util::format_age(age)],
        name,
        namespace: ns,
        extra,
    }
}
