
use k8s_openapi::api::batch::v1::Job;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s Job into a generic ResourceRow.
pub(crate) fn job_to_row(job: Job) -> ResourceRow {
    let meta = CommonMeta::from_k8s(job.metadata);

    let spec = job.spec.unwrap_or_default();
    let desired_completions = spec.completions.unwrap_or(1);
    let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());

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

    let drill_target = if !meta.uid.is_empty() {
        Some(DrillTarget::PodsByOwner {
            uid: meta.uid.clone(),
            kind: crate::kube::resource_def::BuiltInKind::Job,
            name: meta.name.clone(),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
    };

    let health = if succeeded >= desired_completions { RowHealth::Normal }
        else { RowHealth::Pending };

    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            completions, duration,
            containers.names, containers.images,
            meta.labels_str, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        health,
        drill_target,
        ..Default::default()
    }
}
