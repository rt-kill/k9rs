
use k8s_openapi::api::batch::v1::CronJob;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s CronJob into a generic ResourceRow.
pub(crate) fn cronjob_to_row(cj: CronJob) -> ResourceRow {
    let meta = CommonMeta::from_k8s(cj.metadata);

    let spec = cj.spec.unwrap_or_default();
    let schedule = spec.schedule;
    let suspend = spec.suspend.unwrap_or(false);

    // Containers live two templates deep: CronJobSpec → JobTemplateSpec →
    // PodTemplateSpec → PodSpec. We only want names/images; port-forward
    // isn't offered for cron-scheduled workloads.
    let pod_spec = spec.job_template.spec.as_ref()
        .and_then(|js| js.template.spec.as_ref());
    let containers = WorkloadContainers::from_pod_spec(pod_spec);

    let status = cj.status.unwrap_or_default();
    let active = status.active.as_ref().map(|a| a.len() as i32).unwrap_or(0);
    let last_schedule = status.last_schedule_time.map(|t| t.0);

    let drill = if !meta.uid.is_empty() {
        Some(crate::kube::resources::row::DrillTarget::JobsByOwner {
            uid: meta.uid.clone(),
            kind: crate::kube::resource_def::BuiltInKind::CronJob,
            name: meta.name.clone(),
        })
    } else {
        None
    };

    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            schedule, suspend.to_string(),
            active.to_string(), crate::util::format_age(last_schedule),
            containers.names, containers.images,
            meta.labels_str,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        drill_target: drill,
        ..Default::default()
    }
}
