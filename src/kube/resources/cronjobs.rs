use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::CronJob;

use crate::kube::resources::row::{ExtraValue, ResourceRow};

/// Convert a k8s CronJob into a generic ResourceRow.
pub(crate) fn cronjob_to_row(cj: CronJob) -> ResourceRow {
    let metadata = cj.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let spec = cj.spec.unwrap_or_default();
    let schedule = spec.schedule;
    let suspend = spec.suspend.unwrap_or(false);

    let job_template_containers = spec.job_template.spec.as_ref()
        .and_then(|js| js.template.spec.as_ref());
    let containers = job_template_containers
        .map(|ps| ps.containers.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let images = job_template_containers
        .map(|ps| ps.containers.iter().map(|c| c.image.clone().unwrap_or_default()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();

    let status = cj.status.unwrap_or_default();
    let active = status.active.as_ref().map(|a| a.len() as i32).unwrap_or(0);
    let last_schedule = status.last_schedule_time.map(|t| t.0);

    let mut extra = BTreeMap::new();
    extra.insert("suspend".into(), ExtraValue::Str(suspend.to_string()));

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), schedule, suspend.to_string(),
            active.to_string(), crate::util::format_age(last_schedule),
            containers, images,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        extra,
    }
}
