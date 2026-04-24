
use k8s_openapi::api::batch::v1::CronJob;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// CronJobDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct CronJobDef;

impl ResourceDef for CronJobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CronJob }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "CronJob",
            plural: "cronjobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cj", "cronjob", "cronjobs"] }
    fn short_label(&self) -> &str { "CronJobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST SCHEDULE",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, TriggerCronJob, ToggleSuspendCronJob]
    }
}

impl ConvertToRow<CronJob> for CronJobDef {
    fn convert(cj: CronJob) -> ResourceRow {
        let meta = CommonMeta::from_k8s(cj.metadata);

        let spec = cj.spec.unwrap_or_default();
        let schedule = spec.schedule;
        let suspend = spec.suspend.unwrap_or(false);

        // Containers live two templates deep: CronJobSpec -> JobTemplateSpec ->
        // PodTemplateSpec -> PodSpec. We only want names/images; port-forward
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
                kind: BuiltInKind::CronJob,
                name: meta.name.clone(),
            })
        } else {
            None
        };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(schedule),
            CellValue::Bool(suspend),
            CellValue::Count(active as i64),
            CellValue::Age(last_schedule.map(|t| t.timestamp())),
            CellValue::from_comma_str(&containers.names),
            CellValue::from_comma_str(&containers.images),
            CellValue::from_comma_str(&meta.labels_str),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];
        ResourceRow {
            cells,
            name: meta.name,
            namespace: Some(meta.namespace),
            drill_target: drill,
            ..Default::default()
        }
    }
}
