
use k8s_openapi::api::batch::v1::Job;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::k8s_const::STATUS_TRUE;
use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

/// Job condition type meaning "the controller has stopped retrying". Local
/// to this file because only Jobs have it — the shared constants module in
/// `resources::mod` is for values two or more converters need.
const COND_FAILED: &str = "Failed";

// ---------------------------------------------------------------------------
// JobDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct JobDef;

impl ResourceDef for JobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Job }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "Job",
            plural: "jobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["job", "jobs"] }
    fn short_label(&self) -> &str { "Jobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "COMPLETIONS", "DURATION", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, PortForward]
    }
}

impl ConvertToRow<Job> for JobDef {
    fn convert(job: Job) -> ResourceRow {
        let meta = CommonMeta::from_k8s(job.metadata);

        let spec = job.spec.unwrap_or_default();
        let desired_completions = spec.completions.unwrap_or(1);
        let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());

        let status_obj = job.status.unwrap_or_default();
        let succeeded = status_obj.succeeded.unwrap_or(0);
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
                kind: BuiltInKind::Job,
                name: meta.name.clone(),
            })
        } else {
            Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
        };

        // A Job that has GIVEN UP is a fault; a Job still retrying is not.
        // The authoritative signal is the `Failed` condition, which the Job
        // controller sets once `backoffLimit` is exhausted (or
        // `activeDeadlineSeconds` passes) — not `status.failed`, which counts
        // individual pod attempts and is nonzero on any retrying Job.
        //
        // This read nothing but `succeeded`, so a Job that had permanently
        // failed showed `Pending` — yellow, "in progress" — for ever.
        //
        // k9s flags `completions unmet && status.failed > 0` (`render/job.go`
        // → its VALID column → red), which paints a Job red on its FIRST
        // transient pod failure while it is still happily retrying. That is a
        // colour saying something untrue, so we key on the condition instead.
        let gave_up = status_obj.conditions.iter().flatten().any(|c| {
            c.type_ == COND_FAILED && c.status == STATUS_TRUE
        });
        let health = if gave_up { RowHealth::Failed }
            else if succeeded >= desired_completions { RowHealth::Normal }
            else { RowHealth::Pending };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Ratio { num: succeeded as u32, denom: desired_completions as u32 },
            CellValue::Text(duration),
            CellValue::from_comma_str(&containers.names),
            CellValue::from_comma_str(&containers.images),
            CellValue::from_comma_str(&meta.labels_str),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];
        ResourceRow {
            cells,
            name: meta.name,
            namespace: Some(meta.namespace),
            health,
            drill_target,
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "../../tests/kube/resources/jobs.rs"]
mod tests;
