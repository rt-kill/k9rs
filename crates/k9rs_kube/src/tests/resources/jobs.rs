use k8s_openapi::api::batch::v1::{Job, JobCondition, JobSpec, JobStatus};
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use crate::resources::ConvertToRow;
use crate::resources::jobs::JobDef;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

// Column indices from `JobDef::default_headers`.
const COMPLETIONS: usize = 2;

fn cond(type_: &str, status: &str) -> JobCondition {
    JobCondition {
        type_: type_.to_owned(),
        status: status.to_owned(),
        last_probe_time: None,
        last_transition_time: None,
        message: None,
        reason: None,
    }
}

fn row(completions: i32, status: JobStatus) -> ResourceRow {
    JobDef::convert(Job {
        metadata: ObjectMeta {
            name: Some("import".to_owned()),
            namespace: Some("default".to_owned()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            completions: Some(completions),
            template: PodTemplateSpec::default(),
            ..Default::default()
        }),
        status: Some(status),
    })
}

/// A Job that has exhausted its `backoffLimit` is FAILED, and said so only in
/// its conditions. The converter read nothing but `succeeded`, so such a Job
/// showed `Pending` — yellow, "in progress" — for ever.
#[test]
fn a_job_that_gave_up_is_failed() {
    let r = row(1, JobStatus {
        failed: Some(6),
        conditions: Some(vec![cond("Failed", "True")]),
        ..Default::default()
    });
    assert_eq!(r.cells[COMPLETIONS], CellValue::Ratio { num: 0, denom: 1 });
    assert_eq!(r.health, RowHealth::Failed);
}

/// ...but a Job still RETRYING is not failed, however many attempts have
/// died. `status.failed` counts pod attempts and is nonzero on any retrying
/// Job, which is why the verdict keys on the condition instead. k9s flags
/// `completions unmet && failed > 0` (`render/job.go`), so it paints this row
/// red on the first transient failure — a colour saying something untrue.
#[test]
fn a_retrying_job_is_not_failed() {
    let r = row(1, JobStatus { failed: Some(3), active: Some(1), ..Default::default() });
    assert_eq!(r.health, RowHealth::Pending, "still trying is not a fault");
}

/// A `Failed` condition that is no longer true must not condemn the row —
/// the status field is a list of conditions, not a set of flags.
#[test]
fn a_false_failed_condition_is_not_a_failure() {
    let r = row(1, JobStatus {
        succeeded: Some(1),
        conditions: Some(vec![cond("Failed", "False"), cond("Complete", "True")]),
        ..Default::default()
    });
    assert_eq!(r.health, RowHealth::Normal);
}

#[test]
fn a_completed_job_is_normal_and_a_partial_one_is_pending() {
    let done = row(3, JobStatus {
        succeeded: Some(3),
        conditions: Some(vec![cond("Complete", "True")]),
        ..Default::default()
    });
    assert_eq!(done.cells[COMPLETIONS], CellValue::Ratio { num: 3, denom: 3 });
    assert_eq!(done.health, RowHealth::Normal);

    let partial = row(3, JobStatus { succeeded: Some(1), active: Some(2), ..Default::default() });
    assert_eq!(partial.cells[COMPLETIONS], CellValue::Ratio { num: 1, denom: 3 });
    assert_eq!(partial.health, RowHealth::Pending);
}

/// `spec.completions` is absent for a single-shot Job and means 1 — the
/// denominator must not come out zero and make the ratio meaningless.
#[test]
fn absent_completions_means_one() {
    let r = JobDef::convert(Job {
        metadata: ObjectMeta { name: Some("once".to_owned()), ..Default::default() },
        spec: Some(JobSpec { template: PodTemplateSpec::default(), ..Default::default() }),
        status: Some(JobStatus { succeeded: Some(1), ..Default::default() }),
    });
    assert_eq!(r.cells[COMPLETIONS], CellValue::Ratio { num: 1, denom: 1 });
    assert_eq!(r.health, RowHealth::Normal);
}
