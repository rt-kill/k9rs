use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use std::collections::BTreeMap;

use crate::kube::resource_def::ConvertToRow;
use crate::kube::resources::deployments::DeploymentDef;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// Column indices from `DeploymentDef::default_headers`.
const READY: usize = 2;
const UP_TO_DATE: usize = 3;
const AVAILABLE: usize = 4;

/// Build a Deployment carrying exactly the four status numbers that decide
/// the row, so the test pins the FIELD MAPPING — the half
/// `WorkloadTally::health` cannot see. Reading the wrong status field is the
/// mistake this file exists to catch; it has happened once already.
fn row(desired: i32, status: DeploymentStatus) -> ResourceRow {
    DeploymentDef::convert(Deployment {
        metadata: ObjectMeta {
            name: Some("web".to_owned()),
            namespace: Some("default".to_owned()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(desired),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".to_owned(), "web".to_owned())])),
                ..Default::default()
            },
            ..Default::default()
        }),
        status: Some(status),
    })
}

/// `READY` is `readyReplicas / spec.replicas` — what `kubectl get deploy`
/// shows and what the column header means.
///
/// It read `availableReplicas`. A pod is Ready when its probe passes and
/// Available only after `minReadySeconds` on top, so the two agree at the
/// default of 0 and diverge exactly during a rollout — the window in which
/// you are watching this column. Worse, it made READY and AVAILABLE the
/// same number by construction, so one of the two columns carried nothing:
/// `128/128` sitting next to `128` was not a coincidence.
#[test]
fn ready_counts_ready_replicas_not_available_ones() {
    // Mid-rollout: three pods pass their probes, one has not cleared
    // `minReadySeconds`. kubectl says 3/4; reading `available` said 2/4.
    let r = row(4, DeploymentStatus {
        replicas: Some(4),
        ready_replicas: Some(3),
        available_replicas: Some(2),
        updated_replicas: Some(4),
        ..Default::default()
    });
    assert_eq!(r.cells[READY], CellValue::Ratio { num: 3, denom: 4 });
    assert_eq!(r.cells[AVAILABLE], CellValue::Count(2), "AVAILABLE keeps its own number");
}

/// k8s omits `readyReplicas` entirely when it is 0, so a converter that
/// defaulted it to anything but 0 would report a dead deployment ready.
#[test]
fn absent_ready_replicas_means_zero() {
    let r = row(1, DeploymentStatus { replicas: Some(1), ..Default::default() });
    assert_eq!(r.cells[READY], CellValue::Ratio { num: 0, denom: 1 });
    assert_eq!(r.health, RowHealth::Failed);
}

/// THE reported case. `kubectl rollout restart` on a one-replica Deployment:
/// `maxUnavailable` rounds down to 0 and `maxSurge` rounds up to 1, so READY
/// never falls below `1/1` — it rises to `2/1`. The ratio is rendered
/// unclamped, and the row is yellow for the whole rollout.
#[test]
fn one_replica_rollout_surges_and_colours_yellow() {
    // New pod created, still starting: every displayed number looks settled.
    let starting = row(1, DeploymentStatus {
        replicas: Some(2),
        ready_replicas: Some(1),
        available_replicas: Some(1),
        updated_replicas: Some(1),
        ..Default::default()
    });
    assert_eq!(starting.cells[READY], CellValue::Ratio { num: 1, denom: 1 });
    assert_eq!(starting.cells[UP_TO_DATE], CellValue::Count(1));
    assert_eq!(
        starting.health, RowHealth::Pending,
        "the colour is the only thing in this row that knows a rollout is running",
    );

    // Both ready for an instant: more pods than we are scaled to.
    let surged = row(1, DeploymentStatus {
        replicas: Some(2),
        ready_replicas: Some(2),
        available_replicas: Some(2),
        updated_replicas: Some(1),
        ..Default::default()
    });
    assert_eq!(
        surged.cells[READY], CellValue::Ratio { num: 2, denom: 1 },
        "the ratio is not clamped to the scale target",
    );
    assert_eq!(surged.health, RowHealth::Pending);

    // Old pod gone.
    let settled = row(1, DeploymentStatus {
        replicas: Some(1),
        ready_replicas: Some(1),
        available_replicas: Some(1),
        updated_replicas: Some(1),
        ..Default::default()
    });
    assert_eq!(settled.health, RowHealth::Normal);
}

/// The colour and the number beside it must agree: driving red from
/// `available` while READY showed `ready` would paint a row red next to a
/// `3/3`.
#[test]
fn red_follows_the_ready_column_not_available() {
    let r = row(3, DeploymentStatus {
        replicas: Some(3),
        ready_replicas: Some(3),
        // One pod is inside `minReadySeconds` — Ready but not yet Available.
        available_replicas: Some(2),
        updated_replicas: Some(3),
        ..Default::default()
    });
    assert_eq!(r.cells[READY], CellValue::Ratio { num: 3, denom: 3 });
    assert_eq!(r.health, RowHealth::Normal, "no red beside a 3/3");
}

/// A deliberately scaled-to-zero workload is parked, not broken — the same
/// distinction every other converter here makes.
#[test]
fn scaled_to_zero_is_parked() {
    let r = row(0, DeploymentStatus::default());
    assert_eq!(r.cells[READY], CellValue::Ratio { num: 0, denom: 0 });
    assert_eq!(r.health, RowHealth::Pending);
}
