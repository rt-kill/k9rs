//! Unit tests for the shared workload converter helpers (mirrored out of
//! `src/kube/resources/mod.rs`).

use super::WorkloadTally;
use crate::kube::resources::row::RowHealth;

/// A settled fleet: every pod present, ready, and on the current template.
fn settled(n: i32) -> WorkloadTally {
    WorkloadTally { desired: n, ready: n, total: n, up_to_date: Some(n) }
}

#[test]
fn settled_fleet_is_normal() {
    assert_eq!(settled(3).health(), RowHealth::Normal);
    assert_eq!(settled(1).health(), RowHealth::Normal);
}

#[test]
fn scaled_to_zero_is_parked_not_failed() {
    // A deliberately scaled-to-zero workload is not broken. k9s paints this
    // `PendingColor` too, keyed on READY's denominator being `0`.
    assert_eq!(settled(0).health(), RowHealth::Pending);
    // Still parked while the last pods drain, even though `total` disagrees
    // with `desired` — clause 1 outranks the unsettled check.
    assert_eq!(
        WorkloadTally { desired: 0, ready: 2, total: 2, up_to_date: Some(2) }.health(),
        RowHealth::Pending,
    );
}

#[test]
fn missing_capacity_is_failed() {
    assert_eq!(
        WorkloadTally { desired: 3, ready: 2, total: 3, up_to_date: Some(3) }.health(),
        RowHealth::Failed,
        "a missing replica is a problem",
    );
    // The absent-means-zero case: k8s omits `readyReplicas` entirely when it
    // is 0, so a converter defaulting it to anything but 0 would report a
    // dead workload ready.
    assert_eq!(
        WorkloadTally { desired: 1, ready: 0, total: 1, up_to_date: Some(1) }.health(),
        RowHealth::Failed,
    );
}

/// THE reported case: `kubectl rollout restart` on a one-replica Deployment.
/// `maxUnavailable` rounds *down* to 0 and `maxSurge` rounds *up* to 1, so
/// the ratio can never fall below `1/1` — it goes *up*, and the row has to
/// say something about that.
#[test]
fn one_replica_surge_rollout_is_pending_throughout() {
    // t0: spec changed, new ReplicaSet not scaled up yet.
    assert_eq!(
        WorkloadTally { desired: 1, ready: 1, total: 1, up_to_date: Some(0) }.health(),
        RowHealth::Pending,
        "no pod is on the new template yet",
    );
    // t1: new pod created, still starting. READY reads `1/1` and UP-TO-DATE
    // reads `1` — `total` is the only field that knows, and the colour is
    // the only thing in the row that can say so.
    assert_eq!(
        WorkloadTally { desired: 1, ready: 1, total: 2, up_to_date: Some(1) }.health(),
        RowHealth::Pending,
        "a surge pod exists; no number in the row shows it",
    );
    // t2: both ready for an instant — READY renders `2/1`.
    assert_eq!(
        WorkloadTally { desired: 1, ready: 2, total: 2, up_to_date: Some(1) }.health(),
        RowHealth::Pending,
        "more pods ready than we are scaled to",
    );
    // t3: old pod gone.
    assert_eq!(settled(1).health(), RowHealth::Normal);
}

/// Above one replica the rollout passes through states where every pod is
/// present but some are still the old template. `total` cannot see those,
/// which is why `up_to_date` is in the tally at all.
#[test]
fn stale_template_at_full_count_is_pending() {
    assert_eq!(
        WorkloadTally { desired: 3, ready: 3, total: 3, up_to_date: Some(1) }.health(),
        RowHealth::Pending,
    );
}

/// ...and conversely, a surge that `up_to_date` cannot see. Both halves of
/// `rollout_in_flight` are load-bearing; neither subsumes the other.
#[test]
fn surge_at_current_template_is_pending() {
    assert_eq!(
        WorkloadTally { desired: 3, ready: 4, total: 4, up_to_date: Some(3) }.health(),
        RowHealth::Pending,
    );
}

/// Red outranks yellow. A rollout being the *cause* of a capacity shortfall
/// does not make the shortfall untrue — a rollout wedged at `3/4` must not
/// read as "relax, it's in progress". k9s agrees (it paints the whole
/// rollout red).
#[test]
fn under_ready_during_a_rollout_stays_failed() {
    assert_eq!(
        WorkloadTally { desired: 4, ready: 3, total: 5, up_to_date: Some(2) }.health(),
        RowHealth::Failed,
    );
}

/// A scale-down is also an unsettled fleet: you asked for one pod and three
/// still exist. This was `Normal` before — full capacity, nothing to see.
#[test]
fn draining_scale_down_is_pending() {
    assert_eq!(
        WorkloadTally { desired: 1, ready: 3, total: 3, up_to_date: Some(3) }.health(),
        RowHealth::Pending,
    );
}

/// `up_to_date: None` means "this workload has no revision axis to be behind
/// on" (ReplicaSet) or "its table shows no column that would explain the
/// verdict" (StatefulSet). It must not be read as zero, which would paint
/// every such row yellow forever.
#[test]
fn absent_up_to_date_is_not_behind() {
    assert_eq!(
        WorkloadTally { desired: 3, ready: 3, total: 3, up_to_date: None }.health(),
        RowHealth::Normal,
    );
    // The `total` half still applies to those kinds.
    assert_eq!(
        WorkloadTally { desired: 3, ready: 3, total: 4, up_to_date: None }.health(),
        RowHealth::Pending,
    );
}
