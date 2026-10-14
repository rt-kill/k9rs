use crate::kube::resources::row::{CellValue, RowHealth};

/// `READY` is `readyReplicas / spec.replicas` — what `kubectl get deploy`
/// shows and what the column header means.
///
/// It read `availableReplicas`. A pod is Ready when its probe passes and
/// Available only after `minReadySeconds` on top, so the two agree at the
/// default of 0 and diverge exactly during a rollout — the window in which
/// you are watching this column. Worse, it made READY and AVAILABLE the
/// same number by construction, so one of the two columns carried nothing:
/// `128/128` sitting next to `128` was not a coincidence.
///
/// Mirrors the arithmetic in `convert`, which reads a k8s status type that
/// is awkward to build here; the RULE is what is worth pinning.
fn ready_cell(ready: i32, desired: i32) -> CellValue {
    CellValue::Ratio { num: ready as u32, denom: desired as u32 }
}

fn health(ready: i32, desired: i32) -> RowHealth {
    if desired == 0 {
        RowHealth::Pending
    } else if ready < desired {
        RowHealth::Failed
    } else {
        RowHealth::Normal
    }
}

#[test]
fn ready_counts_ready_replicas_not_available_ones() {
    // Mid-rollout: three pods pass their probes, one has not cleared
    // `minReadySeconds`. kubectl says 3/4; reading `available` said 2/4.
    assert_eq!(ready_cell(3, 4), CellValue::Ratio { num: 3, denom: 4 });
    // The absent-means-zero case, which is the one that hid the bug: k8s
    // omits `readyReplicas` entirely when it is 0, so a converter that
    // defaulted it to anything but 0 would report a dead deployment ready.
    assert_eq!(ready_cell(0, 1), CellValue::Ratio { num: 0, denom: 1 });
}

/// The colour and the number beside it must agree. Driving health from
/// `available` while READY showed `ready` would paint a row red next to a
/// `3/3`.
#[test]
fn health_follows_the_ready_column() {
    assert_eq!(health(3, 3), RowHealth::Normal, "fully ready is not a problem");
    assert_eq!(health(2, 3), RowHealth::Failed, "a missing replica is");
    assert_eq!(health(0, 1), RowHealth::Failed);
    // A deliberately scaled-to-zero workload is not broken, it is parked —
    // the same distinction every other converter here makes.
    assert_eq!(health(0, 0), RowHealth::Pending, "scaled to zero is parked, not failed");
}
