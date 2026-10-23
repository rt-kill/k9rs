use k9rs_proto::row::RowHealth;

/// Mirrors the health rule in `convert`, which reads from a k8s status type
/// that is awkward to build in a test. The rule itself is the thing worth
/// pinning: it was inverted, and the inversion painted the healthy steady
/// state red.
fn health(current: i64, desired: i64, allowed: i64, expected: i32) -> RowHealth {
    if current < desired {
        RowHealth::Failed
    } else if allowed == 0 && expected > 0 {
        RowHealth::Pending
    } else {
        RowHealth::Normal
    }
}

/// A singleton with `minAvailable: 1` sits at zero allowed disruptions
/// forever. That is satisfied-with-no-slack, not broken — it used to read
/// red permanently, which is the state most PDBs in a cluster are in.
#[test]
fn a_satisfied_budget_with_no_slack_is_not_a_failure() {
    assert_eq!(health(1, 1, 0, 1), RowHealth::Pending);
    // Same shape for every `maxUnavailable: 0` budget.
    assert_eq!(health(3, 3, 0, 3), RowHealth::Pending);
}

/// The real breach. This arm used to be UNREACHABLE: a breach also forces
/// `disruptionsAllowed` to 0, so the old `allowed == 0` test caught it first
/// and both states came out the same colour.
#[test]
fn a_breached_budget_is_a_failure() {
    assert_eq!(health(1, 3, 0, 3), RowHealth::Failed);
}

/// Slack available, budget met.
#[test]
fn a_budget_with_room_is_normal() {
    assert_eq!(health(5, 3, 2, 5), RowHealth::Normal);
}
