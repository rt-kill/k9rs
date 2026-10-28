//! The metrics poller's two decisions, without a cluster.
//!
//! `streaming.rs` had no mirror: its one tested function lived in
//! `server_session`'s, because `metrics_backoff` is `pub(super)` and
//! `ConsecutiveFails` is private here — so the counter that FEEDS the
//! backoff could not be reached from there at all. The pair is the point,
//! and a mirror on this file is where the pair can be stated.

use super::*;

/// The metrics poll backs off while the API is refusing, and stops growing.
///
/// A cluster with no metrics-server would otherwise be asked every 30s
/// forever. Monotonic, capped, and starting at the healthy interval — all
/// three are the point, and none of them is checked by the type.
#[test]
fn the_metrics_poll_backs_off_and_caps() {
    let seq: Vec<u64> = (0..8).map(|n| metrics_backoff(n).as_secs()).collect();
    assert_eq!(seq[0], 30, "healthy cadence");
    assert!(
        seq.windows(2).all(|w| w[1] >= w[0]),
        "never gets faster while failing: {seq:?}",
    );
    assert_eq!(*seq.last().expect("non-empty"), 300, "and caps at five minutes");
    assert!(
        seq.iter().all(|&s| s <= 300),
        "the cap is a cap, not a waypoint: {seq:?}",
    );
}


/// The counter and the backoff are one mechanism: a failure run lengthens
/// the poll, and the first success puts it straight back to the healthy
/// cadence. Neither half says that alone.
#[test]
fn a_failure_run_lengthens_the_poll_and_recovery_restores_it() {
    let err = std::io::Error::other("no metrics-server");
    let mut fails = ConsecutiveFails::default();
    assert_eq!(metrics_backoff(fails.count).as_secs(), 30, "healthy to begin with");

    let mut cadence = Vec::new();
    for _ in 0..5 {
        fails.record_failure("pod metrics", &err);
        cadence.push(metrics_backoff(fails.count).as_secs());
    }
    assert_eq!(cadence, vec![60, 120, 300, 300, 300], "30s → 5min, then held");

    // One success is enough — a cluster whose metrics-server was just
    // installed must not keep being asked every five minutes.
    fails.record_success("pod metrics");
    assert_eq!(fails.count, 0);
    assert_eq!(metrics_backoff(fails.count).as_secs(), 30, "straight back, not halved");
}

/// The warn fires on the FIRST failure of a run and not again — that is what
/// `count == 1` means, and it is why the counter exists rather than a bool:
/// the recovery line needs to say how many failures it is recovering from.
#[test]
fn only_the_first_failure_of_a_run_is_the_loud_one() {
    let err = std::io::Error::other("unreachable");
    let mut fails = ConsecutiveFails::default();
    fails.record_failure("node metrics", &err);
    assert_eq!(fails.count, 1, "the loud one");
    for _ in 0..3 {
        fails.record_failure("node metrics", &err);
    }
    assert_eq!(fails.count, 4, "quiet, and counted for the recovery line");
}

/// A success with no preceding failure changes nothing — the poller does not
/// announce a recovery from nothing every thirty seconds.
#[test]
fn a_success_without_a_failure_run_is_a_no_op() {
    let mut fails = ConsecutiveFails::default();
    fails.record_success("pod metrics");
    assert_eq!(fails.count, 0);
}

/// The two metrics are tracked SEPARATELY, and the backoff follows the
/// worse of them. A cluster serving pod metrics but not node metrics must
/// not have its pod polling slowed to five minutes — nor be hammered every
/// thirty seconds for the node half it will never get.
#[test]
fn the_two_metrics_fail_independently_and_the_worse_one_sets_the_cadence() {
    let err = std::io::Error::other("nodes are not served here");
    let mut pod_fails = ConsecutiveFails::default();
    let mut node_fails = ConsecutiveFails::default();
    for _ in 0..4 {
        pod_fails.record_success("pod metrics");
        node_fails.record_failure("node metrics", &err);
    }
    assert_eq!(pod_fails.count, 0, "the healthy half stays healthy");
    assert_eq!(node_fails.count, 4);
    // What the loop does with them.
    let worst = pod_fails.count.max(node_fails.count);
    assert_eq!(metrics_backoff(worst).as_secs(), 300, "the worse half sets the cadence");
}
