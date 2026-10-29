//! Unit tests for the shared workload converter helpers (mirrored out of
//! `src/kube/resources/mod.rs`).

use super::{AtZero, WorkloadTally};
use k9rs_proto::row::RowHealth;

/// A tally whose zero means "someone scaled it down" — the Deployment,
/// StatefulSet and DaemonSet reading. The ReplicaSet one has its own tests.
fn parked(desired: i32, ready: i32, total: i32, up_to_date: Option<i32>) -> WorkloadTally {
    WorkloadTally { desired, ready, total, up_to_date, at_zero: AtZero::Parked }
}

/// A settled fleet: every pod present, ready, and on the current template.
fn settled(n: i32) -> WorkloadTally {
    parked(n, n, n, Some(n))
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
        parked(0, 2, 2, Some(2)).health(),
        RowHealth::Pending,
    );
}

#[test]
fn missing_capacity_is_failed() {
    assert_eq!(
        parked(3, 2, 3, Some(3)).health(),
        RowHealth::Failed,
        "a missing replica is a problem",
    );
    // The absent-means-zero case: k8s omits `readyReplicas` entirely when it
    // is 0, so a converter defaulting it to anything but 0 would report a
    // dead workload ready.
    assert_eq!(
        parked(1, 0, 1, Some(1)).health(),
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
        parked(1, 1, 1, Some(0)).health(),
        RowHealth::Pending,
        "no pod is on the new template yet",
    );
    // t1: new pod created, still starting. READY reads `1/1` and UP-TO-DATE
    // reads `1` — `total` is the only field that knows, and the colour is
    // the only thing in the row that can say so.
    assert_eq!(
        parked(1, 1, 2, Some(1)).health(),
        RowHealth::Pending,
        "a surge pod exists; no number in the row shows it",
    );
    // t2: both ready for an instant — READY renders `2/1`.
    assert_eq!(
        parked(1, 2, 2, Some(1)).health(),
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
        parked(3, 3, 3, Some(1)).health(),
        RowHealth::Pending,
    );
}

/// ...and conversely, a surge that `up_to_date` cannot see. Both halves of
/// `rollout_in_flight` are load-bearing; neither subsumes the other.
#[test]
fn surge_at_current_template_is_pending() {
    assert_eq!(
        parked(3, 4, 4, Some(3)).health(),
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
        parked(4, 3, 5, Some(2)).health(),
        RowHealth::Failed,
    );
}

/// A scale-down is also an unsettled fleet: you asked for one pod and three
/// still exist. This was `Normal` before — full capacity, nothing to see.
#[test]
fn draining_scale_down_is_pending() {
    assert_eq!(
        parked(1, 3, 3, Some(3)).health(),
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
        parked(3, 3, 3, None).health(),
        RowHealth::Normal,
    );
    // The `total` half still applies to those kinds.
    assert_eq!(
        parked(3, 3, 4, None).health(),
        RowHealth::Pending,
    );
}

// ---------------------------------------------------------------------------
// What zero MEANS — the same numbers, two verdicts
// ---------------------------------------------------------------------------

/// A retired ReplicaSet is history, not a parked workload. A Deployment keeps
/// its old ReplicaSets at zero per `revisionHistoryLimit`, so `:rs` in any
/// real cluster is mostly these — and marking them all `Pending` turned that
/// whole view yellow, diluting the yellow that means "a rollout is in
/// flight". k9s shows them normal too.
#[test]
fn a_retired_replicaset_is_not_pending() {
    let retired = WorkloadTally {
        desired: 0, ready: 0, total: 0, up_to_date: None, at_zero: AtZero::Retired,
    };
    assert_eq!(retired.health(), RowHealth::Normal);

    // Still history while its last pods drain.
    let draining = WorkloadTally {
        desired: 0, ready: 2, total: 2, up_to_date: None, at_zero: AtZero::Retired,
    };
    assert_eq!(draining.health(), RowHealth::Normal);
}

/// The distinction is NOT recoverable from the numbers — identical tallies,
/// opposite verdicts — which is why it is declared per kind rather than
/// derived.
#[test]
fn identical_tallies_differ_only_by_what_zero_means() {
    let numbers = |at_zero| WorkloadTally {
        desired: 0, ready: 0, total: 0, up_to_date: None, at_zero,
    };
    assert_eq!(numbers(AtZero::Parked).health(), RowHealth::Pending);
    assert_eq!(numbers(AtZero::Retired).health(), RowHealth::Normal);
}

/// `AtZero` only speaks for a zero scale target; above zero it must not
/// change a single verdict.
#[test]
fn at_zero_is_silent_above_zero() {
    for at_zero in [AtZero::Parked, AtZero::Retired] {
        let under = WorkloadTally { desired: 3, ready: 2, total: 3, up_to_date: Some(3), at_zero };
        assert_eq!(under.health(), RowHealth::Failed);
        let rolling = WorkloadTally { desired: 3, ready: 3, total: 4, up_to_date: Some(3), at_zero };
        assert_eq!(rolling.health(), RowHealth::Pending);
        let done = WorkloadTally { desired: 3, ready: 3, total: 3, up_to_date: Some(3), at_zero };
        assert_eq!(done.health(), RowHealth::Normal);
    }
}

// ---------------------------------------------------------------------------
// Which ports a port-forward may offer
// ---------------------------------------------------------------------------

use k8s_openapi::api::core::v1::{Container, ContainerPort, PodSpec};

fn container(name: &str, ports: &[(i32, Option<&str>)]) -> Container {
    Container {
        name: name.to_string(),
        ports: Some(
            ports
                .iter()
                .map(|(p, proto)| ContainerPort {
                    container_port: *p,
                    protocol: proto.map(str::to_string),
                    ..Default::default()
                })
                .collect(),
        ),
        ..Default::default()
    }
}

/// UDP ports are not offered. The forward rides a websocket tunnel, so a
/// UDP port produces a forward that loops `Reconnecting` for ever — and the
/// pod converter had no protocol filter while the workload helper did, so
/// `Shift-F` on a Pod offered one and `Shift-F` on its Deployment did not.
#[test]
fn a_udp_port_is_not_forwardable() {
    let spec = PodSpec {
        containers: vec![
            container("web", &[(80, Some("TCP")), (53, Some("UDP"))]),
            container("dns", &[(5353, Some("UDP"))]),
        ],
        ..Default::default()
    };
    assert_eq!(super::forwardable_ports(&spec), vec![80]);
}

/// An unspecified protocol is TCP, which is what K8s defaults it to — so an
/// ordinary manifest that omits `protocol:` still gets its port.
#[test]
fn an_unspecified_protocol_is_forwardable() {
    let spec = PodSpec {
        containers: vec![container("web", &[(8080, None)])],
        ..Default::default()
    };
    assert_eq!(super::forwardable_ports(&spec), vec![8080]);
}

/// Read from the SPEC, never the status — which is what made a PENDING pod
/// offer nothing while its own Deployment offered the real ports. A spec has
/// its ports before anything has started.
#[test]
fn ports_come_from_the_spec_not_from_a_running_container() {
    let spec = PodSpec {
        containers: vec![container("web", &[(80, Some("TCP"))]), container("side", &[(9090, None)])],
        ..Default::default()
    };
    // No statuses anywhere in sight; the answer is the declaration.
    assert_eq!(super::forwardable_ports(&spec), vec![80, 9090]);
}

/// A container declaring no ports contributes none, and a spec of those is
/// empty rather than a stand-in — the form's free-text fallback is the
/// honest answer there.
#[test]
fn a_spec_with_no_declared_ports_offers_none() {
    let spec = PodSpec {
        containers: vec![Container { name: "web".into(), ..Default::default() }],
        ..Default::default()
    };
    assert!(super::forwardable_ports(&spec).is_empty());
    assert!(super::forwardable_ports(&PodSpec::default()).is_empty());
}

/// `WorkloadContainers` reads the same rule, so every workload converter
/// that goes through it agrees with the pod converter.
#[test]
fn the_workload_helper_reads_the_same_rule() {
    let spec = PodSpec {
        containers: vec![container("web", &[(80, Some("TCP")), (53, Some("UDP"))])],
        ..Default::default()
    };
    let wc = super::WorkloadContainers::from_pod_spec(Some(&spec));
    assert_eq!(wc.tcp_ports, super::forwardable_ports(&spec));
    assert_eq!(wc.names, "web");
}

// ---------------------------------------------------------------------------
// One LABELS cell type
// ---------------------------------------------------------------------------

/// Every converter that declares a `LABELS` column builds the SAME cell
/// type, so `O` on that header means one thing.
///
/// Seven built a `CellValue::List` and five a `CellValue::Text`. Display is
/// identical — `List`'s `Display` re-joins on `,` and `labels_str` is
/// already comma-joined — so the split was invisible until you sorted:
/// `List`'s `Ord` compares LENGTH first and then the joined string, `Text`'s
/// compares the string. `O` on LABELS was "by label count" in `:pods` and
/// "alphabetically" in `:svc`, under one header.
#[test]
fn every_labels_cell_is_a_list() {
    use k9rs_proto::row::CellValue;
    let meta = super::CommonMeta {
        namespace: "prod".into(),
        name: "api".into(),
        uid: String::new(),
        labels: Default::default(),
        labels_str: "app=web,tier=front".into(),
        age: None,
    };
    let cell = meta.labels_cell();
    assert!(matches!(cell, CellValue::List(_)), "got {cell:?}");
    // …and it still READS the same, which is why the split was invisible.
    assert_eq!(cell.to_string(), "app=web,tier=front");
}

/// A row with no labels is an empty LIST, not an empty string — so it sorts
/// with the other label-less rows rather than against them.
#[test]
fn a_row_with_no_labels_is_an_empty_list() {
    use k9rs_proto::row::CellValue;
    let meta = super::CommonMeta {
        namespace: String::new(),
        name: String::new(),
        uid: String::new(),
        labels: Default::default(),
        labels_str: String::new(),
        age: None,
    };
    assert!(matches!(meta.labels_cell(), CellValue::List(ref v) if v.is_empty()));
}

/// The sort the type decides: by COUNT first. Two rows whose labels sort the
/// same alphabetically order by how many they have, which is what makes
/// `List` the useful answer for this column.
#[test]
fn labels_sort_by_count_then_text() {
    use k9rs_proto::row::CellValue;
    let one = CellValue::from_comma_str("zz=1");
    let two = CellValue::from_comma_str("aa=1,bb=2");
    assert!(one < two, "one label before two, whatever the letters say");
}
