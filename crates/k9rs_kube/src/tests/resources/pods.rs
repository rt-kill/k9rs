use super::*;
use k9rs_proto::resource_def::ResourceDef;
use crate::resources::ConvertToRow;

/// Pods are the highest-risk converter: 24 columns and a metrics overlay
/// that writes cells *by index* derived from `column_defs()`. Guard that
/// the `cells` vec aligns with the headers so a one-off drift can't silently
/// land a CPU value under the wrong column. (The headers↔column_defs half is
/// covered registry-wide in `resource_defs::registry::tests`.)
#[test]
fn pod_cells_align_with_headers() {
    let row = PodDef::convert(k8s_openapi::api::core::v1::Pod::default());
    assert_eq!(
        row.cells.len(),
        PodDef.default_headers().len(),
        "pod cells vec must align with default_headers",
    );
}

// ---------------------------------------------------------------------------
// Outward references — what `U` actually answers with (v15)
// ---------------------------------------------------------------------------

use k9rs_proto::resource_def::BuiltInKind;
use k9rs_proto::row::ResourceRefInfo;

fn refs_of(yaml: &str) -> Vec<ResourceRefInfo> {
    let spec: k8s_openapi::api::core::v1::PodSpec =
        serde_yaml::from_str(yaml).expect("a well-formed PodSpec");
    super::pod_references(&spec)
}

/// The cases a name grep got wrong. A PVC is found by its claim name, not by
/// a pod that happens to be called something similar; a secret mounted by
/// `api-7d9f` was previously invisible because nothing about the two names
/// matches.
#[test]
fn a_pods_volumes_name_their_claims_and_secrets() {
    let refs = refs_of(
        r#"
containers:
  - name: app
    image: app
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-postgres-0
  - name: creds
    secret:
      secretName: db-creds
  - name: cfg
    configMap:
      name: app-config
"#,
    );
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::PersistentVolumeClaim,
        name: "data-postgres-0".into(),
    }));
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::Secret,
        name: "db-creds".into(),
    }));
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::ConfigMap,
        name: "app-config".into(),
    }));
}

/// Environment is the other half, and the one no naming convention could
/// ever have hinted at.
#[test]
fn environment_references_are_extracted_too() {
    let refs = refs_of(
        r#"
serviceAccountName: api-runner
imagePullSecrets:
  - name: registry-creds
containers:
  - name: app
    image: app
    envFrom:
      - secretRef:
          name: api-keys
      - configMapRef:
          name: feature-flags
    env:
      - name: TOKEN
        valueFrom:
          secretKeyRef:
            name: session-token
            key: t
"#,
    );
    for (kind, name) in [
        (BuiltInKind::ServiceAccount, "api-runner"),
        (BuiltInKind::Secret, "registry-creds"),
        (BuiltInKind::Secret, "api-keys"),
        (BuiltInKind::ConfigMap, "feature-flags"),
        (BuiltInKind::Secret, "session-token"),
    ] {
        assert!(
            refs.contains(&ResourceRefInfo { kind, name: name.into() }),
            "{name} missing from {refs:?}",
        );
    }
}

/// A projected volume is several sources wearing one mount, and init
/// containers reference things too.
#[test]
fn projected_sources_and_init_containers_count() {
    let refs = refs_of(
        r#"
initContainers:
  - name: setup
    image: setup
    envFrom:
      - configMapRef:
          name: bootstrap
containers:
  - name: app
    image: app
volumes:
  - name: bundle
    projected:
      sources:
        - secret:
            name: tls
        - configMap:
            name: ca
"#,
    );
    for (kind, name) in [
        (BuiltInKind::ConfigMap, "bootstrap"),
        (BuiltInKind::Secret, "tls"),
        (BuiltInKind::ConfigMap, "ca"),
    ] {
        assert!(
            refs.contains(&ResourceRefInfo { kind, name: name.into() }),
            "{name} missing from {refs:?}",
        );
    }
}

/// The same object named twice is one edge — a config mounted as a volume
/// AND read through `envFrom` shouldn't list the pod twice.
#[test]
fn a_reference_named_twice_is_recorded_once() {
    let refs = refs_of(
        r#"
containers:
  - name: app
    image: app
    envFrom:
      - configMapRef:
          name: shared
volumes:
  - name: v
    configMap:
      name: shared
"#,
    );
    assert_eq!(
        refs.iter()
            .filter(|r| r.name == "shared" && r.kind == BuiltInKind::ConfigMap)
            .count(),
        1,
    );
}

/// A pod that names nothing has no references — not a guess at one.
#[test]
fn a_pod_that_names_nothing_has_no_references() {
    assert!(refs_of("containers:\n  - name: app\n    image: app\n").is_empty());
}

/// Every kind `pod_references` can emit is declared by
/// `referenceable_kinds`, and nothing else is.
///
/// `referenceable_kinds` is what `U` gates on: a kind missing from it opens
/// a view titled "Pods that reference …" containing nothing, which reports a
/// miss as an answer — the exact failure the reverse lookup replaced. The
/// comment above the list said the two were "declared NEXT to it so the two
/// cannot drift", and proximity is not a mechanism: a fifth `push(…)` in
/// `pod_references` would compile, pass every test, and be invisible to `U`.
/// This is the mechanism.
///
/// The fixture is maximal on purpose — one of every place a pod can name
/// another object — so the emitted set is the whole emittable set.
#[test]
fn every_emittable_reference_kind_is_declared() {
    let refs = refs_of(
        r#"
serviceAccountName: api-sa
imagePullSecrets:
  - name: registry-creds
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-pvc
  - name: tls
    secret:
      secretName: tls-secret
  - name: settings
    configMap:
      name: app-config
  - name: bundle
    projected:
      sources:
        - secret:
            name: projected-secret
        - configMap:
            name: projected-config
containers:
  - name: app
    image: app
    envFrom:
      - secretRef:
          name: env-secret
      - configMapRef:
          name: env-config
    env:
      - name: A
        valueFrom:
          secretKeyRef:
            name: keyed-secret
            key: a
      - name: B
        valueFrom:
          configMapKeyRef:
            name: keyed-config
            key: b
initContainers:
  - name: init
    image: init
    envFrom:
      - secretRef:
          name: init-secret
"#,
    );
    assert!(refs.len() >= 8, "sanity: the fixture names several objects: {refs:?}");

    let declared = super::referenceable_kinds();
    let mut emitted: Vec<BuiltInKind> = refs.iter().map(|r| r.kind).collect();
    emitted.sort_by_key(|k| format!("{k:?}"));
    emitted.dedup();

    for kind in &emitted {
        assert!(
            declared.contains(kind),
            "`pod_references` emits {kind:?}, which `referenceable_kinds` does \
             not declare — `U` on it would open an empty view: declared {declared:?}",
        );
    }
    // And the other direction: a declared kind this fixture cannot produce is
    // a kind `U` offers and the converter never fills.
    for kind in declared {
        assert!(
            emitted.contains(kind),
            "`referenceable_kinds` declares {kind:?}, which a maximal pod spec \
             never emits: emitted {emitted:?}",
        );
    }
}

// ---------------------------------------------------------------------------
// `compute_pod_status` — the STATUS column and the row colour for every pod,
// and it had no tests at all.
//
// The property worth holding is the one `classify_pod_reason`'s doc now
// states: the classifier only ever sees a RAW apiserver reason, never one of
// the display forms this module composes. Feeding a composed form back in
// would hit the `_` arm and come out Pending, which costs a failing pod its
// red.
// ---------------------------------------------------------------------------

use k8s_openapi::api::core::v1::{
    Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
    ContainerStateWaiting, ContainerStatus, PodCondition,
};

fn waiting(reason: &str) -> ContainerStatus {
    ContainerStatus {
        state: Some(ContainerState {
            waiting: Some(ContainerStateWaiting {
                reason: Some(reason.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn terminated(exit_code: i32, reason: Option<&str>, signal: Option<i32>) -> ContainerStatus {
    ContainerStatus {
        state: Some(ContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code,
                reason: reason.map(str::to_string),
                signal,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn running(ready: bool) -> ContainerStatus {
    ContainerStatus {
        ready,
        state: Some(ContainerState {
            running: Some(ContainerStateRunning::default()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn condition(type_: &str, status: &str, reason: Option<&str>) -> PodCondition {
    PodCondition {
        type_: type_.to_string(),
        status: status.to_string(),
        reason: reason.map(str::to_string),
        ..Default::default()
    }
}

/// The ordinary case: phase and containers, nothing else.
fn status_of(phase: &str, containers: &[ContainerStatus]) -> PodStatus {
    compute_pod_status(phase, containers, &[], &[], &[], &None, false)
}

/// With init containers, and their spec (which is where `restartPolicy`
/// marks a sidecar). The phase is explicit because it is the fallback
/// display when nothing blocks: a pod past its init phase with every
/// container running reports the PHASE, so passing "Pending" there would
/// assert a stall that isn't one.
fn init_status(
    phase: &str,
    inits: &[ContainerStatus],
    specs: &[Container],
    containers: &[ContainerStatus],
) -> PodStatus {
    compute_pod_status(phase, containers, inits, specs, &[], &None, false)
}

/// An init container's reason is classified RAW and decorated afterwards, in
/// that order — never the other way round.
///
/// `Init:ImagePullBackOff` is not in the classifier's set, so a composed
/// string fed back in would fall to `_ => Pending` and the row would go
/// yellow for a pod that is never going to start.
#[test]
fn a_composed_display_form_never_reaches_the_classifier() {
    let stuck = init_status("Pending", &[waiting("ImagePullBackOff")], &[], &[]);
    assert_eq!(stuck.display, "Init:ImagePullBackOff");
    assert_eq!(stuck.health, RowHealth::Failed, "red, not yellow");
    assert_eq!(
        classify_pod_reason(&stuck.display),
        RowHealth::Pending,
        "and the proof that the order matters: the DISPLAY string does not \
         classify — only the raw reason does",
    );

    // Still-progressing init states stay Pending, which is the other half of
    // the same arm.
    let starting = init_status("Pending", &[waiting("PodInitializing")], &[], &[]);
    assert_eq!(starting.display, "Init:0/1");
    assert_eq!(starting.health, RowHealth::Pending);
}

/// A dead init container is Failed however it died, and says which way.
#[test]
fn a_dead_init_container_names_its_death() {
    let by_reason = init_status("Pending", &[terminated(137, Some("OOMKilled"), None)], &[], &[]);
    assert_eq!(by_reason.display, "Init:OOMKilled");
    assert_eq!(by_reason.health, RowHealth::Failed);

    let by_signal = init_status("Pending", &[terminated(137, None, Some(9))], &[], &[]);
    assert_eq!(by_signal.display, "Init:Signal:9", "no reason, so the signal");
    assert_eq!(by_signal.health, RowHealth::Failed);

    let by_code = init_status("Pending", &[terminated(2, None, None)], &[], &[]);
    assert_eq!(by_code.display, "Init:ExitCode:2");
    assert_eq!(by_code.health, RowHealth::Failed);

    // Exit 0 is init DONE — it must not stop the scan, or every pod past its
    // init phase would report the init container for ever.
    let done = init_status(
        "Running",
        &[terminated(0, Some("Completed"), None)],
        &[],
        &[running(true)],
    );
    assert_eq!(done.display, "Running");
    assert_eq!(done.health, RowHealth::Normal);

    // And exit 0 is DONE even with a signal recorded alongside it: the
    // zero-code skip comes first, exactly as k9s's own
    // `checkInitContainerStatus` orders it (`internal/render/pod.go:529`).
    // I asserted the opposite here first, from the name of the field.
    let zero_with_signal = init_status(
        "Running",
        &[terminated(0, None, Some(9))],
        &[],
        &[running(true)],
    );
    assert_eq!(zero_with_signal.display, "Running");
}

/// A started, ready sidecar init container (`restartPolicy: Always`) is a
/// long-running helper, not a blocked init step — it must not pin the pod at
/// `Init:0/1` for its whole life.
#[test]
fn a_ready_sidecar_does_not_block_init() {
    let sidecar_spec = vec![Container {
        name: "proxy".to_string(),
        restart_policy: Some("Always".to_string()),
        ..Default::default()
    }];
    let with_sidecar = init_status("Running", &[running(true)], &sidecar_spec, &[running(true)]);
    assert_eq!(with_sidecar.display, "Running");
    assert_eq!(with_sidecar.health, RowHealth::Normal);

    // The same container WITHOUT the sidecar policy is an init step that is
    // still running, and does block.
    let plain =
        init_status("Running", &[running(true)], &[Container::default()], &[running(true)]);
    assert_eq!(plain.display, "Init:0/1");
    assert_eq!(plain.health, RowHealth::Pending);
}

/// `ExitCode:0` is a clean exit — a finished Job's pod is not a failure —
/// while any other code, or a signal, is.
#[test]
fn a_clean_exit_is_not_a_failure_and_everything_else_is() {
    let clean = status_of("Succeeded", &[terminated(0, None, None)]);
    assert_eq!(clean.display, "ExitCode:0");
    assert_eq!(clean.health, RowHealth::Normal);

    let crashed = status_of("Running", &[terminated(1, None, None)]);
    assert_eq!(crashed.display, "ExitCode:1");
    assert_eq!(crashed.health, RowHealth::Failed);

    let killed = status_of("Running", &[terminated(0, None, Some(15))]);
    assert_eq!(killed.display, "Signal:15");
    assert_eq!(killed.health, RowHealth::Failed);
}

/// A pod being deleted says `Terminating`, whatever its containers are
/// doing — and `NodeLost` while deleting is `Unknown`, because nothing can
/// confirm what that pod is doing at all.
#[test]
fn deletion_overrides_the_container_verdict() {
    let dying = compute_pod_status(
        "Running",
        &[waiting("CrashLoopBackOff")],
        &[],
        &[],
        &[],
        &None,
        true,
    );
    assert_eq!(dying.display, "Terminating");
    assert_eq!(dying.health, RowHealth::Pending, "going away is not a fault");

    let lost = compute_pod_status(
        "Running",
        &[running(true)],
        &[],
        &[],
        &[],
        &Some("NodeLost".to_string()),
        true,
    );
    assert_eq!(lost.display, "Unknown");
    assert_eq!(lost.health, RowHealth::Pending);
}

/// A pod gated by a scheduling plugin reports the gate, not `Pending` —
/// it is waiting on a human, not on capacity.
#[test]
fn a_scheduling_gate_is_reported_by_name() {
    let gated = compute_pod_status(
        "Pending",
        &[],
        &[],
        &[],
        &[condition("PodScheduled", "False", Some("SchedulingGated"))],
        &None,
        false,
    );
    assert_eq!(gated.display, "SchedulingGated");
    assert_eq!(gated.health, RowHealth::Pending);
}

/// `Completed` with containers still running and ready is a stale phase —
/// the pod is Running if it is Ready, and `NotReady` if it is not.
#[test]
fn a_completed_phase_with_running_containers_defers_to_readiness() {
    let ready = compute_pod_status(
        "Completed",
        &[running(true)],
        &[],
        &[],
        &[condition("Ready", "True", None)],
        &None,
        false,
    );
    assert_eq!(ready.display, "Running");
    assert_eq!(ready.health, RowHealth::Normal);

    let not_ready = compute_pod_status(
        "Completed",
        &[running(true)],
        &[],
        &[],
        &[condition("Ready", "False", None)],
        &None,
        false,
    );
    assert_eq!(not_ready.display, "NotReady");
    assert_eq!(not_ready.health, RowHealth::Pending);
}

/// Several unhealthy containers: the scan runs in reverse and assigns rather
/// than returning, so the FIRST container's reason is the one shown —
/// matching k9s, and the one a user reads top-down in the container view.
#[test]
fn the_first_containers_reason_is_the_one_shown() {
    let r = status_of(
        "Running",
        &[waiting("CrashLoopBackOff"), waiting("ImagePullBackOff")],
        );
    assert_eq!(r.display, "CrashLoopBackOff");
    assert_eq!(r.health, RowHealth::Failed);
}

/// An unknown reason — a custom controller's own string — is Pending:
/// visible, not painted healthy.
#[test]
fn an_unrecognised_reason_is_not_healthy() {
    let r = status_of("Running", &[waiting("WaitingForMyOperatorToDoAThing")]);
    assert_eq!(r.display, "WaitingForMyOperatorToDoAThing");
    assert_eq!(r.health, RowHealth::Pending);
}
