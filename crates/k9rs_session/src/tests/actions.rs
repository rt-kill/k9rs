//! What lands on the clipboard when rows are marked.

use super::*;
use k9rs_app::Action;
use k9rs_app::{apply_event, AppEvent};
use k9rs_app::StorePayload;
use k9rs_proto::protocol::{ObjectKey, TableBaseline};
use k9rs_proto::row::{CellValue, ResourceRow};

fn row(name: &str) -> ResourceRow {
    ResourceRow {
        cells: vec![CellValue::Text(name.into()), CellValue::Text("Running".into())],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    }
}

/// Three rows in the root table, with `marked` selected.
fn seeded_app(marked: &[&str]) -> App {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().expect("test root is a list"));
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into(), "STATUS".into()],
            rows: vec![row("a"), row("b"), row("c")],
        }),
    );
    for name in marked {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    // The dump reads the LAST PAINTED view — the user copies what they see.
    app.nav.top_mut().ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    app
}

#[test]
fn copying_a_selection_yields_the_marked_rows_only() {
    // With rows marked, `c` used to dump the ENTIRE visible table: mark three
    // pods out of forty-seven and you got forty-seven. Select mode exists to
    // keep single and batch from being the same action with different data.
    let app = seeded_app(&["a", "c"]);
    let marked = app.nav.top().marked_snapshot();
    let dump = table_dump(&app, Some(&marked));

    let lines: Vec<&str> = dump.lines().collect();
    assert_eq!(lines.len(), 3, "header + two marked rows, got: {dump:?}");
    assert!(lines[0].starts_with("NAME"), "the header survives: {:?}", lines[0]);
    assert!(lines[1].starts_with('a'), "{:?}", lines[1]);
    assert!(lines[2].starts_with('c'), "{:?}", lines[2]);
    assert!(!dump.contains('b'), "the unmarked row is not copied: {dump:?}");
}

#[test]
fn copying_without_a_selection_still_yields_the_whole_table() {
    // The unmarked path is unchanged — `c` outside select mode is the
    // whole-table copy it always was.
    let app = seeded_app(&[]);
    let dump = build_table_dump(&app);
    assert_eq!(dump.lines().count(), 4, "header + three rows, got: {dump:?}");
}

#[test]
fn a_selection_that_matches_nothing_copies_nothing() {
    // Not even a lone header row: pasting a bare column of names with no
    // values under it is worse than an obvious no-op, and `send_to_clipboard`
    // skips empty text, so the user gets no misleading "Copied 0 rows" flash.
    let app = seeded_app(&[]);
    let mut absent = std::collections::HashSet::new();
    absent.insert(ObjectKey::new("ns".to_string(), "gone".to_string()));
    assert!(table_dump(&app, Some(&absent)).is_empty());
}

// ---------------------------------------------------------------------------
// What `Ctrl-L` is OVER (v13 `LogTarget`)
// ---------------------------------------------------------------------------

use k9rs_proto::protocol::{LogContainer, LogLeaf, LogTarget};
use k9rs_proto::row::{ContainerInfo, DrillTarget};

fn pod_row(name: &str, ns: &str, containers: usize) -> ResourceRow {
    ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some(ns.into()),
        containers: (0..containers)
            .map(|i| ContainerInfo { health: Default::default(),
                name: format!("c{i}"),
                kind: k9rs_proto::row::ContainerKind::Regular,
                image: String::new(),
                state: k9rs_proto::row::ContainerState::default(),
                reason: None,
                ready: true,
                restart_count: 0,
            })
            .collect(),
        ..Default::default()
    }
}

/// A one-row table whose single row is `row`, selected.
fn app_with_row(row: ResourceRow) -> App {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().expect("test root is a list"));
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![row],
        }),
    );
    app.nav.top_mut().ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    app
}

fn selected_ref(app: &App) -> k9rs_proto::protocol::ObjectRef {
    get_selected_resource_info(app).expect("a row is selected")
}

/// THE reported bug: `Ctrl-L` on a deployment used to send the kubectl-style
/// ref `deployments/my-app` as the pod, and kubectl picked ONE replica. A
/// workload's logs are every replica's logs, so the row's drill target — the
/// same one Enter uses to list those pods — defines the stream.
#[test]
fn a_workload_logs_every_pod_its_drill_target_names() {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app".to_string(), "api".to_string());
    let mut row = pod_row("my-app", "ns", 0);
    row.drill_target = Some(DrillTarget::PodsByLabels {
        labels: labels.clone(),
        breadcrumb: "deploy/my-app".into(),
    });
    let app = app_with_row(row);

    let target = workload_log_target(&app, &selected_ref(&app));
    assert_eq!(target, LogLeaf::Selector { labels, label: "pods/my-app".into() }.into());
    assert!(target.is_fanout(), "a selector can name more than one pod");
}

/// Jobs and replicasets identify their pods by ownerReference, not by a
/// selector — same fan-out, different question for the daemon to ask.
#[test]
fn an_owner_drilled_workload_logs_its_owned_pods() {
    let mut row = pod_row("my-job", "ns", 0);
    row.drill_target = Some(DrillTarget::PodsByOwner {
        uid: "uid-1".into(),
        kind: k9rs_proto::resource_def::BuiltInKind::Job,
        name: "my-job".into(),
    });
    let app = app_with_row(row);

    assert_eq!(
        workload_log_target(&app, &selected_ref(&app)),
        LogLeaf::Owner { uid: "uid-1".into(), label: "pods/my-job".into() }.into(),
    );
}

/// The shapes nothing can resolve to a pod set (a cronjob owns jobs, a
/// name-grep isn't a selector) keep the pre-fan-out behaviour: hand the
/// kubectl-style ref over and let kubectl resolve or refuse it.
#[test]
fn an_unresolvable_workload_falls_back_to_the_kubectl_ref() {
    let app = app_with_row(pod_row("mystery", "ns", 0));
    let target = workload_log_target(&app, &selected_ref(&app));
    assert_eq!(target, LogTarget::one("pods/mystery"));
    assert!(!target.is_fanout(), "one ref, one source, lines stay untagged");
}

/// A marked selection is ONE merged stream over exactly those pods — not
/// the whole table, and not N separate log views.
#[test]
fn a_marked_selection_logs_exactly_those_pods() {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod_row("a", "ns", 1), pod_row("b", "ns", 1), pod_row("c", "ns", 1)],
        }),
    );
    for name in ["a", "c"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    let _ = &mut app;

    let spec = batch_log_spec(&app).expect("three single-container pods in one namespace");
    assert_eq!(
        spec.target,
        LogTarget::Union {
            targets: vec![LogLeaf::One("a".into()), LogLeaf::One("c".into())],
            label: "2 pods".into(),
        },
    );
    assert_eq!(spec.namespace, k9rs_proto::protocol::Namespace::Named("ns".into()));
    assert!(spec.follow);
    assert!(!spec.previous, "batch logs is the live stream, never --previous");
    assert!(spec.target.is_fanout(), "two sources, so lines must be tagged");
}

/// Single-container pods stay on the daemon's API path (`Default`); one
/// multi-container pod in the set forces `--all-containers`, the only shape
/// that can name them all.
#[test]
fn the_container_stance_follows_the_marked_pods() {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod_row("solo", "ns", 1), pod_row("sidecar", "ns", 2)],
        }),
    );
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "solo".to_string()));
    assert_eq!(batch_log_spec(&app).unwrap().container, LogContainer::Default);

    store.toggle_mark(&ObjectKey::new("ns".to_string(), "sidecar".to_string()));
    assert_eq!(batch_log_spec(&app).unwrap().container, LogContainer::All);
    let _ = &mut app;
}

/// THE ask: a marked set of WORKLOADS is one merged stream over every pod
/// they own. Each row resolves through the same `row_log_target` a single
/// `Ctrl-L` uses, so the hovered deployment and the marked one can't
/// disagree about which pods they mean — and the union is what makes the
/// set expressible at all. It used to be refused outright ("Batch logs
/// needs pods"), because a flat list of pod names could not say
/// "these two label selectors".
#[test]
fn a_marked_set_of_workloads_unions_their_pod_sources() {
    let labels = |v: &str| {
        let mut m = std::collections::BTreeMap::new();
        m.insert("app".to_string(), v.to_string());
        m
    };
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    let workload = |name: &str, app_label: &str| {
        let mut r = pod_row(name, "ns", 0);
        r.drill_target = Some(DrillTarget::PodsByLabels {
            labels: labels(app_label),
            breadcrumb: format!("deploy/{name}"),
        });
        r
    };
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![workload("api", "api"), workload("web", "web")],
        }),
    );
    for name in ["api", "web"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    let _ = &mut app;

    let spec = batch_log_spec(&app).expect("two workloads in one namespace");
    let LogTarget::Union { targets, .. } = &spec.target else {
        panic!("a selection is a union, got {:?}", spec.target)
    };
    assert_eq!(targets.len(), 2);
    assert!(
        targets.iter().all(|t| matches!(t, LogLeaf::Selector { .. })),
        "each workload contributes the selector its drill target names: {targets:?}",
    );
    assert_eq!(
        spec.container,
        LogContainer::All,
        "a workload's replicas may carry sidecars, and we have not seen them",
    );
}

/// A MIXED selection is the case a flat pod list could never express: two
/// shapes, one stream. Resolution is per row, so nothing has to be
/// homogeneous.
#[test]
fn a_mixed_selection_carries_each_rows_own_shape() {
    let rid = k9rs_proto::resource_def::BuiltInKind::Deployment.to_resource_id();

    // A pod: it lists containers, so it is its own target.
    let pod = pod_row("api-7d9f", "ns", 1);
    assert_eq!(row_log_target(&pod, &rid), Some(LogLeaf::One("api-7d9f".into())));

    // A job: no containers, pods found by ownerReference.
    let mut job = pod_row("nightly", "ns", 0);
    job.drill_target = Some(DrillTarget::PodsByOwner {
        uid: "uid-9".into(),
        kind: k9rs_proto::resource_def::BuiltInKind::Job,
        name: "nightly".into(),
    });
    assert_eq!(
        row_log_target(&job, &rid),
        Some(LogLeaf::Owner { uid: "uid-9".into(), label: "deployments/nightly".into() }),
    );

    // A row with no logs at all earns no place in the union.
    let ns_rid = k9rs_proto::resource_def::BuiltInKind::Namespace.to_resource_id();
    assert_eq!(row_log_target(&pod_row("kube-system", "", 0), &ns_rid), None);
}

/// One stream carries one namespace (`LogInit.namespace`), so a selection
/// made in an all-namespaces view that spans them is refused — the
/// alternative is silently logging only one namespace's worth.
#[test]
fn a_selection_spanning_namespaces_is_refused() {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod_row("here", "ns-a", 1), pod_row("there", "ns-b", 1)],
        }),
    );
    store.toggle_mark(&ObjectKey::new("ns-a".to_string(), "here".to_string()));
    store.toggle_mark(&ObjectKey::new("ns-b".to_string(), "there".to_string()));
    let err = batch_log_spec(&app).expect_err("two namespaces, one stream");
    assert!(err.contains("namespace"), "{err}");
    let _ = &mut app;
}

/// The header names what you drilled into, not how the daemon finds it: a
/// raw label selector in the title bar is the mechanism, not the thing.
#[test]
fn the_header_names_the_thing_not_the_selector() {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app".to_string(), "api".to_string());
    assert_eq!(
        LogLeaf::Selector { labels, label: "deploy/api".into() }.display(),
        "deploy/api",
    );
    assert_eq!(
        LogTarget::Union { targets: vec![], label: "2 pods".into() }.display(),
        "2 pods",
    );
    assert_eq!(LogTarget::one("pod-x").display(), "pod-x");
}

/// A multi-container pod's logs used to be unreachable as a WHOLE:
/// `LogContainer::All` existed on the wire, but every UI path that produced
/// it was for a row with NO containers, so "logs of this pod" always meant
/// one container. The log picker now leads with it.
#[test]
fn the_log_picker_offers_every_container_at_once() {
    use k9rs_app::{ContainerAction, ContainerChoice};
    let containers = pod_row("p", "ns", 2).containers;

    let logs = ContainerChoice::list(ContainerAction::Logs, &containers);
    assert_eq!(logs.len(), 3, "all + two containers");
    assert_eq!(logs[0].container, LogContainer::All);
    assert_eq!(logs[1].container, LogContainer::Named("c0".into()));
    assert_eq!(logs[2].container, LogContainer::Named("c1".into()));

    // Same for the previous-logs picker — "what did they all say before the
    // crash" is the same question.
    assert_eq!(
        ContainerChoice::list(ContainerAction::PreviousLogs, &containers)[0].container,
        LogContainer::All,
    );
}

/// There is no shell into every container at once, so the shell picker never
/// offers one — the impossibility lives in the list, not in a guard at the
/// far end that a future call site could forget.
#[test]
fn the_shell_picker_offers_no_such_thing_as_all() {
    use k9rs_app::{ContainerAction, ContainerChoice};
    let containers = pod_row("p", "ns", 2).containers;
    let shells = ContainerChoice::list(ContainerAction::Shell, &containers);
    assert_eq!(shells.len(), 2, "two containers, no 'all' row");
    assert!(
        shells.iter().all(|c| matches!(c.container, LogContainer::Named(_))),
        "every shell choice names exactly one container",
    );
}

/// The other axis: inside a container view the marked rows are CONTAINERS of
/// one pod, so the selection lands on `LogContainer` instead of `LogTarget`.
///
/// `All` would not do — it is `--all-containers`, every container of the
/// pod, which is precisely what you said no to by marking three of the eight.
#[test]
fn a_marked_set_of_containers_streams_exactly_those() {
    use k9rs_app::Element;
    use k9rs_app::DerivedViewKind;

    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    let pod = pod_row("web-0", "ns", 3); // containers c0, c1, c2
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod.clone()],
        }),
    );
    app.nav.top_mut().ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    let mut derived = Element::derive_projection(app.nav.top(), &pod, DerivedViewKind::Containers)
        .expect("containers projection");
    derived.ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    derived.select(0);
    derived.toggle_mark();
    derived.select(2);
    derived.toggle_mark();
    app.nav.push(derived);

    let spec = batch_log_spec(&app).expect("two marked containers of one pod");
    assert_eq!(spec.target, LogTarget::one("web-0"), "one pod, named once");
    assert_eq!(
        spec.container,
        LogContainer::Set(vec!["c0".into(), "c2".into()]),
        "exactly the marked containers — not `All`, which would be all three",
    );
    assert_eq!(spec.container.label(), "2 containers");
}

// ---------------------------------------------------------------------------
// `E` — the events about one object
// ---------------------------------------------------------------------------

/// The selector pins the SUBJECT, not something that resembles it: a Service
/// and a Deployment can share a name, and so can two objects in different
/// namespaces. `:events` could only ever show the whole cluster's events and
/// leave you grepping a name by eye.
#[test]
fn the_events_selector_pins_kind_name_and_namespace() {
    use k9rs_proto::row::K8sFieldSelector;
    let sel = K8sFieldSelector::InvolvedObject {
        kind: "Pod".into(),
        name: "api-7d9f".into(),
        namespace: "prod".into(),
    };
    assert_eq!(
        sel.to_wire(),
        "involvedObject.kind=Pod,involvedObject.name=api-7d9f,involvedObject.namespace=prod",
    );
    assert_eq!(sel.breadcrumb(), "pod/api-7d9f");
}

/// A cluster-scoped subject has no namespace to pin, and its events land in
/// whatever namespace the reporting component chose — so that clause is
/// omitted rather than sent empty, which would match nothing at all.
#[test]
fn a_cluster_scoped_subject_omits_the_namespace_clause() {
    use k9rs_proto::row::K8sFieldSelector;
    let sel = K8sFieldSelector::InvolvedObject {
        kind: "Node".into(),
        name: "ip-10-0-1-5".into(),
        namespace: String::new(),
    };
    assert_eq!(
        sel.to_wire(),
        "involvedObject.kind=Node,involvedObject.name=ip-10-0-1-5",
    );
}

// ---------------------------------------------------------------------------
// Batch capture and the one-batch-at-a-time rule
//
// These two lived in `event/handler_tests.rs` and were the whole of the
// `event -> session` edge: they probe `session::actions`, not key handling.
// Here they reach their subject through `super::*` like every other mirror,
// so the qualified path goes away too.
// ---------------------------------------------------------------------------

/// Batch targets are captured as marks ∩ PRESENT rows. During the Ctrl-R
/// window (marks survive `clear()`, rows unknown/Initializing) capture is
/// honestly empty — nothing can operate on an identity the store cannot
/// vouch for. The recovery baseline re-anchors and capture works again.
#[test]
fn test_batch_capture_is_empty_during_refresh_window() {
    use k9rs_app::StorePayload;
    use k9rs_proto::protocol::{ObjectKey, TableBaseline};
    use k9rs_proto::row::{CellValue, ResourceRow};

    let row = |name: &str| ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    let baseline = |rows: Vec<ResourceRow>| {
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows,
        })
    };

    let app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(1, baseline(vec![row("a"), row("b")]));
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "a".to_string()));

    let captured = get_marked_resource_infos(&app);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].name, "a");

    // Ctrl-R: rows drop, marks survive — capture must go empty, and the
    // app is STILL in select mode (the title says so; batch keys flash).
    store.clear();
    assert!(app.select_mode());
    assert!(get_marked_resource_infos(&app).is_empty());

    // Recovery baseline re-anchors the surviving mark.
    store.apply(2, baseline(vec![row("a")]));
    let captured = get_marked_resource_infos(&app);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].name, "a");
}

/// ONE batch at a time: while a tracker is outstanding, a second batch
/// dispatch is refused (a silent tracker replacement would duplicate the
/// sends and let the first batch's late results clobber the second's
/// summary).
#[test]
fn test_second_batch_refused_while_one_outstanding() {
    use k9rs_app::StorePayload;
    use k9rs_proto::protocol::{ObjectKey, TableBaseline};
    use k9rs_proto::row::{CellValue, ResourceRow};

    let row = |name: &str| ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(1, StorePayload::Baseline(TableBaseline {
        headers: vec!["NAME".into()],
        rows: vec![row("a")],
    }));
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "a".to_string()));

    // Outstanding tracker → refuse, no dialog, selection untouched.
    let t = k9rs_proto::protocol::ObjectRef::new(
        k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
        "a".to_string(),
        k9rs_proto::protocol::Namespace::from_row("ns"),
    );
    app.pending_batch = Some(k9rs_app::BatchTracker::new(
        "Deleted", "pod".to_string(),
        k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
        std::slice::from_ref(&t), 0, std::sync::Weak::new(),
        k9rs_proto::protocol::OperationKind::Delete,
    ));
    handle_batch_op(&mut app, Action::BatchDelete);
    assert!(!matches!(app.nav.top(), k9rs_app::Element::Confirm(_)));
    assert!(app.ui.flash.as_ref().is_some_and(|f| f.message.contains("in flight")));
    assert!(app.nav.top().has_marks(), "refusal leaves the selection untouched");

    // Tracker cleared → the same dispatch opens the confirm dialog.
    app.pending_batch = None;
    handle_batch_op(&mut app, Action::BatchDelete);
    assert!(matches!(app.nav.top(), k9rs_app::Element::Confirm(_)));
}

#[test]
fn every_context_switch_lands_on_home_immediately() {
    // A switch is a ROOT-level change: the stack describes a cluster you
    // just asked to leave. Enter-on-a-context-row used to do this and a
    // typed `:ctx <name>` did not, so the same action felt different
    // depending on how you spelled it.
    use k9rs_proto::protocol::ContextName;
    for start_on_picker in [true, false] {
        let mut app = App::new_for_test();
        if start_on_picker {
            apply_event(&mut app, AppEvent::NoContextConfigured);
        } else {
            // A deep-ish stack belonging to the outgoing context.
            assert!(app.nav.depth() >= 1);
        }
        begin_context_switch(
            &mut app,
            &ContextName::new("staging").unwrap(),
        );
        assert!(
            matches!(app.nav.top(), Element::Overview(_)),
            "start_on_picker={start_on_picker}: the switch lands on home right away",
        );
        assert_eq!(app.nav.depth(), 1, "…as the root, not pushed over the old view");
    }
}


#[test]
fn an_unconfirmed_context_never_becomes_the_confirmed_one() {
    // SEV-1 from the 2026-09-09 audit. `KubeconfigLoaded` fires BEFORE the
    // handshake and names the context the session is aiming at. It used to be
    // written into `kube.context` under an `is_none()` guard — which is
    // permanently true after a no-context start — so a failed pick fell back
    // to the context that had just failed, and the second failure tripped the
    // initial-failure-is-fatal rule and QUIT THE APP.
    use k9rs_proto::protocol::{ClusterIdentity, ContextName};
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::NoContextConfigured);
    assert!(app.kube.context.is_none());

    // Pick `prod` — the real path: the picker's Enter, then the main loop
    // taking the request and building a session for it.
    begin_context_switch(
        &mut app,
        &ContextName::new("prod").unwrap(),
    );
    assert_eq!(
        app.kube.context_switch.take_requested(),
        Some(ContextName::new("prod").unwrap()),
    );

    // That session announces its candidate before the handshake.
    apply_event(&mut app, AppEvent::KubeconfigLoaded {
        contexts: vec![],
        current_context: Some(ContextName::new("prod").unwrap()),
        current_identity: ClusterIdentity::new("prod-eks".into(), "prod-admin".into()),
    });
    assert!(
        app.kube.context.is_none(),
        "aiming at a context is not being on it — nothing is confirmed yet",
    );
    assert!(app.kube.connecting.is_some(), "…but the candidate is remembered");

    // It fails. The fallback must have no context to retry, so the app
    // returns to the picker instead of retrying prod and quitting.
    apply_event(&mut app, AppEvent::ConnectionFailed("unreachable".into()));
    assert!(!app.should_quit, "a failed pick must not exit the TUI");
    assert!(app.kube.context.is_none(), "still nothing confirmed to fall back to");
    // …and the fallback rebuild therefore aims at NO context, so it re-reports
    // `NoContextConfigured` and lands back on the picker — instead of
    // retrying the context that just failed and tripping the fatal rule.
    assert!(app.kube.context_switch.is_stable(), "the switch unlocked");
    assert!(
        app.kube.context_switch.target().is_none() && app.kube.context.is_none(),
        "nothing for the rebuild to aim at",
    );
}


#[test]
fn the_identity_never_describes_the_context_being_left() {
    // The same root cause's second symptom: the header named the TARGET while
    // the three lines under it still described the context we were leaving.
    use k9rs_proto::protocol::{ClusterIdentity, ContextName};
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::ConnectionEstablished {
        context: ContextName::new("prod").unwrap(),
        identity: ClusterIdentity::new("prod-eks".into(), "prod-admin".into()),
        namespaces: vec![],
    });
    // Switch away. This goes through the REAL `begin_context_switch`, which is
    // what `k9rs_app`'s own attachment tests can only simulate — they set
    // `switch_requested()` + `Requested(..)` by hand and claim that is what
    // this function does. This is where that claim is checked.
    begin_context_switch(
        &mut app,
        &ContextName::new("staging").unwrap(),
    );

    // BEFORE the new session reports. The window the bug lived in: the label
    // already names staging, so the identity must not still be prod's.
    let at = app.attachment();
    let label = k9rs_ui::context_label(&at);
    assert!(label.contains("staging"), "names what we're connecting to: {label}");
    assert_eq!(
        at.identity().cluster(),
        None,
        "nothing is known about staging yet, and prod's cluster is not a stand-in: {label}",
    );

    // Then the candidate arrives and fills it in, consistently.
    apply_event(&mut app, AppEvent::KubeconfigLoaded {
        contexts: vec![],
        current_context: Some(ContextName::new("staging").unwrap()),
        current_identity: ClusterIdentity::new("staging-eks".into(), "staging-admin".into()),
    });
    let at = app.attachment();
    let label = k9rs_ui::context_label(&at);
    assert!(label.contains("staging"), "still names the target: {label}");
    assert_eq!(at.identity().cluster(), Some("staging-eks"), "…and the cluster line agrees");
    assert_eq!(at.identity().user(), Some("staging-admin"));
}

/// `ds_try!` says whether the command went, and a gated success flash
/// cannot bury its error.
///
/// `app.ui.flash` is ONE slot and last write wins. The macro evaluated to
/// `()`, so two call sites followed it with an unconditional
/// `FlashMessage::info(..)` that overwrote the error it had just set.
/// `send_command` is a `try_send` on a bounded channel — after the daemon
/// dies every call fails — so the user was told "Scale: nginx" while nothing
/// had been sent, with the form already popped off the stack so nothing on
/// screen contradicted it.
///
/// Driven on the MACRO rather than through a `ClientSession`, which has no
/// test constructor: the defect was never in the sending, it was in not
/// being able to ask whether it happened.
#[test]
fn a_gated_success_flash_cannot_bury_a_send_failure() {
    use k9rs_app::FlashLevel;

    // The happy path: reports true, says nothing.
    let mut app = App::new_for_test();
    let sent: anyhow::Result<()> = Ok(());
    assert!(crate::ds_try!(app, sent), "a successful send reports true");
    assert!(app.ui.flash.is_none(), "and flashes nothing of its own");

    // The failure: reports false, and names the cause.
    let mut app = App::new_for_test();
    let failed: anyhow::Result<()> = Err(anyhow::anyhow!("Daemon writer task closed"));
    assert!(!crate::ds_try!(app, failed), "a failed send reports false");
    let flash = app.ui.flash.as_ref().expect("the failure is flashed");
    assert_eq!(flash.level, FlashLevel::Error);
    assert!(
        flash.message.contains("Daemon writer task closed"),
        "and says what went wrong: {:?}",
        flash.message,
    );

    // The shape every call site now uses. The error must survive it.
    let failed: anyhow::Result<()> = Err(anyhow::anyhow!("command queue full"));
    if crate::ds_try!(app, failed) {
        app.ui.flash = Some(k9rs_app::FlashMessage::info("Scale: nginx"));
    }
    let flash = app.ui.flash.as_ref().expect("still flashed");
    assert_eq!(
        flash.level,
        FlashLevel::Error,
        "an ungated success flash here is the bug: {:?}",
        flash.message,
    );
    assert!(flash.message.contains("command queue full"));
}

/// A flag and its value are never separated, even when the value is absent.
///
/// `SHELL_TEMPLATE` spelled the namespace as `Literal("-n")` beside
/// `Placeholder(Namespace)`, and `ConditionalPair` — the variant built to
/// bind a flag to its value — is what every other pair in either template
/// uses. Two different failures came out of that:
///
/// - With a pod target, `resolve_placeholder` answers `Some(namespace)` even
///   when it is empty, so the invocation carried `-n ""` — an explicit empty
///   `--namespace` rather than none at all. Measured: `["exec", "-it", "-n",
///   "", "api-7d9f", "--", …]`.
/// - With a MISMATCHED target it answers `None`, `build_exec_init` skips the
///   placeholder and keeps the literal, and the dangling `-n` takes the next
///   argument as its value — shifting everything after it.
///
/// The second needs an op paired with the wrong target variant, which none
/// of the four `ActionResult::Exec` constructions does. That is why it is
/// worth a type and a test rather than a reading.
#[test]
fn a_shell_invocation_never_separates_a_flag_from_its_value() {
    use crate::ExecTarget;
    use k9rs_proto::protocol::OperationKind;

    let template = OperationKind::Shell
        .exec_template()
        .expect("Shell has a template");

    // The ordinary case: namespace present.
    let (init, _title) = crate::build_exec_init(
        template,
        &ExecTarget::Pod {
            namespace: "default".to_string(),
            pod: "api-7d9f".to_string(),
            container: String::new(),
        },
    );
    let flat = init.kubectl_args.join(" ");
    assert!(flat.contains("-n default"), "the namespace rides its flag: {flat}");
    assert!(flat.contains("api-7d9f"), "and the pod is a positional: {flat}");

    // The case that bit: no namespace at all.
    let (init, _title) = crate::build_exec_init(
        template,
        &ExecTarget::Pod {
            namespace: String::new(),
            pod: "api-7d9f".to_string(),
            container: String::new(),
        },
    );
    let args = &init.kubectl_args;
    assert!(
        !args.iter().any(|a| a == "-n"),
        "no value, no flag — a dangling `-n` eats the next argument: {args:?}",
    );
    // And the pod is still the pod, not something's namespace.
    let pod_at = args.iter().position(|a| a == "api-7d9f").expect("the pod is there");
    assert!(
        pod_at == 0 || args[pod_at - 1] != "-n",
        "the pod must not be sitting where a namespace goes: {args:?}",
    );
}

/// The config reaches a batch log spec. `batch_log_spec` hardcoded
/// `follow: true`, so `defaultFollow: false` was discarded here as well as
/// everywhere else.
#[test]
fn a_batch_log_spec_honours_default_follow() {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod_row("a", "ns", 1), pod_row("b", "ns", 1)],
        }),
    );
    for name in ["a", "b"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    assert!(
        batch_log_spec(&app).expect("two pods in one namespace").follow,
        "the shipped default follows",
    );

    app.config.ui.logs.default_follow = false;
    assert!(
        !batch_log_spec(&app).expect("two pods in one namespace").follow,
        "and a user who turned it off gets a spec that does not follow",
    );
}

// ---------------------------------------------------------------------------
// One answer to "what namespace does this shell use"
// ---------------------------------------------------------------------------

fn pod_ref(name: &str, ns: k9rs_proto::protocol::Namespace) -> k9rs_proto::protocol::ObjectRef {
    k9rs_proto::protocol::ObjectRef::new(
        k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
        name.to_string(),
        ns,
    )
}

/// A named namespace is the answer; `All` and empty are refused BY NAME.
///
/// This was three call sites with three arities. The picker's was
/// `namespace.display().to_string()` with no guard at all — and
/// `Namespace::All` displays as the literal `"all"`, so `All` would have
/// become a namespace NAME, reintroducing by stringification the magic
/// string the derived path's comment brags about having removed.
#[test]
fn an_exec_namespace_is_a_real_name_or_a_refusal() {
    use k9rs_proto::protocol::Namespace;
    assert_eq!(
        exec_namespace(&pod_ref("api", Namespace::Named("prod".into()))),
        Ok("prod".to_string()),
    );

    let err = exec_namespace(&pod_ref("api", Namespace::All)).expect_err("All is not a namespace");
    assert!(err.contains("pods/api"), "the refusal names the target: {err}");
    assert!(!err.contains("\"all\""), "and does not treat `all` as a name: {err}");

    assert!(exec_namespace(&pod_ref("api", Namespace::Named(String::new()))).is_err());
}

/// A namespace literally CALLED "all" still works — the old magic-string
/// guard (`ns != "all"`) wrongly rejected it, which is why the typed
/// `Namespace` exists.
#[test]
fn a_namespace_named_all_is_a_real_namespace() {
    use k9rs_proto::protocol::Namespace;
    assert_eq!(
        exec_namespace(&pod_ref("api", Namespace::Named("all".into()))),
        Ok("all".to_string()),
    );
}

/// Both batch projections read ONE intersection, so they cannot disagree
/// about which rows are marked-and-present.
///
/// `batch_log_spec` had its own `with_read` closure over the same rows and
/// the same `marked()`, under a copy of `marked_rows`'s doc paragraph — and
/// without its `has_marks` short-circuit. The Ctrl-R window, which both docs
/// call load-bearing, had to be got right twice.
#[test]
fn the_two_batch_projections_read_one_marked_set() {
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into()],
            rows: vec![pod_row("a", "ns", 1), pod_row("b", "ns", 1), pod_row("c", "ns", 1)],
        }),
    );
    for name in ["a", "c"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    let _ = &mut app;

    let rows = marked_rows(&app);
    let refs = get_marked_resource_infos(&app);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        refs.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        "same rows, same order — store order, which both rely on",
    );
}

/// With no marks, BOTH are empty — the short-circuit the inline copy
/// lacked, and the honest answer during the Ctrl-R window.
#[test]
fn an_unmarked_view_yields_no_batch_targets() {
    let app = App::new_for_test();
    assert!(marked_rows(&app).is_empty());
    assert!(get_marked_resource_infos(&app).is_empty());
}

// ---------------------------------------------------------------------------
// Saved-dump filenames
// ---------------------------------------------------------------------------

/// The parts are sanitized and the stamp is appended — one builder for the
/// three sites that each had their own copy of the closure, and three
/// copies of the strftime format.
#[test]
fn a_dump_filename_sanitizes_its_parts() {
    let name = dump_filename(&["logs", "pods/api-7d9f", "web:8080"], "log");
    assert!(name.starts_with("logs-pods-api-7d9f-web-8080-"), "got {name}");
    assert!(name.ends_with(".log"), "got {name}");
    assert!(
        name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.'),
        "nothing a path can mean survives: {name}",
    );
}

/// The stamp is there and is a UTC `YYYYmmdd-HHMMSS`, so two saves in one
/// session do not collide and the file sorts by time.
#[test]
fn a_dump_filename_carries_a_sortable_stamp() {
    let name = dump_filename(&["logs"], "log");
    let stamp = name
        .strip_prefix("logs-")
        .and_then(|r| r.strip_suffix(".log"))
        .expect("the stamp is between the parts and the extension");
    let (date, time) = stamp.split_once('-').expect("date-time");
    assert_eq!(date.len(), 8, "YYYYmmdd: {stamp}");
    assert_eq!(time.len(), 6, "HHMMSS: {stamp}");
    assert!(date.chars().all(|c| c.is_ascii_digit()), "{stamp}");
    assert!(time.chars().all(|c| c.is_ascii_digit()), "{stamp}");
}

/// An empty part list is still a valid filename — the stamp alone.
#[test]
fn a_dump_filename_with_no_parts_is_still_a_name() {
    let name = dump_filename(&[], "txt");
    assert!(name.ends_with(".txt"));
    assert!(!name.starts_with('.'), "not a hidden file: {name}");
}
