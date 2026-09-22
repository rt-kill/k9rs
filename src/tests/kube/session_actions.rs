//! What lands on the clipboard when rows are marked.

use super::*;
use crate::app::store::StorePayload;
use crate::kube::protocol::{ObjectKey, TableBaseline};
use crate::kube::resources::row::{CellValue, ResourceRow};

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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
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

use crate::kube::protocol::{LogContainer, LogTarget};
use crate::kube::resources::row::{ContainerInfo, DrillTarget};

fn pod_row(name: &str, ns: &str, containers: usize) -> ResourceRow {
    ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some(ns.into()),
        containers: (0..containers)
            .map(|i| ContainerInfo { health: Default::default(),
                name: format!("c{i}"),
                kind: crate::kube::resources::row::ContainerKind::Regular,
                image: String::new(),
                state: crate::kube::resources::row::ContainerState::default(),
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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![row],
        }),
    );
    app.nav.top_mut().ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    app
}

fn selected_ref(app: &App) -> crate::kube::protocol::ObjectRef {
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
    assert_eq!(target, LogTarget::Selector { labels, label: "pods/my-app".into() });
    assert!(target.is_fanout(), "a selector can name more than one pod");
}

/// Jobs and replicasets identify their pods by ownerReference, not by a
/// selector — same fan-out, different question for the daemon to ask.
#[test]
fn an_owner_drilled_workload_logs_its_owned_pods() {
    let mut row = pod_row("my-job", "ns", 0);
    row.drill_target = Some(DrillTarget::PodsByOwner {
        uid: "uid-1".into(),
        kind: crate::kube::resource_def::BuiltInKind::Job,
        name: "my-job".into(),
    });
    let app = app_with_row(row);

    assert_eq!(
        workload_log_target(&app, &selected_ref(&app)),
        LogTarget::Owner { uid: "uid-1".into(), label: "pods/my-job".into() },
    );
}

/// The shapes nothing can resolve to a pod set (a cronjob owns jobs, a
/// name-grep isn't a selector) keep the pre-fan-out behaviour: hand the
/// kubectl-style ref over and let kubectl resolve or refuse it.
#[test]
fn an_unresolvable_workload_falls_back_to_the_kubectl_ref() {
    let app = app_with_row(pod_row("mystery", "ns", 0));
    let target = workload_log_target(&app, &selected_ref(&app));
    assert_eq!(target, LogTarget::One("pods/mystery".into()));
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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![pod_row("a", "ns", 1), pod_row("b", "ns", 1), pod_row("c", "ns", 1)],
        }),
    );
    for name in ["a", "c"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    let _ = &mut app;

    let spec = batch_log_spec(&app, 100).expect("three single-container pods in one namespace");
    assert_eq!(
        spec.target,
        LogTarget::Union {
            targets: vec![LogTarget::One("a".into()), LogTarget::One("c".into())],
            label: "2 pods".into(),
        },
    );
    assert_eq!(spec.namespace, crate::kube::protocol::Namespace::Named("ns".into()));
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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![pod_row("solo", "ns", 1), pod_row("sidecar", "ns", 2)],
        }),
    );
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "solo".to_string()));
    assert_eq!(batch_log_spec(&app, 100).unwrap().container, LogContainer::Default);

    store.toggle_mark(&ObjectKey::new("ns".to_string(), "sidecar".to_string()));
    assert_eq!(batch_log_spec(&app, 100).unwrap().container, LogContainer::All);
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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Deployment),
            headers: vec!["NAME".into()],
            rows: vec![workload("api", "api"), workload("web", "web")],
        }),
    );
    for name in ["api", "web"] {
        store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string()));
    }
    let _ = &mut app;

    let spec = batch_log_spec(&app, 100).expect("two workloads in one namespace");
    let LogTarget::Union { targets, .. } = &spec.target else {
        panic!("a selection is a union, got {:?}", spec.target)
    };
    assert_eq!(targets.len(), 2);
    assert!(
        targets.iter().all(|t| matches!(t, LogTarget::Selector { .. })),
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
    let rid = crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Deployment);

    // A pod: it lists containers, so it is its own target.
    let pod = pod_row("api-7d9f", "ns", 1);
    assert_eq!(row_log_target(&pod, &rid), Some(LogTarget::One("api-7d9f".into())));

    // A job: no containers, pods found by ownerReference.
    let mut job = pod_row("nightly", "ns", 0);
    job.drill_target = Some(DrillTarget::PodsByOwner {
        uid: "uid-9".into(),
        kind: crate::kube::resource_def::BuiltInKind::Job,
        name: "nightly".into(),
    });
    assert_eq!(
        row_log_target(&job, &rid),
        Some(LogTarget::Owner { uid: "uid-9".into(), label: "deployments/nightly".into() }),
    );

    // A row with no logs at all earns no place in the union.
    let ns_rid = crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Namespace);
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
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![pod_row("here", "ns-a", 1), pod_row("there", "ns-b", 1)],
        }),
    );
    store.toggle_mark(&ObjectKey::new("ns-a".to_string(), "here".to_string()));
    store.toggle_mark(&ObjectKey::new("ns-b".to_string(), "there".to_string()));
    let err = batch_log_spec(&app, 100).expect_err("two namespaces, one stream");
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
        LogTarget::Selector { labels, label: "deploy/api".into() }.display(),
        "deploy/api",
    );
    assert_eq!(
        LogTarget::Union { targets: vec![], label: "2 pods".into() }.display(),
        "2 pods",
    );
    assert_eq!(LogTarget::One("pod-x".into()).display(), "pod-x");
}

/// A multi-container pod's logs used to be unreachable as a WHOLE:
/// `LogContainer::All` existed on the wire, but every UI path that produced
/// it was for a row with NO containers, so "logs of this pod" always meant
/// one container. The log picker now leads with it.
#[test]
fn the_log_picker_offers_every_container_at_once() {
    use crate::app::{ContainerAction, ContainerChoice};
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
    use crate::app::{ContainerAction, ContainerChoice};
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
    use crate::app::element::Element;
    use crate::app::view::DerivedViewKind;

    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    let pod = pod_row("web-0", "ns", 3); // containers c0, c1, c2
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
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

    let spec = batch_log_spec(&app, 100).expect("two marked containers of one pod");
    assert_eq!(spec.target, LogTarget::One("web-0".into()), "one pod, named once");
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
    use crate::kube::resources::row::K8sFieldSelector;
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
    use crate::kube::resources::row::K8sFieldSelector;
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
