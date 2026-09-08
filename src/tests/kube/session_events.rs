use super::*;
use crate::app::element::{ContentPhase, ContentSpec, ContentView, Element};
use crate::app::App;
use crate::event::ResourceUpdate;
use crate::kube::protocol::{Namespace, ObjectRef};

fn pod_target() -> ObjectRef {
    ObjectRef::new(
        crate::kube::protocol::ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        "web",
        Namespace::Named("ns".to_string()),
    )
}

#[test]
fn content_delivery_reaches_covered_views() {
    // The 2026-08 audit wedge: a yaml/describe view pushed (Fetching),
    // covered by another element before the response lands, then popped —
    // the old deliver-to-top-only rule dropped the response and the
    // revealed view spun forever. Delivery now routes by content identity
    // anywhere in the stack, so the covered view fills while hidden.
    let mut app = App::new_for_test();
    let target = pod_target();
    app.nav.push(Element::ContentView(ContentView::new(
        ContentSpec::Yaml(target.clone()),
        crate::app::ContentViewState::default(),
        ContentPhase::Fetching,
    )));
    app.nav.push(Element::ContentView(ContentView::new(
        ContentSpec::Aliases,
        crate::app::ContentViewState::default(),
        ContentPhase::Ready,
    )));

    apply_event(
        &mut app,
        AppEvent::ResourceUpdate(ResourceUpdate::Yaml {
            target: target.clone(),
            content: "kind: Pod".to_string(),
        }),
    );

    app.nav.pop();
    let Element::ContentView(cv) = app.nav.top() else { panic!("content view expected") };
    assert_eq!(cv.phase, ContentPhase::Ready);
    assert_eq!(cv.state.content, "kind: Pod");
    // The in-flight fetch cache-writes exactly as a top delivery would.
    assert!(app.kube.kubectl_cache.get(&target, crate::app::ContentKind::Yaml).is_some());
}

#[test]
fn decoded_secret_delivery_never_touches_the_describe_cache() {
    // DecodedSecret rides the same wire event as Describe but must not
    // poison the target's describe cache — the distinct spec kind is what
    // makes that unrepresentable.
    let mut app = App::new_for_test();
    let target = pod_target();
    app.nav.push(Element::ContentView(ContentView::new(
        ContentSpec::DecodedSecret(target.clone()),
        crate::app::ContentViewState::default(),
        ContentPhase::Fetching,
    )));

    apply_event(
        &mut app,
        AppEvent::ResourceUpdate(ResourceUpdate::Describe {
            target: target.clone(),
            lines: vec![crate::kube::protocol::DescribeLine {
                text: "password: hunter2".to_string(),
                kind: crate::kube::protocol::DescribeLineKind::Plain,
            }],
        }),
    );

    let Element::ContentView(cv) = app.nav.top() else { panic!("content view expected") };
    assert_eq!(cv.phase, ContentPhase::Ready, "decoded content delivered");
    assert!(
        app.kube.kubectl_cache.get_describe_lines(&target).is_none(),
        "decoded secret bytes must never become the cached describe text"
    );
}

#[test]
fn orphaned_fetch_fails_instead_of_spinning() {
    // The session-rebuild half of the wedge: a Fetching view — top OR
    // covered — whose session died flips to Failed via the choke-point
    // walk, so it renders an honest error instead of an eternal spinner.
    let mut app = App::new_for_test();
    app.nav.push(Element::ContentView(ContentView::new(
        ContentSpec::Describe(pod_target()),
        crate::app::ContentViewState::default(),
        ContentPhase::Fetching,
    )));
    app.nav.push(Element::ContentView(ContentView::new(
        ContentSpec::Aliases,
        crate::app::ContentViewState::default(),
        ContentPhase::Ready,
    )));

    // The same walk + predicate the session.rs choke point runs.
    app.nav.for_each_content_view(|cv| {
        if cv.phase == ContentPhase::Fetching {
            cv.phase = ContentPhase::Failed("fetch interrupted by reconnect".to_string());
        }
    });

    let Element::ContentView(top) = app.nav.top() else { panic!("content view expected") };
    assert_eq!(top.phase, ContentPhase::Ready, "settled views stay untouched");
    app.nav.pop();
    let Element::ContentView(cv) = app.nav.top() else { panic!("content view expected") };
    assert!(matches!(cv.phase, ContentPhase::Failed(_)));
}

// ---------------------------------------------------------------------------
// No usable context: the TUI stays up and hands the user the picker
// ---------------------------------------------------------------------------

#[test]
fn no_context_lands_in_the_picker_and_stops_trying() {
    // `kubectl config unset current-context` writes `current-context: ""`,
    // which used to travel all the way to the daemon as a context named ""
    // and kill the TUI with "Failed to read Ready: early eof". Nothing here
    // failed — there is just no target — so the app must stay up, show what
    // it CAN (the contexts it read from disk), and stop attempting.
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::KubeconfigLoaded {
        contexts: vec![crate::app::KubeContext {
            name: crate::kube::protocol::ContextName::new("prod").unwrap(),
            identity: crate::kube::protocol::ClusterIdentity::default(),
            is_current: false,
        }],
        current_context: None,
        current_identity: crate::kube::protocol::ClusterIdentity::default(),
    });
    apply_event(&mut app, AppEvent::NoContextConfigured);

    assert_eq!(
        app.nav.top().rid(),
        Some(&crate::kube::local::LocalResourceKind::Context.to_resource_id()),
        "the picker is the whole UI when there is nothing to connect to",
    );
    assert_eq!(
        app.nav.depth(), 1,
        "…and it is the ROOT: everything below belonged to a context we don't have",
    );
    assert!(
        app.nav.pop().is_none(),
        "Esc must not escape the picker — there is nowhere to go back to",
    );
    assert_eq!(app.conn.link(), crate::app::LinkState::NoContext);
    assert!(
        !app.conn.rebuild_due(std::time::Instant::now()),
        "retrying against a target that doesn't exist is just a loop",
    );
    assert!(app.kube.context.is_none(), "no context is confirmed");
    let flash = app.ui.flash.as_ref().expect("the user is told why");
    assert!(flash.message.contains("--context"), "…and how to skip the picker");
}

#[test]
fn the_picker_is_never_stacked() {
    // A failed pick falls back to "no context", which re-enters this path.
    // Reset (not push) makes repetition idempotent by construction.
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::NoContextConfigured);
    apply_event(&mut app, AppEvent::NoContextConfigured);
    assert_eq!(app.nav.depth(), 1, "the picker is entered, not restacked");
}

#[test]
fn the_contexts_view_is_an_ordinary_table() {
    // The point of the refactor. `/` used to reach the action layer and die
    // on `filter_input_mut() == None`, because the contexts view had opted
    // out of every framework accessor — and so had sort, column filter,
    // marks and column movement, all silently. Being a real table element
    // is what makes them work; none of them is wired up by name.
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::KubeconfigLoaded {
        contexts: vec!["prod", "staging"]
            .into_iter()
            .map(|n| crate::app::KubeContext {
                name: crate::kube::protocol::ContextName::new(n).unwrap(),
                identity: crate::kube::protocol::ClusterIdentity::default(),
                is_current: false,
            })
            .collect(),
        current_context: None,
        current_identity: crate::kube::protocol::ClusterIdentity::default(),
    });
    apply_event(&mut app, AppEvent::NoContextConfigured);

    let top = app.nav.top_mut();
    assert!(top.table_interaction().is_some(), "it has a cursor like any table");
    assert!(top.filter_input_mut().is_some(), "…and `/` has somewhere to go");
    assert!(top.data_store().is_some(), "…and rows in a real store");

    // The rows really are there, and Enter carries the switch on the row.
    let store = top.data_store().expect("store").clone();
    store.with_read(|i| {
        assert_eq!(i.rows.len(), 2);
        assert_eq!(i.headers, crate::app::context_headers());
        assert!(matches!(
            i.rows[0].drill_target,
            Some(crate::kube::resources::row::DrillTarget::SwitchContext(_)),
        ), "Enter is row-carried, not a key-handler special case");
    });
}



#[test]
fn switching_away_and_back_never_locks_the_switch_state() {
    // The 2026-07-30 context-lock class: any switch edge that fails to
    // settle refuses every LATER `:ctx` with "switch already in progress".
    // Re-entering a context you were previously on is the cycle most likely
    // to expose one, so walk it explicitly: A → B → A.
    use crate::app::ContextSwitchState;
    use crate::kube::protocol::{ClusterIdentity, ContextName};
    let a = ContextName::new("ctx-a").unwrap();
    let b = ContextName::new("ctx-b").unwrap();

    let mut app = App::new_for_test();
    let established = |app: &mut App, name: &ContextName| {
        // Mirrors session.rs: settle only from InFlight, then apply.
        let was_switch = matches!(app.kube.context_switch, ContextSwitchState::InFlight(_));
        apply_event(app, AppEvent::ConnectionEstablished {
            context: name.clone(),
            identity: ClusterIdentity::default(),
            namespaces: vec![],
        });
        if was_switch {
            app.kube.context_switch.settle();
        }
    };

    established(&mut app, &a);
    for target in [&b, &a, &b, &a] {
        assert!(
            app.kube.context_switch.is_stable(),
            "a switch to {target} must not be refused by a stranded predecessor",
        );
        app.kube.context_switch = ContextSwitchState::Requested(target.clone());
        app.conn.switch_requested();
        assert_eq!(
            app.kube.context_switch.take_requested().as_ref(),
            Some(target),
            "the main loop takes the request",
        );
        established(&mut app, target);
    }
    assert_eq!(app.kube.context, Some(a.clone()), "back on the context we started from");
    assert!(app.kube.context_switch.is_stable());
}

#[test]
fn a_column_can_cap_its_own_width() {
    // The contexts view is the motivating case: on EKS, CLUSTER and USER are
    // full ARNs, and one global `maxColumnWidth` for every column of every
    // resource let them push NAME and ACTIVE off the screen. The ceiling is
    // declared next to the column, so it travels with it.
    let mut app = App::new_for_test();
    let long = "arn:aws:eks:us-west-2:123456789012:cluster/prod-eks-cluster-name";
    apply_event(&mut app, AppEvent::KubeconfigLoaded {
        contexts: vec![crate::app::KubeContext {
            name: crate::kube::protocol::ContextName::new("prod").unwrap(),
            identity: crate::kube::protocol::ClusterIdentity::new(long.into(), long.into()),
            is_current: true,
        }],
        current_context: None,
        current_identity: crate::kube::protocol::ClusterIdentity::default(),
    });
    apply_event(&mut app, AppEvent::NoContextConfigured);

    // A generous global cap: anything narrower than it came from the column.
    let view = app.nav.top_mut().view(crate::app::ColumnLevel::Default, 200);
    let by_header: std::collections::HashMap<&str, u16> = view
        .headers
        .iter()
        .map(String::as_str)
        .zip(view.col_widths.iter().copied())
        .collect();
    assert!(by_header["CLUSTER"] <= 28, "CLUSTER capped, got {}", by_header["CLUSTER"]);
    assert!(by_header["USER"] <= 24, "USER capped, got {}", by_header["USER"]);
    // …and a short column is never padded out to its ceiling.
    assert!(by_header["ACTIVE"] <= 10, "ACTIVE stays narrow, got {}", by_header["ACTIVE"]);
}

#[test]
fn every_context_switch_lands_on_home_immediately() {
    // A switch is a ROOT-level change: the stack describes a cluster you
    // just asked to leave. Enter-on-a-context-row used to do this and a
    // typed `:ctx <name>` did not, so the same action felt different
    // depending on how you spelled it.
    use crate::kube::protocol::ContextName;
    for start_on_picker in [true, false] {
        let mut app = App::new_for_test();
        if start_on_picker {
            apply_event(&mut app, AppEvent::NoContextConfigured);
        } else {
            // A deep-ish stack belonging to the outgoing context.
            assert!(app.nav.depth() >= 1);
        }
        crate::kube::session_actions::begin_context_switch(
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
fn returning_to_a_context_shows_its_rows_at_once_but_marked_stale() {
    // The daemon keeps its watchers warm for a minute; the client used to
    // throw its own rows away on every switch, so A→B→A walked
    // Initializing→Loading→baseline from zero with the answer already sitting
    // in the daemon. Rows come back immediately now — and NOT as `Ready`,
    // because nothing is feeding them until the fresh baseline lands. That is
    // the line between a cache and the stale-rows-painted-as-live bug.
    use crate::kube::resource_def::BuiltInKind;
    use crate::kube::protocol::ContextName;
    let (a, b) = (
        ContextName::new("ctx-a").unwrap(),
        ContextName::new("ctx-b").unwrap(),
    );
    let ns_rows = |n: &str| {
        vec![crate::kube::resources::row::ResourceRow {
            name: n.into(),
            cells: vec![crate::kube::resources::row::CellValue::Text(n.to_string())],
            ..Default::default()
        }]
    };

    let mut app = App::new_for_test();
    app.kube.context = Some(a.clone());
    app.core.seed(BuiltInKind::Namespace, ns_rows("a-only-namespace"));

    // A → B: a-namespaces are parked, B starts cold.
    app.core.switch_context(Some(a.clone()), &b);
    app.core.namespaces.with_read(|i| {
        assert!(i.rows.is_empty(), "B does not inherit A's rows");
        assert_eq!(i.state, crate::app::TableDataState::Initializing);
    });

    // B → A: A's rows are back, immediately, and honestly labelled.
    app.core.switch_context(Some(b.clone()), &a);
    app.core.namespaces.with_read(|i| {
        assert_eq!(i.rows.len(), 1, "A's rows come back without a round trip");
        assert_eq!(i.rows[0].name, "a-only-namespace");
        assert!(
            matches!(i.state, crate::app::TableDataState::Stale(_)),
            "resident but not being fed — never Ready, got {:?}",
            i.state,
        );
    });
    // …and the live baseline clears the mark rather than needing an all-clear.
    app.core.seed(BuiltInKind::Namespace, ns_rows("a-only-namespace"));
    app.core
        .namespaces
        .with_read(|i| assert_eq!(i.state, crate::app::TableDataState::Ready));
}

#[test]
fn only_the_previous_context_is_parked() {
    // One slot, not a map: A→B→C must not still be pinning A's rows.
    use crate::kube::resource_def::BuiltInKind;
    use crate::kube::protocol::ContextName;
    let names: Vec<ContextName> = ["a", "b", "c"]
        .iter()
        .map(|n| ContextName::new(*n).unwrap())
        .collect();
    let mut app = App::new_for_test();
    app.core.seed(BuiltInKind::Namespace, vec![crate::kube::resources::row::ResourceRow {
        name: "a-only-namespace".into(),
        ..Default::default()
    }]);
    app.core.switch_context(Some(names[0].clone()), &names[1]);
    app.core.switch_context(Some(names[1].clone()), &names[2]);
    // Back to A: its slot was evicted by B, so this is a cold load.
    app.core.switch_context(Some(names[2].clone()), &names[0]);
    app.core.namespaces.with_read(|i| {
        assert!(i.rows.is_empty(), "A was evicted when B took the slot");
    });
}
