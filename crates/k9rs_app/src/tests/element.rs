use super::*;
use crate::store::StorePayload;
use k9rs_proto::protocol::{RowChange, TableBaseline, TableDelta};
use k9rs_proto::resource_def::BuiltInKind;
use k9rs_proto::row::{CellValue, ContainerInfo};

fn pod_rid() -> ResourceId {
    ResourceId::BuiltIn(BuiltInKind::Pod)
}

fn row(name: &str, ns: &str, cells: &[&str]) -> ResourceRow {
    ResourceRow {
        name: name.into(),
        namespace: Some(ns.into()),
        cells: cells.iter().map(|c| CellValue::Text((*c).to_string())).collect(),
        ..Default::default()
    }
}

fn list_element(namespace: Namespace) -> Element {
    Element::ResourceList(ResourceList::open_for_test(
        QuerySpec { rid: pod_rid(), namespace, filter: None },
        &MetricsHub::new(),
        "pods".to_string(),
    ))
}

fn seed(el: &Element, headers: &[&str], rows: Vec<ResourceRow>) {
    el.data_store().expect("table element").apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: pod_rid(),
            headers: headers.iter().map(|h| (*h).to_string()).collect(),
            rows,
        }),
    );
}

#[test]
fn element_owns_its_namespace_column_decision() {
    // The flagship regression: an all-namespaces element SHOWS the
    // NAMESPACE column, a named-namespace element hides it — decided
    // at construction, no ambient selector anywhere to consult.
    let headers = ["NAMESPACE", "NAME", "STATUS"];
    let rows = vec![row("a", "ns1", &["ns1", "a", "ok"])];

    let mut all = list_element(Namespace::All);
    seed(&all, &headers, rows.clone());
    let view = all.view(ColumnLevel::Default, 40);
    assert!(view.headers.iter().any(|h| h == "NAMESPACE"));

    let mut named = list_element(Namespace::Named("ns1".to_string()));
    seed(&named, &headers, rows);
    let view = named.view(ColumnLevel::Default, 40);
    assert!(!view.headers.iter().any(|h| h == "NAMESPACE"));
    assert_eq!(named.scope_label(), "ns1");
    assert_eq!(all.scope_label(), "");
}

#[test]
fn view_is_memoized_and_invalidated_by_data_draft_and_sort() {
    let mut el = list_element(Namespace::All);
    seed(&el, &["NAME"], vec![row("b", "ns", &["b"]), row("a", "ns", &["a"])]);

    let v1 = el.view(ColumnLevel::Default, 40);
    let v2 = el.view(ColumnLevel::Default, 40);
    assert!(Arc::ptr_eq(&v1, &v2), "same inputs must hit the memo");

    // Data change invalidates.
    el.data_store().unwrap().apply(
        1,
        StorePayload::Delta(TableDelta {
            changes: vec![RowChange::Upsert(row("c", "ns", &["c"]))],
        }),
    );
    let v3 = el.view(ColumnLevel::Default, 40);
    assert!(!Arc::ptr_eq(&v2, &v3));
    assert_eq!(v3.total_rows, 3);

    // Draft change invalidates (the draft is a derive input — no
    // explicit re-filter call exists anywhere).
    el.filter_input_mut().unwrap().start();
    el.filter_input_mut().unwrap().input_mut().insert('a');
    let v4 = el.view(ColumnLevel::Default, 40);
    assert_eq!(v4.keys.len(), 1);
    assert_eq!(v4.rows[0][0], "a");

    // Sort change invalidates.
    el.filter_input_mut().unwrap().cancel();
    // Default sort is already (col 0, ascending) — one call toggles
    // to descending.
    el.sort_by(crate::SortTarget::Column(0));
    let v5 = el.view(ColumnLevel::Default, 40);
    assert_eq!(v5.rows[0][0], "c");
}

#[test]
fn derive_filter_chains_and_shares_the_store() {
    let mut root = list_element(Namespace::All);
    seed(
        &root,
        &["NAME"],
        vec![
            row("web-api", "ns", &["web-api"]),
            row("web-cache", "ns", &["web-cache"]),
            row("db-api", "ns", &["db-api"]),
        ],
    );
    let _ = root.view(ColumnLevel::Default, 40);

    let mut first = Element::derive_filter(
        &root,
        RowPredicate::Grep(CompiledGrep::new("web")),
    )
    .expect("root has row output");
    assert_eq!(first.label(), "/web");
    // Same store, narrowed chain.
    assert!(Arc::ptr_eq(
        root.data_store().unwrap(),
        first.data_store().unwrap()
    ));
    let v = first.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 2);

    let mut second = Element::derive_filter(
        &first,
        RowPredicate::Grep(CompiledGrep::new("api")),
    )
    .expect("filters compose");
    let v = second.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 1);
    assert_eq!(v.rows[0][0], "web-api");
    // The fault helpers see through the chain.
    assert!(!second.is_fault_filter());
}

#[test]
fn fault_filter_identity() {
    let mut root = list_element(Namespace::All);
    seed(&root, &["NAME"], vec![row("a", "ns", &["a"])]);
    let _ = root.view(ColumnLevel::Default, 40);
    let fault = Element::derive_filter(&root, RowPredicate::Fault).unwrap();
    assert!(fault.is_fault_filter());
    assert_eq!(fault.label(), "⚠ fault");
    // A grep on top of the fault is NOT itself a fault filter.
    let grep = Element::derive_filter(&fault, RowPredicate::Grep(CompiledGrep::new("x"))).unwrap();
    assert!(!grep.is_fault_filter());
}

#[test]
fn derived_projection_is_live() {
    let mut root = list_element(Namespace::All);
    let mut pod = row("web-1", "ns", &["web-1", "Running"]);
    pod.containers = vec![ContainerInfo { health: Default::default(),
        name: "api".into(),
        image: "img:1".into(),
        state: k9rs_proto::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
        kind: k9rs_proto::row::ContainerKind::Regular,
    }];
    seed(&root, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = root.view(ColumnLevel::Default, 40);
    root.select(0);

    let mut containers =
        Element::derive_projection(&root, &pod, DerivedViewKind::Containers).unwrap();
    assert_eq!(containers.label(), "containers(web-1)");
    let v = containers.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 1);

    // LIVE: the parent row's container status changes → the projection
    // re-derives from the parent's store (no frozen snapshot).
    let mut updated = pod.clone();
    updated.containers[0].restart_count = 3;
    root.data_store().unwrap().apply(
        1,
        StorePayload::Delta(TableDelta { changes: vec![RowChange::Upsert(updated)] }),
    );
    let v = containers.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 1);
    let restarts_col = v.headers.iter().position(|h| h == "RESTARTS").unwrap();
    assert_eq!(v.rows[0][restarts_col], "3");

    // Row gone → honest empty view.
    containers.data_store().unwrap().apply(
        1,
        StorePayload::Delta(TableDelta {
            changes: vec![RowChange::Remove(crate::store::row_key(&pod))],
        }),
    );
    let v = containers.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 0);
}

#[test]
fn log_filters_chain_and_own_their_lines() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    let mut session = Element::LogSession(Box::new(LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()), LogContainer::Default,
    )));
    let store = Arc::clone(session.log_store().unwrap());
    for i in 0..10 {
        store.push(1, LogLine { timestamp: None, content: format!("line-{i} {}", if i % 2 == 0 { "err" } else { "ok" }), source: None });
    }
    assert_eq!(session.log_visible().unwrap().len(), 10);
    assert_eq!(session.log_header().unwrap().0, "pod-x");

    let mut filter =
        Element::derive_log_filter(&session, CompiledGrep::new("err"), crate::element::Sense::Keep).unwrap();
    assert_eq!(filter.label(), "/err");
    assert_eq!(filter.log_committed_patterns(), vec!["err".to_string()]);
    assert_eq!(filter.log_visible().unwrap().len(), 5);
    // Draft narrows further (a derive input — no rebuild call).
    filter.log_view_mut().unwrap().draft = Some(crate::text_input::TextInput::seeded("line-2"));
    assert_eq!(filter.log_visible().unwrap().len(), 1);
    filter.log_view_mut().unwrap().draft = None;

    // Deriving the visible set must not touch the viewport at all. It used
    // to shift it up by the PARENT's eviction count — a logical line count
    // applied to a physical row offset over a DIFFERENT line set — so a
    // parent evicting 100 lines of which one matched yanked the filtered view
    // a hundred rows. Park it, evict the parent hard, and it must not move.
    {
        let vp = &mut filter.log_view_mut().unwrap().viewport;
        vp.set_metrics(100, 10, false); // give it scroll headroom
        vp.scroll_to(4);
    }
    let cap = 50_000; // default LogConfig max_lines
    for i in 0..(cap - 10 + 3) {
        store.push(1, LogLine { content: format!("fill-{i}"), source: None, timestamp: None });
    }
    let _ = filter.log_visible();
    assert_eq!(
        filter.log_view().unwrap().viewport.offset(),
        4,
        "the parent's evictions are not this view's evictions",
    );

    // And the matches themselves SURVIVE the parent's eviction — the whole
    // point. The parent's ring has long since dropped `line-0 err`; the
    // filter kept it because it owns its lines.
    let kept = filter.log_visible().unwrap();
    assert_eq!(kept.len(), 5, "all five matches retained past a full-ring eviction");
    let (idx, filter_store) = filter.log_lines().unwrap();
    filter_store.with_read(|inner| {
        assert!(
            inner.lines[idx[0]].content.starts_with("line-0"),
            "the OLDEST match is still here, evicted from the parent or not",
        );
    });
    assert!(
        store.with_read(|i| i.lines.iter().all(|l| !l.content.starts_with("line-0"))),
        "…and the parent really did drop it",
    );
}

#[test]
fn span_mark_toggles_at_block_granularity() {
    let mut el = list_element(Namespace::All);
    seed(
        &el,
        &["NAME"],
        (0..8).map(|i| row(&format!("p{i}"), "ns", &[&format!("p{i}")])).collect(),
    );
    let _ = el.view(ColumnLevel::Default, 40);

    // Mark p1, span to p4 → block [1..=4] marked.
    el.select(1);
    el.toggle_mark();
    el.select(4);
    el.span_mark();
    assert_eq!(el.marked_keys().len(), 4);

    // Cursor on a marked row (p2): span-mark UNMARKS the contiguous
    // block containing it — the whole [1..=4] run.
    el.select(2);
    el.span_mark();
    assert!(el.marked_keys().is_empty(), "contiguous block unselected");

    // Two separate blocks: unmarking one leaves the other.
    el.select(0);
    el.toggle_mark(); // p0
    el.select(6);
    el.toggle_mark();
    el.select(7);
    el.span_mark(); // p6..p7
    assert_eq!(el.marked_keys().len(), 3);
    el.select(7);
    el.span_mark(); // unmark the p6..p7 block only
    assert_eq!(el.marked_keys().len(), 1, "p0 survives");
}

#[test]
fn selected_data_col_maps_through_the_rendered_view() {
    // With NAMESPACE hidden (named scope), the cursor's visible index
    // maps to the DATA index through the element's own last view —
    // the `~`/`S` mis-map is unrepresentable.
    let headers = ["NAMESPACE", "NAME", "STATUS"];
    let mut named = list_element(Namespace::Named("ns1".to_string()));
    seed(&named, &headers, vec![row("a", "ns1", &["ns1", "a", "ok"])]);
    let _ = named.view(ColumnLevel::Default, 40);
    named.col_right(); // visible col 1 = STATUS (NAME=0, NAMESPACE hidden)
    let (data_idx, header) = named.selected_data_col().unwrap();
    assert_eq!(header, "STATUS");
    assert_eq!(data_idx, 2, "data index counts the hidden NAMESPACE column");
}

/// A cursor carried from a longer view (child seeded from a deep
/// parent position, or data that shrank) re-anchors to the position
/// the user SEES on the first move — one `k` from the (clamped)
/// bottom row moves up one row, instead of burning dozens of
/// invisible keypresses re-entering range.
#[test]
fn moves_normalize_an_out_of_range_cursor() {
    let mut el = list_element(Namespace::All);
    seed(
        &el,
        &["NAME"],
        (0..5).map(|i| row(&format!("p{i}"), "ns", &[&format!("p{i}")])).collect(),
    );
    let _ = el.view(ColumnLevel::Default, 40);

    // Simulate the seeded-from-parent case: raw cursor way past the
    // 5-row view (display clamps to row 4).
    el.table_interaction_mut().unwrap().selected = 50;
    el.select_prev();
    assert_eq!(
        el.table_interaction().unwrap().selected, 3,
        "one PrevItem from the visible bottom row lands on row 3",
    );

    el.table_interaction_mut().unwrap().selected = 50;
    el.select_next();
    assert_eq!(
        el.table_interaction().unwrap().selected, 4,
        "NextItem from past-the-end normalizes to the last row",
    );

    el.table_interaction_mut().unwrap().selected = 50;
    el.page_up();
    assert_eq!(el.table_interaction().unwrap().selected, 0);

    // Empty view: moves are no-ops and the cursor survives untouched
    // (a transient refresh window must not zero the position).
    let mut empty = list_element(Namespace::All);
    seed(&empty, &["NAME"], vec![]);
    let _ = empty.view(ColumnLevel::Default, 40);
    empty.table_interaction_mut().unwrap().selected = 7;
    empty.select_prev();
    empty.select_next();
    assert_eq!(empty.table_interaction().unwrap().selected, 7);
}

/// Span with NO visible anchor refuses (it used to anchor at row 0
/// and silently bulk-mark from the top of the view).
#[test]
fn span_mark_without_visible_anchor_is_refused() {
    let mut el = list_element(Namespace::All);
    seed(
        &el,
        &["NAME"],
        (0..4).map(|i| row(&format!("p{i}"), "ns", &[&format!("p{i}")])).collect(),
    );
    let _ = el.view(ColumnLevel::Default, 40);
    el.select(2);
    assert_eq!(el.span_mark(), SpanOutcome::NoAnchor);
    assert!(el.marked_keys().is_empty(), "nothing was marked");

    // With an anchor it applies as before.
    el.select(0);
    assert_eq!(el.toggle_mark(), MarkOutcome::Toggled);
    el.select(2);
    assert_eq!(el.span_mark(), SpanOutcome::Applied);
    assert_eq!(el.marked_keys().len(), 3);
}

/// A container projection owns its marks, and they stay ITS marks.
///
/// Projected rows have no backing store to key marks in (`data_store()`
/// points at the PARENT's store, where "main" is not a row), so the marks
/// live on the element and die with it. What must never happen is the two
/// sets bleeding into each other in either direction.
#[test]
fn derived_rows_own_their_marks() {
    let mut el = list_element(Namespace::All);
    let mut pod = row("web", "ns", &["web", "Running"]);
    pod.containers = ["main", "sidecar", "proxy"]
        .into_iter()
        .map(|name| ContainerInfo { health: Default::default(),
            name: name.into(),
            kind: Default::default(),
            image: "img".into(),
            state: k9rs_proto::row::ContainerState::Running,
            reason: None,
            ready: true,
            restart_count: 0,
        })
        .collect();
    seed(&el, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = el.view(ColumnLevel::Default, 40);
    let mut derived = Element::derive_projection(&el, &pod, DerivedViewKind::Containers)
        .expect("containers projection");
    let _ = derived.view(ColumnLevel::Default, 40);

    derived.select(0);
    assert_eq!(derived.toggle_mark(), MarkOutcome::Toggled);
    derived.select(2);
    assert_eq!(derived.span_mark(), SpanOutcome::Applied);
    assert_eq!(
        derived.marked_keys().iter().map(|k| k.name.as_str()).collect::<Vec<_>>(),
        ["main", "proxy", "sidecar"],
        "span marks the block, and reads back in VIEW order (sorted), not declaration order",
    );
    assert!(derived.has_marks());

    // Neither set reaches the other: the projection's marks are not in the
    // parent store, and the parent's marks don't put the projection into
    // select mode.
    assert!(!el.data_store().unwrap().has_marks(), "projection marks stay off the store");
    el.data_store().unwrap().toggle_mark(&crate::store::row_key(&pod));
    derived.clear_marks();
    assert!(!derived.has_marks());
    assert!(
        el.data_store().unwrap().has_marks(),
        "projection clear_marks can\'t reach the parent store",
    );
}

/// A projection has no delta to prune a mark with — its rows are re-derived
/// from the parent every frame — so a container that disappears must simply
/// stop counting. Otherwise a pod respec would leave the app in a select
/// mode with nothing marked on screen and no key that could clear it.
#[test]
fn a_vanished_container_stops_counting_as_marked() {
    let mut el = list_element(Namespace::All);
    let mut pod = row("web", "ns", &["web", "Running"]);
    let container = |name: &str| ContainerInfo { health: Default::default(),
        name: name.into(),
        kind: Default::default(),
        image: "img".into(),
        state: k9rs_proto::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
    };
    pod.containers = vec![container("main"), container("sidecar")];
    seed(&el, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = el.view(ColumnLevel::Default, 40);
    let mut derived = Element::derive_projection(&el, &pod, DerivedViewKind::Containers)
        .expect("containers projection");
    let _ = derived.view(ColumnLevel::Default, 40);
    derived.select(1);
    assert_eq!(derived.toggle_mark(), MarkOutcome::Toggled);
    assert_eq!(derived.marked_count(), 1);

    // The sidecar is removed from the parent row the projection derives from.
    let mut shrunk = pod.clone();
    shrunk.containers = vec![container("main")];
    seed(&el, &["NAME", "STATUS"], vec![shrunk]);
    // The derive key carries the store generation, so re-seeding is what
    // invalidates the cached projection.
    let _ = derived.view(ColumnLevel::Default, 40);

    assert_eq!(derived.marked_count(), 0, "the mark has no row behind it any more");
    assert!(!derived.has_marks(), "and so select mode has ended");
}

// ---------------------------------------------------------------------------
// Selection totality: reads resolve WITHOUT a painted view (2026-08 audit —
// the "describe→/" root cause was selection returning None on a cold memo)
// ---------------------------------------------------------------------------

#[test]
fn selection_is_total_without_a_painted_view() {
    // NO view() call anywhere here — the memo stays cold, as it is right
    // after a pop reveals a covered element or a fresh element appears.
    let el = list_element(Namespace::All);
    seed(&el, &["NAME"], vec![row("b", "ns", &["b"]), row("a", "ns", &["a"])]);

    let key = el.selected_key().expect("cold cache must still resolve");
    assert_eq!(key.name, "a", "fallback derives the sorted order (default sort col 0)");
    let picked = el.selected_row().expect("selected_row rides selected_key");
    assert_eq!(picked.name, "a");
}

#[test]
fn cold_selection_matches_what_the_next_paint_shows() {
    let mut el = list_element(Namespace::All);
    seed(
        &el,
        &["NAME"],
        vec![row("c", "ns", &["c"]), row("a", "ns", &["a"]), row("b", "ns", &["b"])],
    );
    let cold = el.selected_key().expect("cold resolve");
    let view = el.view(ColumnLevel::Default, 40);
    assert_eq!(
        Some(&cold),
        view.keys.first(),
        "the fallback order IS the order the paint materializes"
    );
}

#[test]
fn cold_selection_honors_cursor_and_clamps() {
    let mut el = list_element(Namespace::All);
    seed(&el, &["NAME"], vec![row("b", "ns", &["b"]), row("a", "ns", &["a"])]);

    el.table_interaction_mut().unwrap().selected = 1;
    assert_eq!(el.selected_key().unwrap().name, "b");

    // A cursor carried from a longer view clamps, same as the cached path.
    el.table_interaction_mut().unwrap().selected = 99;
    assert_eq!(el.selected_key().unwrap().name, "b");
}

#[test]
fn cold_selection_on_empty_store_is_honestly_none() {
    let el = list_element(Namespace::All);
    seed(&el, &["NAME"], vec![]);
    assert_eq!(el.selected_key(), None, "totality means resolving, not inventing");
}

// ---------------------------------------------------------------------------
// Framework conformance — the pin that would have caught the contexts view
// ---------------------------------------------------------------------------

/// The kind's name, from a match with NO wildcard.
///
/// This is what makes the fixture below exhaustive. A `vec!` literal cannot
/// be exhaustive on its own — and for a long time the fixture's doc comment
/// claimed "the match below has no wildcard, so a new kind fails to compile"
/// when there was no match at all. A seventh `Element` kind would simply have
/// been absent from every conformance check, silently, which is precisely the
/// failure the contexts view taught us to pin against.
///
/// Adding a variant now fails to compile HERE; the coverage assertion then
/// fails until an instance of it is built in `one_of_every_kind`.
fn kind_name(el: &crate::element::Element) -> &'static str {
    use crate::element::Element;
    match el {
        Element::ResourceList(_) => "ResourceList",
        Element::RowFilter(_) => "RowFilter",
        Element::DerivedRows(_) => "DerivedRows",
        Element::LogSession(_) => "LogSession",
        Element::LogFilter(_) => "LogFilter",
        Element::ContentView(_) => "ContentView",
        Element::Overview(_) => "Overview",
        Element::Confirm(_) => "Confirm",
        Element::Picker(_) => "Picker",
        Element::Form(_) => "Form",
    }
}

/// Every name `kind_name` can return. Kept beside it deliberately: the match
/// forces the compile error, this forces an instance to actually exist.
const EVERY_KIND: [&str; 10] = [
    "ResourceList",
    "RowFilter",
    "DerivedRows",
    "LogSession",
    "LogFilter",
    "ContentView",
    "Overview",
    "Confirm",
    "Picker",
    "Form",
];

/// One live instance of EVERY element kind — enforced by
/// [`every_kind_has_an_instance`], not by this comment.
fn one_of_every_kind() -> Vec<crate::element::Element> {
    use crate::element::{ContentPhase, ContentSpec, ContentView, Element, LogSession};
    use crate::store::RowPredicate;
    use k9rs_proto::protocol::{LogContainer, Namespace};

    let hub = std::sync::Arc::new(crate::store::MetricsHub::default());
    let rid = crate::nav::rid(BuiltInKind::Pod);
    let table = Element::ResourceList(crate::element::ResourceList::client(
        crate::element::QuerySpec { rid: rid.clone(), namespace: Namespace::All, filter: None },
        crate::store::RowStore::client("pods"),
        &hub,
        "pods".to_string(),
    ));
    let filtered = Element::derive_filter(
        &table,
        RowPredicate::Grep(crate::nav::CompiledGrep::new("x")),
    )
    .expect("a table can be filtered");

    let log = Element::LogSession(Box::new(LogSession::for_test(k9rs_proto::protocol::LogTarget::One("pod".to_string()), LogContainer::Default)));
    let log_filtered =
        Element::derive_log_filter(&log, crate::nav::CompiledGrep::new("y"), crate::element::Sense::Keep)
            .expect("a log can be filtered");

    let content = Element::ContentView(ContentView::new(
        ContentSpec::Describe(k9rs_proto::protocol::ObjectRef::new(
                crate::nav::rid(k9rs_proto::resource_def::BuiltInKind::Pod),
                "some-pod".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            )),
        crate::ContentViewState::default(),
        ContentPhase::Ready,
    ));

    // A projection (a pod's containers). It was MISSING from this fixture for
    // as long as the fixture existed, so every conformance check below simply
    // skipped it — which is exactly the hole `every_kind_has_an_instance`
    // now closes.
    let mut projected_from = list_element(Namespace::All);
    let mut pod = row("web-1", "ns", &["web-1", "Running"]);
    pod.containers = vec![k9rs_proto::row::ContainerInfo { health: Default::default(),
        name: "api".into(),
        image: "img:1".into(),
        state: k9rs_proto::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
        kind: k9rs_proto::row::ContainerKind::Regular,
    }];
    seed(&projected_from, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = projected_from.view(ColumnLevel::Default, 40);
    projected_from.select(0);
    let projection =
        Element::derive_projection(&projected_from, &pod, DerivedViewKind::Containers)
            .expect("a pod row projects to containers");

    vec![
        table,
        filtered,
        projection,
        log,
        log_filtered,
        content,
        Element::Overview(crate::element::Overview),
        Element::Confirm(Box::new(crate::ConfirmDialog {
            message: "Delete pod/canary?".to_string(),
            action_label: "Delete".to_string(),
            pending: crate::PendingAction::Single {
                op: crate::SingleOp::Delete,
                target: k9rs_proto::protocol::ObjectRef::new(
                    rid.clone(),
                    "canary".to_string(),
                    Namespace::Named("default".to_string()),
                ),
            },
            action_focused: false,
        })),
        Element::Picker(Box::new(crate::ContainerPicker {
            target: k9rs_proto::protocol::ObjectRef::new(
                rid.clone(),
                "canary".to_string(),
                Namespace::Named("default".to_string()),
            ),
            choices: vec![crate::ContainerChoice {
                label: "web".into(),
                container: LogContainer::Named("web".into()),
            }],
            selected: 0,
            action: crate::ContainerAction::Logs,
        })),
        Element::Form(Box::new(crate::FormDialog {
            submit: crate::FormSubmit::Scale,
            title: "Scale deploy/api".to_string(),
            subtitle: "namespace: default".to_string(),
            target: k9rs_proto::protocol::ObjectRef::new(
                rid.clone(),
                "api".to_string(),
                Namespace::Named("default".to_string()),
            ),
            fields: vec![crate::FormFieldState {
                name: "replicas".to_string(),
                label: "Replicas".to_string(),
                kind: crate::FormFieldKind::Number { min: 0, max: 100 },
                value: crate::text_input::TextInput::seeded("3"),
            }],
            focused: crate::FormFocus::Field(0),
        })),
    ]
}

/// Every element's ACCESSORS must deliver what its declared class promises.
///
/// This is the test that did not exist when the contexts view was written.
/// That view claimed to be a table, then answered `None` from
/// `table_interaction`, `filter_input`, `data_store` and `rid` — so `/`,
/// sort, column filters, column movement and marks all dispatched cleanly
/// and then did nothing at all, silently, for as long as nobody tried them.
#[test]
fn element_accessors_match_their_class() {
    use crate::element::ElementClass;
    for mut el in one_of_every_kind() {
        let class = el.class();
        let label = format!("{:?} ({:?})", el.label(), class);
        // Class-determined for every kind: table-shaped views and the
        // overview draw the `:` prompt inside their own layout; sub-views
        // rely on the overlay. Asserted here so the two stay one decision.
        assert_eq!(
            el.renders_command_inline(),
            matches!(class, ElementClass::Table | ElementClass::Chrome),
            "{label}: inline command prompt must follow the class",
        );
        match class {
            ElementClass::Table => {
                assert!(el.is_table(), "{label}: must report is_table");
                // A resource identity is promised only by a table that IS a
                // resource. A projection is rows-of-something-else — you
                // cannot navigate to `:containers` — so it answers `None`
                // here by design. Adding the projection to this fixture is
                // what surfaced that the blanket assertion was wrong; the
                // contexts-view case it exists to catch is still covered,
                // because that view was resource-backed and lied.
                if el.origin().is_none() {
                    assert!(
                        el.rid().is_some(),
                        "{label}: a resource-backed table has a resource identity",
                    );
                }
                assert!(el.data_store().is_some(), "{label}: a table has rows");
                assert!(
                    el.table_interaction().is_some(),
                    "{label}: a table has a cursor/sort — without it every column \
                     and selection key is a silent no-op",
                );
                assert!(
                    el.filter_input_mut().is_some(),
                    "{label}: a table takes `/` — this is exactly what the contexts \
                     view lacked while looking like it worked",
                );
                assert!(el.log_store().is_none(), "{label}: a table is not a log");
                // The five accessors below all end in `_ =>` arms that
                // answer "nothing here" — correct for a log or a content
                // view, and a SILENT no-op for a table kind that forgot to
                // join them. Each one is a key that stops working with no
                // flash and no error:
                assert!(
                    el.derive_selection_keys().is_some(),
                    "{label}: a table resolves its selection WITHOUT a painted                      view — else Enter/d/y/l do nothing right after a drill",
                );
                assert!(
                    el.column_policy().rid.is_some() || el.rid().is_none(),
                    "{label}: a resource-backed table carries its column policy                      — else it loses its declared levels and force-shows NAMESPACE",
                );
                if !el.marked_keys().is_empty() || el.markable() {
                    // Only meaningful for markable kinds; the point is that
                    // `markable()` and the mark accessors agree.
                    assert_eq!(
                        el.marked_count(),
                        el.marked_snapshot().len(),
                        "{label}: the mark accessors must agree with each other",
                    );
                }
            }
            ElementClass::Log => {
                assert!(el.log_store().is_some(), "{label}: a log has a line store");
                assert!(el.log_view().is_some(), "{label}: a log has a view");
                assert!(el.log_lines().is_some(), "{label}: a log can materialize lines");
                assert!(
                    el.line_output().is_some(),
                    "{label}: a log hands its lines to a child — else `/` opens,                      you type, and Enter silently discards the filter",
                );
                assert!(!el.is_table(), "{label}: a log is not a table");
                assert!(el.data_store().is_none(), "{label}: a log has no row store");
            }
            ElementClass::Dialog => {
                // A dialog carries a question and a continuation. No rows,
                // no lines, no cursor — and crucially it FLOATS, which is
                // the property that lets it be an element at all rather
                // than a modal slot.
                assert!(!el.is_table(), "{label}: not a table");
                assert!(el.data_store().is_none(), "{label}: no row store");
                assert!(el.log_store().is_none(), "{label}: no line store");
                assert!(el.table_interaction().is_none(), "{label}: no cursor");
                assert!(
                    el.floats(),
                    "{label}: a dialog must declare a Dialog extent",
                );
            }
            ElementClass::Content | ElementClass::Chrome => {
                assert!(!el.is_table(), "{label}: not a table");
                assert!(el.data_store().is_none(), "{label}: no row store");
                assert!(el.log_store().is_none(), "{label}: no line store");
                assert!(
                    el.table_interaction().is_none(),
                    "{label}: claims no cursor, so it must not have one",
                );
            }
        }
    }
}

/// The fixture really does contain one of every kind.
///
/// Two-step by design: adding an `Element` variant breaks `kind_name`'s
/// wildcard-free match at compile time, and once an arm is added THIS fails
/// until an instance is built — so a new kind cannot slip past the
/// conformance checks in either direction.
#[test]
fn every_kind_has_an_instance() {
    use std::collections::HashSet;
    let built: HashSet<&str> = one_of_every_kind().iter().map(kind_name).collect();
    for kind in EVERY_KIND {
        assert!(
            built.contains(kind),
            "no {kind} instance in one_of_every_kind — every conformance check \
             below silently skips it until there is one",
        );
    }
    assert_eq!(built.len(), EVERY_KIND.len(), "and nothing outside the known set");
}

/// Every class is actually exercised above — otherwise the test could pass
/// by covering only the easy kinds.
#[test]
fn every_element_class_is_covered() {
    use crate::element::ElementClass;
    use std::collections::HashSet;
    let seen: HashSet<ElementClass> = one_of_every_kind().iter().map(|e| e.class()).collect();
    for expected in [
        ElementClass::Table,
        ElementClass::Log,
        ElementClass::Content,
        ElementClass::Chrome,
    ] {
        assert!(seen.contains(&expected), "no element built for {expected:?}");
    }
}

/// A semantic sort target resolves against the REAL headers, and refuses when
/// the column isn't there.
///
/// The bug this replaces: `N` computed a data index from a single boolean —
/// `if is_cluster_scoped { 0 } else { 1 }` — on the assumption that a
/// namespaced table's second column is NAME. Events is namespaced and its
/// headers are `[NAMESPACE, TYPE, REASON, …]`, so `N` sorted by event TYPE and
/// looked like it worked. The positional guess always found *a* column, which
/// is exactly why nobody noticed.
#[test]
fn semantic_sort_resolves_against_the_real_headers() {
    use crate::{SortTarget, actions::{COL_AGE, COL_NAME}};

    let sorted_col = |headers: &[&str], target: SortTarget| -> Option<usize> {
        let mut el = list_element(Namespace::All);
        seed(&el, headers, vec![]);
        el.sort_by(target).then(|| el.table_interaction().expect("table").sort.col)
    };

    // Namespaced shape: NAME is second.
    let pods = ["NAMESPACE", "NAME", "STATUS", "AGE"];
    assert_eq!(sorted_col(&pods, SortTarget::Named(COL_NAME)), Some(1));
    assert_eq!(sorted_col(&pods, SortTarget::Named(COL_AGE)), Some(3));

    // Cluster-scoped shape: NAME is first. The old rule got this one right,
    // which is half of why it survived.
    let nodes = ["NAME", "STATUS", "ROLES", "AGE"];
    assert_eq!(sorted_col(&nodes, SortTarget::Named(COL_NAME)), Some(0));

    // Events: namespaced, and NO name column at all. The old rule sorted by
    // TYPE here; the honest answer is to refuse.
    let events = ["NAMESPACE", "TYPE", "REASON", "OBJECT", "AGE"];
    assert_eq!(
        sorted_col(&events, SortTarget::Named(COL_NAME)),
        None,
        "no NAME column means no sort, not a sort by whatever is at index 1",
    );
    assert_eq!(sorted_col(&events, SortTarget::Named(COL_AGE)), Some(4));

    // A table with no AGE column refuses too — `A` used to take the LAST
    // column, which is ACTIVE on the contexts table.
    let contexts = ["NAME", "CLUSTER", "USER", "ACTIVE"];
    assert_eq!(sorted_col(&contexts, SortTarget::Named(COL_AGE)), None);
    assert_eq!(sorted_col(&contexts, SortTarget::Named(COL_NAME)), Some(0));
}

/// `o` (show node) works on a container, because the projection carries the
/// pod's node onto every container row.
///
/// Among built-ins, pods are the ONLY resource with a first-class node —
/// `spec.nodeName` — and `ResourceRow.node` is populated nowhere else. A
/// container is the one honest extension: it runs wherever its pod runs, and
/// the answer is carried in from the parent rather than inferred. PVs
/// (nodeAffinity, local volumes only), EndpointSlices (many nodeNames per
/// row) and Events (`source.host` is not always a node) are all guesses, and
/// deliberately do not get this.
#[test]
fn a_container_knows_the_node_its_pod_runs_on() {
    use k9rs_proto::protocol::OperationKind;

    let mut root = list_element(Namespace::All);
    let mut pod = row("web-1", "ns", &["web-1", "Running"]);
    pod.node = Some("ip-10-0-1-7".to_string());
    pod.containers = vec![k9rs_proto::row::ContainerInfo { health: Default::default(),
        name: "api".into(),
        image: "img:1".into(),
        state: k9rs_proto::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
        kind: k9rs_proto::row::ContainerKind::Regular,
    }];
    seed(&root, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = root.view(ColumnLevel::Default, 40);
    root.select(0);

    let mut containers =
        Element::derive_projection(&root, &pod, DerivedViewKind::Containers).expect("projects");
    let _ = containers.view(ColumnLevel::Default, 40);
    containers.select(0);

    let selected = containers.selected_row().expect("a container row");
    assert_eq!(
        selected.node.as_deref(),
        Some("ip-10-0-1-7"),
        "the node rides in from the parent pod",
    );
    assert!(
        DerivedViewKind::Containers.operations().contains(&OperationKind::ShowNode),
        "…and the view offers the key, or carrying it would be pointless",
    );

    // An UNSCHEDULED pod has no node, and neither do its containers — the
    // action layer refuses on an empty name rather than inventing one.
    let mut unscheduled = pod.clone();
    unscheduled.node = None;
    let projected = crate::derived::project_containers(&unscheduled);
    assert_eq!(projected[0].node, None);
}

/// A filter hides rows from VIEW; it does not deselect them. The store-backed
/// side has always worked that way — the projection used to intersect its
/// marks with the painted (draft-filtered) view instead, so typing `/c0` with
/// two containers marked silently dropped the other from the selection, and a
/// draft matching neither ended select mode outright.
#[test]
fn a_draft_filter_does_not_shrink_a_projections_marks() {
    let mut el = list_element(Namespace::All);
    let mut pod = row("web", "ns", &["web", "Running"]);
    pod.containers = ["c0", "c1", "c2"]
        .into_iter()
        .map(|name| ContainerInfo {
            health: Default::default(),
            name: name.into(),
            kind: Default::default(),
            image: "img".into(),
            state: k9rs_proto::row::ContainerState::Running,
            reason: None,
            ready: true,
            restart_count: 0,
        })
        .collect();
    seed(&el, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = el.view(ColumnLevel::Default, 40);
    let mut derived = Element::derive_projection(&el, &pod, DerivedViewKind::Containers)
        .expect("containers projection");
    let _ = derived.view(ColumnLevel::Default, 40);

    derived.select(0);
    derived.toggle_mark();
    derived.select(2);
    derived.toggle_mark();
    assert_eq!(derived.marked_count(), 2);

    // Type a draft that shows only one of them.
    {
        let fi = derived.filter_input_mut().unwrap();
        fi.start();
        for c in "c0".chars() {
            fi.input_mut().insert(c);
        }
    }
    let _ = derived.view(ColumnLevel::Default, 40);
    assert_eq!(
        derived.marked_count(),
        2,
        "both marks survive: the filter changed what is VISIBLE, not what is selected",
    );

    // And a draft matching nothing must not end select mode.
    {
        let fi = derived.filter_input_mut().unwrap();
        fi.start();
        for c in "zzz".chars() {
            fi.input_mut().insert(c);
        }
    }
    let _ = derived.view(ColumnLevel::Default, 40);
    assert!(derived.has_marks(), "select mode survives a draft that matches no row");
}

/// Clearing marks on purpose must not trip the stale-frame guard. Keys are
/// drained in batches with no paint between them, so `Ctrl-\` then `Ctrl-D`
/// used to answer "Selection changed — the marked rows are gone" to a user
/// who had just cleared them deliberately.
#[test]
fn a_deliberate_clear_is_not_a_stale_frame() {
    let mut el = list_element(Namespace::All);
    seed(&el, &["NAME", "STATUS"], vec![row("a", "ns", &["a", "Running"])]);
    let _ = el.view(ColumnLevel::Default, 40);
    el.select(0);
    el.toggle_mark();
    el.table_interaction_mut().unwrap().rendered_select_mode = true;

    el.clear_marks();
    assert!(!el.has_marks());
    assert!(
        !el.table_interaction().unwrap().rendered_select_mode,
        "the guard must not fire for marks WE emptied",
    );
}

/// `initial_load` suppresses autoscroll while the tail batch streams in. It
/// is SET by `restart_range` — which reaches a BURIED session, because
/// `with_log_session_of` walks the whole stack by store identity — and was
/// CLEARED only on the top element. Commit a grep, press a range key, Esc
/// back, and the session rendered its OLDEST retained lines at the top of
/// the box while the indicator said "AutoScroll: On".
#[test]
fn the_tail_load_latch_clears_on_buried_log_views_too() {
    use crate::element::LogSession;
    use k9rs_proto::protocol::{LogContainer, LogTarget};

    let mut app = crate::App::new_for_test();
    let session = LogSession::for_test(LogTarget::One("web".into()), LogContainer::Default);
    let store = std::sync::Arc::clone(session.store());
    app.nav.push(Element::LogSession(Box::new(session)));

    // A filter on top; the session is now buried.
    let filter = Element::derive_log_filter(
        app.nav.top(),
        crate::nav::CompiledGrep::new("err"),
            crate::element::Sense::Keep,
    )
    .expect("a log session yields a line output");
    app.nav.push(filter);

    // A range key resolves the owner by store identity, below the top.
    app.nav.with_log_session_of(&store, |s| s.view.initial_load = true);

    // The drain-batch clear, as the event loop performs it.
    app.nav.for_each_log_view(|view| view.initial_load = false);

    let mut checked = false;
    app.nav.with_log_session_of(&store, |s| {
        checked = true;
        assert!(
            !s.view.initial_load,
            "the buried session's latch must clear too, or autoscroll stays suppressed \
             and Esc lands you on the oldest retained lines",
        );
    });
    assert!(checked, "the session is on the stack, below the filter");
}

/// A filter with something stacked above it must keep collecting.
///
/// `log_visible` — the only other drain — runs on `nav.top_mut()` alone, so a
/// covered filter's cursor would never advance: the parent would evict past
/// it and, on reveal, it would report a hole for lines it could have had.
#[test]
fn a_buried_filter_keeps_collecting() {
    use crate::element::LogSession;
    use k9rs_proto::protocol::{LogContainer, LogLine, LogTarget};

    let mut app = crate::App::new_for_test();
    let session = LogSession::for_test(LogTarget::One("web".into()), LogContainer::Default);
    let store = Arc::clone(session.store());
    app.nav.push(Element::LogSession(Box::new(session)));

    let filter =
        Element::derive_log_filter(
            app.nav.top(),
            CompiledGrep::new("err"),
            crate::element::Sense::Keep,
        )
        .expect("log output");
    app.nav.push(filter);
    // Something else on top of the filter.
    let deeper =
        Element::derive_log_filter(
            app.nav.top(),
            CompiledGrep::new("err"),
            crate::element::Sense::Keep,
        )
        .expect("log output");
    app.nav.push(deeper);

    for i in 0..6 {
        store.push(
            1,
            LogLine {
                timestamp: None,
                source: None,
                content: format!("line-{i} {}", if i % 2 == 0 { "err" } else { "ok" }),
            },
        );
    }

    // The event loop's per-batch sweep.
    app.nav.keep_log_filters_current();

    // Pop back to the middle filter: it collected while covered.
    app.nav.pop();
    assert_eq!(
        app.nav.top_mut().log_visible().unwrap().len(),
        3,
        "a covered filter must have collected its matches, not a hole",
    );
    assert!(
        !app.nav.top().log_lost_history(),
        "…and must not claim it lost history it never lost",
    );
}

/// A stream that ENDED is not a stream that DIED. `is_alive() == false`
/// stands for three things — the transport dropped, the log finished, or we
/// stopped it ourselves — and `restart_range` begins by clearing the store,
/// so reviving either of the orderly endings threw away a completed log.
///
/// The discriminator needs no new state: both orderly endings run
/// `mark_ended`, so the store says `live = false`. A transport death is
/// exactly the case that leaves it `true`.
#[test]
fn only_a_stream_that_died_is_revived() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    let mut session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    );
    for i in 0..20 {
        session.store().push(1, LogLine { timestamp: None, content: format!("line-{i}"), source: None });
    }
    assert_eq!(session.store().with_read(|i| i.lines.len()), 20);

    // The container exited: the daemon closed the substream and the store
    // recorded it. Nothing to recover — the log is complete.
    assert!(!session.needs_revival(), "a live stream needs nothing");

    // Transport death: the bridge is gone, but its `Ended` died in the closed
    // channel, so the store still believes it is receiving. THIS is the case
    // the revival exists for.
    session.kill_stream_for_test();
    assert!(session.needs_revival(), "a dead stream over a live-looking store");

    // Orderly end: the container exited, the daemon closed the substream and
    // `mark_ended` ran. The log is complete — refetching it would clear it.
    session.store().mark_ended(1);
    assert!(!session.needs_revival(), "a finished log must not be refetched");

    // And the deliberate stop: a full `Retain::Head` range is stopped ON
    // PURPOSE, so popping back into the view must not re-download it.
    let mut session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-y".to_string()),
        LogContainer::Default,
    );
    session.store().set_retain(crate::store::Retain::Head);
    for i in 0..session.store().max_lines() {
        session.store().push(1, LogLine { timestamp: None, content: format!("l{i}"), source: None });
    }
    assert!(session.stop_if_head_complete(), "a full Head store stops its own stream");
    session.kill_stream_for_test(); // the abort has landed
    assert!(!session.needs_revival(), "we stopped this one on purpose; do not undo that");
}

/// The other half: `restart_range` empties the session store, but a grep over
/// it owns its OWN lines, and `clear()` leaves the filter's cursor exactly at
/// the new front — so the next drain APPENDED the new range's matches onto
/// the old range's. A reconnect under a grep duplicated every retained match,
/// and the duplicates evicted real history to make room for themselves.
#[test]
fn restarting_a_range_resets_the_greps_over_it() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    let session = Element::LogSession(Box::new(LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    )));
    let store = Arc::clone(session.log_store().unwrap());
    for i in 0..10 {
        store.push(1, LogLine { timestamp: None, content: format!("line-{i} err"), source: None });
    }
    let mut filter = Element::derive_log_filter(&session, CompiledGrep::new("err"), crate::element::Sense::Keep).unwrap();
    assert_eq!(filter.log_visible().unwrap().len(), 10);

    let mut nav = crate::nav::NavStack::new(session);
    nav.push(filter);

    // A range restart: the store is cleared and refilled with the SAME lines
    // (the overlapping-range case, which a reconnect reproduces exactly).
    store.clear();
    nav.reset_log_filters_of(&store);
    for i in 0..10 {
        store.push(1, LogLine { timestamp: None, content: format!("line-{i} err"), source: None });
    }
    nav.keep_log_filters_current();

    let visible = nav.top_mut().log_visible().unwrap();
    assert_eq!(visible.len(), 10, "the new range's matches, not both ranges' — got {}", visible.len());
}

/// Help opens on the section for the view you pressed `?` in.
///
/// That is the whole reason help is contextual, and it was off-screen:
/// `SortSpec` has no "unsorted", so column 0 (SECTION) is sorted, and
/// alphabetically "This view (…)" comes LAST — after "Everywhere",
/// "Resource Actions", "Search & filter syntax" and "Select mode". Pressing
/// `?` from a pod table opened on the global keys and buried the pod keys 36
/// rows down.
///
/// Pinned as an ORDER, not as a sort direction: renaming a section must fail
/// here rather than silently bury the content again.
#[test]
fn help_opens_on_the_section_for_the_view_you_came_from() {
    let mut app = crate::App::new_for_test();
    let class = crate::keymap::ViewClass::of(app.nav.top());
    let store = app.core.seed_keybindings(
        Some(app.current_capabilities()),
        app.config.keys,
        class,
        app.nav.top().markable(),
    );
    let rid = k9rs_proto::local_types::LocalResourceKind::Keybinding.to_resource_id();
    app.nav.push(crate::element::Element::ResourceList(
        crate::element::ResourceList::client(
            crate::element::QuerySpec {
                rid,
                namespace: k9rs_proto::protocol::Namespace::All,
                filter: None,
            },
            store,
            &app.kube.metrics,
            "keys".to_string(),
        ),
    ));

    let view = app.nav.top_mut().view(ColumnLevel::Default, 200);
    let first_section = view.rows.first().map(|r| r[0].clone()).unwrap_or_default();
    assert_eq!(
        first_section,
        class.title(),
        "help must open on the keys for the view it documents, not on the alphabet",
    );

    // …and the grouping still holds: every row of that section is contiguous.
    let n = view.rows.iter().take_while(|r| r[0] == first_section).count();
    assert!(n > 1, "the contextual section has more than one key");
    assert!(
        view.rows.iter().skip(n).all(|r| r[0] != first_section),
        "a section must not be split across the table",
    );
}

/// The straggler that defeated the first version of this fix.
///
/// `stop_if_head_complete` aborts the stream and marks the store ended — but
/// `abort` is asynchronous and the event queue is already full, so lines keep
/// arriving for a moment. The first version asked `store.live`, which every
/// arrival sets, so the first straggler flipped a deliberately-stopped range
/// back to "needs reviving" and the next pop cleared it and re-downloaded it.
/// The ending is STICKY now, so an arrival cannot undo it.
#[test]
fn a_straggler_cannot_un_end_a_stopped_range() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    let mut session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    );
    session.store().set_retain(crate::store::Retain::Head);
    for i in 0..session.store().max_lines() {
        session.store().push(1, LogLine { timestamp: None, content: format!("l{i}"), source: None });
    }
    assert!(session.stop_if_head_complete(), "a full Head store stops its own stream");
    session.kill_stream_for_test();
    assert!(!session.needs_revival(), "we stopped it on purpose");

    // A line that was already in flight when we aborted.
    session.store().push(1, LogLine { timestamp: None, content: "straggler".into(), source: None });
    assert!(
        !session.needs_revival(),
        "an arrival must not undo a deliberate stop — it did, on the normal path",
    );

    // A restart DOES take it back down, or a genuinely dead stream could
    // never be recovered afterwards — the opposite failure.
    session.store().clear();
    assert!(session.needs_revival(), "after a restart the store is revivable again");
}

/// A grep searches the parts `Role::searchable` declares, and nothing else.
///
/// The rule used to be hand-written at both matching sites as
/// `content || source`, while `Role` also stated it — three copies, so
/// "the timestamp is not searchable" could become true in some of them.
/// That is the shape that had `is_modal()` sitting unused beside five
/// hand-written checks, right up until wiring it in broke every modal.
#[test]
fn a_grep_searches_exactly_the_roles_that_declare_themselves_searchable() {
    use k9rs_proto::protocol::{LogContainer, LogLine, Role};
    let mut session = Element::LogSession(Box::new(LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    )));
    let store = Arc::clone(session.log_store().unwrap());
    store.push(1, LogLine {
        source: Some("api-7f9".into()),
        timestamp: Some("2026-09-19T10:00:00.5Z".into()),
        content: "the message body".into(),
    });

    let visible = |el: &mut Element, pat: &str| {
        el.log_view_mut().unwrap().draft = Some(crate::text_input::TextInput::seeded(pat));
        el.log_view_mut().unwrap().drop_cache();
        el.log_visible().unwrap().len()
    };

    assert!(Role::Body.searchable() && Role::Source.searchable());
    assert_eq!(visible(&mut session, "message body"), 1, "the body is searched");
    assert_eq!(visible(&mut session, "api-7f9"), 1, "so is the source tag");

    // The timestamp is chrome: matching it would make `/2026` select every
    // line in the buffer.
    assert!(!Role::Timestamp.searchable());
    assert_eq!(visible(&mut session, "2026-09-19"), 0, "the stamp is not searched");

    // And the committed path agrees with the draft path — they are the two
    // places the rule used to be written out by hand.
    session.log_view_mut().unwrap().draft = None;
    session.log_view_mut().unwrap().drop_cache();
    let mut f = Element::derive_log_filter(&session, CompiledGrep::new("2026-09-19"), crate::element::Sense::Keep).unwrap();
    assert_eq!(f.log_visible().unwrap().len(), 0, "committed greps skip it too");
    let mut f = Element::derive_log_filter(&session, CompiledGrep::new("api-7f9"), crate::element::Sense::Keep).unwrap();
    assert_eq!(f.log_visible().unwrap().len(), 1);
}

/// A bare grep scans the columns you can SEE, not every cell the row carries.
///
/// Deployment rows carry NAMESPACE, NAME, READY, UP-TO-DATE, AVAILABLE,
/// CONTAINERS, IMAGES, LABELS, AGE; the default level shows five of them.
/// `/pimp` used to match `IMAGES` and `LABELS` too, returning rows with no
/// visible `pimp` anywhere — convenient exactly when it happened to hit the
/// column you meant, and unexplainable otherwise.
#[test]
fn a_bare_grep_scans_only_the_displayed_columns() {
    use crate::store::StorePayload;
    use k9rs_proto::protocol::TableBaseline;
    use k9rs_proto::row::{CellValue, ResourceRow};

    let mut app = crate::App::new_for_test();
    let store: Arc<crate::store::RowStore> =
        Arc::clone(app.nav.top().data_store().expect("root is a list"));
    let row = |name: &str, hidden: &str| ResourceRow {
        cells: vec![
            CellValue::Text(name.into()),
            CellValue::Text("1/1".into()),
            CellValue::Text(hidden.into()),
        ],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::nav::rid(k9rs_proto::resource_def::BuiltInKind::Pod),
            // Only the first two are shown at the default level; the test
            // rows put the needle in the third.
            headers: vec!["NAME".into(), "READY".into(), "IMAGES".into()],
            rows: vec![
                row("fetcher-chromium-pimp", "registry/other:v1"),
                row("fetcher-playwright", "registry/pimp-base:v2"),
                row("parser-ocr", "registry/plain:v3"),
            ],
        }),
    );

    let visible_names = |el: &mut Element, pat: &str| -> Vec<String> {
        {
            let fi = el.filter_input_mut().unwrap();
            fi.start();
            for c in pat.chars() {
                fi.input_mut().insert(c);
            }
        }
        el.table_interaction_mut().unwrap().drop_cache();
        el.view(ColumnLevel::Default, 200).keys.iter().map(|k| k.name.clone()).collect()
    };

    let el = app.nav.top_mut();
    let names = visible_names(el, "pimp");
    assert!(
        names.contains(&"fetcher-chromium-pimp".to_string()),
        "the row whose NAME contains it still matches: {names:?}",
    );
    assert!(
        !names.contains(&"fetcher-playwright".to_string()),
        "a match in a HIDDEN column must not select the row: {names:?}",
    );
    assert!(!names.contains(&"parser-ocr".to_string()));
}

/// THE reported bug, through the real log paths.
///
/// `/workers=` against lines that read `workers=0` on screen. It failed
/// because the filter ran on raw `content`, where a reset sequence sits
/// between the `s` and the `=` — while the renderer stripped the escapes.
/// Four views of one line, and the filter had the one nobody could see.
///
/// Both the DRAFT (as you type) and the COMMITTED path are checked: they are
/// the two places that consume a segment, and a rule true in one of them is
/// how this class of bug survives.
#[test]
fn a_grep_matches_across_a_colour_boundary() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    let mut session = Element::LogSession(Box::new(LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    )));
    let store = Arc::clone(session.log_store().unwrap());
    // Exactly what `tracing` emits with ANSI on: a dim timestamp, a coloured
    // level, and a coloured field NAME — which is what puts a reset between
    // `workers` and `=`.
    for c in [
        "\u{1b}[2mts\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \u{1b}[2mworkers\u{1b}[0m=0, depth=1",
        "\u{1b}[2mts\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m workers=256, depth=2",
        "\u{1b}[2mts\u{1b}[0m \u{1b}[33m WARN\u{1b}[0m nothing relevant here",
    ] {
        store.push(1, LogLine { source: None, timestamp: None, content: c.to_string() });
    }

    let draft_hits = |el: &mut Element, pat: &str| {
        el.log_view_mut().unwrap().draft =
            Some(crate::text_input::TextInput::seeded(pat));
        el.log_view_mut().unwrap().drop_cache();
        el.log_visible().unwrap().len()
    };

    assert_eq!(draft_hits(&mut session, "workers="), 2, "both lines READ `workers=`");
    assert_eq!(draft_hits(&mut session, "workers=0"), 1, "…and a value can follow it");
    assert_eq!(draft_hits(&mut session, "INFO"), 2, "a pattern inside a coloured run");
    assert_eq!(draft_hits(&mut session, "\u{1b}"), 0, "the escapes are NOT searchable");

    // The committed path agrees — it is the other consumer of a segment.
    session.log_view_mut().unwrap().draft = None;
    session.log_view_mut().unwrap().drop_cache();
    let mut f = Element::derive_log_filter(&session, CompiledGrep::new("workers="), crate::element::Sense::Keep).unwrap();
    assert_eq!(f.log_visible().unwrap().len(), 2, "committed greps see the same text");
}

/// Help must document select mode exactly where select mode exists.
///
/// Marking is a per-ELEMENT answer: `Element::markable` is false for a
/// resource that declares no batch-capable operation, which is why marking a
/// context does nothing (it used to silently enter select mode and block
/// `Enter`, the picker's only function). Help gated its Select-mode section
/// on the view CLASS instead, so `?` on the contexts view — a table —
/// documented `Space`, `Ctrl-Space`, `Ctrl-\` and `c` as if they worked.
///
/// Pinned as an AGREEMENT between the two, not as a fact about either, so
/// neither can move without the other. This is the surface the keymap
/// agreement test cannot see: the section is hand-written, not projected.
#[test]
fn help_documents_select_mode_exactly_where_marking_works() {
    let app = crate::App::new_for_test();
    let section = "Select mode (while rows are marked)";

    let documents_select_mode = |el: &crate::element::Element| -> bool {
        let class = crate::keymap::ViewClass::of(el);
        let (_, rows) = crate::help::HelpContent::rows(
            Some(el.capabilities()),
            app.config.keys,
            class,
            el.markable(),
        );
        rows.iter().any(|r| {
            matches!(
                &r.cells[0],
                k9rs_proto::row::CellValue::Text(t) if t == section
            )
        })
    };

    let contexts = app
        .core
        .client_root_element(
            &app.kube.metrics,
            &k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id(),
            k9rs_proto::protocol::Namespace::All,
        )
        .expect("contexts is a client-owned root element");

    // The reported case: a table you cannot mark.
    assert_eq!(
        crate::keymap::ViewClass::of(&contexts),
        crate::keymap::ViewClass::Table,
        "contexts IS a table — gating on the class could never have excluded it",
    );
    assert!(!contexts.markable(), "a context feeds no batch operation");
    assert!(
        !documents_select_mode(&contexts),
        "so help must not document marking there",
    );

    // …and the pod table, where every one of those keys really does work.
    let pods = app.nav.top();
    assert!(pods.markable(), "pods feed Delete/Restart batches");
    assert!(
        documents_select_mode(pods),
        "where marking works, help still says how",
    );
}

/// `:alias` / `Ctrl-a` is an ordinary table now, not a formatted document.
///
/// It used to be a `ContentView` holding hand-formatted text: no grep, no
/// sort, no scroll, no copy — and it carried its own "Key Bindings" block,
/// a seventh hand-written copy of the keymap that the agreement test could
/// not see and that had already drifted (missing `Ctrl-w`, `Ctrl-z`,
/// `Ctrl-Space`, `Ctrl-\`). Keys belong to `?`.
///
/// The resource aliases come from the registry, which is already their one
/// declaration; the command words come from `COMMAND_WORDS`, which the
/// parser's own test probes.
#[test]
fn the_aliases_view_is_a_table_of_what_the_prompt_accepts() {
    use k9rs_proto::row::CellValue;

    let app = crate::App::new_for_test();
    let rid = k9rs_proto::local_types::LocalResourceKind::Alias.to_resource_id();
    let el = app
        .core
        .client_root_element(&app.kube.metrics, &rid, k9rs_proto::protocol::Namespace::All)
        .expect("aliases routes through the client-owned path, like contexts");

    // A table: so grep, sort, column movement and copy come for free.
    assert_eq!(el.class(), crate::element::ElementClass::Table);
    assert_eq!(
        crate::keymap::ViewClass::of(&el),
        crate::keymap::ViewClass::Table,
    );

    let cells = |r: &k9rs_proto::row::ResourceRow| -> Vec<String> {
        r.cells
            .iter()
            .map(|c| match c {
                CellValue::Text(t) => t.clone(),
                other => format!("{other:?}"),
            })
            .collect()
    };
    let rows = app.core.aliases.with_read(|i| i.rows.iter().map(cells).collect::<Vec<_>>());
    assert!(!rows.is_empty(), "the view has content without any daemon");

    // Every resource alias the prompt accepts is listed…
    let pods = rows
        .iter()
        .find(|r| r[2] == "Pod")
        .expect("pods is in the registry, so it is in the view");
    assert_eq!(pods[0], "resource");
    for word in pods[1].split(" / ") {
        assert!(
            k9rs_proto::protocol::ResourceId::from_alias(word).is_some(),
            "`:{word}` is advertised but the parser does not resolve it",
        );
    }

    // …including the client-owned ones, which are resources like any other.
    assert!(
        rows.iter().any(|r| r[1].split(" / ").any(|w| w == "ctx")),
        "contexts are an ordinary resource and `:ctx` is how you reach them",
    );
    // …and NOT the one that deliberately has no alias.
    assert!(
        !rows.iter().any(|r| r[2] == "Keybinding"),
        "`?` is not a noun you browse; advertising a word for it would lie",
    );

    // The command words, from the declaration the parser is tested against.
    for doc in crate::commands::COMMAND_WORDS {
        assert!(
            rows.iter().any(|r| r[0] == "command" && r[1] == doc.display()),
            "`{}` is a documented command word but is not in the view",
            doc.display(),
        );
    }

    // And no keys: that is `?`'s table, with its own agreement test.
    for key in ["Ctrl-r", "Ctrl-e", "Ctrl-a", "Ctrl-s"] {
        assert!(
            !rows.iter().any(|r| r.iter().any(|c| c.contains(key))),
            "`{key}` is a KEY — it belongs to help, not to the alias list",
        );
    }
}

/// A key that still works must still be documented — especially a
/// destructive one.
///
/// `Ctrl-D`, `Ctrl-K` and `Shift-F` are matched in the handler on the CHORD,
/// guarded only by whether the resource declares the operation. Rebinding
/// `delete` therefore ADDS a key; it does not move one. Help used to skip the
/// `Ctrl-d` row exactly when the user had rebound `delete`, so the override
/// was documented and the chord that went on deleting things was not.
#[test]
fn rebinding_delete_does_not_hide_the_chord_that_still_deletes() {
    let app = crate::App::new_for_test();
    let caps = Some(app.nav.top().capabilities());
    assert!(
        caps.as_ref().unwrap().supports(k9rs_proto::protocol::OperationKind::Delete),
        "pods declare Delete",
    );

    let keys_of = |cfg: crate::KeysConfig| -> Vec<String> {
        let (_, rows) = crate::help::HelpContent::rows(
            caps.clone(),
            cfg,
            crate::keymap::ViewClass::Table,
            app.nav.top().markable(),
        );
        rows.iter()
            .filter_map(|r| match (&r.cells[1], &r.cells[2]) {
                (
                    k9rs_proto::row::CellValue::Text(k),
                    k9rs_proto::row::CellValue::Text(d),
                ) if d == "Delete" => Some(k.clone()),
                _ => None,
            })
            .collect()
    };

    // Default: the structural chord is the only way in, and it is listed.
    assert!(keys_of(app.config.keys).contains(&"Ctrl-d".to_string()));

    // Rebound: BOTH work, so both are listed.
    let mut rebound = app.config.keys;
    rebound.delete = Some(crate::KeyCombo::plain('x'));
    let listed = keys_of(rebound);
    assert!(
        listed.contains(&"x".to_string()),
        "the override is documented: {listed:?}",
    );
    assert!(
        listed.contains(&"Ctrl-d".to_string()),
        "…and so is the chord that still deletes: {listed:?}",
    );
}

/// Completion offers every `:` word something else declares — no third copy.
///
/// There were two hand-written slices in the completer beside a loop that
/// derived every resource's aliases from the registry: `["alias", "aliases",
/// "a"]` (duplicating what the loop now covers, invisible only because a
/// `dedup` followed) and a list of command words that was missing
/// `namespace` outright, so `:names…` completed to nothing while `:ns`
/// worked. Both are gone; this is the pin that keeps them from returning.
#[test]
fn completion_offers_every_word_something_else_declares() {
    let mut app = crate::App::new_for_test();
    let complete = |app: &mut crate::App, typed: &str| -> Vec<String> {
        app.ui.open(crate::Modal::Command {
            input: crate::text_input::TextInput::seeded(typed),
            history_index: None,
        });
        app.command_completions()
    };

    // The aliases VIEW, whose words come from `LocalResourceKind`.
    for word in k9rs_proto::local_types::LocalResourceKind::Alias.aliases() {
        let hits = complete(&mut app, word);
        assert!(
            hits.iter().any(|h| h == word),
            "`:{word}` is advertised by the Alias kind but not offered: {hits:?}",
        );
    }

    // Every static command word, from `COMMAND_WORDS`.
    for doc in crate::commands::COMMAND_WORDS {
        for word in doc.words {
            let hits = complete(&mut app, word);
            assert!(
                hits.iter().any(|h| h == word),
                "`:{word}` ({}) is documented but not offered: {hits:?}",
                doc.label,
            );
        }
    }

    // The one the old hand-written slice had simply left out.
    let hits = complete(&mut app, "names");
    assert!(
        hits.iter().any(|h| h == "namespace"),
        "`:names…` must complete to `namespace`: {hits:?}",
    );
}

/// Help repeats a key across sections only when its MEANING differs.
///
/// The sections are partly hand-written, so they can re-describe a key the
/// keymap already owns — and did: `Space`, `Ctrl-Space` and `Ctrl-\` were
/// listed under Select mode in different words from the keymap rows that
/// declare them ("Mark / unmark row" vs "Toggle the mark on the cursor
/// row"). Marking behaves the same marked or not, so those rows said nothing
/// the section exists to say, were free to drift, and made the sheet longer.
///
/// The repeats that remain are the ones worth having: `Ctrl-d` is "Delete" in
/// general and "Delete — EVERY marked row" in select mode, which is exactly
/// what contextual help is for.
#[test]
fn help_repeats_a_key_only_to_say_something_new_about_it() {
    use k9rs_proto::row::CellValue;

    let app = crate::App::new_for_test();
    let (_, rows) = crate::help::HelpContent::rows(
        Some(app.current_capabilities()),
        app.config.keys,
        crate::keymap::ViewClass::Table,
        app.nav.top().markable(),
    );
    let text = |c: &CellValue| match c {
        CellValue::Text(t) => t.clone(),
        other => format!("{other:?}"),
    };

    let mut seen: Vec<(String, String, String)> = Vec::new();
    for r in &rows {
        let (section, key, desc) = (text(&r.cells[0]), text(&r.cells[1]), text(&r.cells[2]));
        if key.is_empty() {
            continue; // prose lines belonging to a section
        }
        if let Some((prev_section, _, prev_desc)) =
            seen.iter().find(|(_, k, _)| *k == key)
        {
            assert_ne!(
                &desc, prev_desc,
                "`{key}` is documented identically in `{prev_section}` and \
                 `{section}` — one of them is a hand-written copy of the \
                 keymap row that owns it",
            );
        }
        seen.push((section, key, desc));
    }
}

/// A structural chord fires only where the resource declares the operation,
/// and is spelled the same everywhere.
///
/// `Ctrl-D`, `Ctrl-K` and `Shift-F` were string literals in two places in
/// `help.rs` and matched separately in `event::handler` — three copies of one
/// fact, none of them the authority. `Shift-F` was the copy that went wrong:
/// the handler's arm had NO capability check, so it fired on any resource and
/// failed later with "No target found for port-forward", while help only
/// advertised it where port-forward was declared. The two disagreed about the
/// same key.
#[test]
fn a_structural_chord_fires_only_where_its_operation_is_declared() {
    use k9rs_proto::protocol::OperationKind;

    // The declaration, and nothing else, decides the chord.
    assert_eq!(
        crate::keymap::structural_chord(&OperationKind::PortForward).map(|c| c.label()),
        Some("Shift-f".to_string()),
    );
    assert_eq!(
        crate::keymap::structural_chord(&OperationKind::Delete).map(|c| c.label()),
        Some("Ctrl-d".to_string()),
    );
    // An operation WITH a descriptor default has no structural chord: it is
    // reached by that key, and by whatever the user rebound it to.
    assert_eq!(crate::keymap::structural_chord(&OperationKind::Describe), None);
    assert!(OperationKind::Describe.descriptor().default_key.is_some());

    // Pods declare port-forward, so `F` reaches it.
    let mut app = crate::App::new_for_test();
    assert!(app.current_capabilities().supports(OperationKind::PortForward));
    let shift_f = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('F'),
        crossterm::event::KeyModifiers::SHIFT,
    );
    assert!(
        matches!(
            crate::handler::handle_key_event(&app, shift_f),
            Some(crate::actions::Action::PortForward)
        ),
        "where the resource declares it, the chord works",
    );

    // Contexts declare NO operations, so the same chord does nothing —
    // rather than raising an action that fails with a flash two layers down.
    app.nav.reset(
        app.core
            .client_root_element(
                &app.kube.metrics,
                &k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id(),
                k9rs_proto::protocol::Namespace::All,
            )
            .expect("contexts is client-owned"),
    );
    assert!(app.current_capabilities().operations.is_empty());
    assert!(
        !matches!(
            crate::handler::handle_key_event(&app, shift_f),
            Some(crate::actions::Action::PortForward)
        ),
        "where it is not declared, the chord is inert — it used to fire anyway",
    );
}

/// The grep chain can narrow AWAY from something, not only toward it.
///
/// `/health` showed you the health checks and nothing hid them — and that was
/// a wall, not a gap: vim's `\@!` is refused by name and `regex` 1.x has no
/// look-around at all (a finite-automaton engine, which is what makes a
/// pathological pattern compile promptly). So exclusion cannot be a property
/// of the PATTERN; it is a property of the chain STEP.
#[test]
fn a_chain_step_can_exclude_what_it_matches() {
    use crate::element::{sense_of, Sense};
    use k9rs_proto::protocol::{LogContainer, LogLine};

    // The prefix is parsed before the vim translator sees the text, so that
    // grammar stays exactly vim magic.
    assert_eq!(sense_of("health"), (Sense::Keep, "health"));
    assert_eq!(sense_of("!health"), (Sense::Drop, "health"));
    assert_eq!(sense_of("\\!urgent"), (Sense::Keep, "!urgent"));
    assert_eq!(sense_of("!"), (Sense::Drop, ""));

    let session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    );
    for text in ["GET /healthz 200", "worker started", "GET /healthz 200", "worker failed"] {
        session.store().push(0, LogLine::untagged(text));
    }
    let log = Element::LogSession(Box::new(session));

    // Exclude: everything the pattern matched is gone, the rest stays.
    let mut excluded = Element::derive_log_filter(
        &log,
        CompiledGrep::new("healthz"),
        Sense::Drop,
    )
    .expect("a log can be filtered");
    let visible = excluded.log_visible().expect("a filter has a visible set");
    assert_eq!(visible.len(), 2, "both health lines are gone");
    let kept: Vec<String> = excluded
        .log_store()
        .unwrap()
        .with_read(|i| i.lines.iter().map(|l| l.content.clone()).collect());
    assert!(kept.iter().all(|l| !l.contains("healthz")), "and really gone: {kept:?}");
    assert!(kept.iter().any(|l| l.contains("worker failed")), "the rest survive: {kept:?}");

    // It LABELS itself with the `!`, so a chain of four is readable.
    assert_eq!(excluded.log_committed_patterns(), vec!["!healthz".to_string()]);

    // And contributes NO highlight: every line it matched is off screen, so
    // painting its matches would colour the one thing certainly not there.
    assert!(
        excluded.log_patterns().is_empty(),
        "an excluding step highlights nothing: {:?}",
        excluded.log_patterns(),
    );

    // Chains mix: exclude then narrow.
    let mut both = Element::derive_log_filter(&excluded, CompiledGrep::new("failed"), Sense::Keep)
    .expect("a filter can be filtered");
    assert_eq!(both.log_visible().expect("visible").len(), 1, "worker failed alone");
    assert_eq!(
        both.log_committed_patterns(),
        vec!["!healthz".to_string(), "failed".to_string()],
        "the chain reads in order, senses included",
    );
    assert_eq!(
        both.log_patterns(),
        vec!["failed".to_string()],
        "only the narrowing step highlights",
    );
}

/// "From the beginning" is reachable, which is the other half of P1.
///
/// `restart_range` took an `Option<String>`: `None` meant tail and `Some(d)`
/// meant the last `d`. There was no third state, so the digits capped the
/// reachable history at 24h — and the original complaint was that the OLDEST
/// logs could not be reached. `Retain::Head` and `stop_if_head_complete` were
/// built for exactly that and then left unaskable.
///
/// No wire change: `since` and `tail` were always independent, and both
/// absent is "everything" to the apiserver. What was missing was a name for
/// that combination.
#[test]
fn a_log_range_can_ask_for_everything_from_the_beginning() {
    use crate::actions::LogRange;

    let session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        k9rs_proto::protocol::LogContainer::Default,
    );
    // A fresh stream is a tail: a bounded number of lines, newest kept.
    assert_eq!(session.current_range(), LogRange::Tail);
    let (_, _, label) = session.header();
    assert_eq!(label, "tail", "and says so");

    // All three ranges round-trip through the spec, which is the only place
    // the choice is stored — so a revival cannot change what you were
    // looking at.
    for range in [
        LogRange::Tail,
        LogRange::Head,
        LogRange::Since("24h".to_string()),
    ] {
        let mut s = LogSession::for_test(
            k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
            k9rs_proto::protocol::LogContainer::Default,
        );
        s.write_range(range.clone());
        assert_eq!(s.current_range(), range, "the spec remembers {range:?}");
    }

    // The header distinguishes all three, so you can tell a bounded tail
    // from an unbounded history at a glance.
    let labels: Vec<String> = [LogRange::Tail, LogRange::Head, LogRange::Since("15m".into())]
        .into_iter()
        .map(|r| {
            let mut s = LogSession::for_test(
                k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
                k9rs_proto::protocol::LogContainer::Default,
            );
            s.write_range(r);
            s.header().2
        })
        .collect();
    assert_eq!(labels, vec!["tail", "all", "15m"]);
}

/// The two accessors where a wildcard would be a GUESS are exhaustive.
///
/// A 2026-09 audit counted the `_ =>` arms as one hazard; most are answers
/// (an `Option` for "the kinds that have one", or delegation to the general
/// case). Two were guesses a new kind would silently inherit and be wrong
/// about, and those are the ones pinned here — adding an element kind must
/// force a decision about its liveness and its stream, and nowhere else.
#[test]
fn a_new_element_kind_must_declare_liveness_and_stream_ownership() {
    // This test is a STATEMENT, checked by the compiler rather than by
    // asserting: `is_live` and `abort_data_stream` both match every variant
    // by name, so adding one breaks the build in those two arms. What the
    // assertions pin is the ANSWERS, because an exhaustive match that
    // answered wrongly would still compile.
    let app = crate::App::new_for_test();

    // A table with a live query reports its query's liveness — not a
    // blanket `true`, which is what suppressed the stale banner.
    assert!(app.nav.top().is_live(), "a fresh list is live");

    // A derived kind rides its ancestor's stream, so it reports live and
    // owns nothing to abort. Both are deliberate, not defaults.
    let filter = crate::element::Element::derive_filter(
        app.nav.top(),
        crate::store::RowPredicate::Grep(crate::nav::CompiledGrep::new("x")),
    )
    .expect("a table can be filtered");
    assert!(filter.is_live(), "a filter's liveness is its ancestor's");
    // Aborting a stream it does not own is a no-op, not a panic: the
    // ancestor is aborted on its own turn in the walk.
    filter.abort_data_stream();

    // A dialog is chrome: live by definition, owns no stream.
    let confirm = crate::element::Element::Confirm(Box::new(crate::ConfirmDialog {
        message: "x".to_string(),
        action_label: "Delete".to_string(),
        pending: crate::PendingAction::Single {
            op: crate::SingleOp::Delete,
            target: k9rs_proto::protocol::ObjectRef::new(
                crate::nav::rid(k9rs_proto::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
        },
        action_focused: false,
    }));
    assert!(confirm.is_live());
    confirm.abort_data_stream();
}

/// `Esc` peels ONE grep off the chain, which is the only way to undo one.
///
/// I documented this in the keymap (`q / Esc` — "Back — pops one grep off the
/// chain") and never tested it: the agreement test probes `plain('q')`, so
/// `Esc` was a claim help made with nothing behind it. Written up as "worth
/// re-checking on the next run", which is how a documented-but-unverified
/// behaviour becomes a lie later.
#[test]
fn esc_peels_one_grep_off_the_chain() {
    use crate::actions::Action;
    use crate::element::Sense;
    use k9rs_proto::protocol::{LogContainer, LogLine};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let mut app = crate::App::new_for_test();
    let session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::One("pod-x".to_string()),
        LogContainer::Default,
    );
    for text in ["alpha beta", "alpha only", "beta only"] {
        session.store().push(0, LogLine::untagged(text));
    }
    app.nav.push(Element::LogSession(Box::new(session)));

    for pat in ["alpha", "beta"] {
        let el = Element::derive_log_filter(
            app.nav.top(),
            CompiledGrep::new(pat),
            Sense::Keep,
        )
        .expect("a log can be filtered");
        app.nav.push(el);
    }
    assert_eq!(app.nav.top().log_committed_patterns().len(), 2, "two greps deep");

    // BOTH keys the log class documents reach `Back`, which is what pops.
    for key in [KeyCode::Esc, KeyCode::Char('q')] {
        let action = crate::handler::handle_key_event(
            &app,
            KeyEvent::new(key, KeyModifiers::NONE),
        );
        assert!(
            matches!(action, Some(Action::Back)),
            "{key:?} in a log view must reach Back — the keymap says so",
        );
    }

    // And Back really takes ONE level, not the whole chain.
    app.nav.pop();
    assert_eq!(
        app.nav.top().log_committed_patterns(),
        vec!["alpha".to_string()],
        "one press, one grep — the outer one survives",
    );
    app.nav.pop();
    assert!(
        app.nav.top().log_committed_patterns().is_empty(),
        "the second press leaves the bare log session",
    );
    assert!(
        matches!(app.nav.top(), Element::LogSession(_)),
        "…and that IS the session, not something popped too far",
    );
}
