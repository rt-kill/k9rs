use super::*;
use crate::app::store::StorePayload;
use crate::kube::protocol::{RowChange, TableBaseline, TableDelta};
use crate::kube::resource_def::BuiltInKind;
use crate::kube::resources::row::{CellValue, ContainerInfo};

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
    el.filter_input_mut().unwrap().push_char('a');
    let v4 = el.view(ColumnLevel::Default, 40);
    assert_eq!(v4.keys.len(), 1);
    assert_eq!(v4.rows[0][0], "a");

    // Sort change invalidates.
    el.filter_input_mut().unwrap().cancel();
    // Default sort is already (col 0, ascending) — one call toggles
    // to descending.
    el.sort_by(crate::app::SortTarget::Column(0));
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
    pod.containers = vec![ContainerInfo {
        name: "api".into(),
        image: "img:1".into(),
        state: crate::kube::resources::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
        kind: crate::kube::resources::row::ContainerKind::Regular,
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
            changes: vec![RowChange::Remove(crate::app::store::row_key(&pod))],
        }),
    );
    let v = containers.view(ColumnLevel::Default, 40);
    assert_eq!(v.keys.len(), 0);
}

#[test]
fn log_filters_chain_and_scroll_heals_on_eviction() {
    use crate::kube::protocol::{LogContainer, LogLine};
    let mut session = Element::LogSession(Box::new(LogSession::for_test(
        crate::app::ContainerRef::new("pod-x", "default", LogContainer::Default),
    )));
    let store = Arc::clone(session.log_store().unwrap());
    for i in 0..10 {
        store.push(1, LogLine { content: format!("line-{i} {}", if i % 2 == 0 { "err" } else { "ok" }), container: None });
    }
    assert_eq!(session.log_visible().unwrap().len(), 10);
    assert_eq!(session.log_header().unwrap().0, "pod-x");

    let mut filter =
        Element::derive_log_filter(&session, CompiledGrep::new("err")).unwrap();
    assert_eq!(filter.label(), "/err");
    assert_eq!(filter.log_committed_count(), 1);
    assert_eq!(filter.log_visible().unwrap().len(), 5);
    // Draft narrows further (a derive input — no rebuild call).
    filter.log_view_mut().unwrap().draft = Some("line-2".to_string());
    assert_eq!(filter.log_visible().unwrap().len(), 1);
    filter.log_view_mut().unwrap().draft = None;

    // Scroll healing: park the viewport (physical offset), evict past the
    // ring cap, and the next read pulls the offset back by the evicted count.
    {
        let vp = &mut filter.log_view_mut().unwrap().viewport;
        vp.set_metrics(100, 10, false); // give it scroll headroom
        vp.scroll_to(4);
    }
    let cap = 50_000; // default LogConfig max_lines
    for i in 0..(cap - 10 + 3) {
        store.push(1, LogLine { content: format!("fill-{i}"), container: None });
    }
    let _ = filter.log_visible();
    assert_eq!(filter.log_view().unwrap().viewport.offset(), 1, "offset healed by 3 evictions");
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

/// DerivedRows (container projections) refuse marking outright: their
/// rows have no backing store to prune marks against and no batch
/// operation consumes them — an unprunable decoration would hold the
/// app in a phantom select mode.
#[test]
fn derived_rows_refuse_marks() {
    let mut el = list_element(Namespace::All);
    let mut pod = row("web", "ns", &["web", "Running"]);
    pod.containers = vec![ContainerInfo {
        name: "main".into(),
        kind: Default::default(),
        image: "img".into(),
        state: crate::kube::resources::row::ContainerState::Running,
        reason: None,
        ready: true,
        restart_count: 0,
    }];
    seed(&el, &["NAME", "STATUS"], vec![pod.clone()]);
    let _ = el.view(ColumnLevel::Default, 40);
    let mut derived = Element::derive_projection(&el, &pod, DerivedViewKind::Containers)
        .expect("containers projection");
    let _ = derived.view(ColumnLevel::Default, 40);
    derived.select(0);
    assert_eq!(derived.toggle_mark(), MarkOutcome::Unsupported);
    assert_eq!(derived.span_mark(), SpanOutcome::Unsupported);
    assert!(!derived.has_marks());
    assert!(derived.marked_keys().is_empty());
    // And crucially: its mark reads never leak the PARENT store's
    // marks (data_store() points at the parent's store).
    el.data_store().unwrap().toggle_mark(&crate::app::store::row_key(&pod));
    assert!(!derived.has_marks(), "parent marks don't put a projection in select mode");
    derived.clear_marks();
    assert!(el.data_store().unwrap().has_marks(), "projection clear_marks can't reach the parent store");
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

/// One live instance of EVERY element kind. Deliberately exhaustive by
/// construction: the match below has no wildcard, so a new kind fails to
/// compile until it is built here and given a class.
fn one_of_every_kind() -> Vec<crate::app::element::Element> {
    use crate::app::element::{ContentPhase, ContentSpec, ContentView, Element, LogSession};
    use crate::app::store::RowPredicate;
    use crate::kube::protocol::{LogContainer, Namespace};

    let hub = std::sync::Arc::new(crate::app::store::MetricsHub::default());
    let rid = crate::app::nav::rid(BuiltInKind::Pod);
    let table = Element::ResourceList(crate::app::element::ResourceList::client(
        crate::app::element::QuerySpec { rid: rid.clone(), namespace: Namespace::All, filter: None },
        crate::app::store::RowStore::client("pods"),
        &hub,
        "pods".to_string(),
    ));
    let filtered = Element::derive_filter(
        &table,
        RowPredicate::Grep(crate::app::nav::CompiledGrep::new("x")),
    )
    .expect("a table can be filtered");

    let log = Element::LogSession(Box::new(LogSession::for_test(crate::app::ContainerRef::new(
        "pod", "ns", LogContainer::Default,
    ))));
    let log_filtered =
        Element::derive_log_filter(&log, crate::app::nav::CompiledGrep::new("y"))
            .expect("a log can be filtered");

    let content = Element::ContentView(ContentView::new(
        ContentSpec::Aliases,
        crate::app::ContentViewState::default(),
        ContentPhase::Ready,
    ));

    vec![table, filtered, log, log_filtered, content, Element::Overview(crate::app::element::Overview)]
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
    use crate::app::element::ElementClass;
    for mut el in one_of_every_kind() {
        let class = el.class();
        let label = format!("{:?} ({:?})", el.label(), class);
        match class {
            ElementClass::Table => {
                assert!(el.is_table(), "{label}: must report is_table");
                assert!(el.rid().is_some(), "{label}: a table has a resource identity");
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
            }
            ElementClass::Log => {
                assert!(el.log_store().is_some(), "{label}: a log has a line store");
                assert!(el.log_view().is_some(), "{label}: a log has a view");
                assert!(el.log_lines().is_some(), "{label}: a log can materialize lines");
                assert!(!el.is_table(), "{label}: a log is not a table");
                assert!(el.data_store().is_none(), "{label}: a log has no row store");
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

/// Every class is actually exercised above — otherwise the test could pass
/// by covering only the easy kinds.
#[test]
fn every_element_class_is_covered() {
    use crate::app::element::ElementClass;
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
