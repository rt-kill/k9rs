use super::*;
use crate::app::element::{ContentSpec, ContentView, Element, QuerySpec, ResourceList};
use crate::app::store::{MetricsHub, RowPredicate};
use crate::kube::protocol::Namespace;
use crate::kube::resource_def::BuiltInKind;

fn list(kind: BuiltInKind) -> Element {
    Element::ResourceList(ResourceList::open_for_test(
        QuerySpec {
            rid: ResourceId::BuiltIn(kind),
            namespace: Namespace::All,
            filter: None,
        },
        &MetricsHub::new(),
        ResourceId::BuiltIn(kind).short_label().to_lowercase(),
    ))
}

#[test]
fn pop_refuses_the_root_and_lifo_holds() {
    let mut stack = NavStack::new(list(BuiltInKind::Pod));
    assert!(stack.pop().is_none(), "root never pops");
    stack.push(list(BuiltInKind::Deployment));
    assert_eq!(stack.depth(), 2);
    assert!(stack.is_drilled());
    assert!(stack.pop().is_some());
    assert_eq!(stack.depth(), 1);
    assert!(!stack.is_drilled());
    assert!(stack.pop().is_none());
}

#[test]
fn reset_drains_everything_and_records_prev_root() {
    let mut stack = NavStack::new(list(BuiltInKind::Pod));
    let root_store = std::sync::Arc::clone(stack.top().data_store().unwrap());
    stack.push(
        Element::derive_filter(
            stack.top(),
            RowPredicate::Grep(CompiledGrep::new("x")),
        )
        .unwrap(),
    );
    assert_eq!(stack.depth(), 2);
    stack.reset(list(BuiltInKind::Node));
    assert_eq!(stack.depth(), 1);
    // Every old element dropped — no leaked backward Arcs (only our
    // local handle survives).
    assert_eq!(std::sync::Arc::strong_count(&root_store), 1);
    // The recipe of the OLD root was recorded for `-`.
    assert_eq!(
        stack.prev_root(),
        Some(&RootSpec::Resource(ResourceId::BuiltIn(BuiltInKind::Pod)))
    );
    assert_eq!(
        stack.root_spec(),
        Some(RootSpec::Resource(ResourceId::BuiltIn(BuiltInKind::Node)))
    );
}

#[test]
fn breadcrumb_is_a_label_fold() {
    let mut stack = NavStack::new(list(BuiltInKind::Pod));
    stack.push(
        Element::derive_filter(
            stack.top(),
            RowPredicate::Grep(CompiledGrep::new("api")),
        )
        .unwrap(),
    );
    stack.push(Element::ContentView(ContentView::new(
        ContentSpec::Aliases,
        crate::app::ContentViewState::default(),
        crate::app::element::ContentPhase::Ready,
    )));
    assert_eq!(stack.breadcrumb(), "pods > /api > aliases");
}

#[test]
fn fault_helpers_see_only_elements() {
    let mut stack = NavStack::new(list(BuiltInKind::Pod));
    assert!(!stack.top_is_fault());
    assert!(!stack.any_fault());
    stack.push(Element::derive_filter(stack.top(), RowPredicate::Fault).unwrap());
    assert!(stack.top_is_fault());
    assert!(stack.any_fault());
    // Bury it: a grep on top — Ctrl-Z must NOT splice; the helpers
    // report "buried" (top no, any yes).
    stack.push(
        Element::derive_filter(
            stack.top(),
            RowPredicate::Grep(CompiledGrep::new("x")),
        )
        .unwrap(),
    );
    assert!(!stack.top_is_fault());
    assert!(stack.any_fault());
}

#[test]
fn apply_resolved_updates_matching_lists_in_place() {
    let unresolved = ResourceId::CrdUnresolved("widgets".to_string());
    let mut stack = NavStack::new(Element::ResourceList(ResourceList::open_for_test(
        QuerySpec { rid: unresolved.clone(), namespace: Namespace::All, filter: None },
        &MetricsHub::new(),
        "widgets".to_string(),
    )));
    let resolved = ResourceId::BuiltIn(BuiltInKind::Pod);
    stack.apply_resolved(&unresolved, &resolved);
    assert_eq!(stack.resource_id(), Some(&resolved));
}
