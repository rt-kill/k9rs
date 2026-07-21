//! The navigation stack: a strict-LIFO vector of self-contained
//! [`Element`]s (see [`crate::app::element`]).
//!
//! The stack owns exactly two things: HISTORY (which scopes the user
//! walked through) and UNDO (pop = drop, RAII releases the popped
//! element's handles). Elements are fully self-defining — the stack never
//! interprets them, and display blindly renders the top.
//!
//! Invariants, held structurally:
//! - **Never empty**: [`NavStack::pop`] refuses to remove the root, so
//!   [`NavStack::top`] needs no `expect`.
//! - **Peek-only derivation**: there is no index accessor; push-sites
//!   receive `&Element` (the top) from [`NavStack::top`], and the
//!   derivation constructors on [`Element`] take an element, never the
//!   stack.
//! - **Top-to-bottom teardown**: children hold backward `Arc`s into
//!   parents' stores, so parents must outlive children. A bare `Vec`
//!   drops front-to-back (= bottom-first = WRONG); [`Drop`] and
//!   [`NavStack::reset`] drain via `pop()` back-to-front.
//! - Ancestor access happens only through named semantic methods that
//!   walk internally (`ensure_top_live`, `apply_resolved`) — data-plane
//!   maintenance, not scope interpretation.

use std::sync::Arc;

use crate::app::element::Element;
use crate::kube::client_session::ClientSession;
use crate::kube::protocol::ResourceId;
use crate::util::SearchPattern;

/// Typed K8s field selector. Defined in [`crate::kube::resources::row`]
/// because it rides the wire inside `DrillTarget` (WIRE-FROZEN there);
/// re-exported here because drill construction is its main client-side
/// consumer.
pub use crate::kube::resources::row::K8sFieldSelector;

// ---------------------------------------------------------------------------
// CompiledGrep — a pattern compiled exactly once
// ---------------------------------------------------------------------------

/// A grep pattern compiled exactly once, at predicate construction.
/// Carries both the source text (for crumbs and completion round-trips)
/// and the compiled [`SearchPattern`] so per-frame derives never
/// recompile.
#[derive(Debug, Clone)]
pub struct CompiledGrep {
    source: String,
    pattern: SearchPattern,
}

impl CompiledGrep {
    /// Compile from raw user text. Smartcase rules live inside
    /// [`SearchPattern::new`].
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let pattern = SearchPattern::new(&source);
        Self { source, pattern }
    }

    /// The original pattern text (crumb rendering, debug output).
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The compiled pattern — no allocation, no recompile in hot paths.
    pub fn pattern(&self) -> &SearchPattern {
        &self.pattern
    }
}

// ---------------------------------------------------------------------------
// RootSpec — the recipe for reconstructing a root (NOT a live element)
// ---------------------------------------------------------------------------

/// What kind of root the stack had before the last reset — a construction
/// recipe for the `-` (toggle last view) key, deliberately NOT a live
/// element (a stashed element would keep dead subscriptions alive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootSpec {
    Resource(ResourceId),
}

// ---------------------------------------------------------------------------
// NavStack
// ---------------------------------------------------------------------------

/// The navigation stack. `items[0]` is the root, `items.last()` the top.
#[derive(Debug)]
pub struct NavStack {
    items: Vec<Element>,
    prev_root: Option<RootSpec>,
}

impl NavStack {
    pub fn new(root: Element) -> Self {
        Self { items: vec![root], prev_root: None }
    }

    /// The top element — the only element the TUI interprets.
    pub fn top(&self) -> &Element {
        self.items.last().expect("NavStack is never empty (pop refuses the root)")
    }

    pub fn top_mut(&mut self) -> &mut Element {
        self.items.last_mut().expect("NavStack is never empty (pop refuses the root)")
    }

    /// Push a derived element. The outgoing top is covered: its memoized
    /// view is dropped (ephemeral by rule — rebuilt on first read after
    /// pop), its persistent interaction state stays untouched.
    pub fn push(&mut self, element: Element) {
        if let Some(covered) = self.items.last_mut() {
            covered.drop_view_cache();
        }
        self.items.push(element);
    }

    /// Pop the top element (undo one scope transition). `None` at the
    /// root — callers flash "at root" rather than crash. The returned
    /// element drops at the caller's discretion; its backward Arcs
    /// release then.
    pub fn pop(&mut self) -> Option<Element> {
        if self.items.len() > 1 {
            self.items.pop()
        } else {
            None
        }
    }

    /// Replace the whole stack with a fresh root (`:cmd`, tab cycling,
    /// namespace/context switch). Records the outgoing ROOT's recipe for
    /// `-`, then drains the old stack **top to bottom** — children release
    /// their backward Arcs before the parents they point into drop.
    pub fn reset(&mut self, new_root: Element) {
        self.prev_root = self.items.first().and_then(root_spec_of);
        while self.items.pop().is_some() {}
        self.items.push(new_root);
    }

    /// Bottom→top walk for the crumb bar and indicator scans. Read-only:
    /// nothing below the top can be interpreted, only labeled.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Element> {
        self.items.iter()
    }

    pub fn depth(&self) -> usize {
        self.items.len()
    }

    pub fn is_drilled(&self) -> bool {
        self.items.len() > 1
    }

    /// The crumb bar: a fold over the elements' self-owned labels.
    pub fn breadcrumb(&self) -> String {
        self.items.iter().map(Element::label).collect::<Vec<_>>().join(" > ")
    }

    /// The recipe of the root before the last reset (`-` toggle).
    pub fn prev_root(&self) -> Option<&RootSpec> {
        self.prev_root.as_ref()
    }

    /// The current root's recipe (recorded by the ns/context-switch paths
    /// to rebuild the same root under a new scope).
    pub fn root_spec(&self) -> Option<RootSpec> {
        self.items.first().and_then(root_spec_of)
    }

    /// Convenience: the top's resource id, if resource-backed.
    pub fn resource_id(&self) -> Option<&ResourceId> {
        self.top().rid()
    }

    // -- Named data-plane maintenance (the sanctioned below-top walks) ------

    /// Revive the subscription feeding the TOP's data, if its bridge died
    /// (reconnect, failure while covered). The owner is found by STORE
    /// POINTER IDENTITY — never by rid, so two same-rid elements with
    /// different filters can never cross-revive.
    pub fn ensure_top_live(&mut self, session: &ClientSession) {
        // Table kinds: revive the owning ResourceList's subscription.
        if let Some(store) = self.top().data_store().cloned() {
            for element in self.items.iter_mut().rev() {
                if let Element::ResourceList(list) = element {
                    if Arc::ptr_eq(list.query().store(), &store) {
                        if !list.query().is_live() {
                            list.query_mut().resubscribe(session);
                        }
                        return;
                    }
                }
            }
            return;
        }
        // Log kinds: revive the owning LogSession's stream. A dead log
        // stream otherwise renders as an eternally-following live view
        // (its `Ended` died in the closed channel), so this is what makes
        // a log view honest across a reconnect or a pop that reveals it.
        if let Some(store) = self.top().log_store().cloned() {
            for element in self.items.iter_mut().rev() {
                if let Element::LogSession(s) = element {
                    if Arc::ptr_eq(s.store(), &store) {
                        s.revive_if_dead(session);
                        return;
                    }
                }
            }
        }
    }

    /// Ctrl-R: force-refresh the subscription feeding the TOP's data —
    /// found by store pointer identity, re-run with the element's OWN
    /// stored query spec (never the ambient selector).
    pub fn refresh_top_query(&mut self, session: &ClientSession) {
        let Some(store) = self.top().data_store().cloned() else { return };
        for element in self.items.iter_mut().rev() {
            if let Element::ResourceList(list) = element {
                if Arc::ptr_eq(list.query().store(), &store) {
                    list.query_mut().refresh(session);
                    return;
                }
            }
        }
    }

    /// Route a log-stream control (range restart) to the SESSION that
    /// owns the top's line store — found by pointer identity, the same
    /// named-walk discipline as `ensure_top_live`.
    pub fn with_log_session_of(
        &mut self,
        store: &Arc<crate::app::store::LineStore>,
        f: impl FnOnce(&mut crate::app::element::LogSession),
    ) {
        for element in self.items.iter_mut().rev() {
            if let Element::LogSession(session) = element {
                if Arc::ptr_eq(session.store(), store) {
                    f(session);
                    return;
                }
            }
        }
    }

    /// The server resolved a rid to its true identity: elements update
    /// themselves in place (the element IS the identity — no global maps
    /// to rekey). Refinements carry a VALUE COPY of the rid, so they
    /// update alongside their lists — an `ObjectRef` built from a
    /// filter-on-top must never carry the stale pre-resolution identity.
    pub fn apply_resolved(&mut self, original: &ResourceId, resolved: &ResourceId) {
        for element in self.items.iter_mut() {
            match element {
                Element::ResourceList(list) => {
                    if list.rid() == original {
                        list.apply_resolved(resolved.clone());
                    }
                }
                Element::RowFilter(filter) => filter.apply_resolved(original, resolved),
                _ => {}
            }
        }
    }

    // -- Fault-filter helpers (Ctrl-Z) ---------------------------------------

    /// Whether the TOP element is a fault filter (poppable by Ctrl-Z).
    pub fn top_is_fault(&self) -> bool {
        self.top().is_fault_filter()
    }

    /// Whether ANY element on the stack is a fault filter. Under strict
    /// LIFO a buried fault step can't be spliced out (the old mid-chain
    /// splice is gone — children's value-copied predicate chains would
    /// not have updated anyway); Ctrl-Z on a buried fault flashes instead.
    pub fn any_fault(&self) -> bool {
        self.items.iter().any(Element::is_fault_filter)
    }
}

impl Drop for NavStack {
    /// Whole-stack teardown (context switch, app exit) must ALSO run
    /// top-to-bottom — a bare Vec drop is bottom-first, which would tear
    /// parents down under children still holding backward Arcs into them.
    fn drop(&mut self) {
        while self.items.pop().is_some() {}
    }
}

fn root_spec_of(root: &Element) -> Option<RootSpec> {
    root.rid().cloned().map(RootSpec::Resource)
}

/// Convenience helper: build a built-in `ResourceId` from a typed
/// [`BuiltInKind`](crate::kube::resource_def::BuiltInKind). Compile-time
/// checked: typos become E0599, not runtime panics.
pub fn rid(kind: crate::kube::resource_def::BuiltInKind) -> ResourceId {
    ResourceId::BuiltIn(kind)
}

// ---------------------------------------------------------------------------
// FilterInputState — the input widget state (while the user types `/`/`~`)
// ---------------------------------------------------------------------------

/// The filter input widget state — element-owned draft interaction state
/// (each element keeps its own; covering an element never leaks its draft
/// into another). Fields are private — mutation goes through typed
/// methods that enforce valid transitions.
#[derive(Debug, Default, Clone)]
pub struct FilterInputState {
    active: bool,
    text: String,
    column: Option<ColumnTarget>,
}

/// Column restriction for `~` mode: the DATA index the filter matches
/// against plus the header name captured at activation — one value, so
/// the pair can't drift (the predicate uses the index, the crumb shows
/// the header).
#[derive(Debug, Clone)]
pub(crate) struct ColumnTarget {
    pub index: usize,
    pub header: String,
}

/// Committed filter text + optional column restriction, returned by
/// [`FilterInputState::commit`].
#[derive(Debug)]
pub(crate) struct CommittedFilter {
    pub text: String,
    pub column: Option<ColumnTarget>,
}

impl FilterInputState {
    // -- Read-only accessors --------------------------------------------------

    /// Whether the filter bar is in edit mode (listening for keystrokes).
    pub fn active(&self) -> bool {
        self.active
    }
    /// The text being typed (not yet committed as a RowFilter element).
    pub fn text(&self) -> &str {
        &self.text
    }
    /// Column restriction index, if any (`~` mode).
    pub fn column(&self) -> Option<usize> {
        self.column.as_ref().map(|c| c.index)
    }

    // -- State transitions ----------------------------------------------------

    /// Enter filter mode for all columns (`/`).
    pub fn start(&mut self) {
        self.active = true;
        self.text.clear();
        self.column = None;
    }

    /// Enter column-restricted filter mode (`~`). Takes the header name
    /// alongside the data index — captured here because the caller has
    /// the visible-column mapping in hand at activation.
    pub fn start_column(&mut self, col: usize, header: String) {
        self.active = true;
        self.text.clear();
        self.column = Some(ColumnTarget { index: col, header });
    }

    /// Cancel without committing — discards text, resets column, exits
    /// edit mode.
    pub fn cancel(&mut self) {
        self.text.clear();
        self.column = None;
        self.active = false;
    }

    /// Commit the typed text. Exits edit mode; returns `None` if the text
    /// is empty (nothing to commit).
    pub(crate) fn commit(&mut self) -> Option<CommittedFilter> {
        self.active = false;
        let text = std::mem::take(&mut self.text);
        let column = self.column.take();
        if text.is_empty() {
            None
        } else {
            Some(CommittedFilter { text, column })
        }
    }

    // -- Keystroke handling ---------------------------------------------------

    pub fn push_char(&mut self, c: char) {
        self.text.push(c);
    }
    pub fn pop_char(&mut self) {
        self.text.pop();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
            false,
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
}
