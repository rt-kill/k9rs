//! The navigation stack: a strict-LIFO vector of self-contained
//! [`Element`]s (see [`crate::element`]).
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
//!   parents' stores. `Arc` makes any order memory-SAFE (a child's clone
//!   keeps the store alive), so this is about ordering EFFECTS, not
//!   soundness: bottom-first would abort a parent's subscription while a
//!   child was still reading its store. A bare `Vec` drops front-to-back
//!   (= bottom-first); [`Drop`] and [`NavStack::reset`] drain via `pop()`
//!   back-to-front instead.
//! - Ancestor access happens only through named semantic methods that
//!   walk internally (`ensure_top_live`, `apply_resolved`) — data-plane
//!   maintenance, not scope interpretation.

use std::sync::Arc;

use crate::element::Element;
use crate::client::ClientSession;
use k9rs_proto::protocol::ResourceId;
use k9rs_util::SearchPattern;

/// Typed K8s field selector. Defined in [`k9rs_proto::row`]
/// because it rides the wire inside `DrillTarget` (WIRE-FROZEN there);
/// re-exported here because drill construction is its main client-side
/// consumer.
pub use k9rs_proto::row::K8sFieldSelector;

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

    /// Commit the filter draft on the top element: the typed text becomes one
    /// `RowFilter` pushed over the top's output.
    ///
    /// ONE operation, because every part of it belongs to this stack — the
    /// draft lives on the top element, the predicate is derived from that
    /// element's columns, and the result is a push. The session layer used to
    /// do the four steps itself, which forced `CommittedFilter` and
    /// `ColumnTarget` — two types whose whole job is to carry three strings
    /// from `commit()` to `predicate_for_commit()` on the next line — to be
    /// part of this crate's API.
    ///
    /// An empty draft commits nothing. So does a top with no row output (a
    /// projection): the draft simply clears, which is what it did before.
    pub fn commit_filter(&mut self) {
        let Some(committed) = self.top_mut().filter_input_mut().and_then(|fi| fi.commit()) else {
            return;
        };
        let predicate = crate::element::predicate_for_commit(
            committed.text,
            committed.column.map(|c| (c.index, c.header)),
        );
        if let Ok(el) = crate::element::Element::derive_filter(self.top(), predicate) {
            self.push(el);
        }
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

    /// RENDER-ONLY: lift the top element off so the view BENEATH it can be
    /// drawn, for a floating element ([`crate::element::ViewExtent`]).
    /// Returns `None` at depth 1 — a float over nothing is just a view.
    ///
    /// Paired with [`Self::replace_lifted`], and the pair exists because
    /// every draw function renders `nav.top()`: the alternative is threading
    /// an element index through all seven of them so that six can ignore it.
    /// The lift lasts exactly one render call and nothing observes the stack
    /// in between — the draw path is synchronous and takes no events.
    pub fn lift_top_for_render(&mut self) -> Option<Element> {
        if self.items.len() < 2 {
            return None;
        }
        self.items.pop()
    }

    /// Put back what [`Self::lift_top_for_render`] took.
    pub fn replace_lifted(&mut self, element: Element) {
        self.items.push(element);
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
        if let Some(store) = self.top().log_root_store().cloned() {
            let mut restarted = false;
            for element in self.items.iter_mut().rev() {
                if let Element::LogSession(s) = element {
                    if Arc::ptr_eq(s.store(), &store) {
                        restarted = s.revive_if_dead(session);
                        break;
                    }
                }
            }
            // A revive CLEARS the session store, so anything deriving from it
            // is holding lines from the range that was just replaced. Done
            // here rather than inside the loop because only the stack can see
            // both sides of that relationship.
            if restarted {
                self.reset_log_filters_of(&store);
            }
        }
    }

    /// Abort every element's owned live stream — called at the reconnect /
    /// switch choke point so old bridges stop retrying against the dropped
    /// session's now-dead mux (the parked ones exit cleanly on their own, but
    /// the mid-retry ones would otherwise burn a backoff loop). Streams are
    /// revived on `ConnectionEstablished` (top) / on pop (covered), or the
    /// elements are dropped by a switch's nav reset. No-op for streamless kinds.
    pub fn abort_all_streams(&self) {
        for element in self.items.iter() {
            element.abort_data_stream();
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

    /// The committed grep chain over the CURRENT log view, oldest step
    /// first: each step as the user typed it, and how many lines survive it.
    ///
    /// The count is the step's own `retained` length, which is what makes a
    /// dead chain diagnosable: the bar used to show one aggregate, so
    /// `[3 filters, 0 visible]` told you the chain was empty but not WHICH
    /// step emptied it — and with a conjunction that is the only question
    /// worth asking.
    ///
    /// Walked from the top DOWN and stopped at the first non-filter, so a
    /// log view reached from inside another one reports its own chain rather
    /// than both.
    pub fn log_chain(&self) -> Vec<(String, usize)> {
        let mut out: Vec<(String, usize)> = Vec::new();
        for element in self.items.iter().rev() {
            match element {
                Element::LogFilter(f) => {
                    out.push((f.own_label(), f.retained_len()));
                }
                _ => break,
            }
        }
        out.reverse();
        out
    }

    /// Stop any range fetch that has collected the oldest N it asked for.
    /// Returns true if one was stopped, so the caller can say so.
    pub fn stop_completed_head_fetches(&mut self) -> bool {
        let mut stopped = false;
        for element in self.items.iter_mut() {
            if let Element::LogSession(s) = element {
                stopped |= s.stop_if_head_complete();
            }
        }
        stopped
    }

    /// Reset every filter derived from `store` — call sites are exactly the
    /// places that RESTART a log range, because `restart_range` empties the
    /// session store and only the stack can see who was deriving from it.
    pub fn reset_log_filters_of(&mut self, store: &Arc<crate::store::LineStore>) {
        for element in self.items.iter_mut() {
            if let Element::LogFilter(f) = element {
                if Arc::ptr_eq(f.source_store(), store) {
                    f.restarted_from_parent();
                }
            }
        }
    }

    /// Advance every BURIED log filter's drain cursor.
    ///
    /// Retention is per-element now, and `log_visible` — the only other
    /// thing that drains — runs on the top element alone. Without this, a
    /// filter with anything stacked above it silently stops collecting and
    /// comes back with a hole.
    pub fn keep_log_filters_current(&mut self) {
        for element in self.items.iter_mut() {
            if let Element::LogFilter(f) = element {
                f.keep_up();
            }
        }
    }

    /// Every log view on the stack, not just the top one.
    ///
    /// `initial_load` is SET by `restart_range`, which reaches a BURIED
    /// session (`with_log_session_of` walks the whole stack by store
    /// identity — a range key or a reconnect while a grep is on top). It was
    /// CLEARED on `nav.top_mut()` only, so the flag latched on the session
    /// while the drain cleared the filter's already-false copy. Escaping
    /// back to the session then rendered the OLDEST retained lines at the
    /// top of the box while the indicator said "AutoScroll: On" — and it
    /// self-healed only if the stream kept producing, so a quiet pod stayed
    /// stuck.
    pub fn for_each_log_view(
        &mut self,
        mut f: impl FnMut(&mut crate::element::LogInteraction),
    ) {
        for element in self.items.iter_mut() {
            if let Some(view) = element.log_view_mut() {
                f(view);
            }
        }
    }

    /// Run `f` over every ContentView in the stack — the delivery walk for
    /// fetched content (yaml / describe / decoded secrets) and the
    /// failure-marking walk at the session-rebuild choke point. Routing is
    /// by each view's OWN spec (content identity), wherever it sits:
    /// covered views fill while hidden, so a pop-reveal shows the content
    /// instead of a spinner orphaned by a deliver-to-top-only rule. Same
    /// named-walk discipline as `ensure_top_live` — data-plane maintenance,
    /// not scope interpretation.
    pub fn for_each_content_view(
        &mut self,
        mut f: impl FnMut(&mut crate::element::ContentView),
    ) {
        for element in self.items.iter_mut() {
            if let Element::ContentView(cv) = element {
                f(cv);
            }
        }
    }

    /// Route a log-stream control (range restart) to the SESSION that
    /// owns the top's line store — found by pointer identity, the same
    /// named-walk discipline as `ensure_top_live`.
    pub fn with_log_session_of(
        &mut self,
        store: &Arc<crate::store::LineStore>,
        f: impl FnOnce(&mut crate::element::LogSession),
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
/// [`BuiltInKind`](k9rs_proto::resource_def::BuiltInKind). Compile-time
/// checked: typos become E0599, not runtime panics.
pub fn rid(kind: k9rs_proto::resource_def::BuiltInKind) -> ResourceId {
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
    text: crate::text_input::TextInput,
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
        self.text.value()
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
    /// is empty (nothing to commit). Private: the only caller is
    /// [`NavStack::commit_filter`], which is the whole operation.
    fn commit(&mut self) -> Option<CommittedFilter> {
        self.active = false;
        let text = self.text.take();
        let column = self.column.take();
        if text.is_empty() {
            None
        } else {
            Some(CommittedFilter { text, column })
        }
    }

    // -- Keystroke handling ---------------------------------------------------

    /// Direct access for editing — the caret and its boundary invariant are
    /// the input's to keep, not this wrapper's.
    pub fn input_mut(&mut self) -> &mut crate::text_input::TextInput {
        &mut self.text
    }

    /// Caret position for the renderer, in columns.
    pub fn caret_col(&self) -> usize {
        self.text.cursor_col()
    }

}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/nav.rs"]
mod tests;
