use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ResourceId, SubscriptionFilter};
use crate::util::SearchPattern;

/// A grep pattern that has been compiled exactly once, at filter
/// construction. Carries both the source text (for breadcrumbs and
/// completion round-trips) and the compiled [`SearchPattern`] so that
/// `reapply_nav_filters` — which runs on every snapshot — doesn't have
/// to recompile.
///
/// Not serializable: `NavFilter` is client-only state and never rides
/// the wire. If it needs to be persisted, persist [`CompiledGrep::source`]
/// and recompile on load.
#[derive(Debug, Clone)]
pub struct CompiledGrep {
    source: String,
    pattern: SearchPattern,
}

impl CompiledGrep {
    /// Compile a grep pattern from raw user text. Smartcase rules live
    /// inside [`SearchPattern::new`] — same behavior as before, just
    /// moved to the construction site instead of reapply time.
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let pattern = SearchPattern::new(&source);
        Self { source, pattern }
    }

    /// The original pattern text, kept for breadcrumb rendering and
    /// debug output.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The compiled pattern. Use this for match checks in hot paths —
    /// no allocation, no recompile.
    pub fn pattern(&self) -> &SearchPattern {
        &self.pattern
    }
}

/// Typed K8s field selector — replaces the previous stringly-typed
/// `NavFilter::Field { field: String, value: String }`. Each variant
/// corresponds to a specific K8s field selector path; the variant data
/// is the value to match against. New field paths get new variants —
/// the wire-format string and breadcrumb are derived from the variant.
///
/// `Serialize`/`Deserialize` so it can ride the wire inside `DrillTarget`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum K8sFieldSelector {
    /// `spec.nodeName=<name>` — pods scheduled on a specific node.
    SpecNodeName(String),
    /// `status.phase=<phase>` — pods in a specific lifecycle phase.
    StatusPhase(String),
}

impl K8sFieldSelector {
    /// Render the selector in the format K8s expects on the wire
    /// (`field.path=value`), suitable for `SubscriptionFilter::Field`.
    pub fn to_wire(&self) -> String {
        match self {
            Self::SpecNodeName(v) => format!("spec.nodeName={}", v),
            Self::StatusPhase(v) => format!("status.phase={}", v),
        }
    }

    /// Short user-facing label for the breadcrumb.
    pub fn breadcrumb(&self) -> String {
        match self {
            Self::SpecNodeName(v) => format!("node={}", v),
            Self::StatusPhase(v) => format!("phase={}", v),
        }
    }
}

/// What subscription changes a nav operation requires. Returned by
/// push/pop/reset so the session layer can open a fresh substream when
/// needed.
///
/// Note: there's no `unsubscribe` field. Unsubscribing is handled by
/// `NavStep::stream`'s `Drop` impl — when a nav step is popped or
/// replaced, its `SubscriptionStream` drops, which aborts the bridge
/// task and closes the yamux substream. There's nothing for the session
/// layer to do explicitly.
pub struct NavChange {
    pub subscribe: Option<ResourceId>,
    /// Server-side filter for the new subscription (Labels, Field, OwnerUid).
    /// None = unfiltered. Only meaningful when `subscribe` is Some.
    pub subscription_filter: Option<SubscriptionFilter>,
}

// ---------------------------------------------------------------------------
// NavFilter — the kinds of filters that can narrow a resource view
// ---------------------------------------------------------------------------

// The fault filter previously carried a regex pattern that matched against
// raw status strings. With `RowHealth` typed by the converter (server-side),
// it now filters directly on `row.health != Normal` — no regex, no string
// match. The constant is gone; the variant is its own implementation.

/// A filter applied at one level of the navigation stack.
#[derive(Debug, Clone)]
pub enum NavFilter {
    /// Text grep across all columns (what `/` does). The pattern is
    /// compiled at construction (see [`CompiledGrep::new`]) so the
    /// hot-path `reapply_nav_filters` never re-parses the regex.
    Grep(CompiledGrep),
    /// Label selector: show only items whose labels contain ALL of these k=v pairs.
    /// Used for deployment→pods, statefulset→pods, etc.
    Labels(BTreeMap<String, String>),
    /// Typed K8s field selector. Replaces the previous stringly-typed
    /// `Field { field: String, value: String }` — the variant carries the
    /// field path so callers can't fat-finger it.
    Field(K8sFieldSelector),
    /// Owner reference chain: show pods owned (directly or transitively) by
    /// the resource with this UID. Follows ownerReferences metadata for 100%
    /// accurate drill-down (Pod → ReplicaSet → Deployment).
    OwnerChain {
        /// UID of the owner resource to match.
        uid: String,
        /// Kind of the owner (typed — breadcrumb display fetches the human
        /// string via [`crate::kube::resource_defs::REGISTRY`]).
        kind: crate::kube::resource_def::BuiltInKind,
        /// Display name of the owner resource.
        display_name: String,
    },
    /// The "fault" filter — show only rows whose typed [`crate::kube::resources::row::RowHealth`]
    /// is `Failed` or `Pending`. The classification happens on the server
    /// (in the row converter), so the filter runs as `row.health != Normal`
    /// instead of regexing status cells.
    Fault,
}

impl NavFilter {
    /// Convert this filter to a server-side subscription filter, if applicable.
    /// Grep and Fault are client-side only and return None.
    pub fn to_subscription_filter(&self) -> Option<SubscriptionFilter> {
        match self {
            NavFilter::Grep(_) | NavFilter::Fault => None,
            NavFilter::Labels(labels) => Some(SubscriptionFilter::Labels(labels.clone())),
            NavFilter::Field(sel) => Some(SubscriptionFilter::Field(sel.to_wire())),
            NavFilter::OwnerChain { uid, .. } => {
                Some(SubscriptionFilter::OwnerUid(uid.clone()))
            }
        }
    }

    /// The compiled grep pattern this filter contributes, if any. Only
    /// [`NavFilter::Grep`] produces a pattern — Fault is now a typed
    /// `row.health` predicate (see [`NavFilter::keep_by_health`]) and
    /// labels/field/owner-chain dispatch through `to_subscription_filter`.
    /// Returns `&CompiledGrep` so callers use the cached pattern without
    /// recompiling — the reason this method replaced the older
    /// `as_grep_pattern() -> Option<&str>` that forced reapply-time
    /// recompiles on every snapshot.
    pub fn as_grep(&self) -> Option<&CompiledGrep> {
        match self {
            NavFilter::Grep(g) => Some(g),
            _ => None,
        }
    }

    /// True if this filter is active and the row's health passes its
    /// predicate. Returns `true` for filter variants that don't filter on
    /// health (so they don't reject rows here — they're handled by other
    /// pipelines).
    pub fn keep_by_health(&self, health: crate::kube::resources::row::RowHealth) -> bool {
        use crate::kube::resources::row::RowHealth;
        match self {
            NavFilter::Fault => !matches!(health, RowHealth::Normal),
            _ => true,
        }
    }
}

// ---------------------------------------------------------------------------
// NavStep — one level in the navigation stack
// ---------------------------------------------------------------------------

/// A single level in the navigation stack.
///
/// Steps form a singly-linked chain — each `NavStep` either is the root
/// (`parent == None`) or carries a boxed `parent` pointing one level
/// deeper. The chain is **owned end-to-end**, so there is no `NavStack`
/// collection to reach into by index: outside code only ever sees the
/// current top, handed out by [`NavStack::current`] /
/// [`NavStack::current_mut`]. Operations that genuinely need ancestor
/// state (subscription-owner lookup, clear-dead-stream sweeps, breadcrumb
/// rendering) go through named methods on [`NavStack`] that walk the
/// chain internally — never through a public `step(i)` accessor.
#[derive(Debug)]
pub struct NavStep {
    /// What resource this step is viewing (universal identity).
    pub resource: ResourceId,
    /// The filter applied at this level (if any).
    pub filter: Option<NavFilter>,
    /// Saved table selection index so "back" restores cursor position.
    pub saved_selected: usize,
    /// Uncommitted filter input for this frame.
    pub filter_input: FilterInputState,
    /// The active subscription substream for this step's view. Dropping it
    /// closes the yamux substream (RST to the daemon), which cleanly
    /// terminates the daemon's bridge for this subscription. `None` before
    /// the first subscribe or for steps that don't own a subscription
    /// (grep filters on the same rid reuse the parent's stream).
    pub stream: Option<crate::kube::client_session::SubscriptionStream>,
    /// Link to the step one level down (`None` at the root). Private —
    /// callers can't reach into the chain; they navigate through
    /// [`NavStack::pop`] or ancestor-aware methods on `NavStack`.
    parent: Option<Box<NavStep>>,
}

impl NavStep {
    fn root(rid: ResourceId) -> Self {
        Self {
            resource: rid,
            filter: None,
            saved_selected: 0,
            filter_input: FilterInputState::default(),
            stream: None,
            parent: None,
        }
    }

    /// Build a fresh step destined for [`NavStack::push`]. `parent` stays
    /// `None`; the stack links it in during push so callers never touch
    /// the chain directly.
    pub fn new(resource: ResourceId, filter: Option<NavFilter>) -> Self {
        Self {
            resource,
            filter,
            saved_selected: 0,
            filter_input: FilterInputState::default(),
            stream: None,
            parent: None,
        }
    }
}

// ---------------------------------------------------------------------------
// NavStack — the full navigation state
// ---------------------------------------------------------------------------

/// Navigation state. Exposes **only the top step** — all ancestor access
/// goes through named semantic methods that walk the private parent chain
/// internally. There is no public `step(i)` / index-based accessor, so the
/// compiler enforces "user can only touch the current view" by
/// construction.
///
/// Popping the root is a no-op: [`NavStack::pop`] returns `None` so the
/// caller can flash a "at root" message rather than crashing.
#[derive(Debug)]
pub struct NavStack {
    /// The current top of the stack. Always present — the chain is
    /// never empty because `pop()` refuses to remove the root.
    top: NavStep,
    /// The root resource before the last `reset()`, used for
    /// "toggle last view" (Ctrl-^).
    prev_root: Option<ResourceId>,
}

impl NavStack {
    /// Create a new NavStack with a root step for the given resource.
    pub fn new(rid: ResourceId) -> Self {
        Self {
            top: NavStep::root(rid),
            prev_root: None,
        }
    }

    /// The current (topmost) step.
    pub fn current(&self) -> &NavStep {
        &self.top
    }

    /// Mutable access to the current step.
    pub fn current_mut(&mut self) -> &mut NavStep {
        &mut self.top
    }

    /// The current resource id.
    pub fn resource_id(&self) -> &ResourceId {
        &self.top.resource
    }

    /// The root resource id (bottom of the stack). Walks the parent
    /// chain to the last link.
    pub fn root_resource_id(&self) -> &ResourceId {
        let mut node = &self.top;
        while let Some(ref p) = node.parent {
            node = p;
        }
        &node.resource
    }

    /// Push a new step (grep or drill-down). Returns subscription changes.
    ///
    /// **Invariant:** same-rid pushes only ever carry **client-side**
    /// filters (`Grep`, `Fault`) whose `to_subscription_filter()` is
    /// `None`. They inherit the parent step's substream — the data is
    /// already flowing through `app.data.unified[rid]`, and the new
    /// step just narrows what the table widget renders.
    ///
    /// A same-rid push that introduced a *server-side* filter would be
    /// a bug: it would leak two concurrent watchers for the same rid,
    /// and the unfiltered one's snapshots would shadow the filtered
    /// one's. The debug-assert below catches any future call site that
    /// forgets this.
    pub fn push(&mut self, mut step: NavStep) -> NavChange {
        debug_assert!(
            step.parent.is_none(),
            "NavStack::push: caller constructed a NavStep with a parent chain \
             — callers own only the top, the stack owns the history",
        );
        let old = self.top.resource.clone();
        let sub_filter = step.filter.as_ref().and_then(|f| f.to_subscription_filter());
        // Splice the old top in as the new step's parent, then install
        // the new step as the top.
        let old_top = std::mem::replace(&mut self.top, NavStep::root(old.clone()));
        step.parent = Some(Box::new(old_top));
        self.top = step;
        let new = self.top.resource.clone();
        if new != old {
            NavChange { subscribe: Some(new), subscription_filter: sub_filter }
        } else {
            debug_assert!(
                sub_filter.is_none(),
                "NavStack::push: same-rid push must not carry a server-side subscription filter \
                 (would leak a concurrent watcher and shadow the parent step's filtered data)"
            );
            NavChange { subscribe: None, subscription_filter: None }
        }
    }

    /// Pop one step. Returns `(popped_step, subscription_changes)`, or
    /// `None` when already at the root — callers treat `None` as "can't
    /// go back" and typically flash a message. Because the root has no
    /// `parent`, popping the root is structurally impossible; that's the
    /// invariant that makes `current()` safe without an `.expect()`.
    pub fn pop(&mut self) -> Option<(NavStep, NavChange)> {
        // `parent.take()` replaces the link with `None` and hands us the
        // boxed parent node. `None` means we're at the root and refuse
        // to pop.
        let parent = self.top.parent.take()?;

        let old = self.top.resource.clone();
        // Install the parent as the new top; the old top is what we return.
        let popped = std::mem::replace(&mut self.top, *parent);
        let new = self.top.resource.clone();

        // When popping back across a rid change, prefer to inherit the
        // **owner step's** existing substream if it's still alive. The
        // new top might be a client-side filter over a same-rid ancestor;
        // in that case `current().stream` is `None` but a deeper ancestor
        // still owns the live stream. Skipping re-subscribe here preserves
        // cached rows for instant pop-back; re-subscribing would open a
        // duplicate watcher AND wipe the ancestor's rows.
        let sub_filter = self.top.filter.as_ref().and_then(|f| f.to_subscription_filter());
        let change = if new != old {
            if self.same_rid_ancestor_has_stream() {
                NavChange { subscribe: None, subscription_filter: None }
            } else {
                NavChange { subscribe: Some(new), subscription_filter: sub_filter }
            }
        } else {
            NavChange { subscribe: None, subscription_filter: None }
        };
        Some((popped, change))
    }

    /// Depth of the subscription-owner ancestor from the top (0 = top,
    /// 1 = top's parent, ...). The owner is the deepest same-rid
    /// ancestor that holds `stream: Some(_)`. Falls back to 0 if no
    /// same-rid ancestor owns a stream — the top is its own owner.
    fn subscription_owner_depth(&self) -> usize {
        let current_rid = &self.top.resource;
        let mut depth = 0usize;
        let mut best = 0usize;
        let mut node = &self.top;
        loop {
            if node.resource != *current_rid { break; }
            if node.stream.is_some() { best = depth; }
            match node.parent.as_deref() {
                Some(p) => { depth += 1; node = p; }
                None => break,
            }
        }
        best
    }

    /// True if any same-rid ancestor (strictly ancestor — not the top)
    /// owns a live subscription stream. Used by `pop` to decide whether
    /// the new top can inherit a parent's stream instead of re-subscribing.
    fn same_rid_ancestor_has_stream(&self) -> bool {
        let current_rid = &self.top.resource;
        let mut node = match self.top.parent.as_deref() {
            Some(p) => p,
            None => return false,
        };
        loop {
            if node.resource != *current_rid { return false; }
            if node.stream.is_some() { return true; }
            match node.parent.as_deref() {
                Some(p) => node = p,
                None => return false,
            }
        }
    }

    /// Run a closure with mutable access to the subscription-owner step.
    /// The owner is the deepest same-rid ancestor with a live stream; if
    /// none exists, the closure runs against the current top. Callers
    /// use this to touch `stream` on the owner (clear during refresh,
    /// replace after a new subscribe) without ever seeing a raw index.
    pub fn with_subscription_owner<R>(
        &mut self,
        f: impl FnOnce(&mut NavStep) -> R,
    ) -> R {
        let depth = self.subscription_owner_depth();
        let mut node: &mut NavStep = &mut self.top;
        for _ in 0..depth {
            // Depth was just computed from a walk along this same chain,
            // so the parent links exist. If they don't, that's a bug in
            // `subscription_owner_depth`, not at a call site.
            node = node.parent.as_deref_mut()
                .expect("subscription_owner_depth out of range — chain changed underneath");
        }
        f(node)
    }

    /// Clear any `stream: Some(_)` handle on steps whose resource matches
    /// `rid`. Called when a subscription for this rid has terminated
    /// server-side (error, EOF) — the bridge task is already finished,
    /// but the handle in `NavStep.stream` is still sitting there claiming
    /// "subscription is live". Without this clear, a subsequent Esc
    /// pop-back past the failing rid would consult
    /// `same_rid_ancestor_has_stream`, find this dead handle, treat it
    /// as an alive owner, and skip re-subscribing — the view would stay
    /// permanently stale until the user hit Ctrl-R.
    pub fn clear_dead_subscription_for(&mut self, rid: &ResourceId) {
        let mut node: &mut NavStep = &mut self.top;
        loop {
            if node.resource == *rid && node.stream.is_some() {
                node.stream = None;
            }
            match node.parent.as_deref_mut() {
                Some(p) => node = p,
                None => break,
            }
        }
    }

    /// Replace the entire stack with a new root. Returns subscription changes.
    ///
    /// Always returns `subscribe: Some(rid)` because resetting drops every
    /// step's subscription stream. Even when the rid is unchanged
    /// (e.g., `:pods` while already on pods), the streams were destroyed
    /// and the caller must open a new one via `apply_nav_change`.
    pub fn reset(&mut self, rid: ResourceId) -> NavChange {
        self.prev_root = Some(self.root_resource_id().clone());
        self.top = NavStep::root(rid.clone());
        NavChange {
            subscribe: Some(rid),
            subscription_filter: None,
        }
    }

    /// Collect all filters that apply to the current view, root-first
    /// (outermost) through leaf (innermost). Walks from top backward
    /// along same-rid ancestors and then reverses.
    pub fn active_filters(&self) -> Vec<&NavFilter> {
        let current_rid = &self.top.resource;
        let mut filters = Vec::new();
        let mut node = &self.top;
        loop {
            if node.resource != *current_rid { break; }
            if let Some(ref f) = node.filter { filters.push(f); }
            match node.parent.as_deref() {
                Some(p) => node = p,
                None => break,
            }
        }
        filters.reverse();
        filters
    }

    pub fn depth(&self) -> usize {
        let mut n = 1usize;
        let mut node = &self.top;
        while let Some(ref p) = node.parent {
            n += 1;
            node = p;
        }
        n
    }

    pub fn is_drilled(&self) -> bool { self.top.parent.is_some() }

    /// Remove the topmost [`NavFilter::Fault`] step from the stack, if
    /// any. Returns true if a step was removed. The root step (which
    /// never carries a filter from the reset path) is left alone even
    /// if a caller somehow set one.
    pub fn pop_fault_filter(&mut self) -> bool {
        // Check the top first: if it's fault-filtered, pop it (uses the
        // normal pop path so a same-rid ancestor's stream is inherited).
        if matches!(self.top.filter, Some(NavFilter::Fault)) && self.top.parent.is_some() {
            self.pop();
            return true;
        }
        // Otherwise walk: find the first ancestor whose parent is
        // fault-filtered, and splice the parent out.
        let mut node: &mut NavStep = &mut self.top;
        loop {
            let parent_is_fault = node.parent.as_deref()
                .map(|p| matches!(p.filter, Some(NavFilter::Fault)) && p.parent.is_some())
                .unwrap_or(false);
            if parent_is_fault {
                // Splice: node.parent = node.parent.parent
                let mut parent_box = node.parent.take().expect("checked by parent_is_fault");
                node.parent = parent_box.parent.take();
                return true;
            }
            match node.parent.as_deref_mut() {
                Some(p) => node = p,
                None => return false,
            }
        }
    }

    pub fn has_fault_filter(&self) -> bool {
        let mut node = &self.top;
        loop {
            if matches!(&node.filter, Some(NavFilter::Fault)) {
                return true;
            }
            match node.parent.as_deref() {
                Some(p) => node = p,
                None => return false,
            }
        }
    }

    pub fn save_selected(&mut self, selected: usize) {
        self.top.saved_selected = selected;
    }

    pub fn prev_root(&self) -> Option<&ResourceId> {
        self.prev_root.as_ref()
    }

    /// The current step's filter input state.
    pub fn filter_input(&self) -> &FilterInputState {
        &self.top.filter_input
    }

    /// Mutable access to the current step's filter input state.
    pub fn filter_input_mut(&mut self) -> &mut FilterInputState {
        &mut self.top.filter_input
    }

    pub fn breadcrumb(&self) -> String {
        // Walk from top to root collecting steps in reverse order, then
        // iterate forward to build the root → ... → current breadcrumb.
        let mut steps: Vec<&NavStep> = Vec::new();
        let mut node = &self.top;
        loop {
            steps.push(node);
            match node.parent.as_deref() {
                Some(p) => node = p,
                None => break,
            }
        }
        steps.reverse();

        let mut parts = Vec::new();
        for (i, step) in steps.iter().enumerate() {
            if i == 0 && step.filter.is_none() {
                parts.push(step.resource.short_label().to_string());
                continue;
            }
            match &step.filter {
                Some(NavFilter::Grep(g)) => parts.push(format!("/{}", g.source())),
                Some(NavFilter::Fault) => parts.push("⚠ fault".to_string()),
                Some(NavFilter::Labels(labels)) => {
                    let label_str: Vec<String> = labels.iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    parts.push(label_str.join(","));
                }
                Some(NavFilter::Field(sel)) => {
                    parts.push(sel.breadcrumb());
                }
                Some(NavFilter::OwnerChain { display_name, kind, .. }) => {
                    let kind_str = crate::kube::resource_defs::REGISTRY.by_kind(*kind).gvr().kind;
                    parts.push(format!("{}/{}", kind_str, display_name));
                }
                None => {
                    parts.push(step.resource.short_label().to_string());
                }
            }
        }
        parts.join(" > ")
    }
}

// ---------------------------------------------------------------------------
// FilterInputState — the input widget state (while user is typing `/`)
// ---------------------------------------------------------------------------

/// Tracks the filter input widget state, separate from committed nav filters.
#[derive(Debug, Default, Clone)]
pub struct FilterInputState {
    /// Whether the filter bar is in edit mode (listening for keystrokes).
    pub active: bool,
    /// The text being typed (not yet committed to NavStack).
    pub text: String,
}

/// Convenience helper: build a built-in `ResourceId` from a typed
/// [`BuiltInKind`]. A one-line wrapper for `ResourceId::BuiltIn` that
/// keeps callsites that frequently reference well-known built-ins
/// (pods, nodes, namespaces, crds) brief. Compile-time checked:
/// typos become E0599, not runtime panics.
pub fn rid(kind: crate::kube::resource_def::BuiltInKind) -> ResourceId {
    ResourceId::BuiltIn(kind)
}
