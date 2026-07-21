//! Nav-stack elements — self-contained "scopes" (the user's model).
//!
//! An [`Element`] is one level of the navigation stack: the durable,
//! self-defining description of *where you are* — its query/predicate,
//! its owned data handles, its label, its column policy — plus the
//! **persistent interaction state** (cursor, sort, draft filter text)
//! that must survive being covered and revealed.
//!
//! The *view* — what's actually on the terminal — is ephemeral by rule:
//! [`Element::view`] derives it on demand from (element, stores) via
//! [`crate::app::store::derive_view`], memoized behind a generation key
//! and droppable at any time with nothing lost.
//!
//! Derivation is a construction-time event: push-sites peek the current
//! TOP element (never the stack), hand the new element live handles
//! ([`RowSource`] Arcs), and forget. An element never knows it was
//! derived; references only ever point backward, so strong Arcs are
//! leak-free by construction. Pop = drop: a [`ResourceList`]'s
//! [`SubscriptionStream`] aborts its bridge, RST closes the substream,
//! the daemon's end tears down.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use crate::app::nav::{CompiledGrep, FilterInputState};
use crate::app::store::{
    derive_view, DeriveSpec, LineStore, MetricsBinding, MetricsHub, PreparedView, RowPredicate,
    RowSource, RowStore, SortSpec, StorePayload,
};
use crate::app::table::TableDataState;
use crate::app::types::ItemCounts;
use crate::app::view::DerivedViewKind;
use crate::app::ColumnLevel;
use crate::kube::client_session::{ClientSession, LogStream, SubscriptionStream};
use crate::kube::overlay::ColumnRenderRules;
use crate::kube::protocol::{
    Namespace, ObjectKey, ObjectRef, ResourceId, SubscriptionFilter, TableBaseline,
};
use crate::kube::resources::row::ResourceRow;
use crate::util::SearchPattern;

// ---------------------------------------------------------------------------
// ColumnPolicy — which columns this element shows
// ---------------------------------------------------------------------------

/// The element's column policy, fixed at construction. `include_namespace`
/// is decided by the element's OWN scope — an all-namespaces node-drill
/// shows NAMESPACE no matter what the ambient selector says (the old
/// `skip_ns = !ambient.is_all()` ambient read is unrepresentable here).
#[derive(Debug, Clone)]
pub struct ColumnPolicy {
    /// Resource identity for column-level metadata (overlay overrides +
    /// built-in defs). `None` for derived projections (header inference).
    rid: Option<ResourceId>,
    include_namespace: bool,
}

impl ColumnPolicy {
    pub fn for_query(rid: ResourceId, scope: &Namespace) -> Self {
        Self { include_namespace: scope.is_all(), rid: Some(rid) }
    }

    pub fn derived() -> Self {
        Self { rid: None, include_namespace: true }
    }

    /// Display level for a column by header name — overlay override, then
    /// built-in def metadata, then inference. (The old `column_level_for`,
    /// minus the `ViewId` indirection.)
    fn level_for(&self, name: &str) -> ColumnLevel {
        let Some(rid) = &self.rid else {
            return crate::app::ColumnDef::infer(name);
        };
        if let Some(overlay) = crate::kube::overlay::overlay_for(rid.plural()) {
            for oc in &overlay.columns {
                if oc.header.eq_ignore_ascii_case(name) && oc.jsonpath.is_none() {
                    return oc.level.into();
                }
            }
        }
        if let Some(k) = rid.built_in_kind() {
            let def = crate::kube::resource_defs::REGISTRY.by_kind(k);
            for col in def.column_defs() {
                if col.header.eq_ignore_ascii_case(name) {
                    return col.level;
                }
            }
        }
        crate::app::ColumnDef::infer(name)
    }

    /// DATA indices of the columns visible at `level`, in header order.
    fn visible_indices(&self, headers: &[String], level: ColumnLevel) -> Vec<usize> {
        headers
            .iter()
            .enumerate()
            .filter(|(_, name)| {
                if !self.include_namespace && name.eq_ignore_ascii_case("NAMESPACE") {
                    return false;
                }
                self.level_for(name) <= level
            })
            .map(|(i, _)| i)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// TableInteraction — the element's persistent interaction state
// ---------------------------------------------------------------------------

/// Persistent interaction state, owned by the element (the user's rule:
/// "persistent state should be part of the nav element" — the VIEW is
/// ephemeral). The cursor is a SCREEN position over the derived order;
/// derive-time clamping is display-only (no write-back) — only
/// interaction-time moves mutate these fields.
#[derive(Debug)]
pub struct TableInteraction {
    pub selected: usize,
    pub offset: usize,
    pub selected_col: usize,
    pub col_offset: u16,
    pub page_size: usize,
    pub sort: SortSpec,
    pub filter_input: FilterInputState,
    /// Whether the last PAINTED frame showed select mode. Mode itself is
    /// derived (`has_marks`), never stored — this records only what the
    /// user last saw, so a batch-intended keypress that lands after an
    /// async prune emptied the marks can be refused instead of silently
    /// falling through to single-item semantics (the "act through the
    /// last painted view" doctrine, applied to mode).
    pub rendered_select_mode: bool,
    /// Memo of the ephemeral view — a pure cache, droppable at any time
    /// (cleared when the element is covered; rebuilt on first read).
    cache: Option<(DeriveKey, Arc<PreparedView>)>,
}

impl Default for TableInteraction {
    fn default() -> Self {
        Self {
            selected: 0,
            offset: 0,
            selected_col: 0,
            col_offset: 0,
            page_size: 40,
            sort: SortSpec::default(),
            filter_input: FilterInputState::default(),
            rendered_select_mode: false,
            cache: None,
        }
    }
}

impl TableInteraction {
    /// Seed a child element's interaction from the parent at derivation:
    /// sort and cursor carry over (visual continuity — the child starts
    /// where the parent's eye was), draft input starts fresh, and the
    /// child's sort is its OWN from here on (sorting a refinement no
    /// longer re-sorts the parent underneath).
    pub fn seeded_from(parent: &TableInteraction) -> Self {
        Self {
            selected: parent.selected,
            offset: parent.offset,
            selected_col: parent.selected_col,
            col_offset: parent.col_offset,
            page_size: parent.page_size,
            sort: parent.sort,
            filter_input: FilterInputState::default(),
            rendered_select_mode: false,
            cache: None,
        }
    }

    /// Drop the memoized view (called when the element is covered — the
    /// ephemeral view is fully re-derivable, so retaining O(depth) of
    /// them buys nothing).
    pub fn drop_cache(&mut self) {
        self.cache = None;
    }

    /// The last materialized view, if any. Key handlers act through this
    /// — it is exactly what the user last saw painted.
    fn cached_view(&self) -> Option<&Arc<PreparedView>> {
        self.cache.as_ref().map(|(_, v)| v)
    }

    /// Cursor clamped to the given view length (display/act-time clamp;
    /// the stored cursor is not written back).
    fn clamped_selected(&self, len: usize) -> usize {
        if len == 0 { 0 } else { self.selected.min(len - 1) }
    }

    fn adjust_offset(&mut self) {
        if self.page_size == 0 {
            return;
        }
        if self.selected < self.offset {
            self.offset = self.selected;
        }
        if self.selected >= self.offset + self.page_size {
            self.offset = self.selected - self.page_size + 1;
        }
    }
}

/// Cache key for the memoized view. The derive is pure over these plus
/// the store contents (captured by `generation`) and metrics (captured by
/// `metrics_version`).
///
/// NOTE: the draft is keyed by TEXT only, which is correct precisely
/// because `compile_draft` matches ALL columns while typing (the draft's
/// column restriction only takes effect at COMMIT, as a predicate that
/// bumps `generation`). If `compile_draft` is ever changed to honor the
/// draft's column, that column MUST be added here — otherwise two drafts
/// with the same text but different target columns would collide on this
/// key and one would render the other's (now-different) view.
#[derive(Debug, Clone, PartialEq)]
struct DeriveKey {
    generation: u64,
    metrics_version: u64,
    sort: SortSpec,
    draft: String,
    level: ColumnLevel,
    max_col_width: u16,
}

// ---------------------------------------------------------------------------
// LiveQuery — a subscription an element owns
// ---------------------------------------------------------------------------

/// The wire-facing definition of a live resource list. `namespace` is the
/// element's OWN scope: populated from the ambient selector only at root
/// construction (a sanctioned construction input), intrinsic for drills
/// (All for a node's pods, the owner's namespace for owner drills).
#[derive(Debug, Clone)]
pub struct QuerySpec {
    pub rid: ResourceId,
    pub namespace: Namespace,
    pub filter: Option<SubscriptionFilter>,
}

/// A live subscription: spec + store + stream, none optional — a live
/// query ALWAYS has all three ("`None` means look elsewhere" is gone).
/// Drop aborts the bridge and RSTs the substream.
pub struct LiveQuery {
    spec: QuerySpec,
    store: Arc<RowStore>,
    stream: SubscriptionStream,
}

impl LiveQuery {
    /// Open a fresh subscription. The store's epoch floor is raised by
    /// `subscribe_stream` BEFORE this returns, so any stale predecessor
    /// event targeting a reused store is already dead on arrival.
    pub fn open(session: &ClientSession, spec: QuerySpec) -> Self {
        let store = RowStore::new(spec.rid.plural());
        let stream = session.subscribe_stream(
            spec.rid.clone(),
            spec.namespace.clone(),
            spec.filter.clone(),
            Arc::clone(&store),
            false,
        );
        Self { spec, store, stream }
    }

    /// Seed an already-populated store (local/offline sources, tests).
    pub fn seeded(session: &ClientSession, spec: QuerySpec, baseline: TableBaseline) -> Self {
        let this = Self::open(session, spec);
        this.store.apply(0, StorePayload::Baseline(baseline));
        this
    }

    pub fn spec(&self) -> &QuerySpec {
        &self.spec
    }

    pub fn store(&self) -> &Arc<RowStore> {
        &self.store
    }

    /// Whether the bridge task behind this subscription is still running.
    pub fn is_live(&self) -> bool {
        self.stream.is_alive()
    }

    /// Re-open the stream against the SAME store (reconnect revive). The
    /// fresh epoch floor rejects the dead stream's queued stragglers; the
    /// next Baseline replaces rows in place — no blank flash.
    pub fn resubscribe(&mut self, session: &ClientSession) {
        // Abort the old bridge BEFORE the new subscribe mints its epoch
        // (subscribe_stream raises expect_epoch as it returns) — a
        // lingering old bridge minting a HIGHER epoch would permanently
        // out-floor the successor's baseline.
        self.stream.abort();
        self.stream = session.subscribe_stream(
            self.spec.rid.clone(),
            self.spec.namespace.clone(),
            self.spec.filter.clone(),
            Arc::clone(&self.store),
            false,
        );
    }

    /// Ctrl-R: clear to a spinner and force a fresh server-side watcher —
    /// re-running the element's OWN spec (never the ambient selector; the
    /// old refresh silently narrowed all-namespace drills to the ambient
    /// namespace).
    pub fn refresh(&mut self, session: &ClientSession) {
        // Abort-before-mint (see resubscribe).
        self.stream.abort();
        self.store.clear();
        self.stream = session.subscribe_stream(
            self.spec.rid.clone(),
            self.spec.namespace.clone(),
            self.spec.filter.clone(),
            Arc::clone(&self.store),
            true,
        );
    }

    /// The server resolved our rid to its true identity (`:nodeclaims` →
    /// karpenter's NodeClaim). The element updates its own spec — there
    /// are no global maps to rekey.
    pub fn resolve_rid(&mut self, resolved: ResourceId) {
        self.spec.rid = resolved;
    }
}

#[cfg(test)]
impl LiveQuery {
    /// Test-only: a query with a parked (never-connecting) stream — no
    /// session, no daemon, no network. The store can be seeded directly.
    pub(crate) fn for_test(spec: QuerySpec) -> Self {
        let store = RowStore::new(spec.rid.plural());
        let stream = crate::app::test_support::parked_stream();
        Self { spec, store, stream }
    }
}

impl std::fmt::Debug for LiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveQuery").field("spec", &self.spec).finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// The element kinds
// ---------------------------------------------------------------------------

/// A live, subscription-backed resource list — roots, `:cmd` targets, and
/// server-filtered drills (node's pods, owner chains, label selectors).
#[derive(Debug)]
pub struct ResourceList {
    query: LiveQuery,
    /// The unfiltered output handle this element hands to children
    /// (store + metrics binding, no predicates).
    source: RowSource,
    label: String,
    title: String,
    /// The scope shown in the table title parentheses — the element's OWN
    /// scope, fixed at construction (empty for all-namespaces and
    /// cluster-scoped views). Never the ambient selector.
    scope_label: String,
    policy: ColumnPolicy,
    pub interaction: TableInteraction,
}

impl ResourceList {
    /// Open a fresh list element. `label` is the crumb (element-owned,
    /// fixed here); title + scope label derive from the element's OWN
    /// query spec — never re-derived from ambient state at render time.
    pub fn open(
        session: &ClientSession,
        spec: QuerySpec,
        hub: &Arc<MetricsHub>,
        label: String,
    ) -> Self {
        let policy = ColumnPolicy::for_query(spec.rid.clone(), &spec.namespace);
        let metrics = MetricsBinding::for_rid(&spec.rid, hub);
        let title = spec.rid.short_label().to_lowercase();
        let scope_label = match &spec.namespace {
            Namespace::Named(n) if !spec.rid.is_cluster_scoped() => n.clone(),
            _ => String::new(),
        };
        let query = LiveQuery::open(session, spec);
        let source = RowSource::new(Arc::clone(query.store()), metrics);
        Self {
            query,
            source,
            label,
            title,
            scope_label,
            policy,
            interaction: TableInteraction::default(),
        }
    }

    /// Same, but seed the child cursor/sort from the element being
    /// covered (cross-resource drills keep visual continuity).
    pub fn open_from(
        session: &ClientSession,
        spec: QuerySpec,
        hub: &Arc<MetricsHub>,
        label: String,
        top: &Element,
    ) -> Self {
        let mut el = Self::open(session, spec, hub, label);
        if let Some(parent) = top.table_interaction() {
            el.interaction = TableInteraction::seeded_from(parent);
            el.interaction.selected = 0; // fresh list: cursor starts at the top
            el.interaction.offset = 0;
        }
        el
    }

    /// Test-only: a list element over a parked stream (see
    /// [`LiveQuery::for_test`]).
    #[cfg(test)]
    pub(crate) fn open_for_test(spec: QuerySpec, hub: &Arc<MetricsHub>, label: String) -> Self {
        let policy = ColumnPolicy::for_query(spec.rid.clone(), &spec.namespace);
        let metrics = MetricsBinding::for_rid(&spec.rid, hub);
        let title = spec.rid.short_label().to_lowercase();
        let scope_label = match &spec.namespace {
            Namespace::Named(n) if !spec.rid.is_cluster_scoped() => n.clone(),
            _ => String::new(),
        };
        let query = LiveQuery::for_test(spec);
        let source = RowSource::new(Arc::clone(query.store()), metrics);
        Self {
            query,
            source,
            label,
            title,
            scope_label,
            policy,
            interaction: TableInteraction::default(),
        }
    }

    pub fn query(&self) -> &LiveQuery {
        &self.query
    }

    pub fn query_mut(&mut self) -> &mut LiveQuery {
        &mut self.query
    }

    pub fn rid(&self) -> &ResourceId {
        &self.query.spec().rid
    }

    /// Server resolve: update spec, label, and title in place (the
    /// element IS the identity — nothing else to rekey).
    pub fn apply_resolved(&mut self, resolved: ResourceId) {
        let short = resolved.short_label().to_lowercase();
        if self.label == self.query.spec().rid.short_label().to_lowercase() {
            self.label = short.clone();
        }
        self.title = short;
        if resolved.is_cluster_scoped() {
            self.scope_label.clear();
        }
        self.policy.rid = Some(resolved.clone());
        self.query.resolve_rid(resolved);
    }
}

/// A client-side predicate over the parent's row output (`/`, `~`,
/// fault). Holds the parent's output handle narrowed by its own
/// predicate — it neither knows nor cares where that handle came from.
#[derive(Debug)]
pub struct RowFilter {
    source: RowSource,
    label: String,
    /// Same resource as the parent (a refinement doesn't change WHAT you
    /// look at) — the SINGLE home for this element's identity. `rid()`
    /// reads `policy.rid`; there is no second copy to drift (a separate
    /// `rid` field used to exist and only half of the resolve path
    /// updated it, so a CRD resolved under a filter rendered stale
    /// columns/title).
    policy: ColumnPolicy,
    title: String,
    scope_label: String,
    pub interaction: TableInteraction,
}

impl RowFilter {
    /// The server resolved the underlying rid: a refinement carries a
    /// VALUE COPY of the parent's identity in `policy`, and actions
    /// (single and batch `ObjectRef`s) plus column metadata and the title
    /// scope are all built from it — so the copy must follow the
    /// resolution the same way the list one level below does.
    pub(crate) fn apply_resolved(&mut self, original: &ResourceId, resolved: &ResourceId) {
        if self.policy.rid.as_ref() != Some(original) {
            return;
        }
        self.policy.rid = Some(resolved.clone());
        if self.title == original.short_label().to_lowercase() {
            self.title = resolved.short_label().to_lowercase();
        }
        if resolved.is_cluster_scoped() {
            self.scope_label.clear();
        }
    }
}

/// A live client projection of one parent row (containers of a pod).
/// References the parent's live store; the projection re-derives as the
/// parent row changes ("live views, not frozen snapshots") and empties if
/// the row disappears.
#[derive(Debug)]
pub struct DerivedRows {
    source: RowSource,
    key: ObjectKey,
    kind: DerivedViewKind,
    /// The projected row this view came from, as a typed ref — scopes
    /// actions (logs/shell on a container) back to the parent object.
    origin: ObjectRef,
    headers: Vec<String>,
    rules: Vec<ColumnRenderRules>,
    label: String,
    title: String,
    pub interaction: TableInteraction,
}

/// Why a derivation could not be constructed from the given top element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeriveError {
    /// The top element has no row output to refine/project from.
    NoRows,
    /// Nothing is selected (empty view).
    NoSelection,
}

/// Outcome of a mark keypress. The element stays UI-free — the action
/// layer turns these into flashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkOutcome {
    /// Mark state changed.
    Toggled,
    /// No row under the cursor — empty view, or the row vanished between
    /// paint and keypress (stale coalesced frame).
    RowGone,
    /// This element kind has no markable rows.
    Unsupported,
}

/// Outcome of a span-mark keypress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanOutcome {
    Applied,
    /// No marked row is visible in this view to anchor the span.
    NoAnchor,
    /// This element kind has no markable rows.
    Unsupported,
}

// ---------------------------------------------------------------------------
// Log elements — a live line stream and predicate refinements over it
// ---------------------------------------------------------------------------

/// Persistent interaction state for log-bearing elements (scroll, follow,
/// display toggles, the draft filter being typed). The visible-line set is
/// the ephemeral view: derived on read, memoized, droppable.
#[derive(Debug)]
pub struct LogViewState {
    pub scroll: usize,
    pub follow: bool,
    pub wrap: bool,
    pub show_timestamps: bool,
    /// True during the initial tail fetch — the render path skips
    /// follow-mode auto-scroll so the view doesn't jump as the tail
    /// streams in.
    pub initial_load: bool,
    /// Draft filter text; `Some` = the filter bar is open.
    pub draft: Option<String>,
    /// Ring evictions last accounted for — scroll self-heals at read
    /// time by diffing against the store's counter (the store never
    /// reaches into elements).
    evicted_seen: u64,
    cache: Option<(LogDeriveKey, Arc<Vec<usize>>)>,
}

#[derive(Debug, Clone, PartialEq)]
struct LogDeriveKey {
    generation: u64,
    draft: Option<String>,
}

impl LogViewState {
    fn from_config(cfg: &crate::app::LogConfig, follow: bool) -> Self {
        Self {
            scroll: 0,
            follow,
            wrap: cfg.default_wrap,
            show_timestamps: cfg.default_timestamps,
            initial_load: true,
            draft: None,
            evicted_seen: 0,
            cache: None,
        }
    }

    fn seeded_from(parent: &LogViewState) -> Self {
        Self {
            scroll: parent.scroll,
            follow: parent.follow,
            wrap: parent.wrap,
            show_timestamps: parent.show_timestamps,
            initial_load: false,
            draft: None,
            evicted_seen: parent.evicted_seen,
            cache: None,
        }
    }

    pub fn drop_cache(&mut self) {
        self.cache = None;
    }

    pub fn is_filtering(&self) -> bool {
        self.draft.is_some()
    }
}

/// A composable line source: the backing [`LineStore`] plus the
/// accumulated committed grep chain — the log analogue of [`RowSource`].
#[derive(Debug, Clone)]
pub struct LineSource {
    store: Arc<LineStore>,
    patterns: Vec<Arc<CompiledGrep>>,
}

impl LineSource {
    fn new(store: Arc<LineStore>) -> Self {
        Self { store, patterns: Vec::new() }
    }

    fn narrowed(&self, pattern: Arc<CompiledGrep>) -> Self {
        let mut child = self.clone();
        child.patterns.push(pattern);
        child
    }

    pub fn store(&self) -> &Arc<LineStore> {
        &self.store
    }
}

/// A live log stream: the element's own query spec (`LogInit`), its
/// stream + line store, and its display state.
pub struct LogSession {
    /// The wire-facing definition — restarts (`since` ranges) re-run THIS.
    spec: crate::kube::protocol::LogInit,
    /// Display identity (pod/container labels for the header).
    pub target: crate::app::ContainerRef,
    stream: LogStream,
    store: Arc<LineStore>,
    /// Tail size for rate restarts (from config at construction).
    tail_default: u64,
    label: String,
    pub view: LogViewState,
}

impl std::fmt::Debug for LogSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSession").field("label", &self.label).finish_non_exhaustive()
    }
}

impl LogSession {
    /// Open a fresh log stream element.
    pub fn open(
        session: &ClientSession,
        target: crate::app::ContainerRef,
        spec: crate::kube::protocol::LogInit,
        cfg: &crate::app::LogConfig,
    ) -> Self {
        let store = LineStore::new(cfg.max_lines);
        let stream = session.stream_log_substream(spec.clone(), Arc::clone(&store));
        let label = format!("logs({}/{})", target.pod, target.container_label());
        Self {
            view: LogViewState::from_config(cfg, spec.follow),
            tail_default: cfg.tail_lines,
            spec,
            target,
            stream,
            store,
            label,
        }
    }

    /// Restart the stream with a new time range (digits 0-6). Re-runs the
    /// element's OWN spec against the SAME store (children keep their
    /// handles); the raised epoch floor keeps the dead stream's queued
    /// lines out.
    pub fn restart_range(&mut self, session: &ClientSession, since: Option<String>) {
        self.spec.since = since.clone();
        self.spec.tail = if since.is_none() { Some(self.tail_default) } else { None };
        self.spec.follow = true;
        self.store.clear();
        self.view.follow = true;
        self.view.initial_load = true;
        self.view.scroll = 0;
        self.stream = session.stream_log_substream(self.spec.clone(), Arc::clone(&self.store));
    }

    /// Whether the underlying substream is still running.
    pub fn stream_alive(&self) -> bool {
        self.stream.is_alive()
    }

    /// Re-establish the stream if it died (daemon restart during a reconnect
    /// or while covered). Without this a dead log stream renders forever as
    /// a live, following view — the line store keeps `live = true` because
    /// the bridge's `Ended` died in the closed channel and `mark_ended`
    /// never ran. Reuses `restart_range` (resume from the current `since`):
    /// re-tailing is the honest recovery — the alternative, resuming in
    /// place, would need a "since now" the tail spec can't express.
    pub fn revive_if_dead(&mut self, session: &ClientSession) {
        if !self.stream.is_alive() {
            let since = self.spec.since.clone();
            self.restart_range(session, since);
        }
    }

    /// Test-only: a log session over a parked stream.
    #[cfg(test)]
    pub(crate) fn for_test(target: crate::app::ContainerRef) -> Self {
        let cfg = crate::app::LogConfig::default();
        let store = LineStore::new(cfg.max_lines);
        let spec = crate::kube::protocol::LogInit {
            pod: target.pod.clone(),
            namespace: Namespace::Named("default".to_string()),
            container: target.container.clone(),
            follow: true,
            tail: Some(cfg.tail_lines),
            since: None,
            previous: false,
        };
        let label = format!("logs({}/{})", target.pod, target.container_label());
        Self {
            view: LogViewState::from_config(&cfg, true),
            tail_default: cfg.tail_lines,
            spec,
            target,
            stream: crate::app::test_support::parked_log_stream(),
            store,
            label,
        }
    }

    pub fn store(&self) -> &Arc<LineStore> {
        &self.store
    }

    fn line_source(&self) -> LineSource {
        LineSource::new(Arc::clone(&self.store))
    }
}

/// A committed grep over the parent's line output — one element per
/// committed filter, so Esc pops filters one at a time (the old
/// `LogState.filters` stack, made navigation).
#[derive(Debug)]
pub struct LogFilter {
    source: LineSource,
    label: String,
    /// Display identity, captured from the parent at derivation (the
    /// pod/container never change; the since label can go stale across a
    /// range restart under the filter — cosmetic, the crumb shows the
    /// filter anyway).
    header_pod: String,
    header_container: String,
    header_since: String,
    pub view: LogViewState,
}

// ---------------------------------------------------------------------------
// ContentView / ContextList / Overview elements
// ---------------------------------------------------------------------------

/// What a content element shows. Yaml/Describe carry their target — the
/// response-delivery gate ("does this response belong to the top?") and
/// the refresh spec in one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSpec {
    Yaml(ObjectRef),
    Describe(ObjectRef),
    Aliases,
}

/// A one-shot fetched text view (yaml / describe / aliases).
#[derive(Debug)]
pub struct ContentView {
    pub kind: ContentSpec,
    /// True while a fetch for this element is in flight (gates cache
    /// writes on delivery, exactly like the old route flag).
    pub awaiting_response: bool,
    pub state: crate::app::ContentViewState,
    label: String,
}

impl ContentView {
    pub fn new(kind: ContentSpec, state: crate::app::ContentViewState, awaiting_response: bool) -> Self {
        let label = match &kind {
            ContentSpec::Yaml(t) => format!("yaml({})", t.name),
            ContentSpec::Describe(t) => format!("describe({})", t.name),
            ContentSpec::Aliases => "aliases".to_string(),
        };
        Self { kind, awaiting_response, state, label }
    }

    /// The target this content was fetched for, if any.
    pub fn target(&self) -> Option<&ObjectRef> {
        match &self.kind {
            ContentSpec::Yaml(t) | ContentSpec::Describe(t) => Some(t),
            ContentSpec::Aliases => None,
        }
    }
}

/// The context picker — a real table view (cursor, copy, Enter-to-switch),
/// seeded from the kubeconfig at construction. The top-of-stack copy is
/// refreshed if a later `KubeconfigLoaded` arrives while it is showing.
#[derive(Debug)]
pub struct ContextList {
    pub table: crate::app::table::StatefulTable<crate::app::KubeContext>,
}

impl ContextList {
    pub fn new(contexts: Vec<crate::app::KubeContext>) -> Self {
        let mut table = crate::app::table::StatefulTable::new();
        table.set_items(contexts);
        Self { table }
    }
}

/// The landing page. Its content (cluster health stats) comes from the
/// app-level core stores at render time — chrome data, not navigation.
#[derive(Debug, Default)]
pub struct Overview;

// ---------------------------------------------------------------------------
// Element — the closed sum the stack holds
// ---------------------------------------------------------------------------

/// One nav-stack element. A closed enum: the renderer, key dispatch, and
/// crumb builder all match exhaustively — adding a kind forces every
/// consumer to decide, which is the point.
#[derive(Debug)]
pub enum Element {
    ResourceList(ResourceList),
    RowFilter(RowFilter),
    DerivedRows(DerivedRows),
    LogSession(Box<LogSession>),
    LogFilter(LogFilter),
    ContentView(ContentView),
    ContextList(ContextList),
    Overview(Overview),
}

impl Element {
    // -- Derivation constructors (peek-only: they take the top ELEMENT,
    //    never the stack) ---------------------------------------------------

    /// Push-site for `/`, `~`, and the fault filter: narrow the top's row
    /// output by one predicate.
    pub fn derive_filter(top: &Element, predicate: RowPredicate) -> Result<Element, DeriveError> {
        let source = top.output_source().ok_or(DeriveError::NoRows)?;
        let label = match &predicate {
            RowPredicate::Grep(g) => format!("/{}", g.source()),
            RowPredicate::ColumnGrep { pattern, header, .. } => {
                format!("~{}:{}", header, pattern.source())
            }
            RowPredicate::Fault => "⚠ fault".to_string(),
        };
        Ok(Element::RowFilter(RowFilter {
            source: source.narrowed(Arc::new(predicate)),
            label,
            // The policy already carries the parent's rid — that IS this
            // filter's identity (no separate copy).
            policy: top.column_policy().clone(),
            title: top.title().to_string(),
            scope_label: top.scope_label().to_string(),
            interaction: TableInteraction::seeded_from(
                top.table_interaction().expect("output_source() gated: table kinds only"),
            ),
        }))
    }

    /// Push-site for Enter-on-a-row into a derived projection
    /// (containers): the row's identity + the top's live source ARE the
    /// definition; no namespace parenthetical, no ambient anything.
    pub fn derive_projection(
        top: &Element,
        row: &ResourceRow,
        kind: DerivedViewKind,
    ) -> Result<Element, DeriveError> {
        let source = top.output_source().ok_or(DeriveError::NoRows)?;
        let headers = kind.default_headers();
        let rules = crate::kube::overlay::build_column_rules(&headers, kind.plural());
        let origin = ObjectRef {
            resource: top.rid().ok_or(DeriveError::NoRows)?.clone(),
            // `from_row` maps "" → All (the ""↔All convention); building
            // `Named("")` directly would be the invalid state that
            // convention exists to prevent. Safe today (projections are
            // pod-only, always namespaced) but honest for any future kind.
            namespace: Namespace::from_row(row.namespace.as_deref().unwrap_or("")),
            name: row.name.clone(),
        };
        let label = format!("{}({})", kind.plural(), row.name);
        Ok(Element::DerivedRows(DerivedRows {
            source: source.clone(),
            key: crate::app::store::row_key(row),
            kind,
            origin,
            headers,
            rules,
            title: label.clone(),
            label,
            interaction: TableInteraction::default(),
        }))
    }

    // -- Identity / metadata -------------------------------------------------

    /// The element's self-owned crumb label.
    pub fn label(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.label,
            Element::RowFilter(e) => &e.label,
            Element::DerivedRows(e) => &e.label,
            Element::LogSession(e) => &e.label,
            Element::LogFilter(e) => &e.label,
            Element::ContentView(e) => &e.label,
            Element::ContextList(_) => "contexts",
            Element::Overview(_) => "overview",
        }
    }

    /// The element's self-owned table title (heads the widget border).
    pub fn title(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.title,
            Element::RowFilter(e) => &e.title,
            Element::DerivedRows(e) => &e.title,
            _ => self.label(),
        }
    }

    /// The scope shown in the table title parentheses (element-owned;
    /// empty for all-namespaces / cluster-scoped / projections).
    pub fn scope_label(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.scope_label,
            Element::RowFilter(e) => &e.scope_label,
            _ => "",
        }
    }

    /// Compiled grep patterns for match-highlighting: every committed
    /// grep in this element's chain, plus the live draft.
    pub fn grep_patterns(&self) -> Vec<SearchPattern> {
        let mut pats: Vec<SearchPattern> = self
            .output_source()
            .map(|s| {
                s.predicates()
                    .iter()
                    .filter_map(|p| match p.as_ref() {
                        RowPredicate::Grep(g) => Some(g.pattern().clone()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let draft = self.filter_input().text();
        if !draft.is_empty() {
            pats.push(SearchPattern::new(draft));
        }
        pats
    }

    /// Capabilities for the help footer / key gating on non-table kinds.
    pub fn is_table(&self) -> bool {
        matches!(
            self,
            Element::ResourceList(_) | Element::RowFilter(_) | Element::DerivedRows(_)
        )
    }

    /// The resource this element shows, if it is resource-backed.
    /// Refinements report their parent's rid (a grep over pods still
    /// shows pods — describe/yaml/delete act on it).
    pub fn rid(&self) -> Option<&ResourceId> {
        match self {
            Element::ResourceList(e) => Some(e.rid()),
            Element::RowFilter(e) => e.policy.rid.as_ref(),
            _ => None,
        }
    }

    /// The parent object a derived projection came from (the pod whose
    /// containers are shown).
    pub fn origin(&self) -> Option<&ObjectRef> {
        match self {
            Element::DerivedRows(e) => Some(&e.origin),
            _ => None,
        }
    }

    /// The log-bearing kinds' line output (the handle a log grep narrows).
    fn line_output(&self) -> Option<LineSource> {
        match self {
            Element::LogSession(e) => Some(e.line_source()),
            Element::LogFilter(e) => Some(e.source.clone()),
            _ => None,
        }
    }

    /// Push-site for a committed log grep: narrow the top's line output.
    pub fn derive_log_filter(top: &Element, pattern: CompiledGrep) -> Result<Element, DeriveError> {
        let source = top.line_output().ok_or(DeriveError::NoRows)?;
        let label = format!("/{}", pattern.source());
        let parent_view = match top {
            Element::LogSession(e) => &e.view,
            Element::LogFilter(e) => &e.view,
            _ => unreachable!("line_output() gated"),
        };
        let (header_pod, header_container, header_since) =
            top.log_header().unwrap_or_default();
        Ok(Element::LogFilter(LogFilter {
            source: source.narrowed(Arc::new(pattern)),
            label,
            header_pod,
            header_container,
            header_since,
            view: LogViewState::seeded_from(parent_view),
        }))
    }

    /// Display identity for log views: (pod, container label, since label).
    pub fn log_header(&self) -> Option<(String, String, String)> {
        match self {
            Element::LogSession(e) => Some((
                e.target.pod.clone(),
                e.target.container_label().to_string(),
                e.spec.since.clone().unwrap_or_else(|| "tail".to_string()),
            )),
            Element::LogFilter(e) => Some((
                e.header_pod.clone(),
                e.header_container.clone(),
                e.header_since.clone(),
            )),
            _ => None,
        }
    }

    /// How many committed log greps are active on this element.
    pub fn log_committed_count(&self) -> usize {
        match self {
            Element::LogFilter(e) => e.source.patterns.len(),
            _ => 0,
        }
    }

    /// Log view state, for the kinds that have one.
    pub fn log_view(&self) -> Option<&LogViewState> {
        match self {
            Element::LogSession(e) => Some(&e.view),
            Element::LogFilter(e) => Some(&e.view),
            _ => None,
        }
    }

    pub fn log_view_mut(&mut self) -> Option<&mut LogViewState> {
        match self {
            Element::LogSession(e) => Some(&mut e.view),
            Element::LogFilter(e) => Some(&mut e.view),
            _ => None,
        }
    }

    /// The log store this element reads (session's own, or the filter's
    /// backing store).
    pub fn log_store(&self) -> Option<&Arc<LineStore>> {
        match self {
            Element::LogSession(e) => Some(&e.store),
            Element::LogFilter(e) => Some(e.source.store()),
            _ => None,
        }
    }

    /// The committed log-grep patterns active on this element (labels for
    /// highlight + the filter bar).
    pub fn log_patterns(&self) -> Vec<String> {
        let mut pats: Vec<String> = match self {
            Element::LogFilter(e) => e.source.patterns.iter().map(|p| p.source().to_string()).collect(),
            _ => Vec::new(),
        };
        if let Some(view) = self.log_view() {
            if let Some(d) = &view.draft {
                if !d.is_empty() {
                    pats.push(d.clone());
                }
            }
        }
        pats
    }

    /// The visible line indices for a log element — the ephemeral log
    /// view, derived on read, memoized on (store generation, draft).
    /// Heals the element's scroll against ring evictions first.
    pub fn log_visible(&mut self) -> Option<Arc<Vec<usize>>> {
        let (source_patterns, store) = match self {
            Element::LogSession(e) => (Vec::new(), Arc::clone(&e.store)),
            Element::LogFilter(e) => (e.source.patterns.clone(), Arc::clone(e.source.store())),
            _ => return None,
        };
        let view = self.log_view_mut().expect("log kinds have a log view");
        let key = LogDeriveKey { generation: store.generation(), draft: view.draft.clone() };
        // Scroll healing happens even on a cache hit (evictions bump the
        // generation, so a hit implies no NEW evictions — but the first
        // read after several is a miss; heal before deriving).
        if let Some((k, v)) = &view.cache {
            if *k == key {
                return Some(Arc::clone(v));
            }
        }
        let draft = view.draft.clone().filter(|d| !d.is_empty()).map(|d| SearchPattern::new(&d));
        let indices = store.with_read(|inner| {
            // Heal scroll for lines evicted since this element last looked.
            let newly_evicted = inner.evicted.saturating_sub(view.evicted_seen);
            if newly_evicted > 0 && !view.follow {
                view.scroll = view.scroll.saturating_sub(newly_evicted as usize);
            }
            view.evicted_seen = inner.evicted;
            if source_patterns.is_empty() && draft.is_none() {
                (0..inner.lines.len()).collect::<Vec<usize>>()
            } else {
                (0..inner.lines.len())
                    .filter(|&i| {
                        let line = &inner.lines[i];
                        let hit = |p: &SearchPattern| {
                            p.is_match(&line.content)
                                || line.container.as_deref().is_some_and(|c| p.is_match(c))
                        };
                        source_patterns.iter().all(|p| hit(p.pattern()))
                            && draft.as_ref().is_none_or(hit)
                    })
                    .collect()
            }
        });
        let indices = Arc::new(indices);
        view.cache = Some((key, Arc::clone(&indices)));
        Some(indices)
    }

    pub fn derived_kind(&self) -> Option<&DerivedViewKind> {
        match self {
            Element::DerivedRows(e) => Some(&e.kind),
            _ => None,
        }
    }

    /// The element's column policy (copied into refinements at derivation).
    pub fn column_policy(&self) -> &ColumnPolicy {
        match self {
            Element::ResourceList(e) => &e.policy,
            Element::RowFilter(e) => &e.policy,
            // Projections (and non-table kinds) use header inference.
            _ => {
                static DERIVED: std::sync::LazyLock<ColumnPolicy> =
                    std::sync::LazyLock::new(ColumnPolicy::derived);
                &DERIVED
            }
        }
    }

    /// Whether this element is a fault-filter refinement (Ctrl-Z pops it
    /// when it is the top).
    pub fn is_fault_filter(&self) -> bool {
        match self {
            Element::RowFilter(e) => {
                matches!(e.source.predicates().last().map(AsRef::as_ref), Some(RowPredicate::Fault))
            }
            _ => false,
        }
    }

    /// Whether this element's view is cluster-scoped (no namespace).
    pub fn is_cluster_scoped(&self) -> bool {
        match self {
            Element::DerivedRows(e) => e.kind.is_cluster_scoped(),
            _ => self.rid().map(|r| r.is_cluster_scoped()).unwrap_or(false),
        }
    }

    /// Capability manifest for key-availability and the help footer.
    pub fn capabilities(&self) -> crate::kube::protocol::ResourceCapabilities {
        match self {
            Element::DerivedRows(e) => crate::kube::protocol::ResourceCapabilities {
                operations: e.kind.operations(),
            },
            _ => match self.rid() {
                Some(rid) => rid.capabilities(),
                None => crate::kube::protocol::ResourceCapabilities { operations: Vec::new() },
            },
        }
    }

    /// The row output this element hands to a derived child (the model's
    /// "handed references"): the source INCLUDING this element's own
    /// predicates.
    pub fn output_source(&self) -> Option<&RowSource> {
        match self {
            Element::ResourceList(e) => Some(&e.source),
            Element::RowFilter(e) => Some(&e.source),
            // A projection's output is not row-store-backed; committing
            // filters on top of it is not supported (matches the old
            // behavior where derived views only ever draft-filtered).
            // Log/content/context/overview kinds have no row output.
            _ => None,
        }
    }

    /// The store this element's DATA ultimately lives in (a projection
    /// reads through to its parent's store). Drives the revive walk.
    pub fn data_store(&self) -> Option<&Arc<RowStore>> {
        match self {
            Element::ResourceList(e) => Some(e.query.store()),
            Element::RowFilter(e) => Some(e.source.store()),
            Element::DerivedRows(e) => Some(e.source.store()),
            _ => None,
        }
    }

    // -- Interaction state ----------------------------------------------------

    /// Persistent table-interaction state — `Some` for the row-bearing
    /// kinds only. Non-table kinds keep their own state shapes
    /// ([`LogViewState`], [`crate::app::ContentViewState`], the context
    /// table).
    pub fn table_interaction(&self) -> Option<&TableInteraction> {
        match self {
            Element::ResourceList(e) => Some(&e.interaction),
            Element::RowFilter(e) => Some(&e.interaction),
            Element::DerivedRows(e) => Some(&e.interaction),
            _ => None,
        }
    }

    pub fn table_interaction_mut(&mut self) -> Option<&mut TableInteraction> {
        match self {
            Element::ResourceList(e) => Some(&mut e.interaction),
            Element::RowFilter(e) => Some(&mut e.interaction),
            Element::DerivedRows(e) => Some(&mut e.interaction),
            _ => None,
        }
    }

    /// Whether covering this element should drop its ephemeral view memo.
    pub(crate) fn drop_view_cache(&mut self) {
        if let Some(it) = self.table_interaction_mut() {
            it.drop_cache();
        } else if let Some(lv) = self.log_view_mut() {
            lv.drop_cache();
        }
    }

    pub fn filter_input(&self) -> &FilterInputState {
        static EMPTY: std::sync::LazyLock<FilterInputState> =
            std::sync::LazyLock::new(FilterInputState::default);
        self.table_interaction().map(|i| &i.filter_input).unwrap_or(&EMPTY)
    }

    pub fn filter_input_mut(&mut self) -> Option<&mut FilterInputState> {
        self.table_interaction_mut().map(|i| &mut i.filter_input)
    }

    // -- The ephemeral view ----------------------------------------------------

    /// Materialize (or reuse) the view: the single pipeline feeding
    /// render, selection reads, and export. Pure over (element, stores);
    /// memoized on the derive key; cleared when covered.
    pub fn view(&mut self, level: ColumnLevel, max_col_width: u16) -> Arc<PreparedView> {
        match self {
            Element::LogSession(_)
            | Element::LogFilter(_)
            | Element::ContentView(_)
            | Element::ContextList(_)
            | Element::Overview(_) => {
                unreachable!("view() is table-kind-only; the draw dispatch matches kinds")
            }
            Element::ResourceList(e) => {
                let store = Arc::clone(&e.query.store);
                view_over_store(&store, &e.source, &e.policy, &mut e.interaction, level, max_col_width)
            }
            Element::RowFilter(e) => {
                let store = Arc::clone(e.source.store());
                view_over_store(&store, &e.source, &e.policy, &mut e.interaction, level, max_col_width)
            }
            Element::DerivedRows(e) => {
                let key = DeriveKey {
                    generation: e.source.store().generation(),
                    metrics_version: 0,
                    sort: e.interaction.sort,
                    draft: e.interaction.filter_input.text().to_string(),
                    level,
                    max_col_width,
                };
                if let Some((k, v)) = &e.interaction.cache {
                    if *k == key {
                        return Arc::clone(v);
                    }
                }
                // Live projection: find the parent row by identity, project
                // its current data. Row gone → honest empty view.
                let projected: Vec<ResourceRow> = e.source.store().with_read(|inner| {
                    inner
                        .rows
                        .iter()
                        .find(|r| crate::app::store::row_matches_key(r, &e.key))
                        .map(|r| e.kind.project(r))
                        .unwrap_or_default()
                });
                let draft = compile_draft(&e.interaction.filter_input);
                let visible: Vec<usize> = (0..e.headers.len()).collect();
                let headers: Vec<&str> = e.headers.iter().map(String::as_str).collect();
                let view = Arc::new(derive_view(
                    &projected,
                    &e.rules,
                    None,
                    &DeriveSpec {
                        predicates: &[],
                        draft: draft.as_ref(),
                        sort: e.interaction.sort,
                        visible_cols: &visible,
                        headers: &headers,
                        max_col_width,
                    },
                ));
                e.interaction.cache = Some((key, Arc::clone(&view)));
                view
            }
        }
    }

    /// Lifecycle state for the loading/error chrome.
    pub fn data_state(&self) -> TableDataState {
        match self.data_store() {
            Some(store) => store.with_read(|i| i.state.clone()),
            None => TableDataState::Ready,
        }
    }

    /// `filtered/total` for the count chrome, from the last materialized
    /// view (what the user is looking at).
    pub fn counts(&self) -> ItemCounts {
        match self.table_interaction().and_then(|i| i.cached_view()) {
            Some(v) => ItemCounts { filtered: v.keys.len(), total: v.total_rows },
            None => ItemCounts { filtered: 0, total: 0 },
        }
    }

    /// The last materialized view (what was last painted), if any —
    /// clipboard export and save read THIS, so what you copy is exactly
    /// what you see (the element's own columns, filter, sort, and
    /// metrics-overlaid cell values).
    pub fn last_view(&self) -> Option<Arc<PreparedView>> {
        self.table_interaction().and_then(|i| i.cached_view()).cloned()
    }

    /// The element's current column headers (full data set, not just
    /// visible) — for header-name → data-index lookups (form defaults,
    /// overlay drills).
    pub fn headers_snapshot(&self) -> Vec<String> {
        match self {
            Element::DerivedRows(e) => e.headers.clone(),
            _ => self
                .data_store()
                .map(|s| s.with_read(|i| i.headers.clone()))
                .unwrap_or_default(),
        }
    }

    // -- Selection / cursor (act through the LAST PAINTED view — the user
    //    acts on what they see) ------------------------------------------------

    /// Identity of the row under the cursor.
    pub fn selected_key(&self) -> Option<ObjectKey> {
        let it = self.table_interaction()?;
        let view = it.cached_view()?;
        view.keys.get(it.clamped_selected(view.keys.len())).cloned()
    }

    /// The full row under the cursor, resolved by identity against the
    /// element's data (store rows, or the live projection).
    pub fn selected_row(&self) -> Option<ResourceRow> {
        let key = self.selected_key()?;
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => {
                self.data_store()?.with_read(|inner| {
                    inner.rows.iter().find(|r| crate::app::store::row_matches_key(r, &key)).cloned()
                })
            }
            Element::DerivedRows(e) => e.source.store().with_read(|inner| {
                inner
                    .rows
                    .iter()
                    .find(|r| crate::app::store::row_matches_key(r, &e.key))
                    .map(|parent| e.kind.project(parent))
                    .and_then(|rows| {
                        rows.into_iter().find(|r| crate::app::store::row_matches_key(r, &key))
                    })
            }),
            _ => None,
        }
    }

    fn view_len(&self) -> usize {
        self.table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.keys.len())
            .unwrap_or(0)
    }

    // Every move re-anchors the stored cursor to the position the user
    // SEES (`clamped_selected`) before applying the step. A cursor carried
    // from a longer view — a child seeded from a deep parent position, or
    // data that shrank underneath — would otherwise sit out of range and
    // burn invisible keypresses re-entering it. Moves are interaction
    // time, so writing the normalized value back here is exactly the
    // doctrine ("only interaction-time moves mutate these fields"); an
    // empty view leaves the cursor untouched (nothing to move over, and
    // the position should survive a transient refresh window).

    pub fn select_next(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = (it.clamped_selected(len) + 1).min(len - 1);
        it.adjust_offset();
    }

    pub fn select_prev(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = it.clamped_selected(len).saturating_sub(1);
        it.adjust_offset();
    }

    pub fn page_up(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = it.clamped_selected(len).saturating_sub(it.page_size);
        it.adjust_offset();
    }

    pub fn page_down(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = (it.clamped_selected(len) + it.page_size).min(len - 1);
        it.adjust_offset();
    }

    pub fn go_home(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected = 0;
        it.offset = 0;
    }

    pub fn go_end(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len > 0 {
            it.selected = len - 1;
        }
        it.adjust_offset();
    }

    pub fn select(&mut self, idx: usize) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected = if len == 0 { 0 } else { idx.min(len - 1) };
        it.adjust_offset();
    }

    pub fn col_left(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected_col = it.selected_col.saturating_sub(1);
    }

    pub fn col_right(&mut self) {
        let num_cols = self
            .table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.visible_cols.len())
            .unwrap_or(0);
        let Some(it) = self.table_interaction_mut() else { return };
        if num_cols > 0 && it.selected_col + 1 < num_cols {
            it.selected_col += 1;
        }
    }

    /// Jump the column cursor to the first (leftmost) column — vim `0`.
    pub fn col_first(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected_col = 0;
    }

    /// Jump the column cursor to the last (rightmost) column — vim `$`.
    pub fn col_last(&mut self) {
        let num_cols = self
            .table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.visible_cols.len())
            .unwrap_or(0);
        let Some(it) = self.table_interaction_mut() else { return };
        if num_cols > 0 {
            it.selected_col = num_cols - 1;
        }
    }

    /// The cursor column as (DATA index, header) — the single source for
    /// `~` column-grep and `S` sort-by-column. Reads the same visible set
    /// the renderer used, so a mis-map is unrepresentable.
    pub fn selected_data_col(&self) -> Option<(usize, String)> {
        let it = self.table_interaction()?;
        let view = it.cached_view()?;
        let vis = it.selected_col.min(view.visible_cols.len().saturating_sub(1));
        Some((*view.visible_cols.get(vis)?, view.headers.get(vis)?.clone()))
    }

    // -- Marks -------------------------------------------------------------------
    //
    // Markable = store-backed (ResourceList / RowFilter): the kinds whose
    // marks live where row mutations are applied (pruned atomically with
    // removals) and can feed batch operations. DerivedRows deliberately
    // does NOT mark: its projected rows have no backing store, so a local
    // set could never be pruned by data (dead keys forever) and no batch
    // consumer exists — the future Aggregate feature gets to design
    // container marks together with their consumer.

    fn markable(&self) -> bool {
        matches!(self, Element::ResourceList(_) | Element::RowFilter(_))
    }

    /// Toggle the mark on the row under the cursor.
    pub fn toggle_mark(&mut self) -> MarkOutcome {
        if !self.markable() {
            return MarkOutcome::Unsupported;
        }
        let Some(key) = self.selected_key() else { return MarkOutcome::RowGone };
        let Some(store) = self.data_store() else { return MarkOutcome::Unsupported };
        match store.toggle_mark(&key) {
            Some(_) => MarkOutcome::Toggled,
            None => MarkOutcome::RowGone,
        }
    }

    /// Span-mark, at block granularity and symmetric with Space's
    /// per-row toggle — the state of the row UNDER THE CURSOR decides
    /// the direction:
    /// - cursor row unmarked → mark the span from the nearest VISIBLE
    ///   mark to the cursor (over the derived/visible order);
    /// - cursor row marked → unmark the contiguous marked block
    ///   containing the cursor.
    pub fn span_mark(&mut self) -> SpanOutcome {
        if !self.markable() {
            return SpanOutcome::Unsupported;
        }
        let Some(view) = self.table_interaction().and_then(|i| i.cached_view()).cloned() else {
            return SpanOutcome::NoAnchor;
        };
        if view.keys.is_empty() {
            return SpanOutcome::NoAnchor;
        }
        let Some(current) = self.table_interaction().map(|i| i.clamped_selected(view.keys.len()))
        else {
            return SpanOutcome::NoAnchor;
        };
        let marked = self.marked_snapshot();
        let Some(store) = self.data_store() else { return SpanOutcome::Unsupported };

        if marked.contains(&view.keys[current]) {
            // Unmark the contiguous marked block around the cursor.
            let mut start = current;
            while start > 0 && marked.contains(&view.keys[start - 1]) {
                start -= 1;
            }
            let mut end = current;
            while end + 1 < view.keys.len() && marked.contains(&view.keys[end + 1]) {
                end += 1;
            }
            store.unmark_keys(view.keys[start..=end].iter());
            return SpanOutcome::Applied;
        }

        // Span needs a VISIBLE anchor. With none — no marks at all, or
        // marks that this element's filter hides — refuse: falling back
        // to row 0 would silently bulk-mark from the top of the view and
        // grow an invisible marked set as a side effect of a miss.
        let Some(anchor) = view
            .keys
            .iter()
            .enumerate()
            .filter(|(_, k)| marked.contains(*k))
            .map(|(pos, _)| pos)
            .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
        else {
            return SpanOutcome::NoAnchor;
        };
        let (start, end) = if anchor <= current { (anchor, current) } else { (current, anchor) };
        store.mark_keys(view.keys[start..=end].iter().cloned());
        SpanOutcome::Applied
    }

    /// Clear all marks. `false` = this element kind has no markable rows
    /// (same teaching flash as Toggle/Span — the three marking keys must
    /// not disagree about where marking exists).
    pub fn clear_marks(&mut self) -> bool {
        if !self.markable() {
            return false;
        }
        if let Some(store) = self.data_store() {
            store.clear_marks();
        }
        true
    }

    pub fn has_marks(&self) -> bool {
        self.markable() && self.data_store().map(|s| s.has_marks()).unwrap_or(false)
    }

    pub fn marked_keys(&self) -> Vec<ObjectKey> {
        if !self.markable() {
            return Vec::new();
        }
        self.data_store().map(|s| s.marked_keys()).unwrap_or_default()
    }

    /// Marked count without cloning the set (for the `[N selected]`
    /// chrome — the empty-table branch used to clone the whole set just
    /// to read `.len()`).
    pub fn marked_count(&self) -> usize {
        if !self.markable() {
            return 0;
        }
        self.data_store().map(|s| s.with_read(|i| i.marked_count())).unwrap_or(0)
    }

    /// Marks for render-time row styling (cloned — small sets, per-frame).
    pub fn marked_snapshot(&self) -> HashSet<ObjectKey> {
        if !self.markable() {
            return HashSet::new();
        }
        self.data_store()
            .map(|s| s.with_read(|i| i.marked().clone()))
            .unwrap_or_default()
    }

    /// Recently-changed rows for flash styling (identity → change time).
    pub fn changed_snapshot(&self) -> std::collections::HashMap<ObjectKey, Instant> {
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => self
                .data_store()
                .map(|s| s.with_read(|i| i.flash.changed_rows().clone()))
                .unwrap_or_default(),
            _ => std::collections::HashMap::new(),
        }
    }

    // -- Sort ----------------------------------------------------------------

    /// Toggle sort on a target column (same column → flip direction).
    pub fn sort_by(&mut self, target: crate::app::SortTarget) {
        let col = match target {
            crate::app::SortTarget::Column(c) => c,
            crate::app::SortTarget::Last => match self.data_store() {
                Some(store) => store.with_read(|i| {
                    i.rows.first().map(|r| r.cells.len().saturating_sub(1)).unwrap_or(0)
                }),
                None => 0,
            },
        };
        let Some(it) = self.table_interaction_mut() else { return };
        if it.sort.col == col {
            it.sort.ascending = !it.sort.ascending;
        } else {
            it.sort = SortSpec { col, ascending: true };
        }
    }

    pub fn toggle_sort(&mut self) {
        let Some(col) = self.table_interaction().map(|i| i.sort.col) else { return };
        self.sort_by(crate::app::SortTarget::Column(col));
    }

    // -- Liveness ----------------------------------------------------------------

    /// Whether this element OWNS a live subscription right now. Elements
    /// without their own stream (filters, projections) report `true` —
    /// their data rides an ancestor's stream, revived by the stack's
    /// owner walk.
    pub fn is_live(&self) -> bool {
        match self {
            Element::ResourceList(e) => e.query.is_live(),
            _ => true,
        }
    }
}

/// The store-backed view pipeline shared by `ResourceList` and
/// `RowFilter` (their only difference is the predicate chain riding the
/// source).
fn view_over_store(
    store: &Arc<RowStore>,
    source: &RowSource,
    policy: &ColumnPolicy,
    interaction: &mut TableInteraction,
    level: ColumnLevel,
    max_col_width: u16,
) -> Arc<PreparedView> {
    let key = DeriveKey {
        generation: store.generation(),
        metrics_version: source.metrics().map(|m| m.version()).unwrap_or(0),
        sort: interaction.sort,
        draft: interaction.filter_input.text().to_string(),
        level,
        max_col_width,
    };
    if let Some((k, v)) = &interaction.cache {
        if *k == key {
            return Arc::clone(v);
        }
    }
    let draft = compile_draft(&interaction.filter_input);
    let lens = source.metrics().map(|m| m.lens());
    let view = store.with_read(|inner| {
        let visible = policy.visible_indices(&inner.headers, level);
        let headers: Vec<&str> = visible
            .iter()
            .map(|&i| inner.headers.get(i).map(String::as_str).unwrap_or(""))
            .collect();
        Arc::new(derive_view(
            &inner.rows,
            &inner.column_rules,
            lens.as_ref(),
            &DeriveSpec {
                predicates: source.predicates(),
                draft: draft.as_ref(),
                sort: interaction.sort,
                visible_cols: &visible,
                headers: &headers,
                max_col_width,
            },
        ))
    });
    interaction.cache = Some((key, Arc::clone(&view)));
    view
}

/// Compile the uncommitted draft text (changes per keystroke — caching
/// would churn).
fn compile_draft(input: &FilterInputState) -> Option<SearchPattern> {
    let text = input.text();
    if text.is_empty() {
        return None;
    }
    // A `~` draft is column-restricted even before commit: match only the
    // targeted column by wrapping through the predicate path at commit;
    // during typing we match all columns (parity with the old draft).
    Some(SearchPattern::new(text))
}

/// Build the committed predicate for the filter input's current state.
pub fn predicate_for_commit(text: String, column: Option<(usize, String)>) -> RowPredicate {
    match column {
        Some((col, header)) => RowPredicate::ColumnGrep {
            pattern: CompiledGrep::new(text),
            col,
            header,
        },
        None => RowPredicate::Grep(CompiledGrep::new(text)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
            status: "Running".into(),
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

        // Scroll healing: park the cursor, evict past the ring cap, and
        // the next read pulls the scroll back by the evicted count.
        filter.log_view_mut().unwrap().follow = false;
        filter.log_view_mut().unwrap().scroll = 4;
        let cap = 50_000; // default LogConfig max_lines
        for i in 0..(cap - 10 + 3) {
            store.push(1, LogLine { content: format!("fill-{i}"), container: None });
        }
        let _ = filter.log_visible();
        assert_eq!(filter.log_view().unwrap().scroll, 1, "scroll healed by 3 evictions");
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
            status: "Running".into(),
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
}
