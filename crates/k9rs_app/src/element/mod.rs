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
//! [`crate::store::derive_view`], memoized behind a generation key
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

use crate::nav::FilterInputState;
use k9rs_util::CompiledGrep;
use crate::store::{
    derive_view, DeriveSpec, LineStore, MetricsBinding, MetricsHub, PreparedView, RowPredicate,
    RowSource, RowStore, SortSpec,
};
use crate::table::TableDataState;
use crate::state::ItemCounts;
use crate::view::DerivedViewKind;
use crate::ColumnLevel;
use crate::client::{ClientSession, LogStream, SubscriptionStream};
use k9rs_proto::overlay::ColumnRenderRules;
use k9rs_proto::protocol::{Namespace, ObjectKey, ObjectRef, ResourceId, SubscriptionFilter};
use k9rs_proto::row::ResourceRow;
use k9rs_util::SearchPattern;

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
    /// This column's declared metadata, if any resource declares it.
    /// Built-ins answer from the registry, local resources from their own
    /// `column_defs` — same question, one lookup, so level and width can't
    /// disagree about where a column's description lives.
    fn def_for(&self, name: &str) -> Option<crate::ColumnDef> {
        let rid = self.rid.as_ref()?;
        let find = |defs: &[crate::ColumnDef]| {
            defs.iter().find(|c| c.header.eq_ignore_ascii_case(name)).copied()
        };
        match rid {
            ResourceId::BuiltIn(k) => {
                find(&k.def().column_defs())
            }
            ResourceId::Local(k) => find(k.column_defs()),
            // CRD columns are server-resolved printer columns; nothing
            // declares them client-side, so inference and the global cap
            // stay in charge.
            ResourceId::Crd(_) | ResourceId::CrdUnresolved(_) => None,
        }
    }

    /// Whether this column collapses consecutive equal values when sorted
    /// on — declared by the column, never inferred. See
    /// [`crate::ColumnDef::groups`].
    pub fn groups(&self, name: &str) -> bool {
        self.def_for(name).is_some_and(|d| d.groups)
    }

    fn level_for(&self, name: &str) -> ColumnLevel {
        let Some(rid) = &self.rid else {
            return crate::ColumnDef::infer(name);
        };
        if let Some(overlay) = k9rs_proto::overlay::overlay_for(rid.plural()) {
            for oc in &overlay.columns {
                if oc.header.eq_ignore_ascii_case(name) && oc.jsonpath.is_none() {
                    return oc.level.into();
                }
            }
        }
        if let Some(def) = self.def_for(name) {
            return def.level;
        }
        crate::ColumnDef::infer(name)
    }

    /// This column's width ceiling: the column's own declaration if it has
    /// one, else the global `ui.maxColumnWidth` fallback.
    fn max_width_for(&self, name: &str, fallback: u16) -> u16 {
        self.def_for(name).and_then(|d| d.max_width).unwrap_or(fallback)
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
    pub selected_col: usize,
    pub col_offset: u16,
    /// Vertical scroll relationship (offset + render-published extent). The
    /// cursor (`selected`) is primary; the viewport trails it via
    /// `Viewport::reveal` / `apply_render`.
    pub viewport: crate::viewport::Viewport,
    pub sort: SortSpec,
    pub filter_input: FilterInputState,
    /// Whether the last PAINTED frame showed select mode. Mode itself is
    /// derived (`has_marks`), never stored — this records only what the
    /// user last saw, so a batch-intended keypress that lands after an
    /// async prune emptied the marks can be refused instead of silently
    /// falling through to single-item semantics (the "act through the
    /// last painted view" doctrine, applied to mode).
    pub rendered_select_mode: bool,
    /// The column level the last derive used.
    ///
    /// A bare grep now scans only the DISPLAYED columns, so which rows pass
    /// depends on the level — and the level is ambient UI state the element
    /// cannot see. Recorded here by `view()` for the same reason
    /// `rendered_select_mode` is: the cold-memo fallback has to resolve what
    /// the next paint will show, and this is the only thing it would
    /// otherwise have to guess.
    pub derived_level: ColumnLevel,
    /// Memo of the ephemeral view — a pure cache, droppable at any time
    /// (cleared when the element is covered; rebuilt on first read).
    cache: Option<(DeriveKey, Arc<PreparedView>)>,
}

impl Default for TableInteraction {
    fn default() -> Self {
        Self {
            selected: 0,
            selected_col: 0,
            col_offset: 0,
            viewport: crate::viewport::Viewport::seeded(40),
            sort: SortSpec::default(),
            filter_input: FilterInputState::default(),
            rendered_select_mode: false,
            derived_level: ColumnLevel::Default,
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
            selected_col: parent.selected_col,
            col_offset: parent.col_offset,
            viewport: parent.viewport,
            sort: parent.sort,
            filter_input: FilterInputState::default(),
            rendered_select_mode: false,
            derived_level: ColumnLevel::Default,
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
        // Reveal the cursor within the last-published viewport — the same
        // one-frame-settle contract the old page_size-based reveal had.
        self.viewport.reveal(self.selected);
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

/// Where a query's rows come from. Most resources are fed by the daemon
/// over a subscription; a few are facts the CLIENT already holds and the
/// daemon has no business reporting on — the kubeconfig's contexts, whose
/// whole point is to be listable when there is no session at all.
///
/// Typed rather than conventional because the difference is load-bearing at
/// the reconnect/switch choke point: that path aborts and re-opens every
/// element's stream, and a client-owned query has no stream to re-open.
/// Doing it anyway would ask the daemon to serve a resource it doesn't know
/// — so `Client` simply carries no `SubscriptionStream`, and the compiler
/// keeps the two cases from being confused.
enum QueryOrigin {
    /// Fed by a daemon subscription; the stream must be kept alive, aborted
    /// at the choke point, and revived on reconnect.
    Daemon(SubscriptionStream),
    /// Seeded in-process. Nothing to keep alive, nothing to revive, and no
    /// connection state that could make the rows stale.
    Client,
}

/// A live subscription: spec + store + origin, none optional — a live
/// query ALWAYS has all three ("`None` means look elsewhere" is gone).
/// Drop aborts a daemon bridge and RSTs the substream.
pub struct LiveQuery {
    spec: QuerySpec,
    store: Arc<RowStore>,
    origin: QueryOrigin,
}

impl LiveQuery {
    /// Open a fresh subscription. The store's epoch floor is raised by
    /// `subscribe_stream` BEFORE this returns, so any stale predecessor
    /// event targeting a reused store is already dead on arrival.
    pub fn open(session: &ClientSession, spec: QuerySpec) -> Self {
        let store = RowStore::new(spec.rid.plural());
        let stream = session.subscribe_stream(
            // Fresh store, fresh element — nothing to retire.
            crate::client::Supersedes::Nothing,
            spec.rid.clone(),
            spec.namespace.clone(),
            spec.filter.clone(),
            Arc::clone(&store),
            false,
        );
        Self { spec, store, origin: QueryOrigin::Daemon(stream) }
    }

    /// A query over a store the CLIENT owns and seeds (see
    /// `QueryOrigin::Client`). The store is passed in rather than created
    /// here because its lifetime isn't the element's — it lives in
    /// [`crate::CoreData`] and is re-seeded whenever its source of
    /// truth changes, so opening and closing the view doesn't lose the rows.
    pub fn client(spec: QuerySpec, store: Arc<RowStore>) -> Self {
        Self { spec, store, origin: QueryOrigin::Client }
    }

    pub fn spec(&self) -> &QuerySpec {
        &self.spec
    }

    pub fn store(&self) -> &Arc<RowStore> {
        &self.store
    }

    /// Whether these rows are seeded in-process rather than fed by a
    /// daemon subscription (see `QueryOrigin`).
    /// Delegated to the store so the fact has ONE home: `QueryOrigin`
    /// answers "is there a stream to manage", the store answers "do these
    /// rows depend on the connection". Keeping the second on the store is
    /// what lets a `/` filter over the contexts list inherit it for free.
    pub fn is_client_owned(&self) -> bool {
        self.store.is_client_owned()
    }

    /// Whether the bridge task behind this subscription is still running.
    /// A client-owned query is always live: there is no bridge that could
    /// die, and nothing to re-subscribe if it did.
    pub fn is_live(&self) -> bool {
        match &self.origin {
            QueryOrigin::Daemon(stream) => stream.is_alive(),
            QueryOrigin::Client => true,
        }
    }

    /// Abort the subscription bridge without minting a successor. Called at the
    /// reconnect/switch choke point so the old bridge stops retrying against
    /// the dropped session's dead mux; the stream is revived later by
    /// `resubscribe` (top / on pop) or the element is dropped by a switch's
    /// nav reset. Idempotent, and a no-op for a client-owned query — its
    /// rows never depended on the connection being torn down.
    pub fn abort(&self) {
        match &self.origin {
            QueryOrigin::Daemon(stream) => stream.abort(),
            QueryOrigin::Client => {}
        }
    }

    /// Re-open the stream against the SAME store (reconnect revive). The
    /// fresh epoch floor rejects the dead stream's queued stragglers. Rows
    /// stay RESIDENT so the next Baseline replaces them in place (flash /
    /// mark continuity), but the store is flagged reinitializing so the view
    /// shows a "Connecting…" screen instead of the stale, no-longer-live rows
    /// while the stream re-establishes.
    /// Client-owned queries are exempt: there is no connection whose loss
    /// could have stalled them, so hiding their rows behind a "Connecting…"
    /// screen would be a lie, and re-subscribing would ask the daemon for a
    /// resource it has never heard of.
    pub fn resubscribe(&mut self, session: &ClientSession) {
        let QueryOrigin::Daemon(stream) = &self.origin else { return };
        // Abort the old bridge BEFORE the new subscribe mints its epoch
        // (subscribe_stream raises expect_epoch as it returns) — a
        // lingering old bridge minting a HIGHER epoch would permanently
        // out-floor the successor's baseline.
        // Rows stay put (in-place replacement on recovery), but report
        // Initializing so the stale rows hide behind the loading screen.
        self.store.mark_reinitializing();
        self.origin = QueryOrigin::Daemon(session.subscribe_stream(
            crate::client::Supersedes::Stream(stream),
            self.spec.rid.clone(),
            self.spec.namespace.clone(),
            self.spec.filter.clone(),
            Arc::clone(&self.store),
            false,
        ));
    }

    /// Ctrl-R: clear to a spinner and force a fresh server-side watcher —
    /// re-running the element's OWN spec (never the ambient selector; the
    /// old refresh silently narrowed all-namespace drills to the ambient
    /// namespace).
    /// A client-owned query has no server-side watcher to force; its rows
    /// are re-seeded by whatever owns them, so Ctrl-R leaves it alone rather
    /// than clearing a store nothing will refill.
    pub fn refresh(&mut self, session: &ClientSession) {
        let QueryOrigin::Daemon(stream) = &self.origin else { return };
        self.store.clear();
        self.origin = QueryOrigin::Daemon(session.subscribe_stream(
            crate::client::Supersedes::Stream(stream),
            self.spec.rid.clone(),
            self.spec.namespace.clone(),
            self.spec.filter.clone(),
            Arc::clone(&self.store),
            true,
        ));
    }

    /// The server resolved our rid to its true identity (`:nodeclaims` →
    /// karpenter's NodeClaim). The element updates its own spec — there
    /// are no global maps to rekey.
    pub fn resolve_rid(&mut self, resolved: ResourceId) {
        self.spec.rid = resolved;
    }
}

#[cfg(any(test, feature = "testing"))]
impl LiveQuery {
    /// Test-only: a query with no daemon behind it — no session, no
    /// network. The store can be seeded directly. Shares its shape with
    /// [`LiveQuery::client`], which is the production form of the same idea.
    pub(crate) fn for_test(spec: QuerySpec) -> Self {
        let store = RowStore::new(spec.rid.plural());
        Self { spec, store, origin: QueryOrigin::Client }
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

    /// Same shape as [`Self::open`], but over a store the CLIENT owns and
    /// seeds — no session, no subscription, no connection state. Everything
    /// downstream (columns, sort, grep, column filter, marks, the render
    /// path) is identical, which is the whole point: a resource whose rows
    /// come from disk is still just a resource.
    pub fn client(
        spec: QuerySpec,
        store: Arc<RowStore>,
        hub: &Arc<MetricsHub>,
        label: String,
    ) -> Self {
        let policy = ColumnPolicy::for_query(spec.rid.clone(), &spec.namespace);
        let metrics = MetricsBinding::for_rid(&spec.rid, hub);
        let title = spec.rid.short_label().to_lowercase();
        let source = RowSource::new(Arc::clone(&store), metrics);
        // `SortSpec` has no "unsorted" — it defaults to column 0 ascending —
        // so for help, whose column 0 is SECTION, the alphabet decides which
        // group you see first. Alphabetically "This view (…)" sorts LAST, so
        // pressing `?` from the pods table opened on "Everywhere" and buried
        // the pod keys 36 rows down: the contextual section is the entire
        // point of the rewrite and it was off-screen. Descending puts it
        // first. The ORDER is what matters and it is pinned by a test, so a
        // renamed section fails loudly instead of quietly burying itself.
        let mut interaction = TableInteraction::default();
        if matches!(
            spec.rid,
            k9rs_proto::protocol::ResourceId::Local(
                k9rs_proto::local_types::LocalResourceKind::Keybinding
            )
        ) {
            interaction.sort = crate::store::SortSpec { col: 0, ascending: false };
        }
        Self {
            query: LiveQuery::client(spec, store),
            source,
            label,
            title,
            scope_label: String::new(),
            policy,
            interaction,
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
            el.interaction.viewport.reveal(0);
        }
        el
    }

    /// Test-only: a list element over a parked stream (see
    /// [`LiveQuery::for_test`]).
    #[cfg(any(test, feature = "testing"))]
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
    /// This projection's OWN marks. A store-backed list keys its marks on
    /// the shared `RowStore`; projected rows have no store to key them in,
    /// so they live here and die with the element — the same lifetime the
    /// rows themselves have. Reads intersect with the live projection
    /// (`Element::marked_snapshot`), which stands in for the delta-prune a
    /// projection can never receive.
    marked: HashSet<ObjectKey>,
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

/// How much of the frame a thing claims when it is on top.
///
/// ONE answer for both axes of "on top": a nav ELEMENT
/// ([`Element::extent`]) and an orthogonal operation
/// ([`crate::TakeoverOp::extent`]). That is what lets a floating view be
/// an ordinary element rather than a second kind of thing with its own key
/// routing — and the operations had their own `OpExtent`, with its own
/// `Dialog` and `FullFrame`, until it collapsed into this one. Its `Dialog`
/// could not say how big it was, so its renderer centred itself: a second
/// geometry authority for one rect.
///
/// A `Dialog` is drawn in a centered box over whatever is beneath it (which
/// is rendered first, exactly as it would be if it were on top), placed by
/// the renderer's `dialog_box`. `Full` claims the frame, and the one path that
/// paints OVER a live view erases it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewExtent {
    /// Claims the frame. Every element does this unless it says otherwise.
    Full,
    /// A centered box over the view beneath, EXACTLY this big in cells —
    /// already the smaller of what the content needs and what the element
    /// will allow itself.
    ///
    /// It used to be a `Size` per axis, `Cells` or `Pct`, and the doc here
    /// claimed the box was "at most this big and shrunk to whatever the
    /// content actually needs". Nothing shrank anything: the three
    /// content-sized dialogs passed `Cells(exact)`, so cap and need
    /// coincided and the claim looked true, while help — the only user of
    /// `Pct` — got a flat 80% of the frame whatever it held. Its 105-column
    /// table was therefore CLIPPED by 27 columns at a 100-wide terminal and
    /// floating in 53 columns of DEAD SPACE at 200. Both halves of one
    /// unimplemented sentence.
    ///
    /// So each element resolves its own box against the frame it is given,
    /// which is the rule the three already followed, and `Size` — with its
    /// `Pct` variant existing for exactly one caller — is gone.
    Dialog { width: u16, height: u16 },
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
///
/// Named for what it is, and to pair with `TableInteraction`: it used to be
/// `LogViewState`, which is also the name of the log WIDGET's per-draw
/// parameter block (`ui::widgets::LogViewState`) — two different things one
/// `use` away from each other.
#[derive(Debug)]
pub struct LogInteraction {
    /// Vertical scroll relationship, in PHYSICAL (wrap-expanded) rows — owned
    /// by the render pass, which alone knows the width and wrap layout. Carries
    /// the follow/autoscroll flag too.
    pub viewport: crate::viewport::Viewport,
    /// Horizontal panning, in display columns. Its own type because it is the
    /// other axis, not another viewport — see `viewport::Pan`.
    pub pan: crate::viewport::Pan,
    pub wrap: bool,
    /// How many chrome roles are hidden — see
    /// [`k9rs_proto::protocol::Role::SHED_ORDER`]. ONE number rather than a
    /// boolean per role: two booleans are four states for three meaningful
    /// ones, and the unreachable fourth (time without attribution) was a
    /// state the keys could never produce anyway.
    pub shed: usize,
    /// True during the initial tail fetch — the render path skips
    /// follow-mode auto-scroll so the view doesn't jump as the tail
    /// streams in.
    pub initial_load: bool,
    /// Draft filter text; `Some` = the filter bar is open. Editable with a
    /// caret — see [`crate::text_input::TextInput`].
    pub draft: Option<crate::text_input::TextInput>,
    cache: Option<(LogDeriveKey, Arc<Vec<usize>>)>,
}

#[derive(Debug, Clone, PartialEq)]
struct LogDeriveKey {
    generation: u64,
    draft: Option<String>,
}

impl LogInteraction {
    fn from_config(cfg: &crate::LogConfig, follow: bool) -> Self {
        Self {
            viewport: if follow {
                crate::viewport::Viewport::tailing()
            } else {
                crate::viewport::Viewport::default()
            },
            pan: crate::viewport::Pan::default(),
            wrap: cfg.default_wrap,
            shed: cfg.initial_shed(),
            initial_load: true,
            draft: None,
            cache: None,
        }
    }

    /// The view for a freshly pushed grep: the parent's display PREFERENCES,
    /// none of its position.
    ///
    /// A filter owns its own store, so the parent's scroll position names
    /// nothing here — not a row (the filtered content has a different extent)
    /// and not a line (the child's sequence numbers are its own, since it
    /// counts only the lines it retained). `restarted()` is the existing
    /// answer to exactly this question, asked by the `since`-range restart:
    /// same surface, new content, keep what the user chose.
    fn seeded_from(parent: &LogInteraction) -> Self {
        Self {
            viewport: parent.viewport.restarted(),
            // The pan IS carried, unlike the vertical position: every line in
            // the child wears the same prefix as its parent, so "I have shifted
            // past the chrome" is still true of the narrowed view.
            pan: parent.pan,
            wrap: parent.wrap,
            shed: parent.shed,
            initial_load: false,
            draft: None,
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

/// What a chain step does with the lines its pattern matches.
///
/// The chain could only ever narrow TOWARD something: `/health` shows you the
/// health checks and nothing hid them. That was not a missing feature so much
/// as a wall — vim's negative look-around `\@!` is refused by name in
/// [`k9rs_util`], and `regex` 1.x has no look-around AT ALL, by design (it
/// is a finite-automaton engine, which is what makes a pathological pattern
/// compile promptly instead of exploding). So exclusion cannot be a property
/// of the pattern. It is a property of the STEP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sense {
    /// Keep the lines this pattern matches — `/pattern`.
    Keep,
    /// Drop them — `/!pattern`. Spelled with `!` because `less` spells it
    /// `&!pattern`, and `less` is what everyone has already read a log in.
    Drop,
}

/// One step of the committed grep chain: a pattern and what to do with it.
#[derive(Debug, Clone)]
pub(crate) struct Narrowing {
    pattern: Arc<CompiledGrep>,
    sense: Sense,
}

impl Narrowing {
    /// Whether this step lets a line through, given a tester that says
    /// whether the line matches a pattern. The tester is the caller's,
    /// because WHICH parts of a line a grep sees is `Role::searchable`'s to
    /// say and not this type's.
    pub(crate) fn admits(&self, matches: impl Fn(&k9rs_util::SearchPattern) -> bool) -> bool {
        let hit = matches(self.pattern.pattern());
        match self.sense {
            Sense::Keep => hit,
            Sense::Drop => !hit,
        }
    }

    /// Why this step's pattern is matching literally rather than as a
    /// regex, if it is — the `[literal: …]` chip's input.
    pub(crate) fn fallback_reason(&self) -> Option<&str> {
        self.pattern.pattern().fallback_reason()
    }

    /// As the user typed it, `!` included — what the grep bar and the
    /// breadcrumb label themselves with.
    pub(crate) fn label(&self) -> String {
        match self.sense {
            Sense::Keep => self.pattern.source().to_string(),
            Sense::Drop => format!("!{}", self.pattern.source()),
        }
    }

    /// The pattern to HIGHLIGHT with, if this step is a reason a visible line
    /// is visible. A `Drop` step has none: every line it had anything to say
    /// about is gone, so highlighting its matches would paint the one thing
    /// that is certainly not on screen.
    pub(crate) fn highlight(&self) -> Option<&str> {
        match self.sense {
            Sense::Keep => Some(self.pattern.source()),
            Sense::Drop => None,
        }
    }
}

/// Split a raw draft into its [`Sense`] and the pattern text.
///
/// `!pat` excludes, `\!pat` is a literal `!pat`, anything else keeps.
///
/// Parsed HERE, before the vim translator ever sees the text, so that
/// grammar is untouched: `\!` is not a vim escape (the translator would
/// refuse it by name) and `regex` would reject `\!` as an unknown escape.
/// One prefix, consumed in one place, and the translator keeps speaking
/// exactly vim magic.
pub fn sense_of(draft: &str) -> (Sense, &str) {
    if let Some(rest) = draft.strip_prefix('!') {
        (Sense::Drop, rest)
    } else if draft.starts_with("\\!") {
        // Strip the BACKSLASH only — the `!` is the thing being protected,
        // so it stays in the pattern. Dropping both would turn a search for
        // `!urgent` into a search for `urgent`.
        (Sense::Keep, &draft[1..])
    } else {
        (Sense::Keep, draft)
    }
}

/// A composable line source: the backing [`LineStore`] plus the
/// accumulated committed grep chain — the log analogue of [`RowSource`].
#[derive(Debug, Clone)]
pub(crate) struct LineSource {
    store: Arc<LineStore>,
    patterns: Vec<Narrowing>,
}

impl LineSource {
    fn new(store: Arc<LineStore>) -> Self {
        Self { store, patterns: Vec::new() }
    }

    fn narrowed(&self, pattern: Arc<CompiledGrep>, sense: Sense) -> Self {
        let mut child = self.clone();
        child.patterns.push(Narrowing { pattern, sense });
        child
    }

    pub(crate) fn store(&self) -> &Arc<LineStore> {
        &self.store
    }
}

/// A live log stream: the element's own query spec (`LogInit`), its
/// stream + line store, and its display state.
pub struct LogSession {
    /// The wire-facing definition — restarts (`since` ranges) re-run THIS,
    /// and the header/label/filename derive from it. There is deliberately
    /// no second copy of "which pod, which container": the display identity
    /// used to be a `ContainerRef` beside this spec, and a fan-out target
    /// ("3 pods") is exactly the case where the two would disagree.
    spec: k9rs_proto::protocol::LogInit,
    stream: LogStream,
    store: Arc<LineStore>,
    /// Tail size for rate restarts (from config at construction).
    tail_default: u64,
    label: String,
    pub view: LogInteraction,
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
        spec: k9rs_proto::protocol::LogInit,
        cfg: &crate::LogConfig,
    ) -> Self {
        let store = LineStore::new(cfg.max_lines);
        let stream = session.stream_log_substream(
            // A newly opened log session has no predecessor.
            crate::client::Supersedes::Nothing,
            spec.clone(),
            Arc::clone(&store),
        );
        let label = log_label(&spec);
        Self {
            view: LogInteraction::from_config(cfg, spec.follow),
            tail_default: cfg.tail_lines,
            spec,
            stream,
            store,
            label,
        }
    }

    /// Display identity: (what we're logging, which container(s), range).
    pub fn header(&self) -> (String, String, String) {
        (
            self.spec.target.display(),
            self.spec.container.label().to_string(),
            // Derived from the two wire fields rather than stored twice: the
            // spec IS what we asked the daemon for.
            match (&self.spec.since, &self.spec.tail) {
                (Some(d), _) => d.clone(),
                (None, None) => "all".to_string(),
                (None, Some(_)) => "tail".to_string(),
            },
        )
    }

    /// Restart the stream with a new time range (digits 0-6). Re-runs the
    /// element's OWN spec against the SAME store (children keep their
    /// handles); the raised epoch floor keeps the dead stream's queued
    /// lines out.
    pub fn restart_range(
        &mut self,
        session: &ClientSession,
        range: crate::actions::LogRange,
    ) {
        use crate::actions::LogRange;
        self.write_range(range.clone());
        self.spec.follow = true;
        self.store.clear();
        // A RANGE request keeps its head; a plain tail keeps its tail.
        //
        // The apiserver streams forward from the range's start, so the old
        // lines arrive first — and a tail-anchored ring evicted them with the
        // newer ones behind, leaving the newest N of a range asked for
        // precisely to see the oldest. Head retention also stops the stream
        // once full (see `revive_if_dead`'s sibling check in the session
        // loop), so we no longer pay to receive lines we will discard.
        // Anything that reaches BACKWARD keeps its head; a plain tail keeps
        // its tail.
        self.store.set_retain(match range {
            LogRange::Tail => crate::store::Retain::Tail,
            LogRange::Head | LogRange::Since(_) => crate::store::Retain::Head,
        });
        // The extent is gone with the store, but AUTOSCROLL IS A USER
        // PREFERENCE and survives. Unconditionally re-tailing here meant
        // pressing `s` to stop the scroll and then `2` to change the range
        // silently turned the scroll back on — a toggle undone by an
        // unrelated key is exactly what "finicky" feels like.
        self.view.viewport = self.view.viewport.restarted();
        self.view.initial_load = true;
        self.stream = session.stream_log_substream(
            crate::client::Supersedes::LogStream(&self.stream),
            self.spec.clone(),
            Arc::clone(&self.store),
        );
    }

    /// Stop a range fetch once we have the oldest N it was asked for.
    ///
    /// Dropping the handle aborts the bridge, which RSTs the substream; the
    /// daemon's drain already treats a closed reader as the normal end of a
    /// stream (`out.send(..).is_err()`), so the kubelet read stops with no
    /// wire change. Without this we keep receiving — and discarding — every
    /// line behind the window, which is most of them.
    pub fn stop_if_head_complete(&mut self) -> bool {
        if self.store.head_is_full() && self.stream.is_alive() {
            self.stream.abort();
            self.store.mark_ended(u64::MAX);
            return true;
        }
        false
    }

    /// Abort the log substream without minting a successor — the reconnect/
    /// switch choke-point counterpart to `revive_if_dead`. Idempotent.
    pub fn abort(&self) {
        self.stream.abort();
    }

    /// Re-establish the stream if it DIED — and only then.
    ///
    /// `is_alive() == false` stands for three different things: the transport
    /// died under us, the log legitimately ended (the container exited), or
    /// we stopped it ourselves ([`Self::stop_if_head_complete`]). The first
    /// wants recovery; the other two emphatically do not, and `restart_range`
    /// begins by CLEARING the store — so reviving a finished stream threw
    /// away a completed log and refetched a 100-line tail, and reviving a
    /// deliberately-stopped `Retain::Head` range discarded the oldest-N
    /// window the user had asked for and re-downloaded it, every time they
    /// popped back into the view.
    ///
    /// The discriminator already exists and needs no new state: both orderly
    /// endings run `mark_ended`, so the store reports `live = false`. A
    /// transport death is exactly the case where it still says `live = true`,
    /// because the bridge's `Ended` died in the closed channel — which is the
    /// symptom this function was written for ("renders forever as a live,
    /// following view"). Asking the store keeps the two facts from drifting.
    /// Returns whether it actually restarted — the caller uses that to reset
    /// the filters deriving from the store this just emptied.
    pub fn revive_if_dead(&mut self, session: &ClientSession) -> bool {
        if !self.needs_revival() {
            return false;
        }
        // Re-ask for the SAME range, read back off the spec — a revival must
        // not quietly change what you were looking at.
        self.restart_range(session, self.current_range());
        true
    }

    /// The range this stream is currently over, recovered from the two wire
    /// fields it was built from. The spec is the authority for what we asked
    /// the daemon for, so there is no second copy to drift.
    pub fn current_range(&self) -> crate::actions::LogRange {
        use crate::actions::LogRange;
        match (&self.spec.since, &self.spec.tail) {
            (Some(d), _) => LogRange::Since(d.clone()),
            (None, None) => LogRange::Head,
            (None, Some(_)) => LogRange::Tail,
        }
    }

    /// The decision behind [`Self::revive_if_dead`], named so it can be
    /// tested without a live session: the stream is gone and it did NOT end
    /// in an orderly way. Both orderly endings — EOF, and our own
    /// [`Self::stop_if_head_complete`] — run `mark_ended`, which sets a
    /// STICKY flag. It has to be sticky: `live` is re-set by every arrival,
    /// and a stopped stream keeps delivering for a moment, so asking `live`
    /// meant the first straggler undid the decision.
    pub fn needs_revival(&self) -> bool {
        !self.stream.is_alive() && !self.store.has_ended()
    }

    /// Test-only: replace the stream with one whose bridge has already
    /// finished, i.e. the transport died under us.
    #[cfg(test)]
    pub(crate) fn kill_stream_for_test(&mut self) {
        self.stream = crate::test_support::finished_log_stream();
    }

    /// Turn the user's range choice into the two wire fields — THE mapping,
    /// and the only one.
    ///
    /// `restart_range` calls it and so does the test that checks it. It was
    /// briefly written out twice, once here and once in a `set_range_for_test`
    /// helper, which meant the test asserted that the COPY round-tripped: had
    /// `restart_range` changed, the guard would have gone on passing. The
    /// same "one decision, two copies" shape this codebase spent 2026-10-13
    /// deleting everywhere else, introduced in the test for it.
    pub(crate) fn write_range(&mut self, range: crate::actions::LogRange) {
        use crate::actions::LogRange;
        let (since, tail) = match &range {
            LogRange::Tail => (None, Some(self.tail_default)),
            // Both absent IS "everything" to the apiserver — the state
            // `Option<String>` could not express.
            LogRange::Head => (None, None),
            LogRange::Since(d) => (Some(d.clone()), None),
        };
        self.spec.since = since;
        self.spec.tail = tail;
    }

    /// Test-only: a log session over a parked stream.
    #[cfg(any(test, feature = "testing"))]
    pub fn for_test(
        target: k9rs_proto::protocol::LogTarget,
        container: k9rs_proto::protocol::LogContainer,
    ) -> Self {
        let cfg = crate::LogConfig::default();
        let store = LineStore::new(cfg.max_lines);
        let spec = k9rs_proto::protocol::LogInit {
            target,
            namespace: Namespace::Named("default".to_string()),
            container,
            follow: true,
            tail: Some(cfg.tail_lines),
            since: None,
            previous: false,
        };
        let label = log_label(&spec);
        Self {
            view: LogInteraction::from_config(&cfg, true),
            tail_default: cfg.tail_lines,
            spec,
            stream: crate::test_support::parked_log_stream(),
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

impl LogFilter {
    /// This step's OWN label — the last of the accumulated chain, since a
    /// filter carries every pattern above it as well.
    pub fn own_label(&self) -> String {
        self.source.patterns.last().map(Narrowing::label).unwrap_or_default()
    }

    /// How many lines this step retained.
    pub fn retained_len(&self) -> usize {
        self.retained.with_read(|i| i.lines.len())
    }

    /// Pull everything new from the parent, keep what matches, advance the
    /// cursor. Idempotent and cheap: O(lines arrived since last call), not
    /// O(lines resident) — the derive it replaces re-scanned the whole store
    /// with every pattern on every frame, because the memo key carries the
    /// store's global generation and any append bumps it.
    ///
    /// Runs on the session task like every other store access, and never
    /// holds two locks: the parent's read is collected into a batch first.
    fn drain_from_parent(&mut self) {
        let parent = self.source.store();
        let (evicted, live, batch) = parent.with_read(|inner| {
            let start = self.cursor.max(inner.evicted);
            let end = inner.evicted + inner.lines.len() as u64;
            let matched: Vec<crate::store::StoredLine> = (start..end)
                .filter_map(|seq| inner.lines.get((seq - inner.evicted) as usize))
                .filter(|line| {
                    // Which PARTS a grep sees is `Role::searchable`'s to say,
                    // not this call site's. It used to be spelled out here and
                    // again in the draft derive — two copies of a rule that a
                    // third place (`Role`) also stated, which is how "the
                    // timestamp is not searchable" becomes true in one of them
                    // and not the others.
                    let hit = |p: &SearchPattern| {
                        line.segments()
                            .filter(|(role, ..)| role.searchable())
                            .any(|(_, text, _)| p.is_match(text))
                    };
                    // The FULL committed chain, against the parent's lines.
                    // Applying only our own pattern would rely on the parent
                    // having retained exactly its own matches; the chain is a
                    // conjunction and evaluating it whole keeps that true by
                    // construction, at a cost of one extra `is_match` per
                    // level on lines that are about to be kept anyway.
                    //
                    // Each step decides for itself whether a match is a
                    // reason to keep or to drop — see [`Sense`].
                    self.source.patterns.iter().all(|n| n.admits(|p| hit(p)))
                })
                .cloned()
                .collect();
            (inner.evicted, inner.live, matched)
        });
        // The parent threw lines away before we looked at them. We cannot
        // know how many of them matched — we never saw them — so this is a
        // flag the bar renders, not a count.
        if self.cursor < evicted {
            self.lost_history = true;
        }
        self.cursor = evicted + parent.with_read(|i| i.lines.len() as u64);
        self.retained.extend_derived(batch);
        self.retained.set_live(live);
    }

    /// Whether the parent evicted matches we never got to see.
    /// Start over against a parent whose store was just cleared.
    ///
    /// `restart_range` replaces the session's lines wholesale, but a filter's
    /// `retained` is its OWN store and survives — and since `clear()` leaves
    /// `cursor == evicted`, the next drain appended the new range's matches
    /// onto the old range's. A reconnect under a grep therefore duplicated
    /// every retained match, and the duplicates evicted real history to make
    /// room for themselves.
    pub fn restarted_from_parent(&mut self) {
        self.retained.clear();
        self.cursor = self.source.store().with_read(|i| i.evicted);
        self.lost_history = false;
    }

    /// The store this filter DERIVES from (the session's), not the one it
    /// owns — the identity a restart is announced by.
    pub fn source_store(&self) -> &Arc<LineStore> {
        self.source.store()
    }

    pub fn lost_history(&self) -> bool {
        self.lost_history
    }

    /// Drain without deriving — for elements that are NOT on top.
    ///
    /// `log_visible` runs on `nav.top_mut()` only, so a buried filter (stack
    /// two greps, or open a sub-view over one) would never advance its
    /// cursor: the parent would evict past it and, on reveal, it would
    /// report `lost_history` for lines it could have had. Draining every
    /// frame costs O(lines arrived) per element and keeps a covered filter
    /// exactly as complete as a visible one.
    pub fn keep_up(&mut self) {
        self.drain_from_parent();
    }
}

/// The nav-stack label for a log element, derived from its spec alone.
fn log_label(spec: &k9rs_proto::protocol::LogInit) -> String {
    format!("logs({}/{})", spec.target.display(), spec.container.label())
}

/// A committed grep over the parent's line output — one element per
/// committed filter, so Esc pops filters one at a time (the old
/// `LogState.filters` stack, made navigation).
#[derive(Debug)]
pub struct LogFilter {
    source: LineSource,
    /// This filter's OWN lines — the matches it has seen, with its own cap.
    ///
    /// A filter used to own nothing: it derived indices into the parent's
    /// store on every read, so its retention WAS the parent's. Eviction there
    /// is driven by unmatched noise, which means the rarer the pattern the
    /// faster its matches scrolled out — a grep for `panic` was evicted by
    /// the very traffic it was written to escape. Holding the matches makes
    /// the cap mean "N matches" instead of "N lines, some of which matched".
    retained: Arc<LineStore>,
    /// Next ABSOLUTE line sequence (`evicted + index`) to consider from the
    /// parent. Monotone, and monotone across `clear()` — which is what lets
    /// this be a resumable cursor rather than a position that shifts under
    /// eviction.
    cursor: u64,
    /// The parent evicted past `cursor` before we drained it, so some
    /// unknown number of matches are missing before our oldest line. The
    /// count is unknowable by construction — we never saw the lines — so
    /// this is a flag, not a number.
    lost_history: bool,
    label: String,
    /// Display identity, captured from the parent at derivation (the
    /// pod/container never change; the since label can go stale across a
    /// range restart under the filter — cosmetic, the crumb shows the
    /// filter anyway).
    header_pod: String,
    header_container: String,
    header_since: String,
    pub view: LogInteraction,
}

// ---------------------------------------------------------------------------
// ContentView / Overview elements
// ---------------------------------------------------------------------------

/// What a content element shows. Targeted kinds carry their target — the
/// response-delivery identity and the refresh spec in one.
/// `DecodedSecret` is its own kind, NOT a `Describe`: its content arrives
/// on the same wire event but must never be cached as the target's
/// describe text, and Ctrl-R must re-DECODE, not re-describe — labeling it
/// `Describe` (as it once was) made both mistakes representable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSpec {
    Yaml(ObjectRef),
    Describe(ObjectRef),
    DecodedSecret(ObjectRef),
}

/// Fetch lifecycle of a one-shot content view. Replaces an
/// `awaiting_response: bool` that had no failure edge: a fetch orphaned by
/// a session rebuild left the flag `true` forever — an eternally animated
/// "Loading..." with no request in flight behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentPhase {
    /// A fetch for this element is in flight on the CURRENT session.
    Fetching,
    /// Content present (delivered, cache-served, or locally built).
    Ready,
    /// The fetch can no longer complete (the session it was issued on is
    /// gone). Renders the reason; Ctrl-R re-issues.
    Failed(String),
}

/// A one-shot fetched text view (yaml / describe / decoded secret).
#[derive(Debug)]
pub struct ContentView {
    pub kind: ContentSpec,
    /// See [`ContentPhase`]. Also gates the delivery-time cache write:
    /// only a view with the fetch in flight caches what arrives.
    pub phase: ContentPhase,
    pub state: crate::ContentViewState,
    label: String,
}

impl ContentView {
    pub fn new(kind: ContentSpec, state: crate::ContentViewState, phase: ContentPhase) -> Self {
        let label = match &kind {
            ContentSpec::Yaml(t) => format!("yaml({})", t.name),
            ContentSpec::Describe(t) => format!("describe({})", t.name),
            ContentSpec::DecodedSecret(t) => format!("decoded({})", t.name),
        };
        Self { kind, phase, state, label }
    }

}

/// The landing page. Its content (cluster health stats) comes from the
/// app-level core stores at render time — chrome data, not navigation.
#[derive(Debug, Default)]
pub struct Overview;

// ---------------------------------------------------------------------------
// Element — the closed sum the stack holds
// ---------------------------------------------------------------------------

/// The behavioural classes an [`Element`] can belong to. Closed, so adding
/// an element kind forces a choice — and the choice is then CHECKED against
/// what the element's accessors actually return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementClass {
    /// Rows in a `RowStore`: cursor, sort, grep, column filter, marks.
    Table,
    /// A log stream: line store plus a log view.
    Log,
    /// Static fetched content (yaml, describe, aliases).
    Content,
    /// App chrome with no data source of its own.
    Chrome,
    /// A question with a continuation and no data source: one keymap, no
    /// cursor, no scroll. Distinct from `Chrome` because the keys differ —
    /// mapping it to the Overview keymap would document `Tab`-to-next-view
    /// at a box whose `Tab` switches buttons.
    Dialog,
}

/// One nav-stack element. A closed enum: the renderer, key dispatch, and
/// crumb builder all match exhaustively — adding a kind forces every
/// consumer to decide, which is the point.
///
/// # When an accessor may use `_ =>`, and when it may not
///
/// A 2026-09 audit counted "~28 `_ =>` accessors" as a hazard; it is now 44
/// and most of them are correct. The distinction, triaged 2026-10-14, is
/// whether the wildcard is an ANSWER or a GUESS:
///
/// - **An answer.** `Option` accessors for "the kinds that have one" —
///   `log_view`, `data_store`, `form_dialog`, `table_interaction`. `None`
///   means "not that kind of element", the caller must handle it, and a new
///   kind inheriting `None` is telling the truth.
/// - **An answer.** Delegation to the general case — `title` falls back to
///   `label`, `capabilities` to `rid`'s, `marked_count` to the data store's.
///   The default is the composition, not a shrug.
/// - **A GUESS, and therefore banned.** A bare default that a new kind would
///   silently inherit AND be wrong about. Two such existed and are now
///   exhaustive: [`Self::is_live`] (`_ => true` is what suppresses the stale
///   banner, so a new kind with a dead stream claimed to be tracking the
///   cluster) and [`Self::abort_data_stream`] (`_ => {}` leaks a new kind's
///   stream straight past the choke point that exists to abort it).
///
/// The test is not "is there a wildcard" but "would a new variant be WRONG
/// under it". Adding a kind should force a decision exactly where a wrong
/// answer is silent and harmful, and nowhere else — a hundred exhaustive
/// matches over nine kinds would bury the two that matter.
#[derive(Debug)]
pub enum Element {
    ResourceList(ResourceList),
    RowFilter(RowFilter),
    DerivedRows(DerivedRows),
    LogSession(Box<LogSession>),
    LogFilter(LogFilter),
    ContentView(ContentView),
    Overview(Overview),
    /// A yes/no question about an operation, floating over the view that
    /// raised it.
    ///
    /// An ELEMENT rather than a modal slot: Esc is then the ordinary pop,
    /// its keys arrive through the same per-kind dispatch as every other
    /// view, and there is no second authority deciding who owns input. The
    /// guard that used to arbitrate that (`modal_without_key_handler`) was
    /// wrong twice in one day, and it existed only because dialogs captured
    /// keys from outside the stack.
    Confirm(Box<crate::ConfirmDialog>),
    /// "Which container?" — a pick-one list, floating over the view that
    /// asked. Same reasoning as `Confirm`: a dialog is an element whose
    /// extent happens to be a box.
    Picker(Box<crate::ContainerPicker>),
    /// A form gathering input for one operation (scale, port-forward).
    ///
    /// Storage, extent and Esc are the stack's, like the other dialogs. Its
    /// TYPING still goes through the input sink in `try_handle_input_mode`,
    /// and that is the pattern rather than an exception: every text surface
    /// in the app is handled there, because a keystroke that edits text has
    /// to mutate, and `handle_key_event` only names actions.
    Form(Box<crate::FormDialog>),
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
        let rules = k9rs_proto::overlay::build_column_rules(&headers, kind.plural());
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
            key: crate::store::row_key(row),
            kind,
            origin,
            headers,
            rules,
            title: label.clone(),
            label,
            marked: HashSet::new(),
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
            Element::Confirm(_) => "confirm",
            Element::Picker(_) => "container",
            Element::Form(f) => f.name(),
            Element::LogSession(e) => &e.label,
            Element::LogFilter(e) => &e.label,
            Element::ContentView(e) => &e.label,
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

    /// Whether this element lost matches to its parent's eviction before it
    /// could collect them — the honest answer to "is this the whole story".
    pub fn log_lost_history(&self) -> bool {
        match self {
            Element::LogFilter(e) => e.lost_history(),
            _ => false,
        }
    }

    /// The first grep in play that is NOT running as a regex, and why —
    /// what the filter bars render.
    ///
    /// A pattern that quietly stopped being a regex reads exactly like one
    /// that simply isn't matching, so `0 visible` was the only thing the
    /// screen ever said about `a\{2,3}`. Committed greps are already
    /// compiled (table predicates and log filters both hold a
    /// [`CompiledGrep`]), so this costs at most one compile: the draft,
    /// which changes once per keystroke regardless.
    pub fn grep_fallback(&self) -> Option<String> {
        let committed = match self {
            Element::LogFilter(e) => e
                .source
                .patterns
                .iter()
                .find_map(Narrowing::fallback_reason),
            _ => self.output_source().and_then(|s| {
                s.predicates().iter().find_map(|p| match p.as_ref() {
                    RowPredicate::Grep(g) => g.pattern().fallback_reason(),
                    _ => None,
                })
            }),
        };
        if let Some(why) = committed {
            return Some(why.to_string());
        }
        // The draft lives in two different places depending on the kind;
        // `filter_input` is total (empty for log kinds) and `log_view` is
        // `None` for tables, so exactly one of them speaks.
        let draft = match self.log_view().and_then(|v| v.draft.as_ref().map(|d| d.value())) {
            Some(d) => d,
            None => self.filter_input().text(),
        };
        if draft.is_empty() {
            return None;
        }
        SearchPattern::new(draft).fallback_reason().map(str::to_string)
    }

    /// Capabilities for the help footer / key gating on non-table kinds.
    pub fn is_table(&self) -> bool {
        matches!(self.class(), ElementClass::Table)
    }

    /// What KIND of surface this element is — the ONE declaration the
    /// framework accessors must agree with.
    ///
    /// This exists because nothing could catch the contexts view opting out
    /// of the framework: it answered `None` from `table_interaction`,
    /// `filter_input`, `data_store`, `rid` and `marked_count`, so `/`, sort,
    /// column filters, column movement and marks all dispatched correctly
    /// and then silently did nothing. It was reported a year of little
    /// papercuts later as "`/` doesn't work on the contexts page".
    ///
    /// A kind declares its class here; `element_accessors_match_their_class`
    /// checks the accessors actually deliver it. A future element that
    /// claims `Table` and forgets to wire a `TableInteraction` fails the
    /// test instead of quietly losing half the keymap.
    pub fn class(&self) -> ElementClass {
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) | Element::DerivedRows(_) => {
                ElementClass::Table
            }
            // No data source of its own — it carries the question and the
            // continuation, nothing else.
            Element::Confirm(_) | Element::Picker(_) | Element::Form(_) => ElementClass::Dialog,
            Element::LogSession(_) | Element::LogFilter(_) => ElementClass::Log,
            Element::ContentView(_) => ElementClass::Content,
            Element::Overview(_) => ElementClass::Chrome,
        }
    }

    /// Whether the TOP element renders its own command/filter prompt inside
    /// its own layout, so the shared bottom overlay in `ui::draw` must NOT
    /// also draw one. EXHAUSTIVE over kinds on purpose: a new element kind
    /// must declare its placement here rather than silently defaulting into
    /// the overlay path — which is exactly how the Overview double-command-bar
    /// bug arose (it drew inline AND fell through to the overlay).
    pub fn renders_command_inline(&self) -> bool {
        match self {
            // Table-like views + the overview clone draw the prompt inside
            // their own layout (resource.rs / overview.rs).
            Element::ResourceList(_)
            | Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::Overview(_) => true,
            // Sub-views have no inline prompt row — they rely on the overlay.
            Element::ContentView(_)
            | Element::LogSession(_)
            | Element::LogFilter(_)
            // A dialog answers one question; `:` belongs to the view under it.
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => false,
        }
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
    pub(crate) fn line_output(&self) -> Option<LineSource> {
        match self {
            Element::LogSession(e) => Some(e.line_source()),
            Element::LogFilter(e) => Some(e.source.clone()),
            _ => None,
        }
    }

    /// Push-site for a committed log grep: narrow the top's line output.
    pub fn derive_log_filter(
        top: &Element,
        pattern: CompiledGrep,
        sense: Sense,
    ) -> Result<Element, DeriveError> {
        let source = top.line_output().ok_or(DeriveError::NoRows)?;
        let label = match sense {
            Sense::Keep => format!("/{}", pattern.source()),
            Sense::Drop => format!("/!{}", pattern.source()),
        };
        let parent_view = match top {
            Element::LogSession(e) => &e.view,
            Element::LogFilter(e) => &e.view,
            _ => unreachable!("line_output() gated"),
        };
        let (header_pod, header_container, header_since) =
            top.log_header().unwrap_or_default();
        // Starts where the parent IS, not at zero: everything already
        // evicted from the parent is unreachable, and claiming otherwise
        // would be the silent-gap this design exists to remove.
        let (cursor, cap) = {
            let store = source.store();
            (store.with_read(|i| i.evicted), store.max_lines())
        };
        Ok(Element::LogFilter(LogFilter {
            source: source.narrowed(Arc::new(pattern), sense),
            retained: LineStore::new(cap),
            cursor,
            lost_history: false,
            label,
            header_pod,
            header_container,
            header_since,
            view: LogInteraction::seeded_from(parent_view),
        }))
    }

    /// Display identity for log views: (target, container label, since label).
    pub fn log_header(&self) -> Option<(String, String, String)> {
        match self {
            Element::LogSession(e) => Some(e.header()),
            Element::LogFilter(e) => Some((
                e.header_pod.clone(),
                e.header_container.clone(),
                e.header_since.clone(),
            )),
            _ => None,
        }
    }

    /// The COMMITTED log greps on this element, oldest first — what the grep
    /// bar labels itself with. [`Self::log_patterns`] is the highlight set and
    /// additionally carries the uncommitted draft, so the two are not
    /// interchangeable.
    pub fn log_committed_patterns(&self) -> Vec<String> {
        match self {
            Element::LogFilter(e) => e.source.patterns.iter().map(Narrowing::label).collect(),
            _ => Vec::new(),
        }
    }

    /// Log view state, for the kinds that have one.
    pub fn log_view(&self) -> Option<&LogInteraction> {
        match self {
            Element::LogSession(e) => Some(&e.view),
            Element::LogFilter(e) => Some(&e.view),
            _ => None,
        }
    }

    pub fn log_view_mut(&mut self) -> Option<&mut LogInteraction> {
        match self {
            Element::LogSession(e) => Some(&mut e.view),
            Element::LogFilter(e) => Some(&mut e.view),
            _ => None,
        }
    }

    /// The visible line indices together with the store they index into.
    /// ONE accessor because they are only meaningful as a pair — asking for
    /// them separately forced callers to match on a kind, get an `Option`
    /// back anyway, and unwrap it.
    pub fn log_lines(&mut self) -> Option<(Arc<Vec<usize>>, Arc<LineStore>)> {
        let store = match self {
            Element::LogSession(e) => Arc::clone(&e.store),
            Element::LogFilter(e) => Arc::clone(&e.retained),
            _ => return None,
        };
        Some((self.log_visible()?, store))
    }

    /// The log store this element reads (session's own, or the filter's
    /// backing store).
    pub fn log_store(&self) -> Option<&Arc<LineStore>> {
        match self {
            Element::LogSession(e) => Some(&e.store),
            // The filter's OWN store — what it renders, saves and copies.
            Element::LogFilter(e) => Some(&e.retained),
            _ => None,
        }
    }

    /// The SESSION's store at the root of this log chain — the identity the
    /// stream-owning element is found by (`with_log_session_of`,
    /// `ensure_top_live`) and the one `ClearLogs` empties.
    ///
    /// Distinct from [`Self::log_store`] since a filter grew its own: those
    /// routing walks compare `Arc::ptr_eq` against the session's store, and
    /// handing them a filter's store would silently find nothing.
    pub fn log_root_store(&self) -> Option<&Arc<LineStore>> {
        match self {
            Element::LogSession(e) => Some(&e.store),
            Element::LogFilter(e) => Some(e.source.store()),
            _ => None,
        }
    }

    /// Whether this element is the `?` keybindings view — the one thing `?`
    /// needs to know to toggle rather than stack a second copy.
    pub fn is_keybindings_view(&self) -> bool {
        // Asked of the RID, not the element kind: `/` over help derives a
        // `RowFilter`, and that is still help. Testing the kind meant the
        // first keystroke of the grep the redesign exists to provide turned
        // the float full-screen and made the next `?` push a SECOND help
        // instead of closing the one you were reading.
        matches!(
            self.rid(),
            Some(k9rs_proto::protocol::ResourceId::Local(
                k9rs_proto::local_types::LocalResourceKind::Keybinding
            ))
        )
    }

    /// The form this element IS, if it is one.
    pub fn form_dialog(&self) -> Option<&crate::FormDialog> {
        match self {
            Element::Form(f) => Some(f),
            _ => None,
        }
    }

    pub fn form_dialog_mut(&mut self) -> Option<&mut crate::FormDialog> {
        match self {
            Element::Form(f) => Some(f),
            _ => None,
        }
    }

    /// Whether this element owns EVERY key, global bindings included.
    ///
    /// That is what "modal" means, and it is a property of the element
    /// rather than of where it is stored — which is the whole reason a
    /// dialog can be a nav element at all. `Confirm` answers yes because
    /// its safety net is "any other key cancels": a spurious keystroke (a
    /// stale terminal response misparsed as `r`, say) must dismiss the box
    /// rather than open the command prompt behind it.
    pub fn captures_all_input(&self) -> bool {
        matches!(self.class(), ElementClass::Dialog)
    }

    /// Whether this element floats, without asking how big.
    ///
    /// Derived from [`Self::extent`] with an empty frame, because WHICH
    /// variant it returns never depends on the frame — only the size does.
    /// One match, so a view that floats cannot also be told it is full
    /// frame (which is how a float once drew the cluster header, the
    /// breadcrumb and the flash line a second time inside its own box).
    pub fn floats(&self) -> bool {
        matches!(
            self.extent(ratatui::layout::Rect::new(0, 0, 0, 0)),
            ViewExtent::Dialog { .. }
        )
    }

    /// The cells a TABLE element's content wants: its columns' natural
    /// widths plus the border, and a row each plus the header and border.
    ///
    /// Read from the view memo, which the draw path warms before asking —
    /// the same `col_widths` the renderer lays out with, so the box and the
    /// table inside it cannot disagree about how wide the table is. A cold
    /// memo answers `u16::MAX`, i.e. "no opinion", which leaves the caller's
    /// allowance as the only bound and is exactly the old behaviour.
    fn table_content_size(&self) -> (u16, u16) {
        let Some(view) = self.table_interaction().and_then(|i| i.cached_view()) else {
            return (u16::MAX, u16::MAX);
        };
        let width = view.col_widths.iter().copied().fold(0u16, u16::saturating_add);
        let rows = u16::try_from(view.rows.len()).unwrap_or(u16::MAX);
        // `+2` border on each axis, `+1` for the header row.
        (width.saturating_add(2), rows.saturating_add(3))
    }

    /// How much of the frame this element claims — see [`ViewExtent`].
    ///
    /// EXHAUSTIVE on the kind axis, not a `_ => Full` wildcard: whether a new
    /// view floats is a decision its author has to make, and the renderer
    /// reads the answer from here rather than each view deciding for itself
    /// (which is how the overlay slot used to leave stale content inside a
    /// dialog's border).
    pub fn extent(&self, frame: ratatui::layout::Rect) -> ViewExtent {
        // A dialog is the SMALLER of what its content needs and what it will
        // allow itself of the frame. One rule, applied by the element that
        // knows the content — which is why the frame comes in here.
        let fit = |need: (u16, u16), allow: (u16, u16)| ViewExtent::Dialog {
            width: need.0.min(allow.0),
            height: need.1.min(allow.1),
        };
        let pct = |p: u16, of: u16| (u32::from(of) * u32::from(p) / 100) as u16;
        // What a `ModalOverlay`-chromed dialog needs AROUND its content —
        // asked of the widget rather than written as `+ 4` here, so the two
        // halves of that arithmetic cannot drift apart in different files.
        let margin = |w: u16, h: u16| {
            let (mw, mh) = crate::geometry::DIALOG_MARGIN;
            (w + mw, h + mh)
        };
        match self {
            // Help floats over the view it documents — you are consulting it
            // ABOUT something, and that something should stay on screen, so
            // it never takes the whole frame however much it holds.
            _ if self.is_keybindings_view() => fit(
                self.table_content_size(),
                (pct(80, frame.width), pct(85, frame.height)),
            ),
            // Sized from its own content: a yes/no box wants the rows its
            // message needs, not a percentage of somebody's terminal.
            Element::Confirm(d) => {
                // EXACTLY the size `ConfirmDialogWidget` asks for, so the
                // centring it does inside the box is a no-op and the two
                // do not disagree about where the dialog is. A batch confirm
                // lists its targets on extra lines and the box grows with
                // them — seeing what the batch will hit is the whole point.
                let (w, h) = crate::geometry::confirm_dialog_size(d);
                // The float reserves the OUTER box; the widget still owns
                // the inside of it, and spends `MARGIN` of the box on its own
                // outer gap — so a box sized to the dialog exactly would
                // shrink it until the message had no room. Asking the widget
                // rather than writing `+ 4` here is what stops the two
                // numbers drifting apart in different files.
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::Form(d) => {
                let (w, h) = crate::geometry::form_dialog_size(d);
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::Picker(p) => {
                let (w, h) = crate::geometry::container_picker_size(p);
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::ResourceList(_)
            | Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::Overview(_)
            | Element::ContentView(_)
            | Element::LogSession(_)
            | Element::LogFilter(_) => ViewExtent::Full,
        }
    }

    /// The committed log-grep patterns active on this element (labels for
    /// highlight + the filter bar).
    pub fn log_patterns(&self) -> Vec<String> {
        // HIGHLIGHTS, not labels: an excluding step contributes none, because
        // every line it matched is gone and painting its matches would
        // highlight the one thing certainly not on screen.
        let mut pats: Vec<String> = match self {
            Element::LogFilter(e) => e
                .source
                .patterns
                .iter()
                .filter_map(|n| n.highlight().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        if let Some(view) = self.log_view() {
            if let Some(d) = &view.draft {
                // Same rule for the draft: an excluding draft shows its
                // effect by what disappears, not by colour.
                let (sense, text) = crate::element::sense_of(d.value());
                if !text.is_empty() && sense == Sense::Keep {
                    pats.push(text.to_string());
                }
            }
        }
        pats
    }

    /// The visible line indices for a log element — the ephemeral log
    /// view, derived on read, memoized on (store generation, draft).
    ///
    /// Scroll is NOT healed here, and deliberately: the position is a line
    /// identity now ([`crate::viewport::LineAnchor`]), so eviction moves
    /// nothing that needs repairing. This used to shift the viewport up by the
    /// eviction COUNT — a logical number applied to a physical row offset, so
    /// under wrap it under-corrected (the view crept toward the tail) and
    /// under a filter it over-corrected by the match rate.
    pub fn log_visible(&mut self) -> Option<Arc<Vec<usize>>> {
        // Store, patterns AND view come out of ONE destructure. Fetching the
        // view separately meant asking a question this match had already
        // answered and unwrapping the reply — the Option-as-deferred-lookup
        // shape, which is only ever one refactor away from a panic.
        // A filter pulls from its parent FIRST, then reads only its own
        // store. The committed chain has already been applied by the drain,
        // so the derive below applies the DRAFT alone — which is why the
        // draft gets no storage: it changes on every keystroke.
        if let Element::LogFilter(e) = self {
            e.drain_from_parent();
        }
        let (store, view) = match self {
            Element::LogSession(e) => (Arc::clone(&e.store), &mut e.view),
            Element::LogFilter(e) => (Arc::clone(&e.retained), &mut e.view),
            _ => return None,
        };
        let key = LogDeriveKey {
            generation: store.generation(),
            draft: view.draft.as_ref().map(|d| d.value().to_string()),
        };
        if let Some((k, v)) = &view.cache {
            if *k == key {
                return Some(Arc::clone(v));
            }
        }
        let draft = view
            .draft
            .as_ref()
            .map(|d| d.value())
            .filter(|d| !d.is_empty())
            .map(SearchPattern::new);
        let indices = store.with_read(|inner| {
            // The DRAFT only. Committed patterns were applied by the drain
            // when these lines were retained, so re-testing them here would
            // be doing the same work a second time on every frame — which is
            // exactly what this used to do.
            match &draft {
                None => (0..inner.lines.len()).collect::<Vec<usize>>(),
                Some(d) => (0..inner.lines.len())
                    .filter(|&i| {
                        inner.lines[i]
                            .segments()
                            .filter(|(role, ..)| role.searchable())
                            .any(|(_, text, _)| d.is_match(text))
                    })
                    .collect(),
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
    pub fn capabilities(&self) -> k9rs_proto::protocol::ResourceCapabilities {
        match self {
            Element::DerivedRows(e) => k9rs_proto::protocol::ResourceCapabilities {
                operations: e.kind.operations(),
            },
            _ => match self.rid() {
                Some(rid) => rid.capabilities(),
                None => k9rs_proto::protocol::ResourceCapabilities { operations: Vec::new() },
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

    /// This element's liveness — the ONE question the render layer asks
    /// before painting rows. The element answers rather than the view,
    /// because the element is what knows where its rows came from: over a
    /// CLIENT-owned query the connection is irrelevant, and consulting it
    /// would blank the contexts list exactly when the daemon is unreachable
    /// and you need to pick a different context.
    pub fn liveness(&self, conn: &crate::connection::Connection) -> crate::Liveness {
        if self.is_client_owned() {
            crate::Liveness::of_store(self.data_state())
        } else {
            crate::Liveness::of(conn, self.data_state())
        }
    }

    /// Whether this element's rows are seeded in-process rather than fed by
    /// a daemon subscription. Derived kinds inherit the answer from the
    /// source they ride.
    pub fn is_client_owned(&self) -> bool {
        match self {
            Element::ResourceList(e) => e.query.is_client_owned(),
            // A filter/projection over a client-owned store is equally
            // independent of the connection.
            Element::RowFilter(e) => e.source.is_client_owned(),
            Element::DerivedRows(e) => e.source.is_client_owned(),
            _ => false,
        }
    }

    /// Abort the live stream this element OWNS (if any), pre-empting a retry
    /// loop against a dead mux at the reconnect/switch choke point. Derived
    /// (filter / projection) kinds own no stream — they ride an ancestor's —
    /// so this is a no-op for them (the ancestor is aborted on its own turn in
    /// the walk). Overview / context / content kinds own no subscription.
    pub fn abort_data_stream(&self) {
        match self {
            Element::ResourceList(e) => e.query().abort(),
            Element::LogSession(s) => s.abort(),
            // EXHAUSTIVE, unlike the `_ => {}` this replaced. Every arm here
            // is a claim that the kind owns no stream, and a new kind that
            // DID own one would have inherited the no-op silently — leaking
            // it past the choke point this function exists to be. Owning a
            // stream is the kind of thing a new element is likely to do, so
            // the compiler should ask.
            Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::LogFilter(_)
            | Element::ContentView(_)
            | Element::Overview(_)
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => {}
        }
    }

    // -- Interaction state ----------------------------------------------------

    /// Persistent table-interaction state — `Some` for the row-bearing
    /// kinds only. Non-table kinds keep their own state shapes
    /// ([`LogInteraction`], [`crate::ContentViewState`], the context
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
            Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_)
            | Element::LogSession(_)
            | Element::LogFilter(_)
            | Element::ContentView(_)
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
                        .find(|r| crate::store::row_matches_key(r, &e.key))
                        .map(|r| crate::derived::project(&e.kind, r))
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
                        // Projections declare no columns, so every one takes
                        // the global fallback.
                        max_col_widths: &vec![max_col_width; headers.len()],
                    },
                ));
                e.interaction.cache = Some((key, Arc::clone(&view)));
                view
            }
        }
    }

    /// Warm the memoized view cache if this is a table-kind element (no-op
    /// otherwise). Pure OPTIMIZATION: selection reads are total on their own
    /// (cold cache → `derive_selection_keys` fallback), but warming here
    /// memoizes the full view once so the action's resolution and the next
    /// paint share one derive instead of running two. Also gives relative
    /// cursor moves (`view_len`) something to clamp against pre-paint.
    /// Cheap: `view()` returns the memo when the cache is already warm.
    pub fn ensure_view_cached(&mut self, level: ColumnLevel, max_col_width: u16) {
        if self.is_table() {
            let _ = self.view(level, max_col_width);
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
    //    acts on what they see; with none painted, through what the NEXT
    //    paint will show) -----------------------------------------------------

    /// The screen-order identities a FRESH derive would produce — the
    /// fallback that makes selection reads total when no painted view
    /// exists (the memo is dropped on cover, absent on new elements).
    /// Mirrors `view()`'s per-kind data plumbing minus presentation,
    /// which never affects row order. `None` for non-table kinds.
    pub(crate) fn derive_selection_keys(&self) -> Option<Vec<ObjectKey>> {
        use crate::store::derive_key_order;
        let it = self.table_interaction()?;
        let draft = compile_draft(&it.filter_input);
        let level = it.derived_level;
        let policy = self.column_policy();
        match self {
            Element::ResourceList(e) => {
                let lens = e.source.metrics().map(|m| m.lens());
                Some(e.query.store.with_read(|inner| {
                    let visible = policy.visible_indices(&inner.headers, level);
                    derive_key_order(
                        &inner.rows,
                        lens.as_ref(),
                        e.source.predicates(),
                        draft.as_ref(),
                        it.sort,
                        &visible,
                    )
                }))
            }
            Element::RowFilter(e) => {
                let lens = e.source.metrics().map(|m| m.lens());
                Some(e.source.store().with_read(|inner| {
                    let visible = policy.visible_indices(&inner.headers, level);
                    derive_key_order(
                        &inner.rows,
                        lens.as_ref(),
                        e.source.predicates(),
                        draft.as_ref(),
                        it.sort,
                        &visible,
                    )
                }))
            }
            Element::DerivedRows(e) => {
                let projected: Vec<ResourceRow> = e.source.store().with_read(|inner| {
                    inner
                        .rows
                        .iter()
                        .find(|r| crate::store::row_matches_key(r, &e.key))
                        .map(|r| crate::derived::project(&e.kind, r))
                        .unwrap_or_default()
                });
                {
                    // Projections declare no columns — every one is visible.
                    let visible: Vec<usize> = (0..projected.first().map_or(0, |r| r.cells.len())).collect();
                    Some(derive_key_order(&projected, None, &[], draft.as_ref(), it.sort, &visible))
                }
            }
            _ => None,
        }
    }

    /// Identity of the row under the cursor. TOTAL for table kinds: acts
    /// through the last painted view when one exists; when none does, it
    /// resolves against a fresh derive of the same order — a cold memo
    /// right after a nav reveal is no longer a silent no-op.
    pub fn selected_key(&self) -> Option<ObjectKey> {
        let it = self.table_interaction()?;
        if let Some(view) = it.cached_view() {
            return view.keys.get(it.clamped_selected(view.keys.len())).cloned();
        }
        let keys = self.derive_selection_keys()?;
        keys.get(it.clamped_selected(keys.len())).cloned()
    }

    /// The full row under the cursor, resolved by identity against the
    /// element's data (store rows, or the live projection).
    pub fn selected_row(&self) -> Option<ResourceRow> {
        let key = self.selected_key()?;
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => {
                self.data_store()?.with_read(|inner| {
                    inner.rows.iter().find(|r| crate::store::row_matches_key(r, &key)).cloned()
                })
            }
            Element::DerivedRows(e) => e.source.store().with_read(|inner| {
                inner
                    .rows
                    .iter()
                    .find(|r| crate::store::row_matches_key(r, &e.key))
                    .map(|parent| crate::derived::project(&e.kind, parent))
                    .and_then(|rows| {
                        rows.into_iter().find(|r| crate::store::row_matches_key(r, &key))
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
        it.selected = it.clamped_selected(len).saturating_sub(it.viewport.viewport_rows());
        it.adjust_offset();
    }

    pub fn page_down(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = (it.clamped_selected(len) + it.viewport.viewport_rows()).min(len - 1);
        it.adjust_offset();
    }

    pub fn go_home(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected = 0;
        it.viewport.reveal(0);
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
        // `selected_col` is a VISIBLE index; cells are addressed by DATA
        // index. The clamp and the lookup are the bridge's, not this
        // function's — see `PreparedView::visible_cols`.
        it.cached_view()?.data_col(it.selected_col)
    }

    // -- Marks -------------------------------------------------------------------
    //
    // Marks live in one of two places, and WHICH is a property of where the
    // rows come from rather than a choice:
    //
    // - A store-backed list (ResourceList / RowFilter) keys its marks on the
    //   shared `RowStore`. That is what lets a filter hide a row from view
    //   without dropping it from the selection, and what prunes a mark
    //   atomically with the delta that removes its row.
    // - A PROJECTION (DerivedRows) has no such store — its rows are derived
    //   from one parent row, per frame — so it owns its marks, and they die
    //   with the element, which is exactly the lifetime its rows have. The
    //   prune that can't happen is replaced by an intersection at READ time
    //   (`live_keys`), so a container that vanished leaves no mark anyone
    //   can observe.

    /// The keys a PROJECTION currently yields, independent of any filter or
    /// painted frame — the set its marks are meaningful against.
    ///
    /// Identity here is `(namespace, name)`, for the parent and the projected
    /// rows alike, because that is all a `ResourceRow` carries. So if the
    /// parent pod is deleted and recreated under the same name while this
    /// view is open, a mark on `app` re-attaches to the new pod's `app`.
    /// That is deliberate rather than overlooked: the element's own origin is
    /// name-keyed too, so the view is already showing the new pod — a
    /// selection that followed the object instead of the name would have to
    /// empty itself under a table that visibly still has rows.
    fn projected_keys(&self) -> Vec<ObjectKey> {
        let Element::DerivedRows(e) = self else { return Vec::new() };
        e.source.store().with_read(|inner| {
            inner
                .rows
                .iter()
                .find(|r| crate::store::row_matches_key(r, &e.key))
                .map(|parent| {
                    crate::derived::project(&e.kind, parent)
                        .iter()
                        .map(crate::store::row_key)
                        .collect()
                })
                .unwrap_or_default()
        })
    }

    /// The keys this element currently offers, in view order: the last
    /// PAINTED view when there is one, else a fresh derive of the same —
    /// the totality contract `selected_key` uses.
    fn live_keys(&self) -> Vec<ObjectKey> {
        match self.table_interaction().and_then(|i| i.cached_view()) {
            Some(view) => view.keys.clone(),
            None => self.derive_selection_keys().unwrap_or_default(),
        }
    }

    /// Whether marking rows here can lead to anything.
    ///
    /// Row-bearing is necessary but not sufficient: select mode exists to
    /// feed BATCH operations, so a resource that declares none has no
    /// consumer for a mark. The contexts view is exactly that — marking a
    /// context did nothing except silently enter select mode, which then
    /// blocked `Enter`, the picker's only function, on the one screen a
    /// first-run user is forced onto.
    pub fn markable(&self) -> bool {
        // Any BATCH-CAPABLE operation makes marking meaningful, not just the
        // per-item ones: containers declare no Delete and no Restart, and
        // their marks exist entirely to say which containers the ONE merged
        // log stream is over.
        fn feeds_a_batch(ops: &[k9rs_proto::protocol::OperationKind]) -> bool {
            ops.iter().any(|op| {
                !matches!(op.batch_support(), k9rs_proto::protocol::BatchSupport::SingleOnly)
            })
        }
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => match self.rid() {
                Some(rid) => feeds_a_batch(&rid.capabilities().operations),
                None => false,
            },
            Element::DerivedRows(e) => feeds_a_batch(&e.kind.operations()),
            _ => false,
        }
    }

    /// Toggle the mark on the row under the cursor.
    pub fn toggle_mark(&mut self) -> MarkOutcome {
        if !self.markable() {
            return MarkOutcome::Unsupported;
        }
        let Some(key) = self.selected_key() else { return MarkOutcome::RowGone };
        if let Element::DerivedRows(e) = self {
            // The key came from a row that exists right now, so there is no
            // "row gone" case to distinguish here.
            if !e.marked.remove(&key) {
                e.marked.insert(key);
            }
            return MarkOutcome::Toggled;
        }
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
        let keys = self.live_keys();
        if keys.is_empty() {
            return SpanOutcome::NoAnchor;
        }
        let Some(current) = self.table_interaction().map(|i| i.clamped_selected(keys.len()))
        else {
            return SpanOutcome::NoAnchor;
        };
        let marked = self.marked_snapshot();

        // Which rows the span covers is decided identically either way; only
        // the write differs.
        let span = if marked.contains(&keys[current]) {
            // Unmark the contiguous marked block around the cursor.
            let mut start = current;
            while start > 0 && marked.contains(&keys[start - 1]) {
                start -= 1;
            }
            let mut end = current;
            while end + 1 < keys.len() && marked.contains(&keys[end + 1]) {
                end += 1;
            }
            Err(start..=end)
        } else {
            // Span needs a VISIBLE anchor. With none — no marks at all, or
            // marks that this element's filter hides — refuse: falling back
            // to row 0 would silently bulk-mark from the top of the view and
            // grow an invisible marked set as a side effect of a miss.
            let Some(anchor) = keys
                .iter()
                .enumerate()
                .filter(|(_, k)| marked.contains(*k))
                .map(|(pos, _)| pos)
                .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
            else {
                return SpanOutcome::NoAnchor;
            };
            Ok(if anchor <= current { anchor..=current } else { current..=anchor })
        };

        if let Element::DerivedRows(_) = self {
            // Filtered to rows the projection actually yields, mirroring the
            // store side's `mark_keys` (which intersects with present rows
            // and says why). Extending straight from the painted view banked
            // keys that no longer had a row behind them.
            let present: HashSet<ObjectKey> = self.projected_keys().into_iter().collect();
            let Element::DerivedRows(e) = self else { unreachable!("matched above") };
            match span {
                Ok(add) => {
                    e.marked.extend(keys[add].iter().filter(|k| present.contains(*k)).cloned())
                }
                Err(remove) => {
                    for k in &keys[remove] {
                        e.marked.remove(k);
                    }
                }
            }
            return SpanOutcome::Applied;
        }
        let Some(store) = self.data_store() else { return SpanOutcome::Unsupported };
        match span {
            Ok(add) => store.mark_keys(keys[add].iter().cloned()),
            Err(remove) => store.unmark_keys(keys[remove].iter()),
        }
        SpanOutcome::Applied
    }

    /// Clear all marks. `false` = this element kind has no markable rows
    /// (same teaching flash as Toggle/Span — the three marking keys must
    /// not disagree about where marking exists).
    pub fn clear_marks(&mut self) -> bool {
        if !self.markable() {
            return false;
        }
        match self {
            Element::DerivedRows(e) => e.marked.clear(),
            _ => {
                if let Some(store) = self.data_store() {
                    store.clear_marks();
                }
            }
        }
        // A DELIBERATE clear is not a stale frame. Without this, `Ctrl-\`
        // then `Ctrl-D` inside one key-drain (up to 200 keys between paints)
        // hit the select-mode race guard and answered "Selection changed —
        // the marked rows are gone (nothing was done)" to a user who had just
        // cleared them on purpose. The guard exists for marks that vanished
        // ASYNCHRONOUSLY; this is the one path that empties them synchronously.
        if let Some(it) = self.table_interaction_mut() {
            it.rendered_select_mode = false;
        }
        true
    }

    pub fn has_marks(&self) -> bool {
        if !self.markable() {
            return false;
        }
        match self {
            // Cheapest honest answer: a projection's marks are only real
            // where a row still backs them.
            Element::DerivedRows(_) => !self.marked_snapshot().is_empty(),
            _ => self.data_store().map(|s| s.has_marks()).unwrap_or(false),
        }
    }

    pub fn marked_keys(&self) -> Vec<ObjectKey> {
        if !self.markable() {
            return Vec::new();
        }
        match self {
            Element::DerivedRows(_) => {
                let marked = self.marked_snapshot();
                self.live_keys().into_iter().filter(|k| marked.contains(k)).collect()
            }
            _ => self.data_store().map(|s| s.marked_keys()).unwrap_or_default(),
        }
    }

    /// Marked count without cloning the set (for the `[N selected]`
    /// chrome — the empty-table branch used to clone the whole set just
    /// to read `.len()`).
    pub fn marked_count(&self) -> usize {
        if !self.markable() {
            return 0;
        }
        match self {
            Element::DerivedRows(_) => self.marked_snapshot().len(),
            _ => self.data_store().map(|s| s.with_read(|i| i.marked_count())).unwrap_or(0),
        }
    }

    /// Marks for render-time row styling (cloned — small sets, per-frame).
    pub fn marked_snapshot(&self) -> HashSet<ObjectKey> {
        if !self.markable() {
            return HashSet::new();
        }
        match self {
            // A projection's marks are intersected with the rows it
            // currently yields: there is no delta to prune them, so a
            // container that disappeared must simply stop counting.
            //
            // Against the PROJECTION, not the painted view. `live_keys()` is
            // the last painted order, which has the draft filter applied — so
            // typing `/c0` while two containers were marked silently dropped
            // the other one from the selection, and typing a draft that
            // matched neither ended select mode outright. A filter hides rows
            // from view; it does not deselect them. That is the rule the
            // store-backed side has always followed.
            Element::DerivedRows(e) => {
                if e.marked.is_empty() {
                    return HashSet::new();
                }
                let live: HashSet<ObjectKey> = self.projected_keys().into_iter().collect();
                e.marked.intersection(&live).cloned().collect()
            }
            _ => self
                .data_store()
                .map(|s| s.with_read(|i| i.marked().clone()))
                .unwrap_or_default(),
        }
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
    ///
    /// Returns `false` when the target does not exist in THIS table — a
    /// semantic target names a header, and not every table has every header.
    /// Refusing is the honest answer: the previous positional guess always
    /// found *a* column, which is why `N` silently sorted Events by TYPE.
    pub fn sort_by(&mut self, target: crate::SortTarget) -> bool {
        let col = match target {
            crate::SortTarget::Column(c) => c,
            // Resolved HERE, not at key-mapping time. `selected_data_col`
            // reads the last painted view, which is cold on a fresh element
            // and dropped on cover — and keys are drained in a batch with no
            // paint between them, so `Enter` then `S` used to map to no
            // action at all and the key simply vanished. By dispatch the
            // action layer has warmed the view, exactly as `~` relies on.
            crate::SortTarget::CursorColumn => match self.selected_data_col() {
                Some((c, _)) => c,
                None => return false,
            },
            crate::SortTarget::Named(header) => {
                match self
                    .headers_snapshot()
                    .iter()
                    .position(|h| h.eq_ignore_ascii_case(header))
                {
                    Some(c) => c,
                    None => return false,
                }
            }
        };
        let Some(it) = self.table_interaction_mut() else { return false };
        if it.sort.col == col {
            it.sort.ascending = !it.sort.ascending;
        } else {
            it.sort = SortSpec { col, ascending: true };
        }
        true
    }

    pub fn toggle_sort(&mut self) {
        let Some(col) = self.table_interaction().map(|i| i.sort.col) else { return };
        let _ = self.sort_by(crate::SortTarget::Column(col));
    }

    // -- Liveness ----------------------------------------------------------------

    /// Whether this element OWNS a live subscription right now. Elements
    /// without their own stream (filters, projections) report `true` —
    /// their data rides an ancestor's stream, revived by the stack's
    /// owner walk.
    pub fn is_live(&self) -> bool {
        match self {
            Element::ResourceList(e) => e.query.is_live(),
            // EXHAUSTIVE, and the arms are grouped by WHY they are live, not
            // lumped under a `_ => true`. "Assume live" is the unsafe
            // default: it is what suppresses the stale banner, so a new kind
            // that owned a stream would have claimed to be tracking the
            // cluster while its subscription was dead.
            //
            // Riding an ancestor's stream — revived by the stack's owner
            // walk, so liveness is the ancestor's to report.
            Element::RowFilter(_) | Element::DerivedRows(_) | Element::LogFilter(_) => true,
            // Nothing to be stale ABOUT: a one-shot fetch carries its own
            // phase (`ContentPhase`), a log session's liveness is the link's
            // (`Liveness::of_link`), and the rest are chrome or dialogs.
            Element::ContentView(_)
            | Element::LogSession(_)
            | Element::Overview(_)
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => true,
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
        let caps: Vec<u16> =
            headers.iter().map(|h| policy.max_width_for(h, max_col_width)).collect();
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
                max_col_widths: &caps,
            },
        ))
    });
    interaction.derived_level = level;
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
pub(crate) fn predicate_for_commit(text: String, column: Option<(usize, String)>) -> RowPredicate {
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
#[path = "../tests/element.rs"]
mod tests;
