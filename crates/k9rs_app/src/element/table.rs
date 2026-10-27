//! Table elements: a live query, the list it feeds, and the filters derived
//! over it.
//!
//! `ResourceList` owns a `LiveQuery` (the daemon subscription, or a
//! client-side one for the kinds the daemon does not serve) and hands
//! `RowFilter`/`DerivedRows` their `RowSource` at construction. Everything
//! here is about ROWS: the cursor, the sort, the column policy, the marks.
//!
//! Split out of `mod.rs` when it was 3063 lines holding 24 types. The seam is
//! measured, not guessed: this family and `super::log` reference each other
//! ZERO times in either direction — all 16 type names checked both ways.

use super::Element;
use std::collections::HashSet;
use std::sync::Arc;

use crate::nav::FilterInputState;
use crate::store::{
    derive_view, DeriveSpec, MetricsBinding, MetricsHub, PreparedView,
    RowSource, RowStore, SortSpec,
};
use crate::view::DerivedViewKind;
use crate::ColumnLevel;
use crate::client::{ClientSession, SubscriptionStream};
use k9rs_proto::overlay::ColumnRenderRules;
use k9rs_proto::protocol::{Namespace, ObjectKey, ObjectRef, ResourceId, SubscriptionFilter};
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
    pub(super) rid: Option<ResourceId>,
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
    pub(super) fn visible_indices(&self, headers: &[String], level: ColumnLevel) -> Vec<usize> {
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
    pub(super) cache: Option<(DeriveKey, Arc<PreparedView>)>,
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
    pub(super) fn cached_view(&self) -> Option<&Arc<PreparedView>> {
        self.cache.as_ref().map(|(_, v)| v)
    }

    /// Cursor clamped to the given view length (display/act-time clamp;
    /// the stored cursor is not written back).
    pub(super) fn clamped_selected(&self, len: usize) -> usize {
        if len == 0 { 0 } else { self.selected.min(len - 1) }
    }

    pub(super) fn adjust_offset(&mut self) {
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
pub(super) struct DeriveKey {
    pub(super) generation: u64,
    pub(super) metrics_version: u64,
    pub(super) sort: SortSpec,
    pub(super) draft: String,
    pub(super) level: ColumnLevel,
    pub(super) max_col_width: u16,
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
    pub(super) store: Arc<RowStore>,
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
    pub(super) query: LiveQuery,
    /// The unfiltered output handle this element hands to children
    /// (store + metrics binding, no predicates).
    pub(super) source: RowSource,
    pub(super) label: String,
    pub(super) title: String,
    /// The scope shown in the table title parentheses — the element's OWN
    /// scope, fixed at construction (empty for all-namespaces and
    /// cluster-scoped views). Never the ambient selector.
    pub(super) scope_label: String,
    pub(super) policy: ColumnPolicy,
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
    pub(super) source: RowSource,
    pub(super) label: String,
    /// Same resource as the parent (a refinement doesn't change WHAT you
    /// look at) — the SINGLE home for this element's identity. `rid()`
    /// reads `policy.rid`; there is no second copy to drift (a separate
    /// `rid` field used to exist and only half of the resolve path
    /// updated it, so a CRD resolved under a filter rendered stale
    /// columns/title).
    pub(super) policy: ColumnPolicy,
    pub(super) title: String,
    pub(super) scope_label: String,
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
    pub(super) source: RowSource,
    pub(super) key: ObjectKey,
    pub(super) kind: DerivedViewKind,
    /// The projected row this view came from, as a typed ref — scopes
    /// actions (logs/shell on a container) back to the parent object.
    pub(super) origin: ObjectRef,
    pub(super) headers: Vec<String>,
    pub(super) rules: Vec<ColumnRenderRules>,
    pub(super) label: String,
    pub(super) title: String,
    /// This projection's OWN marks. A store-backed list keys its marks on
    /// the shared `RowStore`; projected rows have no store to key them in,
    /// so they live here and die with the element — the same lifetime the
    /// rows themselves have. Reads intersect with the live projection
    /// (`Element::marked_snapshot`), which stands in for the delta-prune a
    /// projection can never receive.
    pub(super) marked: HashSet<ObjectKey>,
    pub interaction: TableInteraction,
}

/// The store-backed view pipeline shared by `ResourceList` and
/// `RowFilter` (their only difference is the predicate chain riding the
/// source).
pub(super) fn view_over_store(
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
pub(super) fn compile_draft(input: &FilterInputState) -> Option<SearchPattern> {
    let text = input.text();
    if text.is_empty() {
        return None;
    }
    // A `~` draft is column-restricted even before commit: match only the
    // targeted column by wrapping through the predicate path at commit;
    // during typing we match all columns (parity with the old draft).
    Some(SearchPattern::new(text))
}
