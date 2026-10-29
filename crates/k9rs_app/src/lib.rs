pub(crate) mod actions;
pub(crate) mod anim;
pub(crate) mod attachment;
pub(crate) mod batch;
pub(crate) mod config;
pub(crate) mod connection;
pub(crate) mod form;
pub(crate) mod kubectl_cache;
pub(crate) mod derived;
pub(crate) mod element;
// The client half of the daemon wire. It lived under `kube/` but is written
// in app vocabulary — it hands out `RowStore`/`LineStore` handles and builds
// `AppEvent`s — so it is the app's data source, not Kubernetes access.
pub(crate) mod client;
pub(crate) mod commands;
// The event vocabulary, the keymap and the key handler. `AppEvent` is
// DEFINED over `app::store` types and `ViewClass::of` takes an `app::Element`,
// so `event` was never a layer below app — it was app's own event half, kept
// in a sibling module, and the two were the last cycle in the tree.
pub(crate) mod event;
pub(crate) mod geometry;
pub(crate) mod handler;
pub(crate) mod keymap;
// Help's CONTENT, which is a data projection and not a widget — its own first
// line said "NOT a widget — it renders nothing", and it returns
// `(Vec<String>, Vec<ResourceRow>)` with no ratatui in sight. It sat in
// `ui::widgets`, so `app` had to reach up into the renderer to build the help
// element's rows.
pub(crate) mod help;
pub(crate) mod liveness;
pub(crate) mod nav;
pub(crate) mod select_gate;
pub(crate) mod state;
pub(crate) mod store;
pub(crate) mod text_input;
pub(crate) mod table;
pub(crate) mod takeover;
pub(crate) mod viewport;

// ---------------------------------------------------------------------------
// THE API
// ---------------------------------------------------------------------------
//
// Every module above is `pub(crate)`. This list is the whole of what `k9rs_ui`,
// `k9rs_session` and the binary can see, so adding a name here is a deliberate
// act rather than a side effect of declaring a module.
//
// It started as six `pub use <mod>::*;` globs — the API was defined by `*` and
// nothing could say which names were intended. Deleting the globs and asking
// the compiler what the tree actually needs got the list; narrowing the modules
// is what stops it drifting back, because a `pub` item in a `pub(crate)` module
// that nothing re-exports is now an `unreachable_pub` warning. That lint
// reported ZERO while the modules were public, and found a dead 102-line widget
// the moment they were not.
//
// Flat rather than module-qualified because these are distinctly-named types
// and functions; where a module name supplies a missing noun instead, as in
// `k9rs_util::width::truncate`, the module stays in the path.
//
// No count here on purpose. This said "40" and the surface was 91 — a number
// written once, never measured again, and quoted back as current two days
// later. `unreachable_pub` is the thing that actually holds this line, and it
// needs no number to do it.
pub use actions::{Action, LogRange, SortTarget};
pub use anim::{Anim, FRAME_INTERVAL};
pub use attachment::{Attached, Attachment};
pub use batch::BatchTracker;
pub use client::{ClientSession, ConnectionParams};
pub use commands::{ParsedCommand, COMMAND_WORDS};
pub use config::{load as load_config, AppConfig, KeyCombo, KeysConfig, LogConfig, LogFields};
pub use connection::{Connection, ContextSwitchState, LinkState};
pub use element::{ChainStep, 
    sense_of, ContentPhase, ContentSpec, ContentView, Element, ElementClass, LogInteraction,
    LogSession, MarkOutcome, Overview, QuerySpec, ResourceList, Sense, SpanOutcome, ViewExtent,
};
pub use event::{apply_event, AppEvent};
pub use form::{
    ContentViewState, FormDialog, FormFieldKind, FormFieldState, FormFocus, FormSubmit,
    SelectOption,
};
pub use geometry::{
    confirm_dialog_size, form_dialog_size, form_hints, form_hints_width, picker_hints,
    picker_hints_width, DIALOG_CHROME, DIALOG_MARGIN,
};
pub use handler::handle_key_event;
pub use help::HelpContent;
pub use keymap::{bar_hints, ViewClass};
pub use kubectl_cache::{ContentKind, KubectlCache};
pub use liveness::Liveness;
pub use nav::{K8sFieldSelector, RootSpec};
pub use select_gate::{gate_action, Gated};
pub use state::{CrdInfo, FlashLevel, FlashMessage, ItemCounts, KubeState, UiState};
pub use store::{row_key, PreparedView, RowPredicate, StorePayload, StoredLine};
pub use table::TableDataState;
pub use takeover::{
    ConfirmDialog, ConfirmFocus, ContainerAction, ContainerChoice, ContainerPicker, EditState,
    Modal,
    PendingAction, ShellConnectState, ShellState, SingleOp, TakeoverOp, TempFile,
};
pub use text_input::{handle_search_key, SearchInputResult, TextInput, TextKey};
pub use k9rs_proto::row::DerivedViewKind;
pub use viewport::{LineAnchor, Viewport};

use std::time::Duration;

use k9rs_proto::protocol::ResourceId;

pub use k9rs_proto::resource_def::{ColumnDef, ColumnLevel};
// Produced by the kubeconfig reader, projected into rows by `context_rows`
// below, and carried by `AppEvent::KubeconfigLoaded` — so it reads as an
// `app` type at every call site, and is declared where it is parsed.
pub use k9rs_kube::kubeconfig::KubeContext;

// ---------------------------------------------------------------------------
// Pinned resource list for tab cycling
// ---------------------------------------------------------------------------

/// The default ordered list of pinned resources for Tab/BackTab cycling.
/// Uses declaration order from `BuiltInKind`, which is where the resource
/// table now lives — the watcher registry no longer holds the defs.
pub fn default_pinned_resources() -> Vec<ResourceId> {
    k9rs_proto::resource_def::all_defs()
        .map(|def| def.resource_id())
        .collect()
}

/// Project what the `:` prompt accepts into rows.
///
/// Two sections, in the order you reach for them: the resources (whose
/// aliases are declared by the registry and by `LocalResourceKind`, which is
/// already their single source) and the static command words (declared by
/// [`crate::commands::COMMAND_WORDS`], which this module's
/// agreement test probes against the parser).
///
/// `Ctrl-a` used to format this as a plain-text DOCUMENT, so it could not be
/// grepped, sorted or scrolled, and it carried a hand-written copy of the
/// keymap that had drifted. Keys belong to `?`.
///
/// The configured exec resources are listed too, under their own names.
/// They used not to be, and the stated reason was that "its name comes from
/// the DAEMON's config and the client never sees the list" — which
/// `commands::exec_resource_by_name` contradicts on its first line: it
/// resolves `:node-costs` by reading exactly that list out of `AppConfig`.
/// Listing them is what made it safe to delete the generic `:exec` alias,
/// whose answer was `exec_configs.first()`.
fn seed_aliases(store: &store::RowStore, config: &AppConfig) {
    use k9rs_proto::row::{CellValue, ResourceRow};
    let mut rows: Vec<ResourceRow> = Vec::new();
    let mut push = |section: &str, alias: String, target: String| {
        rows.push(ResourceRow {
            cells: vec![
                CellValue::Text(section.to_string()),
                CellValue::Text(alias.clone()),
                CellValue::Text(target),
            ],
            name: format!("{}/{}", section, alias),
            namespace: None,
            ..Default::default()
        });
    };
    for def in k9rs_proto::resource_def::all_defs() {
        push("resource", def.aliases().join(" / "), def.gvr().kind.to_string());
    }
    for kind in k9rs_proto::local_types::LocalResourceKind::all() {
        // A kind with no alias is deliberately not typeable (`?` is not a
        // noun you browse); listing it would advertise a word that does
        // nothing.
        let aliases = kind.aliases();
        if aliases.is_empty() {
            continue;
        }
        push("resource", aliases.join(" / "), kind.kind_str().to_string());
    }
    // Through the SAME validation `exec_resource_by_name` applies, so the
    // view lists exactly what `:` accepts: an entry the daemon would skip
    // is not typeable, and advertising it would promise a view that answers
    // "Unknown local resource".
    for cfg in config.daemon.iter().flat_map(|d| d.exec_resources.iter()) {
        if cfg.validate().is_ok() {
            push("resource", cfg.name.clone(), "ExecResource".to_string());
        }
    }
    for doc in crate::commands::COMMAND_WORDS {
        push("command", doc.display(), doc.label.to_string());
    }
    store.apply(
        CoreData::SEED_EPOCH,
        store::StorePayload::Baseline(k9rs_proto::protocol::TableBaseline {
            headers: vec!["SECTION".into(), "ALIAS".into(), "TARGET".into()],
            rows,
        }),
    );
}

/// Column headers for the contexts table. Same shape as any other
/// resource's `headers()` — the contexts view is an ordinary table, so grep,
/// column filter, sort and column movement all work without knowing that
/// these rows came from a file rather than a cluster.
pub fn context_headers() -> Vec<String> {
    vec!["NAME".into(), "CLUSTER".into(), "USER".into(), "ACTIVE".into()]
}

/// Project the kubeconfig's contexts into rows. Enter rides on the row as a
/// [`k9rs_proto::row::DrillTarget`], so switching needs no special case in the key handler.
pub fn context_rows(contexts: &[KubeContext]) -> Vec<k9rs_proto::row::ResourceRow> {
    use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow, RowHealth};
    contexts
        .iter()
        .map(|c| ResourceRow {
            cells: vec![
                CellValue::Text(c.name.to_string()),
                // `Placeholder` renders "n/a" — the same word the header
                // shows. These used to be raw strings, so a context whose
                // kubeconfig entry has no `user:` rendered a BLANK cell here
                // while the header said "n/a" for the very same field.
                label_cell(c.identity.cluster()),
                label_cell(c.identity.user()),
                CellValue::Text(if c.is_current { "✓".into() } else { String::new() }),
            ],
            name: c.name.to_string(),
            // Cluster-scoped: a context isn't in a namespace.
            namespace: None,
            drill_target: Some(DrillTarget::SwitchContext(c.name.clone())),
            health: RowHealth::Normal,
            ..Default::default()
        })
        .collect()
}

/// A known kubeconfig label, or the table's own "no value" cell.
fn label_cell(label: Option<&str>) -> k9rs_proto::row::CellValue {
    use k9rs_proto::row::CellValue;
    match label {
        Some(s) => CellValue::Text(s.to_string()),
        None => CellValue::Placeholder,
    }
}

// `ColumnLevel` now lives in `kube::resource_def` next to `ColumnDef` — the
// metadata is co-located with the definitions it describes. `pub use`
// re-exports above make them available at `crate::ColumnLevel`.
//
// The old `EXTRA_COLUMNS` string table and `column_level(name: &str)` function
// are deleted — column visibility is declared per-column in each ResourceDef's
// `column_defs()` (or inferred from the header string by `ColumnDef::infer`).
//
// `ColumnSortKind` was deleted: `CellValue::cmp()` handles type-aware ordering
// directly, so no per-column sort-kind dispatch is needed.

// ---------------------------------------------------------------------------
// CoreData — app-level shared stores (chrome services, NOT navigation)
// ---------------------------------------------------------------------------

/// The always-on core stores that app chrome reads — command completion,
/// the namespace picker, overview stats, node-address lookups —
/// independent of what the nav stack is showing. A nav element viewing
/// one of these resources opens its OWN store + subscription like any
/// other resource (the daemon's watcher cache dedupes the upstream
/// watch); these exist for the chrome, not for views.
pub struct CoreData {
    pub namespaces: std::sync::Arc<store::RowStore>,
    pub nodes: std::sync::Arc<store::RowStore>,
    pub crds: std::sync::Arc<store::RowStore>,
    /// The kubeconfig's contexts. Here for the same reason as the others —
    /// command completion reads it and the `:ctx` view points at it, so it
    /// must outlive any single element — but seeded from DISK rather than
    /// watched, and deliberately NOT in `streams`: there is nothing to
    /// subscribe to. See `LiveQuery::client`.
    pub contexts: std::sync::Arc<store::RowStore>,
    /// What you can type at the `:` prompt. Client-owned like `contexts`:
    /// the rows are a fact about this binary's registry and command grammar.
    pub aliases: std::sync::Arc<store::RowStore>,
    /// The always-on subscriptions feeding the stores above. Replaced
    /// wholesale on (re)connect — the old streams drop, RSTing their
    /// substreams.
    streams: Vec<crate::client::SubscriptionStream>,
    /// The previous context's core stores, kept so switching back is not a
    /// cold load. See [`CoreData::switch_context`].
    parked: Option<(k9rs_proto::protocol::ContextName, ParkedCore)>,
}

/// One context's core stores, set aside while another context is active.
struct ParkedCore {
    namespaces: std::sync::Arc<store::RowStore>,
    nodes: std::sync::Arc<store::RowStore>,
    crds: std::sync::Arc<store::RowStore>,
}

impl CoreData {
    /// The core stores, seeded from `config` — which the alias view needs,
    /// because the exec resources a `:` prompt accepts are declared there.
    fn new(config: &AppConfig) -> Self {
        Self {
            namespaces: store::RowStore::new("namespaces"),
            nodes: store::RowStore::new("nodes"),
            crds: store::RowStore::new("customresourcedefinitions"),
            contexts: store::RowStore::client("contexts"),
            aliases: {
                // Seeded once here rather than re-derived per open: the
                // registry and the command grammar are compiled in and the
                // config is already in hand. Pure computation; `new` stays
                // I/O-free.
                let store = store::RowStore::client("aliases");
                seed_aliases(&store, config);
                store
            },
            streams: Vec::new(),
            parked: None,
        }
    }
}

impl Default for CoreData {
    /// The compiled-in default config, which declares no exec resources.
    fn default() -> Self {
        Self::new(&AppConfig::default())
    }
}

impl CoreData {
    /// Seed epoch for cached/handshake/discovery data: 0, always weaker
    /// than any live stream (the epoch allocator starts at 1) — a stale
    /// snapshot can never outrank live data, while an idle store accepts
    /// the seed.
    const SEED_EPOCH: u64 = 0;

    /// The app-level store for `kind`, if it has one.
    ///
    /// Three stores, and they answer TWO different questions — which is how
    /// "which stores are core" came to have three answers that disagreed:
    ///
    /// - `namespaces` and `nodes` are core RESOURCES: `is_core()` is true for
    ///   them, so `open_streams` subscribes them and the Overview counts
    ///   them. [`Self::counted_stores`] is that set, derived from the
    ///   declaration rather than restated.
    /// - `crds` is a core STORE with no subscription and no counter. It is
    ///   seeded from `AppEvent::Discovery` and feeds completion and the
    ///   resource picker.
    ///
    /// All three are parked and restored together by
    /// [`Self::switch_context`], because that is about the slice.
    ///
    /// The `_ => None` is a real "this kind has no app-level store", not a
    /// default: `every_core_resource_has_a_store` asserts that nothing
    /// `is_core()` falls into it, which is the case that used to be silent
    /// (no subscription, no stat, no warning, no compile error).
    pub fn store_for(
        &self,
        kind: k9rs_proto::resource_def::BuiltInKind,
    ) -> Option<&std::sync::Arc<store::RowStore>> {
        use k9rs_proto::resource_def::BuiltInKind as K;
        match kind {
            K::Namespace => Some(&self.namespaces),
            K::Node => Some(&self.nodes),
            K::CustomResourceDefinition => Some(&self.crds),
            _ => None,
        }
    }

    /// The stores behind the Overview's counters, from the per-resource
    /// `is_core()` declaration — one derivation, not a hand-written list.
    pub fn counted_stores(&self) -> impl Iterator<Item = &std::sync::Arc<store::RowStore>> {
        k9rs_proto::resource_def::all_defs()
            .filter(|def| def.is_core())
            .filter_map(|def| def.resource_id().built_in_kind())
            .filter_map(|kind| self.store_for(kind))
    }

    /// Seed a core store from cached data (connection handshake,
    /// discovery). Headers come from the registry def — single source of
    /// truth with the live-watcher path.
    pub fn seed(
        &self,
        kind: k9rs_proto::resource_def::BuiltInKind,
        rows: Vec<k9rs_proto::row::ResourceRow>,
    ) {
        let Some(target) = self.store_for(kind) else { return };
        let def = kind.def();
        target.apply(
            Self::SEED_EPOCH,
            store::StorePayload::Baseline(k9rs_proto::protocol::TableBaseline {
                headers: def.default_headers(),
                rows,
            }),
        );
    }

    /// Open the always-on subscriptions (namespaces + nodes) into the
    /// core stores. Called on (re)connect; previous streams drop.
    pub fn open_streams(&mut self, session: &crate::client::ClientSession) {
        self.streams.clear();
        for def in k9rs_proto::resource_def::all_defs() {
            if !def.is_core() {
                continue;
            }
            let rid = def.resource_id();
            let Some(target) = rid.built_in_kind().and_then(|k| self.store_for(k)) else {
                continue;
            };
            self.streams.push(session.subscribe_stream(
                // Said in the type, not in a comment: `clear()` above
                // dropped every predecessor and `SubscriptionStream::Drop`
                // aborts, so they are already retired.
                crate::client::Supersedes::RetiredByDrop,
                rid,
                k9rs_proto::protocol::Namespace::All,
                None,
                std::sync::Arc::clone(target),
                false,
            ));
        }
    }

    /// The store a CLIENT-OWNED resource reads from, or `None` for the
    /// ordinary daemon-backed kind. One place holds this knowledge, so
    /// `root_list_element` can route without every call site learning which
    /// resources happen to be local — and adding another client-owned
    /// resource later is one arm here, not a new special case everywhere.
    pub fn client_store(&self, rid: &ResourceId) -> Option<&std::sync::Arc<store::RowStore>> {
        match rid {
            ResourceId::Local(k9rs_proto::local_types::LocalResourceKind::Context) => {
                Some(&self.contexts)
            }
            ResourceId::Local(k9rs_proto::local_types::LocalResourceKind::Alias) => {
                Some(&self.aliases)
            }
            _ => None,
        }
    }

    /// Why the core counters can't be vouched for, if they can't.
    ///
    /// The Overview's stats are aggregated across these stores, so liveness
    /// is a property of the SET — a number is only as live as the least live
    /// store behind it. Without this the `mark_stale` in `switch_context` had
    /// no reader at all: A→B→A restored the parked rows and painted the
    /// previous visit's counts byte-identically to live ones.
    ///
    /// EXACTLY [`Self::counted_stores`], because this sentence labels those
    /// counters. It used to walk a hand-written `[namespaces, nodes, crds]`,
    /// and `crds` feeds no counter and has no subscription — its staleness
    /// clears only when `AppEvent::Discovery` arrives with a NON-EMPTY list,
    /// which is an eager two-LIST apiserver poll and strictly slower than
    /// the subscription baselines that clear the other two. So the banner
    /// reliably outlived the staleness of every number it was labelling, and
    /// would have become permanent on any later connect where the CRD list
    /// came back empty (lost permission, or a failed list behind
    /// `unwrap_or_default`).
    pub fn stale_reason(&self) -> Option<String> {
        self.counted_stores().find_map(|store| {
            match store.with_read(|i| i.state.clone()) {
                crate::TableDataState::Stale(reason) => Some(reason),
                _ => None,
            }
        })
    }

    /// A root element for a client-owned resource, or `None` if `rid` names
    /// an ordinary daemon-backed one. Takes NO session — that is the point:
    /// the contexts view has to be buildable with no daemon at all, which is
    /// exactly the situation it exists for.
    pub fn client_root_element(
        &self,
        metrics: &std::sync::Arc<store::MetricsHub>,
        rid: &ResourceId,
        namespace: k9rs_proto::protocol::Namespace,
    ) -> Option<element::Element> {
        let store = self.client_store(rid)?;
        Some(element::Element::ResourceList(element::ResourceList::client(
            element::QuerySpec { rid: rid.clone(), namespace, filter: None },
            std::sync::Arc::clone(store),
            metrics,
            rid.short_label().to_lowercase(),
        )))
    }

    /// Re-seed the contexts table from a fresh kubeconfig read. Not a
    /// subscription: the rows come from the client's own disk, so this is
    /// the only writer, and it re-runs whenever the kubeconfig is re-read.
    pub fn seed_contexts(&self, contexts: &[KubeContext]) {
        self.contexts.apply(
            Self::SEED_EPOCH,
            store::StorePayload::Baseline(k9rs_proto::protocol::TableBaseline {
                headers: context_headers(),
                rows: context_rows(contexts),
            }),
        );
    }

    /// Re-seed the keybindings table for the view `?` was pressed in.
    ///
    /// Re-seeded on every press rather than once at startup, because help is
    /// CONTEXTUAL: the rows depend on which view you were looking at and on
    /// that resource's declared capabilities. Same single-writer shape as
    /// `seed_contexts` — the rows come from this process, so there is no
    /// subscription and no epoch race.
    pub fn seed_keybindings(
        &self,
        caps: Option<k9rs_proto::protocol::ResourceCapabilities>,
        keys: crate::KeysConfig,
        class: crate::keymap::ViewClass,
        markable: bool,
    ) -> std::sync::Arc<store::RowStore> {
        // A FRESH store per press, not one shared field.
        //
        // The rows are contextual — they describe the view `?` was pressed
        // in — so a single store meant a help element already on the stack
        // silently re-rendered with a LATER press's keymap: `?` over a pod
        // table, drill to something else, `?` again, pop, and the first one
        // is now showing the second one's keys under the first one's
        // breadcrumb. Contextual content and shared storage cannot both be
        // true.
        let store = store::RowStore::client("keybindings");
        let (headers, rows) = crate::help::HelpContent::rows(caps, keys, class, markable);
        store.apply(
            Self::SEED_EPOCH,
            store::StorePayload::Baseline(k9rs_proto::protocol::TableBaseline {
                headers,
                rows,
            }),
        );
        store
    }

    /// Context switch: set the OUTGOING context's core stores aside and
    /// bring back the INCOMING context's if we still hold them.
    ///
    /// The daemon already keeps its watchers warm for a minute, but the
    /// client used to throw its own rows away on every switch — so
    /// re-entering a context you were just on walked
    /// Initializing → Loading → baseline from zero while the answer sat
    /// ready in the daemon.
    ///
    /// Restored rows come back marked **stale, not ready**. They were true
    /// when they arrived and are worth showing at once, but nothing is
    /// feeding them until the fresh subscription's baseline lands (which
    /// clears the mark). That is the same honesty rule as a dead
    /// cluster-side watch, and it is what keeps this from being the "stale
    /// rows painted as live" bug wearing a cache for a costume.
    ///
    /// ONE parked slot, not a map: the pattern this serves is A→B→A, and an
    /// unbounded map would pin every cluster's rows for the life of the
    /// process. `contexts` is never parked — the kubeconfig is a property of
    /// this machine, not of any cluster.
    pub fn switch_context(
        &mut self,
        outgoing: Option<k9rs_proto::protocol::ContextName>,
        incoming: &k9rs_proto::protocol::ContextName,
    ) {
        // Drop the old subscriptions FIRST: a live bridge would keep writing
        // into a store we are about to park.
        self.streams.clear();

        let restored = match self.parked.take() {
            Some((name, core)) if &name == incoming => Some(core),
            _ => None,
        };
        let incoming_stores = match restored {
            Some(core) => {
                for store in [&core.namespaces, &core.nodes, &core.crds] {
                    store.mark_stale("from your previous visit — refreshing");
                }
                core
            }
            None => ParkedCore {
                namespaces: store::RowStore::new("namespaces"),
                nodes: store::RowStore::new("nodes"),
                crds: store::RowStore::new("customresourcedefinitions"),
            },
        };
        let outgoing_stores = ParkedCore {
            namespaces: std::mem::replace(&mut self.namespaces, incoming_stores.namespaces),
            nodes: std::mem::replace(&mut self.nodes, incoming_stores.nodes),
            crds: std::mem::replace(&mut self.crds, incoming_stores.crds),
        };
        // No outgoing context (first connect) means nothing worth keeping.
        self.parked = outgoing.map(|name| (name, outgoing_stores));
    }

    /// Abort the always-on subscriptions' bridges — the core-store half of
    /// the reconnect/switch choke point's `abort_all_streams` (which walks
    /// only nav elements). Established bridges hold the DEAD session's mux
    /// watch, whose retained value lets them skip the wait and burn a
    /// backoff-retry loop against the dropped mux for the whole gap; abort
    /// them here, and `open_streams` replaces them wholesale on
    /// `ConnectionEstablished`. Handles stay resident (`abort` ≠ drop) so
    /// the stores keep their rows for the reconnect-continuity display.
    pub fn abort_streams(&self) {
        for stream in &self.streams {
            stream.abort();
        }
    }

    /// Namespace names, for completion and the picker.
    pub fn namespace_names(&self) -> Vec<String> {
        self.namespaces.with_read(|i| i.rows.iter().map(|r| r.name.clone()).collect())
    }
}

// ---------------------------------------------------------------------------
// App — main application state
// ---------------------------------------------------------------------------

/// Why the TUI is exiting. Printed to stderr after terminal restoration.
/// (No "daemon disconnected" variant: a lost daemon triggers auto-reconnect
/// and the user stays in the TUI — disconnection stopped being an exit when
/// that rework landed.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    /// User requested quit (q, :quit, Ctrl-C).
    UserQuit,
    /// An error occurred.
    Error(String),
}

pub struct App {
    pub should_quit: bool,
    pub exit_reason: Option<ExitReason>,

    /// App-level shared stores (completion / picker / overview chrome).
    pub core: CoreData,

    /// The navigation stack: self-contained scope elements, strict LIFO.
    pub nav: nav::NavStack,
    /// Ordered list of pinned resources for Tab/BackTab cycling.
    pub pinned_resources: Vec<ResourceId>,

    /// Command history for `:` command mode (max 50 entries).
    pub command_history: Vec<String>,

    /// The daemon connection lifecycle — link liveness (the render gate),
    /// ever-connected fatality rule, and the reconnect plan/backoff — as
    /// ONE value with total edges. See [`Connection`]. (Replaced five
    /// loose fields whose implicit machine shipped two real bugs.)
    pub conn: Connection,

    /// The in-flight batch operation, if any — correlates per-target
    /// `OpResult`s (unmark per-success, aggregate ONE summary flash).
    /// At most one, ENFORCED: `handle_batch_op` refuses to dispatch
    /// while this is `Some`; it is cleared when the last result lands,
    /// and taken (partial tally folded into the flash) on daemon
    /// disconnect and on context switch — the two paths where the
    /// remaining results can never arrive. Residual: a daemon-side task
    /// that dies WITHOUT sending its result leaves the tracker waiting
    /// until one of those paths runs (no timeout; a wire correlation id
    /// + deadline would be the full fix if it ever bites).
    pub pending_batch: Option<BatchTracker>,

    /// When true, Ctrl-C does not quit the application (`noExitOnCtrlC` config).
    pub no_exit_on_ctrl_c: bool,
    /// When true, destructive actions (delete, edit, scale, restart, force-kill, shell) are disabled.
    /// Refuse mutations CLIENT-side. Resolved by
    /// [`ConnectionParams::read_only`], which the daemon-side gate must use
    /// too — the two were computed separately and disagreed.
    pub read_only: bool,

    /// User config loaded from ~/.config/k9rs/config.yaml.
    pub config: AppConfig,

    /// Pure display/interaction state (flash, dialogs, input mode, theme, …).
    pub ui: UiState,
    /// Cluster/data state (context, namespace, metrics, caches, …).
    pub kube: KubeState,
}

impl App {
    /// What the chrome is describing: the confirmed context while the link is
    /// up, otherwise whatever this session is aiming at — paired with the
    /// identity of THAT context.
    ///
    /// THE answer. The renderer used to derive the name and the identity
    /// separately and they could disagree; see [`Attachment`] for the
    /// bug and why one value fixes it. Distinct from
    /// [`KubeState::reconnect_target`], which answers a different question —
    /// what the NEXT session should aim at — and deliberately ignores the
    /// kubeconfig candidate, since that candidate is produced BY the session
    /// build it would be feeding.
    pub fn attachment(&self) -> Attachment {
        match Liveness::of_link(&self.conn) {
            Liveness::NoContext => Attachment::NoContext,
            live if live.shows_data() => match &self.kube.context {
                Some(context) => {
                    Attachment::live(context.clone(), self.kube.identity.clone())
                }
                // Linked but nothing confirmed yet: the initial handshake.
                None => Attachment::Opening,
            },
            // Link down. What we are AIMING at outranks what we last
            // confirmed: `kube.context` is kept so a failed switch can fall
            // back to it, but right now we are attached to neither it nor the
            // target, and naming it reads as "you are on prod" when prod is
            // what you just left.
            _ => match (
                self.kube.context_switch.target(),
                self.kube.connecting.as_ref(),
                &self.kube.context,
            ) {
                // A switch target. Its identity is the kubeconfig candidate's
                // ONLY when the candidate is that same context — the `filter`
                // is the whole fix. Before it, a target with no candidate yet
                // fell through to `kube.identity`, which describes the context
                // being LEFT; and an A→B→C switch would have shown B's
                // cluster under C's name.
                (Some(target), candidate, _) => Attachment::connecting(
                    target.clone(),
                    candidate
                        .filter(|(name, _)| name == target)
                        .map(|(_, identity)| identity.clone())
                        .unwrap_or_default(),
                ),
                // The session's own candidate from disk: name and identity
                // come from the same kubeconfig entry, so they agree.
                (None, Some((context, identity)), _) => {
                    Attachment::connecting(context.clone(), identity.clone())
                }
                // Reconnecting to the last-confirmed context: its identity is
                // the confirmed one, which is the same context.
                (None, None, Some(context)) => {
                    Attachment::connecting(context.clone(), self.kube.identity.clone())
                }
                (None, None, None) => Attachment::Opening,
            },
        }
    }

    /// `config` is loaded (and validated, loudly) by `main` BEFORE any
    /// TUI setup and threaded in — the App never reads the config file
    /// itself, so tests are hermetic by construction and a config error
    /// is a visible startup failure, not a silent default fallback.
    /// The startup `App`.
    ///
    /// Three parameters, down from five. `session: &ClientSession` was
    /// discarded on the first line (`let _ = session`) — the Overview root
    /// opens no subscription — and `context` was passed `None`
    /// unconditionally by the one caller, which is what `state.rs` already
    /// documents as correct: the context is published by the connection
    /// manager, not chosen at construction. A parameter that carries no
    /// information is a reader's question with no answer.
    pub fn new(
        theme: k9rs_theme::Theme,
        namespace: k9rs_proto::protocol::Namespace,
        config: AppConfig,
    ) -> Self {
        let metrics = store::MetricsHub::new();
        // The startup root is the Overview landing page — no resource
        // watch opens until the user navigates to one (startup args or
        // `:cmd` reset to a resource root).
        let root = element::Element::Overview(element::Overview);
        Self::new_with_root(None, namespace, metrics, root, config, theme)
    }

    /// Test-only: an App whose root element rides a parked stream — no
    /// session, no daemon, no network. The config is the compiled-in
    /// DEFAULT, never the machine's `~/.config/k9rs/config.yaml` — a
    /// user's key rebindings (or any other setting) must not change
    /// what the tests assert. Hermetic by construction: the config is a
    /// parameter, so no test path can open the file.
    ///
    /// `config.yaml` was only HALF of it. The machine's
    /// `~/.config/k9rs/overlays/*.yaml` reached the suite too, through a
    /// `LazyLock` that read `$HOME` on first access — and overlays carry key
    /// bindings (`event::handler::lookup_overlay_key`) and column levels
    /// (`element::ResourceList::level_for`), so a developer's own overlay
    /// really did change what these tests saw. That one is closed the same
    /// way, by ownership rather than by a parameter: `main` loads overlays
    /// and a test binary never does. See `kube::overlay::OVERLAYS`.
    ///
    /// The theme was the remaining `$HOME` read on this path, and it is
    /// gone: `App::new` took an `Appearance` and called `Theme::load`
    /// itself, which reads `$HOME/.config/k9rs/skins`. It was inert here
    /// only because the default skin name is empty and `load` early-returned
    /// on that — a fact about the default, not about construction. `main`
    /// loads the theme now and hands it in, the same hoist the appearance
    /// resolution already had, and for the same reason: this constructor is
    /// I/O-free, and a loaded skin's complaints have somewhere to go.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        let namespace = k9rs_proto::protocol::Namespace::All;
        let metrics = store::MetricsHub::new();
        let root = element::Element::ResourceList(element::ResourceList::open_for_test(
            element::QuerySpec {
                rid: k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                namespace: namespace.clone(),
                filter: None,
            },
            &metrics,
            "pods".to_string(),
        ));
        // Pinned dark, and built rather than LOADED: a test must not depend
        // on the developer's terminal, nor on their skins directory.
        Self::new_with_root(
            None,
            namespace,
            metrics,
            root,
            AppConfig::default(),
            k9rs_theme::Theme::for_appearance(k9rs_theme::term_bg::Appearance::Dark),
        )
    }

    fn new_with_root(
        context: Option<k9rs_proto::protocol::ContextName>,
        namespace: k9rs_proto::protocol::Namespace,
        metrics: std::sync::Arc<store::MetricsHub>,
        root: element::Element,
        config: AppConfig,
        // BUILT by the caller, never loaded here: resolving the appearance
        // queries the terminal and loading a skin reads `$HOME`, and this
        // constructor must stay free of I/O — `new_for_test` would otherwise
        // open `/dev/tty` and the developer's skins directory from the test
        // suite.
        theme: k9rs_theme::Theme,
    ) -> Self {
        let cache_capacity = config.ui.cache_capacity;
        Self {
            should_quit: false,
            exit_reason: None,
            core: CoreData::new(&config),
            nav: nav::NavStack::new(root),
            pinned_resources: default_pinned_resources(),
            command_history: Vec::new(),
            no_exit_on_ctrl_c: config.no_exit_on_ctrl_c,
            read_only: crate::ConnectionParams::read_only(false, &config),
            conn: Connection::new(),
            pending_batch: None,
            config,
            ui: UiState::new(theme),
            kube: KubeState {
                context,
                connecting: None,
                identity: k9rs_proto::protocol::ClusterIdentity::default(),
                selected_ns: namespace,
                context_switch: ContextSwitchState::Stable,
                metrics,
                kubectl_cache: KubectlCache::new(Duration::from_secs(30), cache_capacity),
            },
        }
    }

    /// Whether a DAEMON-backed resource view can be opened at all.
    ///
    /// False with no context: such a root subscribes against a session that
    /// doesn't exist, so it lands the user on a permanently empty table — and
    /// because every one of those paths `nav.reset`s, it takes the contexts
    /// picker with it. The picker is the root precisely so it can't be
    /// escaped; Tab and `:pods` were walking around that.
    ///
    /// Client-owned resources (the picker itself) are exempt — they need no
    /// session, which is the whole point of them.
    pub fn can_open_resource_view(&self, rid: &ResourceId) -> bool {
        self.core.client_store(rid).is_some()
            || !matches!(
                crate::Liveness::of_link(&self.conn),
                crate::Liveness::NoContext
            )
    }

    /// Build a root list element for `rid`, scoped by `namespace`.
    ///
    /// The one construction site where the ambient selector is a legitimate
    /// input: roots are built FROM the selector, while drills carry their own
    /// intrinsic scope. (That paragraph was sitting on
    /// [`Self::can_open_resource_view`] above, 25 lines from the function it
    /// describes, which made that function's rustdoc summary line a sentence
    /// about constructing an element.)
    ///
    /// Routes on where the rows come from: a client-owned resource (the
    /// kubeconfig's contexts) reads a store the app already holds and opens
    /// no subscription, so it works with no daemon at all; everything else
    /// subscribes as usual.
    pub fn root_list_element(
        session: &crate::client::ClientSession,
        metrics: &std::sync::Arc<store::MetricsHub>,
        core: &CoreData,
        rid: ResourceId,
        namespace: k9rs_proto::protocol::Namespace,
    ) -> element::Element {
        if let Some(el) = core.client_root_element(metrics, &rid, namespace.clone()) {
            return el;
        }
        let label = rid.short_label().to_lowercase();
        element::Element::ResourceList(element::ResourceList::open(
            session,
            element::QuerySpec { rid, namespace, filter: None },
            metrics,
            label,
        ))
    }

    /// Same, seeding cursor/sort continuity from the element being
    /// replaced or covered.
    pub fn list_element_from_top(
        &self,
        session: &crate::client::ClientSession,
        rid: ResourceId,
        namespace: k9rs_proto::protocol::Namespace,
        filter: Option<k9rs_proto::protocol::SubscriptionFilter>,
        label: String,
    ) -> element::Element {
        element::Element::ResourceList(element::ResourceList::open_from(
            session,
            element::QuerySpec { rid, namespace, filter },
            &self.kube.metrics,
            label,
            self.nav.top(),
        ))
    }

    // -- Element dispatch -------------------------------------------------------
    //
    // The TUI drives exactly one element: the TOP of the nav stack. The
    // contexts panel (chrome until the route fold-in) keeps its own arm.

    /// The pinned resource `step` places from the current one, wrapping.
    ///
    /// `&self`: neither direction mutates anything, and both took
    /// `&mut self`, which forced every caller to hold a mutable borrow of
    /// the whole `App` to ask a question. Seven of their eight lines were
    /// identical and the `Pod` fallback was written twice.
    ///
    /// An unpinned current resource counts as position 0, so Tab from a
    /// drilled-into view lands on the second pin rather than nowhere.
    pub fn tab(&self, step: isize) -> ResourceId {
        let pinned = &self.pinned_resources;
        let Some(len) = std::num::NonZeroUsize::new(pinned.len()) else {
            return k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id();
        };
        let len = len.get();
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| Some(r) == current).unwrap_or(0);
        // `rem_euclid` over `isize` so a negative step wraps without the
        // `if idx == 0 { len - 1 }` special case each direction had.
        let next = (idx as isize + step).rem_euclid(len as isize) as usize;
        pinned[next].clone()
    }

    /// The next pinned resource.
    pub fn next_tab(&self) -> ResourceId {
        self.tab(1)
    }

    /// The previous pinned resource.
    pub fn prev_tab(&self) -> ResourceId {
        self.tab(-1)
    }

    pub fn col_left(&mut self) {
        self.nav.top_mut().col_left();
    }

    pub fn col_right(&mut self) {
        self.nav.top_mut().col_right();
    }

    pub fn col_first(&mut self) {
        self.nav.top_mut().col_first();
    }

    pub fn col_last(&mut self) {
        self.nav.top_mut().col_last();
    }

    pub fn select_next(&mut self) {
        self.nav.top_mut().select_next();
    }
    pub fn select_prev(&mut self) {
        self.nav.top_mut().select_prev();
    }
    pub fn page_up(&mut self) {
        self.nav.top_mut().page_up();
    }
    pub fn page_down(&mut self) {
        self.nav.top_mut().page_down();
    }
    pub fn go_home(&mut self) {
        self.nav.top_mut().go_home();
    }
    pub fn go_end(&mut self) {
        self.nav.top_mut().go_end();
    }

    /// Toggle mark on the row under the cursor.
    pub fn toggle_mark(&mut self) -> element::MarkOutcome {
        self.nav.top_mut().toggle_mark()
    }

    /// Mark the span from the nearest visible mark to the cursor.
    pub fn span_mark(&mut self) -> element::SpanOutcome {
        self.nav.top_mut().span_mark()
    }

    /// Clear all marks on the top element's data. `false` = the view has
    /// no markable rows.
    pub fn clear_marks(&mut self) -> bool {
        self.nav.top_mut().clear_marks()
    }

    /// Sort the top element by the given target column (same column
    /// toggles direction). The next derive applies it — filters and
    /// draft text ride along by construction, so there is nothing to
    /// re-apply.
    pub fn sort_by(&mut self, target: crate::SortTarget) {
        if self.nav.top_mut().sort_by(target) {
            return;
        }
        // A semantic target this table doesn't have. Say so — a dead key that
        // says nothing is how the old positional guess hid: it always found
        // *a* column, so `N` on Events looked like it worked and sorted by
        // event type.
        if let crate::SortTarget::Named(header) = target {
            self.ui.flash = Some(FlashMessage::warn(format!(
                "No {header} column in this view"
            )));
        }
    }

    /// Toggle the sort direction on the top element's current column.
    pub fn toggle_sort_direction(&mut self) {
        self.nav.top_mut().toggle_sort();
    }

    /// Advance tick counter, expire flash messages, etc.
    /// Returns `true` if the UI should be redrawn (e.g. flash expired, loading animation).
    pub fn tick(&mut self) -> bool {
        self.ui.tick_count = self.ui.tick_count.wrapping_add(1);
        let mut changed = false;
        // Expire flash messages
        if let Some(ref flash) = self.ui.flash {
            if flash.is_expired(&self.config.ui.flash) {
                self.ui.flash = None;
                changed = true;
            }
        }
        // Expire row-change flash on the TOP element's store. Covered
        // stores expire lazily when they become top again.
        if let Some(store) = self.nav.top().data_store() {
            if store.expire_flash(Duration::from_secs(self.config.ui.change_highlight_secs)) {
                changed = true;
            }
        }
        // NOTE: loading-spinner animation is NOT driven from here. It used to
        // be a hand-maintained list of loading states (resource / log / shell)
        // that had to mirror every place a spinner is drawn — and drifted
        // (yaml/describe were never covered). The spinner now owns its own
        // liveness and cadence via `ui::anim::Anim`, driven by the event loop's
        // animation clock. See `crate::anim`.
        changed
    }

    /// Build completion candidates dynamically based on command input.
    pub fn command_completions(&self) -> Vec<String> {
        let Some((cmd_input, _)) = self.ui.command_input() else { return Vec::new() };
        complete_command(cmd_input, &self.core)
    }

    /// Returns the best (first) completion match, if any.
    pub fn best_completion(&self) -> Option<String> {
        let (input, _) = self.ui.command_input()?;
        if input.trim().is_empty() {
            return None;
        }
        self.command_completions().into_iter().next()
    }

    /// Accept the current ghost-text completion into the command input.
    pub fn accept_completion(&mut self) {
        if let Some(completion) = self.best_completion() {
            if let Some((input, _)) = self.ui.command_input_mut() {
                input.set(completion);
            }
        }
    }

    /// Returns filtered and total counts for the top element.
    pub fn active_table_items_count(&self) -> ItemCounts {
        self.nav.top().counts()
    }

    /// Capability manifest for the top element — pure data over the
    /// element's identity, computed on demand.
    pub fn current_capabilities(&self) -> k9rs_proto::protocol::ResourceCapabilities {
        self.nav.top().capabilities()
    }

    /// Compute health statistics for all core resources. Returns
    /// `(label, total, healthy)` tuples. Used by the overview page —
    /// reads the app-level core stores, not navigation state.
    pub fn core_resource_stats(&self) -> Vec<(&'static str, usize, usize)> {
        use k9rs_proto::row::RowHealth;
        let mut stats = Vec::new();
        for def in k9rs_proto::resource_def::all_defs() {
            if !def.is_core() {
                continue;
            }
            let Some(kind) = def.resource_id().built_in_kind() else { continue };
            let Some(store) = self.core.store_for(kind) else { continue };
            let (total, healthy) = store.with_read(|i| {
                (
                    i.rows.len(),
                    i.rows.iter().filter(|r| matches!(r.health, RowHealth::Normal)).count(),
                )
            });
            stats.push((def.short_label(), total, healthy));
        }
        stats
    }

    /// Find a discovered CRD by its kind name (case-insensitive), from
    /// the app-level CRD store (Discovery-fed).
    pub fn find_crd_by_name(&self, cmd: &str) -> Option<CrdInfo> {
        let lower = cmd.to_lowercase();
        self.core.crds.with_read(|i| {
            i.rows.iter().find_map(|row| {
                let info = row.crd_info.as_ref()?;
                let kind_lower = info.kind.to_lowercase();
                let name_lower = row.name.to_lowercase();
                let plural_lower = info.plural.to_lowercase();
                // Match by: kind, plural, full CRD name, kind+"s", or the
                // short plural from the CRD name (before the first dot).
                if kind_lower == lower
                    || plural_lower == lower
                    || name_lower == lower
                    || format!("{}s", kind_lower) == lower
                    || name_lower.split('.').next().is_some_and(|short| short == lower)
                {
                    // CrdInfo is a type alias for CrdRef — clone the row's
                    // stored ref directly, no field-by-field copy.
                    Some(info.clone())
                } else {
                    None
                }
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Command completion (extracted from App to reduce god-object surface)
// ---------------------------------------------------------------------------

/// All known resource command aliases, derived from the registry.
fn resource_commands() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = k9rs_proto::resource_def::all_defs()
        .flat_map(|def| def.aliases().iter().copied())
        .collect();
    for kind in &k9rs_proto::local_types::LocalResourceKind::all() {
        v.extend(kind.aliases().iter().copied());
    }
    // `["alias", "aliases", "a"]` used to be appended by hand here, because
    // `:alias` was a bespoke command rather than a resource. It is a
    // `LocalResourceKind` now, so the loop above already has it — and the
    // hand-written copy was only ever invisible because of the `dedup`.
    v.sort();
    v.dedup();
    v
}

/// Build completion candidates for the given command input. Reads the
/// app-level core stores for namespace/CRD/context names. Pure function —
/// no App needed.
fn complete_command(
    cmd_input: &str,
    core: &CoreData,
) -> Vec<String> {
    let input_lower = cmd_input.trim_start().to_lowercase();

    // CRD name candidates as (kind, plural, short-plural), lowercased —
    // gathered once from the core store for the checks below.
    let crd_candidates: Vec<(String, String, String)> = core.crds.with_read(|i| {
        i.rows
            .iter()
            .filter_map(|row| {
                let info = row.crd_info.as_ref()?;
                Some((
                    info.kind.to_lowercase(),
                    info.plural.to_lowercase(),
                    row.name.split('.').next().unwrap_or("").to_lowercase(),
                ))
            })
            .collect()
    });

    // Namespace completion: "ns <tab>" or "namespace <tab>"
    if input_lower.starts_with("ns ") || input_lower.starts_with("namespace ") {
        let cmd_prefix = if input_lower.starts_with("ns ") { "ns " } else { "namespace " };
        let mut completions: Vec<String> = core.namespace_names().into_iter()
            .map(|ns| format!("{}{}", cmd_prefix, ns))
            .filter(|s| s.to_lowercase().starts_with(&input_lower))
            .collect();
        completions.sort();
        completions.dedup();
        return completions;
    }

    // Context completion: "ctx <tab>" or "context <tab>"
    if input_lower.starts_with("ctx ") || input_lower.starts_with("context ") {
        let cmd_prefix = if input_lower.starts_with("ctx ") { "ctx " } else { "context " };
        // Same store the `:ctx` view renders from — one source, so a
        // context can never be completable but unlistable, or vice versa.
        let names = core.contexts.with_read(|i| {
            i.rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>()
        });
        let mut completions: Vec<String> = names.iter()
            .map(|n| format!("{}{}", cmd_prefix, n))
            .filter(|s| s.to_lowercase().starts_with(&input_lower))
            .collect();
        completions.sort();
        completions.dedup();
        return completions;
    }

    // Resource + namespace completion: "deploy kube-system"
    if let Some(space_pos) = input_lower.find(' ') {
        let resource_part = &input_lower[..space_pos];
        let rc = resource_commands();
        let is_builtin = rc.contains(&resource_part);
        let is_crd = !is_builtin
            && crd_candidates.iter().any(|(kind, plural, short)| {
                resource_part == kind || resource_part == plural || resource_part == short
            });
        if is_builtin || is_crd {
            let mut completions: Vec<String> = core.namespace_names().into_iter()
                .map(|ns| format!("{} {}", resource_part, ns))
                .filter(|s| s.to_lowercase().starts_with(&input_lower))
                .collect();
            completions.sort();
            completions.dedup();
            return completions;
        }
    }

    // Base command completion
    let mut all_commands: Vec<&str> = resource_commands();
    // The static command words, from the ONE declaration the parser is
    // tested against. This used to be a hand-written slice beside
    // `resource_commands()` — which already derived `ctx` and `alias` from
    // the registry, so those three were duplicated, and `namespace` was
    // missing entirely: `:names…` completed to nothing while `:ns` worked.
    all_commands.extend(
        crate::commands::COMMAND_WORDS
            .iter()
            .flat_map(|doc| doc.words.iter().copied()),
    );

    let mut completions: Vec<String> = all_commands.iter()
        .map(|s| String::from(*s))
        .filter(|s| s.starts_with(&input_lower))
        .collect();

    // CRD name completions
    for (kind_lower, plural_lower, short_plural) in &crd_candidates {
        for candidate in [kind_lower, plural_lower, short_plural] {
            if !candidate.is_empty() && candidate.starts_with(&input_lower) {
                completions.push(candidate.clone());
            }
        }
    }

    completions.sort();
    completions.dedup();
    completions
}

// ---------------------------------------------------------------------------
// Test support
// ---------------------------------------------------------------------------

/// Shared runtime plumbing for tests that need real (but inert) task
/// handles — a parked bridge task that never connects to anything.
#[cfg(any(test, feature = "testing"))]
pub(crate) mod test_support {
    use std::sync::LazyLock;

    static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime")
    });

    /// A parked handle for LOG streams: alive until dropped, touches no
    /// socket, no daemon, no cluster. (Table queries no longer need one —
    /// `LiveQuery::for_test` uses the real `QueryOrigin::Client`, which
    /// carries no stream at all.)
    pub(crate) fn parked_log_stream() -> crate::client::LogStream {
        let handle = RT.handle().spawn(std::future::pending::<()>());
        crate::client::LogStream::from_abort_handle(handle.abort_handle())
    }

    /// A log stream whose bridge task has ALREADY finished — what
    /// `is_alive()` reports `false` for. Blocks until the task is really
    /// done, because `AbortHandle::abort` only requests it: right after an
    /// `abort()` the task is still unfinished, so a synchronous test that
    /// aborts a parked stream is testing nothing.
    #[cfg(test)]
    pub(crate) fn finished_log_stream() -> crate::client::LogStream {
        let handle = RT.handle().spawn(std::future::ready(()));
        let abort = handle.abort_handle();
        RT.handle().block_on(handle).expect("the task completes");
        crate::client::LogStream::from_abort_handle(abort)
    }
}

// Theme-consumption tests, returned from `k9rs_theme`'s mirror when that
// became its own crate.
#[cfg(test)]
#[path = "tests/theme_integration.rs"]
mod theme_integration_tests;

#[cfg(test)]
#[path = "tests/lib.rs"]
mod root_tests;
