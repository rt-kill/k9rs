pub mod actions;
pub mod derived;
pub mod element;
pub mod nav;
pub mod store;
pub mod table;
pub mod types;
pub mod view;

pub use actions::SortTarget;
pub use table::*;
pub use types::*;
pub use view::*;

use std::time::Duration;

use crate::kube::protocol::ResourceId;

pub use crate::kube::resource_def::{ColumnDef, ColumnLevel, MetricsColumn};

// ---------------------------------------------------------------------------
// Pinned resource list for tab cycling
// ---------------------------------------------------------------------------

/// The default ordered list of pinned resources for Tab/BackTab cycling.
/// Uses registration order from the trait-based REGISTRY.
pub fn default_pinned_resources() -> Vec<ResourceId> {
    crate::kube::resource_defs::REGISTRY
        .all()
        .map(|def| def.resource_id())
        .collect()
}

#[derive(Debug, Clone)]
pub struct KubeContext {
    pub name: crate::kube::protocol::ContextName,
    pub identity: crate::kube::protocol::ClusterIdentity,
    pub is_current: bool,
}

// ---------------------------------------------------------------------------
// AppData — all resource tables
// ---------------------------------------------------------------------------

// `ColumnLevel` now lives in `kube::resource_def` next to `ColumnDef` — the
// metadata is co-located with the definitions it describes. `pub use`
// re-exports above make them available at `crate::app::ColumnLevel`.
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
    /// The always-on subscriptions feeding the stores above. Replaced
    /// wholesale on (re)connect — the old streams drop, RSTing their
    /// substreams.
    streams: Vec<crate::kube::client_session::SubscriptionStream>,
}

impl Default for CoreData {
    fn default() -> Self {
        Self {
            namespaces: store::RowStore::new("namespaces"),
            nodes: store::RowStore::new("nodes"),
            crds: store::RowStore::new("customresourcedefinitions"),
            streams: Vec::new(),
        }
    }
}

impl CoreData {
    /// Seed epoch for cached/handshake/discovery data: 0, always weaker
    /// than any live stream (the epoch allocator starts at 1) — a stale
    /// snapshot can never outrank live data, while an idle store accepts
    /// the seed.
    const SEED_EPOCH: u64 = 0;

    pub fn store_for(
        &self,
        kind: crate::kube::resource_def::BuiltInKind,
    ) -> Option<&std::sync::Arc<store::RowStore>> {
        use crate::kube::resource_def::BuiltInKind as K;
        match kind {
            K::Namespace => Some(&self.namespaces),
            K::Node => Some(&self.nodes),
            K::CustomResourceDefinition => Some(&self.crds),
            _ => None,
        }
    }

    /// Seed a core store from cached data (connection handshake,
    /// discovery). Headers come from the registry def — single source of
    /// truth with the live-watcher path.
    pub fn seed(
        &self,
        kind: crate::kube::resource_def::BuiltInKind,
        rows: Vec<crate::kube::resources::row::ResourceRow>,
    ) {
        let Some(target) = self.store_for(kind) else { return };
        let def = crate::kube::resource_defs::REGISTRY.by_kind(kind);
        target.apply(
            Self::SEED_EPOCH,
            store::StorePayload::Baseline(crate::kube::protocol::TableBaseline {
                resource: ResourceId::BuiltIn(kind),
                headers: def.default_headers(),
                rows,
            }),
        );
    }

    /// Open the always-on subscriptions (namespaces + nodes) into the
    /// core stores. Called on (re)connect; previous streams drop.
    pub fn open_streams(&mut self, session: &crate::kube::client_session::ClientSession) {
        self.streams.clear();
        for def in crate::kube::resource_defs::REGISTRY.all() {
            if !def.is_core() {
                continue;
            }
            let rid = def.resource_id();
            let Some(target) = rid.built_in_kind().and_then(|k| self.store_for(k)) else {
                continue;
            };
            self.streams.push(session.subscribe_stream(
                rid,
                crate::kube::protocol::Namespace::All,
                None,
                std::sync::Arc::clone(target),
                false,
            ));
        }
    }

    /// Context switch: all core data belongs to the old cluster.
    pub fn clear(&mut self) {
        self.streams.clear();
        self.namespaces.clear();
        self.nodes.clear();
        self.crds.clear();
    }

    /// Namespace names, for completion and the picker.
    pub fn namespace_names(&self) -> Vec<String> {
        self.namespaces.with_read(|i| i.rows.iter().map(|r| r.name.clone()).collect())
    }
}

pub struct AppData {
    pub contexts: StatefulTable<KubeContext>,
}

impl Default for AppData {
    fn default() -> Self {
        Self { contexts: StatefulTable::new() }
    }
}

// ---------------------------------------------------------------------------
// App — main application state
// ---------------------------------------------------------------------------

/// Why the TUI is exiting. Printed to stderr after terminal restoration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    /// User requested quit (q, :quit, Ctrl-C).
    UserQuit,
    /// Daemon connection was lost.
    DaemonDisconnected,
    /// An error occurred.
    Error(String),
}

pub struct App {
    pub should_quit: bool,
    pub exit_reason: Option<ExitReason>,

    pub data: AppData,
    /// App-level shared stores (completion / picker / overview chrome).
    pub core: CoreData,

    /// The navigation stack: self-contained scope elements, strict LIFO.
    pub nav: nav::NavStack,
    /// Ordered list of pinned resources for Tab/BackTab cycling.
    pub pinned_resources: Vec<ResourceId>,

    /// Command history for `:` command mode (max 50 entries).
    pub command_history: Vec<String>,

    /// Set by DaemonDisconnected to trigger auto-reconnection in the
    /// main loop. Same mechanism as context switching.
    pub reconnect_requested: bool,

    /// When true, Ctrl-C does not quit the application (`noExitOnCtrlC` config).
    pub no_exit_on_ctrl_c: bool,
    /// When true, destructive actions (delete, edit, scale, restart, force-kill, shell) are disabled.
    pub read_only: bool,

    /// User config loaded from ~/.config/k9rs/config.yaml.
    pub config: AppConfig,

    /// Pure display/interaction state (flash, dialogs, input mode, theme, …).
    pub ui: UiState,
    /// Cluster/data state (context, namespace, metrics, caches, …).
    pub kube: KubeState,
}

impl App {
    pub fn new(
        context: crate::kube::protocol::ContextName,
        namespace: crate::kube::protocol::Namespace,
        session: &crate::kube::client_session::ClientSession,
    ) -> Self {
        let _ = session; // the Overview root opens no subscription
        let metrics = store::MetricsHub::new();
        // The startup root is the Overview landing page — no resource
        // watch opens until the user navigates to one (startup args or
        // `:cmd` reset to a resource root).
        let root = element::Element::Overview(element::Overview);
        Self::new_with_root(context, namespace, metrics, root)
    }

    /// Test-only: an App whose root element rides a parked stream — no
    /// session, no daemon, no network.
    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        let namespace = crate::kube::protocol::Namespace::All;
        let metrics = store::MetricsHub::new();
        let root = element::Element::ResourceList(element::ResourceList::open_for_test(
            element::QuerySpec {
                rid: nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                namespace: namespace.clone(),
                filter: None,
            },
            &metrics,
            "pods".to_string(),
        ));
        Self::new_with_root(crate::kube::protocol::ContextName::default(), namespace, metrics, root)
    }

    fn new_with_root(
        context: crate::kube::protocol::ContextName,
        namespace: crate::kube::protocol::Namespace,
        metrics: std::sync::Arc<store::MetricsHub>,
        root: element::Element,
    ) -> Self {
        let config = Self::load_config();
        let cache_capacity = config.ui.cache_capacity;
        let skin_name = config.ui.skin.clone();
        Self {
            should_quit: false,
            exit_reason: None,
            data: AppData::default(),
            core: CoreData::default(),
            nav: nav::NavStack::new(root),
            pinned_resources: default_pinned_resources(),
            command_history: Vec::new(),
            no_exit_on_ctrl_c: config.no_exit_on_ctrl_c,
            read_only: config.read_only,
            reconnect_requested: false,
            config,
            ui: UiState {
                flash: None,
                confirm_dialog: None,
                form_dialog: None,
                theme: crate::ui::theme::Theme::load(skin_name.as_deref()),
                input_mode: InputMode::Normal,
                overlay: None,
                show_header: true,
                tick_count: 0,
                column_level: ColumnLevel::Default,
            },
            kube: KubeState {
                context,
                identity: crate::kube::protocol::ClusterIdentity::default(),
                selected_ns: namespace,
                context_switch: ContextSwitchState::Stable,
                metrics,
                kubectl_cache: KubectlCache::new(Duration::from_secs(30), cache_capacity),
            },
        }
    }

    /// Construct a ROOT list element for `rid` scoped by `namespace` —
    /// the one construction site where the ambient selector is a
    /// legitimate input (roots are built FROM the selector; drills carry
    /// their own intrinsic scope).
    pub fn root_list_element(
        session: &crate::kube::client_session::ClientSession,
        metrics: &std::sync::Arc<store::MetricsHub>,
        rid: ResourceId,
        namespace: crate::kube::protocol::Namespace,
    ) -> element::Element {
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
        session: &crate::kube::client_session::ClientSession,
        rid: ResourceId,
        namespace: crate::kube::protocol::Namespace,
        filter: Option<crate::kube::protocol::SubscriptionFilter>,
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

    /// Load settings from `~/.config/k9rs/config.yaml`. Uses the shared
    /// `load_section` helper and serde deserialization — adding a new config
    /// field is just adding a struct field with `#[serde(default)]`.
    fn load_config() -> AppConfig {
        // The top-level k9rs section deserializes directly into AppConfig
        // because AppConfig uses `#[serde(rename_all = "camelCase", default)]`.
        crate::kube::daemon_config::load_section::<AppConfig>("")
            .unwrap_or_default()
    }

    // -- Element dispatch -------------------------------------------------------
    //
    // The TUI drives exactly one element: the TOP of the nav stack. The
    // contexts panel (chrome until the route fold-in) keeps its own arm.

    pub fn next_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid(crate::kube::resource_def::BuiltInKind::Pod);
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| Some(r) == current).unwrap_or(0);
        pinned[(idx + 1) % pinned.len()].clone()
    }

    pub fn prev_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid(crate::kube::resource_def::BuiltInKind::Pod);
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| Some(r) == current).unwrap_or(0);
        pinned[if idx == 0 { pinned.len() - 1 } else { idx - 1 }].clone()
    }

    pub fn col_left(&mut self) {
        self.nav.top_mut().col_left();
    }

    pub fn col_right(&mut self) {
        self.nav.top_mut().col_right();
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

    /// The cursor index on the top element.
    pub fn active_table_selected(&self) -> usize {
        self.nav.top().table_interaction().map(|i| i.selected).unwrap_or(0)
    }

    /// Move the top element's cursor (clamped to its view).
    pub fn select_in_active_table(&mut self, idx: usize) {
        self.nav.top_mut().select(idx);
    }

    /// Toggle mark on the row under the cursor.
    pub fn toggle_mark(&mut self) {
        self.nav.top_mut().toggle_mark();
    }

    /// Mark the span from the nearest mark to the cursor.
    pub fn span_mark(&mut self) {
        self.nav.top_mut().span_mark();
    }

    /// Clear all marks on the top element's data.
    pub fn clear_marks(&mut self) {
        self.nav.top_mut().clear_marks();
    }

    /// Sort the top element by the given target column (same column
    /// toggles direction). The next derive applies it — filters and
    /// draft text ride along by construction, so there is nothing to
    /// re-apply.
    pub fn sort_by(&mut self, target: crate::app::SortTarget) {
        self.nav.top_mut().sort_by(target);
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
        // Keep redrawing while a loading state is active (spinner animation).
        if !changed {
            // Resource table loading
            let top = self.nav.top();
            if top.counts().total == 0
                && matches!(top.data_state(), crate::app::table::TableDataState::Initializing)
            {
                changed = true;
            }
            // Log view: animate while streaming with no lines yet.
            if !changed {
                if let Some(store) = self.nav.top().log_store() {
                    if store.with_read(|i| i.live && i.lines.is_empty()) {
                        changed = true;
                    }
                }
            }
            // Shell overlay: animate while connecting (waiting for first byte).
            if !changed {
                if let Some(Overlay::Shell(ref shell)) = self.ui.overlay {
                    if shell.connect_state == crate::app::ShellConnectState::Connecting {
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    /// Build completion candidates dynamically based on command input.
    pub fn command_completions(&self) -> Vec<String> {
        let cmd_input = match &self.ui.input_mode {
            InputMode::Command { input, .. } => input.as_str(),
            _ => return Vec::new(),
        };
        complete_command(cmd_input, &self.core, &self.data.contexts)
    }

    /// Returns the best (first) completion match, if any.
    pub fn best_completion(&self) -> Option<String> {
        if let InputMode::Command { ref input, .. } = self.ui.input_mode {
            if input.trim().is_empty() { return None; }
            self.command_completions().into_iter().next()
        } else {
            None
        }
    }

    /// Accept the current ghost-text completion into the command input.
    pub fn accept_completion(&mut self) {
        if let Some(completion) = self.best_completion() {
            if let InputMode::Command { ref mut input, .. } = self.ui.input_mode {
                *input = completion;
            }
        }
    }

    /// Returns filtered and total counts for the top element.
    pub fn active_table_items_count(&self) -> ItemCounts {
        self.nav.top().counts()
    }

    /// Capability manifest for the top element — pure data over the
    /// element's identity, computed on demand.
    pub fn current_capabilities(&self) -> crate::kube::protocol::ResourceCapabilities {
        self.nav.top().capabilities()
    }

    /// Compute health statistics for all core resources. Returns
    /// `(label, total, healthy)` tuples. Used by the overview page —
    /// reads the app-level core stores, not navigation state.
    pub fn core_resource_stats(&self) -> Vec<(&'static str, usize, usize)> {
        use crate::kube::resources::row::RowHealth;
        let mut stats = Vec::new();
        for def in crate::kube::resource_defs::REGISTRY.all() {
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

    /// Whether the top element's view is cluster-scoped (no namespace).
    pub fn current_tab_is_cluster_scoped(&self) -> bool {
        self.nav.top().is_cluster_scoped()
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
    let mut v: Vec<&'static str> = crate::kube::resource_defs::REGISTRY
        .all()
        .flat_map(|def| def.aliases().iter().copied())
        .collect();
    for kind in &crate::kube::local::LocalResourceKind::all() {
        v.extend(kind.aliases().iter().copied());
    }
    v.extend(["alias", "aliases", "a"]);
    v.sort();
    v.dedup();
    v
}

/// Build completion candidates for the given command input. Reads the
/// app-level core stores for namespace/CRD names and the contexts panel
/// for context names. Pure function — no App needed.
fn complete_command(
    cmd_input: &str,
    core: &CoreData,
    contexts: &StatefulTable<KubeContext>,
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
        let mut completions: Vec<String> = contexts.items().iter()
            .map(|c| format!("{}{}", cmd_prefix, c.name))
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
    all_commands.extend_from_slice(&[
        "ctx", "context", "contexts",
        "q", "quit", "exit",
        "help", "h",
        "home", "overview",
        "alias", "aliases",
    ]);

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
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::LazyLock;

    static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime")
    });

    /// A subscription handle backed by a parked task: alive until
    /// dropped, touches no socket, no daemon, no cluster.
    pub(crate) fn parked_stream() -> crate::kube::client_session::SubscriptionStream {
        let handle = RT.handle().spawn(std::future::pending::<()>());
        crate::kube::client_session::SubscriptionStream::from_abort_handle(handle.abort_handle())
    }

    /// Same, for log streams.
    pub(crate) fn parked_log_stream() -> crate::kube::client_session::LogStream {
        let handle = RT.handle().spawn(std::future::pending::<()>());
        crate::kube::client_session::LogStream::from_abort_handle(handle.abort_handle())
    }
}
