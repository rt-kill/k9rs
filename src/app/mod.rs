pub mod actions;
pub mod nav;
pub mod table;
pub mod types;

pub use actions::SortTarget;
pub use table::*;
pub use types::*;

use std::collections::HashMap;
use std::time::Duration;

use crate::kube::protocol::{ObjectKey, ResourceId};
use crate::kube::resources::KubeResource;

pub use types::CHANGE_HIGHLIGHT_DURATION;
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

/// Look up the display level for a column by header name. Uses the typed
/// column metadata from the def (via `column_defs()`) when the current
/// resource is a built-in; falls back to `ColumnDef::infer` for CRDs
/// and locals (which have no registered def or no override).
pub fn column_level_for(rid: &ResourceId, name: &str) -> ColumnLevel {
    if let Some(k) = rid.built_in_kind() {
        let def = crate::kube::resource_defs::REGISTRY.by_kind(k);
        for col in def.column_defs() {
            if col.header.eq_ignore_ascii_case(name) {
                return col.level;
            }
        }
    }
    ColumnDef::infer(name).level
}

// ---------------------------------------------------------------------------
// TableDescriptor — runtime column headers for a resource type
// ---------------------------------------------------------------------------

/// Runtime column headers for a resource type (from the server).
#[derive(Debug, Clone, Default)]
pub struct TableDescriptor {
    pub headers: Vec<String>,
}

impl TableDescriptor {
    /// Find the column index for a header name (case-insensitive).
    /// Returns an index into the *full* cell array (not the visible subset).
    pub fn col(&self, name: &str) -> Option<usize> {
        self.headers.iter().position(|h| h.eq_ignore_ascii_case(name))
    }

    /// Return (data_index, header_name) pairs for columns visible at the
    /// given display level. Optionally skips the NAMESPACE column when
    /// viewing a single namespace. Uses the typed column metadata from
    /// the def (when `rid` is a built-in) to determine each column's
    /// level; falls back to `ColumnDef::infer` for CRDs / locals.
    pub fn visible_columns(&self, rid: &ResourceId, level: ColumnLevel, skip_namespace: bool) -> Vec<(usize, &str)> {
        self.headers.iter().enumerate()
            .filter(|(_, name)| {
                if skip_namespace && name.eq_ignore_ascii_case("NAMESPACE") {
                    return false;
                }
                column_level_for(rid, name) <= level
            })
            .map(|(i, name)| (i, name.as_str()))
            .collect()
    }
}

pub struct AppData {
    /// Global tables for core resources (keyed by ResourceId).
    pub tables: std::collections::HashMap<ResourceId, StatefulTable<crate::kube::resources::row::ResourceRow>>,
    /// Runtime column headers for unified tables.
    pub descriptors: std::collections::HashMap<ResourceId, TableDescriptor>,

    pub contexts: StatefulTable<KubeContext>,
}

impl Default for AppData {
    fn default() -> Self {
        let mut tables = std::collections::HashMap::new();
        // Only pre-populate entries for globally-stored resources (Namespace,
        // Node, CRD). All other resources have their tables on NavStep.
        for def in crate::kube::resource_defs::REGISTRY.all() {
            let rid = def.resource_id();
            if nav::is_globally_stored(&rid) {
                tables.insert(rid, StatefulTable::new());
            }
        }
        Self {
            tables,
            descriptors: std::collections::HashMap::new(),
            contexts: StatefulTable::new(),
        }
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
    pub route: Route,
    pub route_stack: Vec<Route>,

    pub data: AppData,

    /// Stackable navigation state for drill-downs and grep filters.
    pub nav: nav::NavStack,
    /// Ordered list of pinned resources for Tab/BackTab cycling.
    pub pinned_resources: Vec<ResourceId>,

    /// Command history for `:` command mode (max 50 entries).
    pub command_history: Vec<String>,

    /// When true, Ctrl-C does not quit the application (`noExitOnCtrlC` config).
    pub no_exit_on_ctrl_c: bool,
    /// When true, destructive actions (delete, edit, scale, restart, force-kill, shell) are disabled.
    pub read_only: bool,

    /// Pure display/interaction state (flash, dialogs, input mode, theme, …).
    pub ui: UiState,
    /// Cluster/data state (context, namespace, metrics, caches, …).
    pub kube: KubeState,
}

impl App {
    pub fn new(context: crate::kube::protocol::ContextName, namespace: String) -> Self {
        let config = Self::load_config();
        Self {
            should_quit: false,
            exit_reason: None,
            route: Route::Overview,
            route_stack: Vec::new(),
            data: AppData::default(),
            nav: nav::NavStack::new(nav::rid(crate::kube::resource_def::BuiltInKind::Pod)),
            pinned_resources: default_pinned_resources(),
            command_history: Vec::new(),
            no_exit_on_ctrl_c: config.no_exit_on_ctrl_c,
            read_only: config.read_only,
            ui: UiState {
                flash: None,
                confirm_dialog: None,
                form_dialog: None,
                theme: crate::ui::theme::Theme::load(),
                input_mode: InputMode::Normal,
                help_scroll: 0,
                show_header: true,
                tick_count: 0,
                column_level: ColumnLevel::Default,
                deltas: DeltaTracker::new(),
            },
            kube: KubeState {
                context,
                identity: crate::kube::protocol::ClusterIdentity::default(),
                selected_ns: crate::kube::protocol::Namespace::from_user_command(&namespace),
                context_switch: ContextSwitchState::Stable,
                pod_metrics: HashMap::new(),
                node_metrics: HashMap::new(),
                core_streams: Vec::new(),
                kubectl_cache: KubectlCache::new(Duration::from_secs(30)),
            },
        }
    }

    /// Load settings from config file. Tries `~/.config/k9rs/config.yaml` first,
    /// falls back to `~/.config/k9s/config.yaml` for compatibility.
    fn load_config() -> AppConfig {
        let default = AppConfig { no_exit_on_ctrl_c: false, read_only: false };
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return default,
        };
        // Try k9rs config first, fall back to k9s config for compatibility.
        let (content, key) = {
            let k9rs_path = std::path::Path::new(&home).join(".config/k9rs/config.yaml");
            if let Ok(c) = std::fs::read_to_string(&k9rs_path) {
                (c, "k9rs")
            } else {
                let k9s_path = std::path::Path::new(&home).join(".config/k9s/config.yaml");
                match std::fs::read_to_string(&k9s_path) {
                    Ok(c) => (c, "k9s"),
                    Err(_) => return default,
                }
            }
        };
        let yaml: serde_yaml::Value = match serde_yaml::from_str(&content) {
            Ok(v) => v,
            Err(_) => return default,
        };
        let section = yaml.get(key);
        AppConfig {
            no_exit_on_ctrl_c: section
                .and_then(|v| v.get("noExitOnCtrlC"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            read_only: section
                .and_then(|v| v.get("readOnly"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }
    }

    /// Navigate to a new route: swaps the current route into the stack and
    /// sets `new_route` as current. No Clone needed — the old route is moved.
    /// When the old route drops off the stack (or is replaced), any resources
    /// it owns (like LogStream in Route::Logs) drop automatically.
    pub fn navigate_to(&mut self, new_route: Route) {
        self.ui.confirm_dialog = None;
        self.ui.form_dialog = None;
        let old = std::mem::replace(&mut self.route, new_route);
        if self.route_stack.len() >= 50 {
            self.route_stack.remove(0);
        }
        self.route_stack.push(old);
    }

    /// Pop the route stack — returns to the previous route. No-op if the
    /// stack is empty (the current route is preserved).
    pub fn pop_route(&mut self) {
        if let Some(prev) = self.route_stack.pop() {
            self.route = prev;
        }
    }

    /// Look up the ResourceId for a given MetricsKind via the registry.
    /// Returns `None` if no registered def declares this metrics kind.
    fn rid_for_metrics(kind: crate::kube::resource_def::MetricsKind) -> Option<crate::kube::protocol::ResourceId> {
        crate::kube::resource_defs::REGISTRY.all()
            .find(|d| d.metrics_kind() == Some(kind))
            .map(|d| d.resource_id())
    }

    /// Bulk-fetch all metrics column indices for a given MetricsKind in one
    /// registry walk. Avoids the O(n) × 6 pattern of calling the single-
    /// column lookup six times.
    fn all_metrics_cols(mk: crate::kube::resource_def::MetricsKind) -> std::collections::HashMap<MetricsColumn, usize> {
        let mut result = std::collections::HashMap::new();
        if let Some(def) = crate::kube::resource_defs::REGISTRY.all().find(|d| d.metrics_kind() == Some(mk)) {
            for (i, col) in def.column_defs().iter().enumerate() {
                if let Some(tag) = col.metrics {
                    result.insert(tag, i);
                }
            }
        }
        result
    }

    /// Apply stored pod metrics to all current pod items.
    /// Writes the CPU/MEM usage cells and computes percentage columns
    /// (%CPU/R, %CPU/L, %MEM/R, %MEM/L) from the row's typed
    /// request/limit fields and the metrics values.
    pub fn apply_pod_metrics(&mut self) {
        use crate::kube::resource_def::MetricsKind;
        let Some(pods_rid) = Self::rid_for_metrics(MetricsKind::Pod) else { return };
        let cols = Self::all_metrics_cols(MetricsKind::Pod);
        let cpu_col = cols.get(&MetricsColumn::Cpu).copied();
        let mem_col = cols.get(&MetricsColumn::Mem).copied();
        let pct_cpu_r = cols.get(&MetricsColumn::CpuPercentRequest).copied();
        let pct_cpu_l = cols.get(&MetricsColumn::CpuPercentLimit).copied();
        let pct_mem_r = cols.get(&MetricsColumn::MemPercentRequest).copied();
        let pct_mem_l = cols.get(&MetricsColumn::MemPercentLimit).copied();
        // Pod is not globally stored, so search the nav stack.
        if let Some(table) = self.nav.find_table_for_resource_mut(&pods_rid) {
            for row in &mut table.items {
                if let Some(usage) = self.kube.pod_metrics.get(&ObjectKey::new(row.namespace.clone().unwrap_or_default(), row.name.clone())) {
                    use crate::kube::resources::row::{CellValue, QuantityUnit};
                    if let Some(col) = cpu_col { row.set_cell(col, CellValue::Quantity { value: usage.cpu_milli, unit: QuantityUnit::Millicores }); }
                    if let Some(col) = mem_col { row.set_cell(col, CellValue::Quantity { value: usage.mem_bytes, unit: QuantityUnit::Bytes }); }

                    fn pct_val(current: u64, limit: Option<u64>) -> CellValue {
                        limit.filter(|&l| l > 0)
                            .map(|l| CellValue::Percentage(Some(current.saturating_mul(100) / l)))
                            .unwrap_or(CellValue::Percentage(None))
                    }
                    if let Some(col) = pct_cpu_r { row.set_cell(col, pct_val(usage.cpu_milli, row.cpu_request)); }
                    if let Some(col) = pct_cpu_l { row.set_cell(col, pct_val(usage.cpu_milli, row.cpu_limit)); }
                    if let Some(col) = pct_mem_r { row.set_cell(col, pct_val(usage.mem_bytes, row.mem_request)); }
                    if let Some(col) = pct_mem_l { row.set_cell(col, pct_val(usage.mem_bytes, row.mem_limit)); }
                }
            }
        }
    }

    /// Apply stored node metrics to all current node items.
    pub fn apply_node_metrics(&mut self) {
        use crate::kube::resource_def::MetricsKind;
        let Some(nodes_rid) = Self::rid_for_metrics(MetricsKind::Node) else { return };
        let cols = Self::all_metrics_cols(MetricsKind::Node);
        let cpu_col = cols.get(&MetricsColumn::CpuPercent).copied();
        let mem_col = cols.get(&MetricsColumn::MemPercent).copied();
        if cpu_col.is_none() && mem_col.is_none() { return; }
        if let Some(table) = self.data.tables.get_mut(&nodes_rid) {
            for row in &mut table.items {
                if let Some(usage) = self.kube.node_metrics.get(row.name.as_str()) {
                    use crate::kube::resources::row::CellValue;
                    if let Some(col) = cpu_col {
                        if let Some(cap) = row.cells.get(col).map(|c| {
                            let s = c.to_string();
                            s.split('/').nth(1).unwrap_or("").to_string()
                        }).filter(|s| !s.is_empty()) {
                            row.set_cell(col, CellValue::Text(format!("{}/{}", usage.cpu, cap)));
                        }
                    }
                    if let Some(col) = mem_col {
                        if let Some(cap) = row.cells.get(col).map(|c| {
                            let s = c.to_string();
                            s.split('/').nth(1).unwrap_or("").to_string()
                        }).filter(|s| !s.is_empty()) {
                            row.set_cell(col, CellValue::Text(format!("{}/{}", usage.mem, cap)));
                        }
                    }
                }
            }
        }
    }

    /// Clear ALL resource table data. Used for context switches where
    /// everything is stale. Namespace switches should NOT call this — the
    /// server only re-subscribes namespaced resources, and cluster-scoped
    /// data (nodes, PVs, etc.) is preserved automatically.
    pub fn clear_data(&mut self) {
        // Clear all table data but keep entries so auto-subscribed resources
        // (namespaces, nodes) can receive data from the new context.
        for table in self.data.tables.values_mut() {
            table.clear_data();
        }
        self.data.descriptors.clear();
    }

    /// Clear data for a specific resource so it shows "Loading..." until
    /// fresh data arrives from the server. Creates the entry if it doesn't
    /// exist yet — required for dynamically-discovered CRDs whose
    /// `ResourceId` isn't in the pre-populated `RESOURCE_TYPES` table.
    /// Without this, the first snapshot for the CRD would land on a missing
    /// table in `apply_resource_update` and be dropped on the floor.
    pub fn clear_resource(&mut self, rid: &ResourceId) {
        if nav::is_globally_stored(rid) {
            let table = self.data.tables.entry(rid.clone()).or_default();
            table.clear_data();
        } else if let Some(table) = self.nav.find_table_for_resource_mut(rid) {
            table.clear_data();
        }
    }

    /// Clear every namespace-scoped resource table. Called from
    /// `do_switch_namespace` so cached rows from the old namespace can't
    /// bleed into the new namespace's view. Cluster-scoped tables (nodes,
    /// PVs, namespaces themselves, etc.) are left alone — their data is
    /// the same across namespaces.
    pub fn clear_namespaced_caches(&mut self) {
        for (rid, table) in self.data.tables.iter_mut() {
            if !rid.is_cluster_scoped() {
                table.clear_data();
            }
        }
    }

    pub fn next_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid(crate::kube::resource_def::BuiltInKind::Pod);
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| r == current).unwrap_or(0);
        pinned[(idx + 1) % pinned.len()].clone()
    }

    pub fn prev_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid(crate::kube::resource_def::BuiltInKind::Pod);
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| r == current).unwrap_or(0);
        pinned[if idx == 0 { pinned.len() - 1 } else { idx - 1 }].clone()
    }

    // Delegate navigation to the currently active table
    fn with_active_table<F: FnOnce(&mut dyn table::TableNav)>(&mut self, f: F) {
        if let Some(table) = self.active_view_table_mut() {
            f(table);
        }
    }

    // Delegate read-only operations to the currently active table
    fn with_active_table_ref<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dyn table::TableNav) -> R,
    {
        if let Some(table) = self.active_view_table() {
            f(table)
        } else {
            // Fallback for unknown CRDs — return default counts
            static EMPTY: std::sync::LazyLock<StatefulTable<crate::kube::resources::row::ResourceRow>> =
                std::sync::LazyLock::new(StatefulTable::new);
            f(&*EMPTY)
        }
    }

    pub fn active_table_selected_col(&self) -> usize {
        self.active_view_table().map(|t| t.selected_col()).unwrap_or(0)
    }

    /// Get the active view's table (immutable).
    pub fn active_view_table(&self) -> Option<&StatefulTable<crate::kube::resources::row::ResourceRow>> {
        self.table_for(self.nav.resource_id())
    }

    /// Get the active view's table (mutable).
    pub fn active_view_table_mut(&mut self) -> Option<&mut StatefulTable<crate::kube::resources::row::ResourceRow>> {
        let rid = self.nav.resource_id().clone();
        self.table_for_mut(&rid)
    }

    /// Get the active view's descriptor.
    pub fn active_view_descriptor(&self) -> Option<&TableDescriptor> {
        self.descriptor_for(self.nav.resource_id())
    }

    /// Route a table lookup by ResourceId (immutable). Checks the global
    /// store for globally-stored resources, otherwise walks the nav stack.
    pub fn table_for(&self, rid: &ResourceId) -> Option<&StatefulTable<crate::kube::resources::row::ResourceRow>> {
        if nav::is_globally_stored(rid) {
            self.data.tables.get(rid)
        } else {
            self.nav.find_table_for_resource(rid)
        }
    }

    /// Route a table lookup by ResourceId (mutable). Checks the global
    /// store for globally-stored resources, otherwise walks the nav stack.
    pub fn table_for_mut(&mut self, rid: &ResourceId) -> Option<&mut StatefulTable<crate::kube::resources::row::ResourceRow>> {
        if nav::is_globally_stored(rid) {
            self.data.tables.get_mut(rid)
        } else {
            self.nav.find_table_for_resource_mut(rid)
        }
    }

    /// Route a descriptor lookup by ResourceId.
    pub fn descriptor_for(&self, rid: &ResourceId) -> Option<&TableDescriptor> {
        if nav::is_globally_stored(rid) {
            self.data.descriptors.get(rid)
        } else {
            self.nav.find_descriptor_for_resource(rid)
        }
    }

    pub fn col_left(&mut self) {
        self.with_active_table(|t| t.nav_col_left());
    }

    pub fn col_right(&mut self) {
        self.with_active_table(|t| t.nav_col_right());
    }

    pub fn select_next(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.next();
        } else {
            self.with_active_table(|t| t.nav_next());
        }
    }
    pub fn select_prev(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.previous();
        } else {
            self.with_active_table(|t| t.nav_prev());
        }
    }
    pub fn page_up(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.page_up();
        } else {
            self.with_active_table(|t| t.nav_page_up());
        }
    }
    pub fn page_down(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.page_down();
        } else {
            self.with_active_table(|t| t.nav_page_down());
        }
    }
    pub fn go_home(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.home();
        } else {
            self.with_active_table(|t| t.nav_home());
        }
    }
    pub fn go_end(&mut self) {
        if matches!(self.route, Route::Contexts) {
            self.data.contexts.end();
        } else {
            self.with_active_table(|t| t.nav_end());
        }
    }

    /// Get the current selection index in the active table.
    pub fn active_table_selected(&self) -> usize {
        self.with_active_table_ref(|t| t.nav_selected())
    }

    /// Set the selection index on the active table (clamped to bounds).
    pub fn select_in_active_table(&mut self, idx: usize) {
        self.with_active_table(|t| t.nav_select(idx));
    }

    /// Toggle mark on the currently selected row, then advance to the next row.
    pub fn toggle_mark(&mut self) {
        self.with_active_table(|t| t.nav_toggle_mark());
    }

    /// Span-mark: mark all rows from the last marked row to the current selection.
    pub fn span_mark(&mut self) {
        self.with_active_table(|t| t.nav_span_mark());
    }

    /// Clear all marks on the current table.
    pub fn clear_marks(&mut self) {
        self.with_active_table(|t| t.nav_clear_marks());
    }

    /// Reapply client-side grep filters (plus any uncommitted filter_input text)
    /// to the current table. Labels, Field, and OwnerChain filters are handled
    /// server-side via SubscriptionFilter — the server only sends matching rows.
    /// Only Grep filtering remains client-side (it operates on display text).
    ///
    /// Committed grep patterns live inside `NavFilter::Grep(CompiledGrep)`
    /// — compiled exactly once at filter push, reused on every snapshot.
    /// The only per-call regex compile is the uncommitted filter-input
    /// draft, which is cheap because there's at most one and the user is
    /// actively typing it.
    pub fn reapply_nav_filters(&mut self) {
        use crate::app::nav::NavFilter;
        use crate::util::SearchPattern;

        // Collect OWNED copies of compiled grep patterns + a flag for the
        // active fault filter. Cloning releases the immutable borrow on
        // `self.nav` before we take the mutable reference to the table
        // (which may also live on the nav stack).
        let mut committed: Vec<SearchPattern> = Vec::new();
        let mut col_greps: Vec<(SearchPattern, usize)> = Vec::new();
        let mut has_fault = false;
        for f in self.nav.active_filters() {
            match f {
                NavFilter::Grep(g) => committed.push(g.pattern().clone()),
                NavFilter::ColumnGrep { pattern, col } => col_greps.push((pattern.pattern().clone(), *col)),
                NavFilter::Fault => has_fault = true,
                _ => {}
            }
        }
        // Uncommitted draft text from the filter input is compiled fresh
        // here — it changes per keystroke, so caching would just churn.
        let draft: Option<SearchPattern> = {
            let text = self.nav.filter_input().text.clone();
            (!text.is_empty()).then(|| SearchPattern::new(&text))
        };

        if committed.is_empty() && col_greps.is_empty() && draft.is_none() && !has_fault {
            self.clear_filter();
            return;
        }

        if let Some(table) = self.active_view_table_mut() {
            table.apply_filter(|item| {
                // Fault check: typed health predicate, not regex.
                if has_fault {
                    use crate::kube::resources::row::RowHealth;
                    if matches!(item.health, RowHealth::Normal) {
                        return false;
                    }
                }
                // Grep check: every committed pattern must match, AND the
                // transient draft pattern (if any) must match.
                let committed_ok = committed.iter().all(|pat| {
                    item.cells().iter().any(|cell| pat.is_match(&cell.to_string()))
                });
                if !committed_ok { return false; }
                // Column-restricted greps: each must match its specific cell.
                let col_ok = col_greps.iter().all(|(pat, col)| {
                    item.cells().get(*col).is_some_and(|cell| pat.is_match(&cell.to_string()))
                });
                if !col_ok { return false; }
                if let Some(ref d) = draft {
                    if !item.cells().iter().any(|cell| d.is_match(&cell.to_string())) {
                        return false;
                    }
                }
                true
            });
        }
    }

    pub fn clear_filter(&mut self) {
        self.with_active_table(|t| t.nav_clear_filter());
    }

    /// Sort the active resource table by the given target column.
    /// If already sorted by this column, toggles ascending/descending.
    /// CellValue::cmp() handles type-aware ordering directly, so no
    /// ColumnSortKind dispatch is needed.
    pub fn sort_by(&mut self, target: crate::app::SortTarget) {
        self.with_active_table(|t| t.nav_sort_by(target));
        // `sort_by_column` -> `rebuild_filter` resets `filtered_indices`
        // to `0..items.len()` (the table is just storage; nav filters are
        // owned by `App`). Without re-applying them here, sorting with an
        // active Grep/Fault filter would silently drop the filter until
        // the next snapshot triggered `apply_resource_update`'s reapply.
        self.reapply_nav_filters();
    }

    /// Toggle the sort direction on the active table's current sort column.
    /// Re-sorts with the same column index, which toggles asc/desc.
    pub fn toggle_sort_direction(&mut self) {
        self.with_active_table(|t| t.nav_toggle_sort());
        self.reapply_nav_filters();
    }

    /// Advance tick counter, expire flash messages, etc.
    /// Returns `true` if the UI should be redrawn (e.g. flash expired, loading animation).
    pub fn tick(&mut self) -> bool {
        self.ui.tick_count = self.ui.tick_count.wrapping_add(1);
        let mut changed = false;
        // Expire flash messages
        if let Some(ref flash) = self.ui.flash {
            if flash.is_expired() {
                self.ui.flash = None;
                changed = true;
            }
        }
        // Expire row-level change highlights.
        if self.ui.deltas.expire(CHANGE_HIGHLIGHT_DURATION) {
            changed = true;
        }
        // Keep redrawing while a loading state is active (spinner animation).
        if !changed {
            // Resource table loading
            let table_loading = self.active_view_table()
                .is_none_or(|t| t.items.is_empty() && !t.has_data && t.error.is_none());
            if table_loading {
                changed = true;
            }
            // Log view: animate while streaming with no lines yet
            if !changed {
                if let Route::Logs { ref state, .. } = self.route {
                    if state.streaming && state.lines.is_empty() {
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
        complete_command(cmd_input, &self.data)
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

    /// Returns filtered and total counts for the currently active resource table.
    pub fn active_table_items_count(&self) -> ItemCounts {
        self.with_active_table_ref(|t| t.nav_items_count())
    }

    /// Build the capability manifest for the current nav resource. Computed
    /// from the typed `ResourceId` via [`ResourceId::capabilities`] — the
    /// client no longer caches a server-sent `ResourceCapabilities` because
    /// the classification is pure data over the closed [`BuiltInKind`] /
    /// [`LocalResourceKind`] enums (CRDs fall back to the always-on trio).
    /// Computing on demand eliminates:
    /// - the three-map rekey dance on `ResourceResolved`
    /// - the `AppEvent::ResourceCapabilities` wire round-trip
    /// - every "caps cache missed after resolve" failure mode
    pub fn current_capabilities(&self) -> crate::kube::protocol::ResourceCapabilities {
        self.nav.resource_id().capabilities()
    }

    /// Compute health statistics for all core resources. Returns
    /// `(label, total, healthy)` tuples. Used by the overview page —
    /// moved here so the view doesn't contain business logic.
    pub fn core_resource_stats(&self) -> Vec<(&'static str, usize, usize)> {
        use crate::kube::resources::row::RowHealth;
        let mut stats = Vec::new();
        for def in crate::kube::resource_defs::REGISTRY.all() {
            if !def.is_core() { continue; }
            let rid = def.resource_id();
            let label = def.short_label();
            if let Some(table) = self.data.tables.get(&rid) {
                let total = table.items.len();
                let healthy = table.items.iter()
                    .filter(|r| matches!(r.health, RowHealth::Normal))
                    .count();
                stats.push((label, total, healthy));
            }
        }
        stats
    }

    /// Whether the current nav resource is cluster-scoped (no namespace).
    pub fn current_tab_is_cluster_scoped(&self) -> bool {
        self.nav.resource_id().is_cluster_scoped()
    }

    /// Find a discovered CRD by its kind name (case-insensitive).
    /// Returns a lightweight CrdInfo extracted from the row's typed `crd_info` field.
    pub fn find_crd_by_name(&self, cmd: &str) -> Option<CrdInfo> {
        let lower = cmd.to_lowercase();
        let crds_rid = nav::rid(crate::kube::resource_def::BuiltInKind::CustomResourceDefinition);
        let table = self.data.tables.get(&crds_rid)?;
        table.items.iter().find_map(|row| {
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
    for kind in crate::kube::local::LocalResourceKind::all() {
        v.extend(kind.aliases().iter().copied());
    }
    v.extend(["alias", "aliases", "a"]);
    v.sort();
    v.dedup();
    v
}

/// Build completion candidates for the given command input. Reads from
/// `AppData` for namespace/context/CRD names. Pure function — no App needed.
fn complete_command(cmd_input: &str, data: &AppData) -> Vec<String> {
    use crate::kube::resources::KubeResource;

    let input_lower = cmd_input.trim_start().to_lowercase();

    // Namespace completion: "ns <tab>" or "namespace <tab>"
    if input_lower.starts_with("ns ") || input_lower.starts_with("namespace ") {
        let cmd_prefix = if input_lower.starts_with("ns ") { "ns " } else { "namespace " };
        let ns_items = data.tables.get(&nav::rid(crate::kube::resource_def::BuiltInKind::Namespace))
            .map(|t| &t.items[..]).unwrap_or(&[]);
        let mut completions: Vec<String> = ns_items.iter()
            .map(|ns| format!("{}{}", cmd_prefix, ns.name()))
            .filter(|s| s.to_lowercase().starts_with(&input_lower))
            .collect();
        completions.sort();
        completions.dedup();
        return completions;
    }

    // Context completion: "ctx <tab>" or "context <tab>"
    if input_lower.starts_with("ctx ") || input_lower.starts_with("context ") {
        let cmd_prefix = if input_lower.starts_with("ctx ") { "ctx " } else { "context " };
        let mut completions: Vec<String> = data.contexts.items.iter()
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
        let crd_items = data.tables.get(&nav::rid(crate::kube::resource_def::BuiltInKind::CustomResourceDefinition))
            .map(|t| &t.items[..]).unwrap_or(&[]);
        let is_crd = !is_builtin && crd_items.iter().any(|row| {
            let info = match row.crd_info.as_ref() { Some(i) => i, None => return false };
            let kind = info.kind.to_lowercase();
            let plural = info.plural.to_lowercase();
            let short = row.name.split('.').next().unwrap_or("").to_lowercase();
            resource_part == kind || resource_part == plural || resource_part == short
        });
        if is_builtin || is_crd {
            let ns_items = data.tables.get(&nav::rid(crate::kube::resource_def::BuiltInKind::Namespace))
                .map(|t| &t.items[..]).unwrap_or(&[]);
            let mut completions: Vec<String> = ns_items.iter()
                .map(|ns| format!("{} {}", resource_part, ns.name()))
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
    let crd_items = data.tables.get(&nav::rid(crate::kube::resource_def::BuiltInKind::CustomResourceDefinition))
        .map(|t| &t.items[..]).unwrap_or(&[]);
    for row in crd_items {
        let Some(info) = row.crd_info.as_ref() else { continue };
        let kind_lower = info.kind.to_lowercase();
        let plural_lower = info.plural.to_lowercase();
        let short_plural = row.name.split('.').next().unwrap_or("").to_lowercase();
        for candidate in [&kind_lower, &plural_lower, &short_plural] {
            if !candidate.is_empty() && candidate.starts_with(&input_lower) {
                completions.push(candidate.clone());
            }
        }
    }

    completions.sort();
    completions.dedup();
    completions
}
