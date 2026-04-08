pub mod actions;
pub mod nav;
pub mod table;
pub mod types;

pub use table::*;
pub use types::*;

use std::collections::HashMap;
use std::time::Duration;

use crate::kube::protocol::{ObjectKey, ResourceId};
use crate::kube::resource_types::RESOURCE_TYPES;
use crate::kube::resources::KubeResource;

pub const CHANGE_HIGHLIGHT_SECS: u64 = 5;

// ---------------------------------------------------------------------------
// Pinned resource list for tab cycling
// ---------------------------------------------------------------------------

/// The default ordered list of pinned resources for Tab/BackTab cycling.
/// This replaces the old ResourceTab::all() enum.
pub fn default_pinned_resources() -> Vec<ResourceId> {
    RESOURCE_TYPES.iter().map(|m| m.to_resource_id()).collect()
}

#[derive(Debug, Clone)]
pub struct KubeContext {
    pub name: String,
    pub cluster: String,
    pub user: String,
    pub is_current: bool,
}

// ---------------------------------------------------------------------------
// AppData — all resource tables
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Column visibility levels
// ---------------------------------------------------------------------------

/// Display level for table columns. Ordered — a column is visible when the
/// app's current display level is >= the column's level.
///
/// Currently two levels; add more between or after without breaking anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ColumnLevel {
    /// Shown by default.
    Default = 0,
    /// Shown only when the user toggles extra columns on.
    Extra = 1,
}

impl ColumnLevel {
    /// Cycle to the next level, wrapping around.
    pub fn next(self) -> Self {
        match self {
            ColumnLevel::Default => ColumnLevel::Extra,
            ColumnLevel::Extra => ColumnLevel::Default,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ColumnLevel::Default => "default",
            ColumnLevel::Extra => "extra",
        }
    }
}

/// Columns that are hidden in the default view. Everything else is Default.
/// Case-insensitive match against header names.
const EXTRA_COLUMNS: &[&str] = &[
    "LABELS",
    "CONTAINERS",
    "IMAGES",
    "SELECTOR",
    "QOS",
    "SERVICE-ACCOUNT",
    "READINESS GATES",
    "LAST RESTART",
    "NODE SELECTOR",
    "INTERNAL-IP",
    "EXTERNAL-IP",
    "ARCH",
    "TAINTS",
    "CPU",
    "MEM",
    "CPU%",
    "MEM%",
];

/// Look up the display level for a column by header name.
pub fn column_level(name: &str) -> ColumnLevel {
    if EXTRA_COLUMNS.iter().any(|&c| c.eq_ignore_ascii_case(name)) {
        ColumnLevel::Extra
    } else {
        ColumnLevel::Default
    }
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
    /// viewing a single namespace.
    pub fn visible_columns(&self, level: ColumnLevel, skip_namespace: bool) -> Vec<(usize, &str)> {
        self.headers.iter().enumerate()
            .filter(|(_, name)| {
                if skip_namespace && name.eq_ignore_ascii_case("NAMESPACE") {
                    return false;
                }
                column_level(name) <= level
            })
            .map(|(i, name)| (i, name.as_str()))
            .collect()
    }
}

pub struct AppData {
    /// Unified tables for all resources (keyed by ResourceId).
    pub unified: std::collections::HashMap<ResourceId, StatefulTable<crate::kube::resources::row::ResourceRow>>,
    /// Runtime column headers for unified tables.
    pub descriptors: std::collections::HashMap<ResourceId, TableDescriptor>,

    pub contexts: StatefulTable<KubeContext>,
}

impl Default for AppData {
    fn default() -> Self {
        let mut unified = std::collections::HashMap::new();
        // Pre-populate entries for all known resource types.
        for meta in RESOURCE_TYPES {
            unified.insert(meta.to_resource_id(), StatefulTable::new());
        }
        Self {
            unified,
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
    pub context: String,
    pub cluster: String,
    pub user: String,
    pub selected_ns: crate::kube::protocol::Namespace,
    pub contexts: Vec<String>,

    pub data: AppData,

    /// Stackable navigation state for drill-downs and grep filters.
    pub nav: nav::NavStack,
    /// Ordered list of pinned resources for Tab/BackTab cycling.
    pub pinned_resources: Vec<ResourceId>,
    pub flash: Option<FlashMessage>,
    pub confirm_dialog: Option<ConfirmDialog>,
    pub port_forward_dialog: Option<PortForwardDialog>,

    pub theme: crate::ui::theme::Theme,
    pub input_mode: InputMode,
    pub help_scroll: usize,

    /// Whether the header (cluster info / key hints / logo) is visible.
    pub show_header: bool,

    pub tick_count: usize,

    /// Command history for `:` command mode (max 50 entries).
    pub command_history: Vec<String>,

    /// When true, Ctrl-C does not quit the application (`noExitOnCtrlC` config).
    pub no_exit_on_ctrl_c: bool,
    /// When true, destructive actions (delete, edit, scale, restart, force-kill, shell) are disabled.
    pub read_only: bool,

    /// Current column display level — controls which columns are visible.
    pub column_level: ColumnLevel,

    /// Cache for kubectl describe/yaml output (30s TTL).
    pub kubectl_cache: KubectlCache,
    /// Pod metrics from metrics-server.
    pub pod_metrics: HashMap<crate::kube::protocol::ObjectKey, crate::kube::protocol::MetricsUsage>,
    /// Node metrics from metrics-server: node_name -> usage.
    pub node_metrics: HashMap<String, crate::kube::protocol::MetricsUsage>,

    /// Delta tracking: previous row data per resource.
    pub prev_rows: HashMap<crate::kube::protocol::ObjectKey, Vec<String>>,
    /// Delta tracking: rows that changed in the last update.
    pub changed_rows: HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,

    /// Guard against rapid context switches: true while a switch is in flight.
    pub context_switch_pending: bool,
}

impl App {
    pub fn new(context: String, contexts: Vec<String>, namespace: String) -> Self {
        let config = Self::load_config();
        Self {
            should_quit: false,
            exit_reason: None,
            route: Route::Overview,
            route_stack: Vec::new(),
            context,
            cluster: String::new(),
            user: String::new(),
            selected_ns: crate::kube::protocol::Namespace::from(namespace),
            contexts,
            data: AppData::default(),
            nav: nav::NavStack::new(nav::rid("pods")),
            pinned_resources: default_pinned_resources(),
            flash: None,
            confirm_dialog: None,
            port_forward_dialog: None,
            theme: crate::ui::theme::Theme::load(),
            input_mode: InputMode::Normal,
            help_scroll: 0,
            show_header: true,
            tick_count: 0,
            command_history: Vec::new(),
            no_exit_on_ctrl_c: config.no_exit_on_ctrl_c,
            read_only: config.read_only,
            column_level: ColumnLevel::Default,
            kubectl_cache: KubectlCache::new(Duration::from_secs(30)),
            pod_metrics: HashMap::new(),
            node_metrics: HashMap::new(),
            prev_rows: HashMap::new(),
            changed_rows: HashMap::new(),
            context_switch_pending: false,
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

    /// Push a route onto the route stack, capping at 50 entries to prevent
    /// unbounded memory growth from deep navigation.
    pub fn push_route(&mut self, route: Route) {
        if self.route_stack.last() == Some(&route) {
            return; // Don't push duplicates
        }
        if self.route_stack.len() >= 50 {
            self.route_stack.remove(0);
        }
        self.route_stack.push(route);
    }

    /// Apply stored pod metrics to all current pod items.
    pub fn apply_pod_metrics(&mut self) {
        let pods_rid = nav::rid("pods");
        let desc = self.data.descriptors.get(&pods_rid);
        let cpu_col = desc.and_then(|d| d.col("CPU"));
        let mem_col = desc.and_then(|d| d.col("MEM"));
        if cpu_col.is_none() && mem_col.is_none() { return; }
        if let Some(table) = self.data.unified.get_mut(&pods_rid) {
            for row in &mut table.items {
                if let Some(usage) = self.pod_metrics.get(&ObjectKey::new(row.namespace.clone(), row.name.clone())) {
                    if let Some(col) = cpu_col { row.set_cell(col, usage.cpu.clone()); }
                    if let Some(col) = mem_col { row.set_cell(col, usage.mem.clone()); }
                }
            }
        }
    }

    /// Apply stored node metrics to all current node items.
    pub fn apply_node_metrics(&mut self) {
        let nodes_rid = nav::rid("nodes");
        let desc = self.data.descriptors.get(&nodes_rid);
        let cpu_col = desc.and_then(|d| d.col("CPU%"));
        let mem_col = desc.and_then(|d| d.col("MEM%"));
        if cpu_col.is_none() && mem_col.is_none() { return; }
        if let Some(table) = self.data.unified.get_mut(&nodes_rid) {
            for row in &mut table.items {
                if let Some(usage) = self.node_metrics.get(&row.name) {
                    if let Some(col) = cpu_col {
                        if let Some(cap) = row.cells.get(col).and_then(|c| c.split('/').nth(1)).map(|s| s.to_string()) {
                            row.set_cell(col, format!("{}/{}", usage.cpu, cap));
                        }
                    }
                    if let Some(col) = mem_col {
                        if let Some(cap) = row.cells.get(col).and_then(|c| c.split('/').nth(1)).map(|s| s.to_string()) {
                            row.set_cell(col, format!("{}/{}", usage.mem, cap));
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
        for table in self.data.unified.values_mut() {
            table.clear_data();
        }
        self.data.descriptors.clear();
    }

    /// Clear data for a specific resource so it shows "Loading..." until
    /// fresh data arrives from the server.
    pub fn clear_resource(&mut self, rid: &ResourceId) {
        if let Some(table) = self.data.unified.get_mut(rid) {
            table.clear_data();
        }
    }

    pub fn next_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid("pods");
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| r == current).unwrap_or(0);
        pinned[(idx + 1) % pinned.len()].clone()
    }

    pub fn prev_tab(&mut self) -> ResourceId {
        let pinned = &self.pinned_resources;
        if pinned.is_empty() {
            return nav::rid("pods");
        }
        let current = self.nav.resource_id();
        let idx = pinned.iter().position(|r| r == current).unwrap_or(0);
        pinned[if idx == 0 { pinned.len() - 1 } else { idx - 1 }].clone()
    }

    // Delegate navigation to the currently active table
    fn with_active_table<F: FnOnce(&mut dyn table::TableNav)>(&mut self, f: F) {
        let rid = self.nav.resource_id().clone();
        if let Some(table) = self.data.unified.get_mut(&rid) {
            f(table);
        }
    }

    // Delegate read-only operations to the currently active table
    fn with_active_table_ref<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dyn table::TableNav) -> R,
    {
        let rid = self.nav.resource_id();
        if let Some(table) = self.data.unified.get(rid) {
            f(table)
        } else {
            // Fallback for unknown CRDs — return default counts
            static EMPTY: std::sync::LazyLock<StatefulTable<crate::kube::resources::row::ResourceRow>> =
                std::sync::LazyLock::new(StatefulTable::new);
            f(&*EMPTY)
        }
    }

    pub fn select_next(&mut self) {
        if self.route == Route::Contexts {
            self.data.contexts.next();
        } else {
            self.with_active_table(|t| t.nav_next());
        }
    }
    pub fn select_prev(&mut self) {
        if self.route == Route::Contexts {
            self.data.contexts.previous();
        } else {
            self.with_active_table(|t| t.nav_prev());
        }
    }
    pub fn page_up(&mut self) {
        if self.route == Route::Contexts {
            self.data.contexts.page_up();
        } else {
            self.with_active_table(|t| t.nav_page_up());
        }
    }
    pub fn page_down(&mut self) {
        if self.route == Route::Contexts {
            self.data.contexts.page_down();
        } else {
            self.with_active_table(|t| t.nav_page_down());
        }
    }
    pub fn go_home(&mut self) {
        if self.route == Route::Contexts {
            self.data.contexts.home();
        } else {
            self.with_active_table(|t| t.nav_home());
        }
    }
    pub fn go_end(&mut self) {
        if self.route == Route::Contexts {
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

    /// Check if a row matches a grep pattern (vim-style smartcase regex).
    /// Handles `!pattern` for inverse matching.
    fn grep_matches_row(text: &str, row: &[String]) -> bool {
        let (pattern, invert) = if let Some(stripped) = text.strip_prefix('!') {
            (stripped, true)
        } else {
            (text, false)
        };
        let pat = crate::util::SearchPattern::new(pattern);
        let matches = row.iter().any(|cell| pat.is_match(cell));
        if invert { !matches } else { matches }
    }

    /// Reapply all nav stack filters (plus any uncommitted filter_input text) to the current table.
    /// Called after: snapshot arrival, nav push/pop, grep commit, filter input keystroke.
    pub fn reapply_nav_filters(&mut self) {
        use crate::app::nav::NavFilter;

        let filters = self.nav.active_filters();
        // Include uncommitted filter input text as a transient grep
        let input_text = if !self.nav.filter_input().text.is_empty() {
            Some(self.nav.filter_input().text.clone())
        } else {
            None
        };

        let has_filters = !filters.is_empty() || input_text.is_some();

        if !has_filters {
            self.clear_filter();
            return;
        }

        // Build a display string for the table title
        // For Pods: apply Labels, Field, OwnerChain, and Grep filters via extra bag.
        // For everything else: only Grep filters apply (Labels/Field are pod-specific).
        let is_pods = self.nav.resource_id().plural == "pods";
        if is_pods {
            let pods_rid = nav::rid("pods");
            let empty_labels = std::collections::BTreeMap::new();
            // Clone the filter data we need before the closure borrows self
            let filters_owned: Vec<NavFilter> = filters.iter().map(|f| (*f).clone()).collect();
            let input = input_text.clone();
            if let Some(table) = self.data.unified.get_mut(&pods_rid) {
                // Mark as nav-managed so set_items_filtered skips its own filtering.
                table.filter_text.clear();
                table.nav_managed = true;
                table.apply_filter(|row| {
                    for f in &filters_owned {
                        match f {
                            NavFilter::Grep(text) => {
                                if !Self::grep_matches_row(text, row.cells()) {
                                    return false;
                                }
                            }
                            NavFilter::Labels(selector) => {
                                let labels = row.extra_map("labels").unwrap_or(&empty_labels);
                                if !selector.iter().all(|(k, v)| {
                                    labels.get(k).map_or(false, |lv| lv == v)
                                }) {
                                    return false;
                                }
                            }
                            NavFilter::Field { field, value } => {
                                let matches = match field.as_str() {
                                    "node" => row.extra_str("node").unwrap_or("") == value.as_str(),
                                    "namespace" => row.namespace == *value,
                                    _ => false,
                                };
                                if !matches { return false; }
                            }
                            NavFilter::OwnerChain { uid, .. } => {
                                let refs = row.extra_owner_refs().unwrap_or(&[]);
                                if !refs.iter().any(|or| or.uid == *uid) {
                                    return false;
                                }
                            }
                        }
                    }
                    // Apply uncommitted input text
                    if let Some(ref text) = input {
                        if !Self::grep_matches_row(text, row.cells()) {
                            return false;
                        }
                    }
                    true
                });
            }
        } else {
            // Non-pod tables: collect all grep texts, apply as composite
            let mut grep_texts: Vec<String> = Vec::new();
            for f in &filters {
                if let NavFilter::Grep(text) = f {
                    grep_texts.push(text.clone());
                }
            }
            if let Some(ref text) = input_text {
                grep_texts.push(text.clone());
            }

            if grep_texts.is_empty() {
                self.clear_filter();
                return;
            }

            macro_rules! apply_greps {
                ($table:expr) => {{
                    // Mark as nav-managed so set_items_filtered skips its own filtering.
                    $table.filter_text.clear();
                    $table.nav_managed = true;
                    $table.apply_filter(|item| {
                        for t in &grep_texts {
                            if !Self::grep_matches_row(t, item.cells()) {
                                return false;
                            }
                        }
                        true
                    })
                }};
            }

            let rid = self.nav.resource_id().clone();
            if let Some(table) = self.data.unified.get_mut(&rid) {
                apply_greps!(table);
            }
        }
    }

    pub fn clear_filter(&mut self) {
        self.with_active_table(|t| t.nav_clear_filter());
    }

    /// Reset only the active table's data so the UI shows "Loading..." while
    /// the watcher fetches fresh data for the new tab.
    pub fn reset_active_table(&mut self) {
        self.with_active_table(|t| t.nav_reset());
    }

    /// Sort the active resource table by the given column index.
    /// If already sorted by this column, toggles ascending/descending.
    pub fn sort_by(&mut self, col: usize) {
        self.with_active_table(|t| t.nav_sort_by(col));
    }

    /// Toggle the sort direction on the active table's current sort column.
    /// Re-sorts with the same column index, which toggles asc/desc.
    pub fn toggle_sort_direction(&mut self) {
        self.with_active_table(|t| t.nav_toggle_sort());
    }

    /// Advance tick counter, expire flash messages, etc.
    /// Returns `true` if the UI should be redrawn (e.g. flash expired, loading animation).
    pub fn tick(&mut self) -> bool {
        self.tick_count = self.tick_count.wrapping_add(1);
        let mut changed = false;
        // Expire flash messages
        if let Some(ref flash) = self.flash {
            if flash.is_expired() {
                self.flash = None;
                changed = true;
            }
        }
        // Expire change highlights
        let now = std::time::Instant::now();
        let before = self.changed_rows.len();
        self.changed_rows.retain(|_, ts| now.duration_since(*ts).as_secs() < CHANGE_HIGHLIGHT_SECS);
        if self.changed_rows.len() != before {
            changed = true;
        }
        // Keep redrawing while a loading state is active (spinner animation).
        if !changed {
            let rid = self.nav.resource_id();
            let table_loading = self.data.unified.get(rid)
                .map_or(true, |t| t.items.is_empty() && !t.has_data);
            if table_loading {
                changed = true;
            }
        }
        changed
    }

    /// All known resource command aliases.
    const RESOURCE_COMMANDS: &'static [&'static str] = &[
        "pods", "po", "deploy", "deployments", "dp",
        "svc", "services", "sts", "statefulsets",
        "ds", "daemonsets", "jobs", "cj", "cronjobs",
        "cm", "configmaps", "sec", "secrets",
        "nodes", "no", "ns", "namespace", "namespaces",
        "ing", "ingress", "rs", "replicasets",
        "pv", "pvs", "pvc", "pvcs",
        "sc", "storageclasses", "sa", "serviceaccounts",
        "np", "networkpolicies", "ev", "events",
        "roles", "cr", "clusterroles",
        "rb", "rolebindings", "crb", "clusterrolebindings",
        "hpa", "horizontalpodautoscaler",
        "ep", "endpoints",
        "limits", "limitrange", "limitranges",
        "quota", "resourcequota", "resourcequotas",
        "pdb", "poddisruptionbudget", "poddisruptionbudgets",
        "crd", "crds", "customresourcedefinition", "customresourcedefinitions",
        "alias", "aliases", "a",
    ];

    /// Build completion candidates dynamically based on command input.
    /// Matching is case-insensitive since commands are lowercased on submit.
    pub fn command_completions(&self) -> Vec<String> {
        let cmd_input = match &self.input_mode {
            InputMode::Command { input, .. } => input.as_str(),
            _ => return Vec::new(),
        };
        let input_lower = cmd_input.trim_start().to_lowercase();

        // If input starts with "ns " or "namespace ", complete namespace names
        if input_lower.starts_with("ns ") || input_lower.starts_with("namespace ") {
            let cmd_prefix = if input_lower.starts_with("ns ") { "ns " } else { "namespace " };
            let ns_items = self.data.unified.get(&nav::rid("namespaces"))
                .map(|t| &t.items[..]).unwrap_or(&[]);
            let mut completions: Vec<String> = ns_items.iter()
                .map(|ns| format!("{}{}", cmd_prefix, ns.name()))
                .filter(|s| s.to_lowercase().starts_with(&input_lower))
                .collect();
            completions.sort();
            completions.dedup();
            return completions;
        }

        // If input starts with "ctx " or "context ", complete context names
        if input_lower.starts_with("ctx ") || input_lower.starts_with("context ") {
            let cmd_prefix = if input_lower.starts_with("ctx ") { "ctx " } else { "context " };
            let mut completions: Vec<String> = self.contexts.iter()
                .map(|c| format!("{}{}", cmd_prefix, c))
                .filter(|s| s.to_lowercase().starts_with(&input_lower))
                .collect();
            completions.sort();
            completions.dedup();
            return completions;
        }

        // If input contains a space, the first word might be a resource type
        // and the second word is a namespace: "deploy kube-system" or "clickhouseinstallation prod"
        if let Some(space_pos) = input_lower.find(' ') {
            let resource_part = &input_lower[..space_pos];
            // Check if it's a built-in resource command OR a discovered CRD name
            let is_builtin = Self::RESOURCE_COMMANDS.iter().any(|&r| r == resource_part);
            let crd_items = self.data.unified.get(&nav::rid("crds"))
                .map(|t| &t.items[..]).unwrap_or(&[]);
            let is_crd = !is_builtin && crd_items.iter().any(|row| {
                let kind = row.extra_str("kind").unwrap_or("").to_lowercase();
                let plural = row.extra_str("plural").unwrap_or("").to_lowercase();
                let short = row.name.split('.').next().unwrap_or("").to_lowercase();
                resource_part == kind || resource_part == plural || resource_part == short
            });
            if is_builtin || is_crd {
                // Complete with namespace names
                let ns_items = self.data.unified.get(&nav::rid("namespaces"))
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

        // Otherwise, complete command names (resource types + special commands)
        let mut all_commands: Vec<&str> = Self::RESOURCE_COMMANDS.to_vec();
        all_commands.extend_from_slice(&["ctx", "context", "contexts", "q", "quit", "help", "h"]);

        let mut completions: Vec<String> = all_commands.iter()
            .map(|s| String::from(*s))
            .filter(|s| s.starts_with(&input_lower))
            .collect();

        // Add discovered CRD names as completions using the actual plural field
        let crd_items = self.data.unified.get(&nav::rid("crds"))
            .map(|t| &t.items[..]).unwrap_or(&[]);
        for row in crd_items {
            let kind_lower = row.extra_str("kind").unwrap_or("").to_lowercase();
            let plural_lower = row.extra_str("plural").unwrap_or("").to_lowercase();
            // Also extract the short plural from the CRD name (before the dot)
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

    /// Returns the best (first) completion match, if any.
    pub fn best_completion(&self) -> Option<String> {
        if let InputMode::Command { ref input, .. } = self.input_mode {
            if input.trim().is_empty() { return None; }
            self.command_completions().into_iter().next()
        } else {
            None
        }
    }

    /// Accept the current ghost-text completion into the command input.
    pub fn accept_completion(&mut self) {
        if let Some(completion) = self.best_completion() {
            if let InputMode::Command { ref mut input, .. } = self.input_mode {
                *input = completion;
            }
        }
    }

    /// Returns filtered and total counts for the currently active resource table.
    pub fn active_table_items_count(&self) -> ItemCounts {
        self.with_active_table_ref(|t| t.nav_items_count())
    }

    /// Whether the current nav resource is cluster-scoped (no namespace).
    pub fn current_tab_is_cluster_scoped(&self) -> bool {
        self.nav.resource_id().is_cluster_scoped()
    }

    /// Find a discovered CRD by its kind name (case-insensitive).
    /// Returns a lightweight CrdInfo extracted from the unified ResourceRow extra bag.
    pub fn find_crd_by_name(&self, cmd: &str) -> Option<CrdInfo> {
        let lower = cmd.to_lowercase();
        let crds_rid = nav::rid("crds");
        let table = self.data.unified.get(&crds_rid)?;
        table.items.iter().find_map(|row| {
            let kind = row.extra_str("kind").unwrap_or("");
            let plural = row.extra_str("plural").unwrap_or("");
            let kind_lower = kind.to_lowercase();
            let name_lower = row.name.to_lowercase();
            let plural_lower = plural.to_lowercase();
            // Match by: kind, plural, full CRD name, kind+"s", or the
            // short plural from the CRD name (before the first dot).
            if kind_lower == lower
                || plural_lower == lower
                || name_lower == lower
                || format!("{}s", kind_lower) == lower
                || name_lower.split('.').next().map_or(false, |short| short == lower)
            {
                Some(CrdInfo {
                    group: row.extra_str("group").unwrap_or("").to_string(),
                    version: row.extra_str("version").unwrap_or("").to_string(),
                    kind: kind.to_string(),
                    plural: plural.to_string(),
                    scope: crate::kube::protocol::ResourceScope::from_scope_str(row.extra_str("scope").unwrap_or("Namespaced")),
                })
            } else {
                None
            }
        })
    }
}
