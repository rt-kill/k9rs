pub mod actions;

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::kube::resources::{
    KubeResource,
    configmaps::KubeConfigMap,
    cronjobs::KubeCronJob,
    daemonsets::KubeDaemonSet,
    deployments::KubeDeployment,
    endpoints::KubeEndpoints,
    events::KubeEvent,
    hpa::KubeHpa,
    ingress::KubeIngress,
    jobs::KubeJob,
    limitranges::KubeLimitRange,
    namespaces::KubeNamespace,
    networkpolicies::KubeNetworkPolicy,
    nodes::KubeNode,
    pdb::KubePdb,
    pods::KubePod,
    pvcs::KubePvc,
    pvs::KubePv,
    rbac::{KubeClusterRole, KubeClusterRoleBinding, KubeRole, KubeRoleBinding},
    replicasets::KubeReplicaSet,
    resourcequotas::KubeResourceQuota,
    secrets::KubeSecret,
    serviceaccounts::KubeServiceAccount,
    services::KubeService,
    statefulsets::KubeStatefulSet,
    storageclasses::KubeStorageClass,
};

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    Resources,
    Yaml {
        resource: String,
        name: String,
        namespace: String,
    },
    Describe {
        resource: String,
        name: String,
        namespace: String,
    },
    Logs {
        pod: String,
        container: String,
        namespace: String,
    },
    Shell {
        pod: String,
        container: String,
        namespace: String,
    },
    Help,
    Contexts,
    ContainerSelect {
        pod: String,
        namespace: String,
    },
    Aliases,
}

// ---------------------------------------------------------------------------
// ResourceTab
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTab {
    Pods,
    Deployments,
    Services,
    StatefulSets,
    DaemonSets,
    Jobs,
    CronJobs,
    ConfigMaps,
    Secrets,
    Nodes,
    Namespaces,
    Ingresses,
    ReplicaSets,
    Pvs,
    Pvcs,
    StorageClasses,
    ServiceAccounts,
    NetworkPolicies,
    Events,
    Roles,
    ClusterRoles,
    RoleBindings,
    ClusterRoleBindings,
    Hpa,
    Endpoints,
    LimitRanges,
    ResourceQuotas,
    Pdb,
}

impl ResourceTab {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pods => "Pods",
            Self::Deployments => "Deploy",
            Self::Services => "Svc",
            Self::StatefulSets => "STS",
            Self::DaemonSets => "DS",
            Self::Jobs => "Jobs",
            Self::CronJobs => "CronJobs",
            Self::ConfigMaps => "CM",
            Self::Secrets => "Secrets",
            Self::Nodes => "Nodes",
            Self::Namespaces => "NS",
            Self::Ingresses => "Ing",
            Self::ReplicaSets => "RS",
            Self::Pvs => "PV",
            Self::Pvcs => "PVC",
            Self::StorageClasses => "SC",
            Self::ServiceAccounts => "SA",
            Self::NetworkPolicies => "NetPol",
            Self::Events => "Events",
            Self::Roles => "Roles",
            Self::ClusterRoles => "CRoles",
            Self::RoleBindings => "RB",
            Self::ClusterRoleBindings => "CRB",
            Self::Hpa => "HPA",
            Self::Endpoints => "EP",
            Self::LimitRanges => "Limits",
            Self::ResourceQuotas => "Quota",
            Self::Pdb => "PDB",
        }
    }

    pub fn all() -> &'static [ResourceTab] {
        &[
            Self::Pods,
            Self::Deployments,
            Self::Services,
            Self::StatefulSets,
            Self::DaemonSets,
            Self::Jobs,
            Self::CronJobs,
            Self::ConfigMaps,
            Self::Secrets,
            Self::Nodes,
            Self::Namespaces,
            Self::Ingresses,
            Self::ReplicaSets,
            Self::Pvs,
            Self::Pvcs,
            Self::StorageClasses,
            Self::ServiceAccounts,
            Self::NetworkPolicies,
            Self::Events,
            Self::Roles,
            Self::ClusterRoles,
            Self::RoleBindings,
            Self::ClusterRoleBindings,
            Self::Hpa,
            Self::Endpoints,
            Self::LimitRanges,
            Self::ResourceQuotas,
            Self::Pdb,
        ]
    }

    pub fn index(&self) -> usize {
        Self::all().iter().position(|t| t == self).unwrap_or(0)
    }

    pub fn from_index(i: usize) -> Self {
        Self::all().get(i).copied().unwrap_or(Self::Pods)
    }
}

// ---------------------------------------------------------------------------
// Flash / Filter / Confirm / Log / Yaml / Describe state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct FlashMessage {
    pub message: String,
    pub level: FlashLevel,
    pub created: Instant,
}

impl FlashMessage {
    pub fn info(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Info, created: Instant::now() }
    }
    pub fn warn(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Warn, created: Instant::now() }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Error, created: Instant::now() }
    }
    pub fn is_expired(&self) -> bool {
        self.created.elapsed().as_secs() >= 5
    }
}

#[derive(Debug, Clone)]
pub struct FilterState {
    pub active: bool,
    pub text: String,
}

impl Default for FilterState {
    fn default() -> Self {
        Self { active: false, text: String::new() }
    }
}

#[derive(Debug, Clone)]
pub struct ConfirmDialog {
    pub message: String,
    pub action: actions::Action,
    pub yes_selected: bool,
}

#[derive(Debug, Clone)]
pub struct LogState {
    pub lines: VecDeque<String>,
    pub max_lines: usize,
    pub scroll: usize,
    pub follow: bool,
    pub wrap: bool,
    pub show_timestamps: bool,
    /// Whether a log streaming task is currently running.
    pub streaming: bool,
    /// The --since time range for kubectl logs. None = use tail_lines instead.
    pub since: Option<String>,
    /// Number of recent log lines to fetch with --tail when since is None.
    pub tail_lines: u64,
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            max_lines: 50_000,
            scroll: 0,
            follow: false,
            wrap: false,
            show_timestamps: true,
            streaming: false,
            since: None,
            tail_lines: 500,
        }
    }
}

impl LogState {
    pub fn new() -> Self {
        Self { follow: true, show_timestamps: true, streaming: false, ..Default::default() }
    }
    pub fn push(&mut self, line: String) {
        if self.lines.len() >= self.max_lines {
            self.lines.pop_front();
            // Adjust scroll to account for removed line
            self.scroll = self.scroll.saturating_sub(1);
        }
        self.lines.push_back(line);
        if self.follow {
            self.scroll = self.lines.len().saturating_sub(1);
        }
    }
    pub fn clear(&mut self) {
        self.lines.clear();
        self.scroll = 0;
    }
}

/// Shared state for YAML and Describe content views (previously duplicated as
/// `YamlState` and `DescribeState`).
#[derive(Debug, Clone, Default)]
pub struct ContentViewState {
    pub content: String,
    pub scroll: usize,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
    pub search_input_active: bool,
    pub search_input: String,
}

impl ContentViewState {
    /// Recompute search matches from current content and search term.
    pub fn update_search(&mut self) {
        self.search_matches.clear();
        self.current_match = 0;
        if let Some(ref term) = self.search {
            if term.is_empty() {
                return;
            }
            let lower_term = term.to_lowercase();
            for (i, line) in self.content.lines().enumerate() {
                if line.to_lowercase().contains(&lower_term) {
                    self.search_matches.push(i);
                }
            }
        }
    }

    /// Jump to the next search match, centering it in the viewport.
    pub fn next_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
    }

    /// Jump to the previous search match, centering it in the viewport.
    pub fn prev_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = if self.current_match == 0 {
            self.search_matches.len() - 1
        } else {
            self.current_match - 1
        };
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
    }

    /// Clear search state.
    pub fn clear_search(&mut self) {
        self.search = None;
        self.search_matches.clear();
        self.current_match = 0;
        self.search_input_active = false;
        self.search_input.clear();
    }
}

/// Type alias for backward compatibility.
pub type YamlState = ContentViewState;
/// Type alias for backward compatibility.
pub type DescribeState = ContentViewState;

// ---------------------------------------------------------------------------
// KubectlCache — TTL cache for describe/yaml kubectl output
// ---------------------------------------------------------------------------

pub struct KubectlCache {
    entries: HashMap<(String, String, String, &'static str), (String, Instant)>,
    ttl: Duration,
}

impl KubectlCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
        }
    }

    pub fn get(&self, resource: &str, name: &str, namespace: &str, kind: &'static str) -> Option<&str> {
        self.entries
            .get(&(resource.to_string(), name.to_string(), namespace.to_string(), kind))
            .and_then(|(content, ts)| {
                if ts.elapsed() < self.ttl {
                    Some(content.as_str())
                } else {
                    None
                }
            })
    }

    pub fn insert(
        &mut self,
        resource: String,
        name: String,
        namespace: String,
        kind: &'static str,
        content: String,
    ) {
        self.entries
            .insert((resource, name, namespace, kind), (content, Instant::now()));
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Debug, Clone)]
pub struct KubeContext {
    pub name: String,
    pub cluster: String,
    pub is_current: bool,
}

// ---------------------------------------------------------------------------
// AppData — all resource tables
// ---------------------------------------------------------------------------

pub struct AppData {
    pub pods: StatefulTable<KubePod>,
    pub deployments: StatefulTable<KubeDeployment>,
    pub services: StatefulTable<KubeService>,
    pub nodes: StatefulTable<KubeNode>,
    pub namespaces: StatefulTable<KubeNamespace>,
    pub configmaps: StatefulTable<KubeConfigMap>,
    pub secrets: StatefulTable<KubeSecret>,
    pub statefulsets: StatefulTable<KubeStatefulSet>,
    pub daemonsets: StatefulTable<KubeDaemonSet>,
    pub jobs: StatefulTable<KubeJob>,
    pub cronjobs: StatefulTable<KubeCronJob>,
    pub replicasets: StatefulTable<KubeReplicaSet>,
    pub ingresses: StatefulTable<KubeIngress>,
    pub network_policies: StatefulTable<KubeNetworkPolicy>,
    pub service_accounts: StatefulTable<KubeServiceAccount>,
    pub storage_classes: StatefulTable<KubeStorageClass>,
    pub pvs: StatefulTable<KubePv>,
    pub pvcs: StatefulTable<KubePvc>,
    pub events: StatefulTable<KubeEvent>,
    pub roles: StatefulTable<KubeRole>,
    pub cluster_roles: StatefulTable<KubeClusterRole>,
    pub role_bindings: StatefulTable<KubeRoleBinding>,
    pub cluster_role_bindings: StatefulTable<KubeClusterRoleBinding>,
    pub hpa: StatefulTable<KubeHpa>,
    pub endpoints: StatefulTable<KubeEndpoints>,
    pub limit_ranges: StatefulTable<KubeLimitRange>,
    pub resource_quotas: StatefulTable<KubeResourceQuota>,
    pub pdb: StatefulTable<KubePdb>,
    pub contexts: StatefulTable<KubeContext>,
}

impl Default for AppData {
    fn default() -> Self {
        Self {
            pods: StatefulTable::new(),
            deployments: StatefulTable::new(),
            services: StatefulTable::new(),
            nodes: StatefulTable::new(),
            namespaces: StatefulTable::new(),
            configmaps: StatefulTable::new(),
            secrets: StatefulTable::new(),
            statefulsets: StatefulTable::new(),
            daemonsets: StatefulTable::new(),
            jobs: StatefulTable::new(),
            cronjobs: StatefulTable::new(),
            replicasets: StatefulTable::new(),
            ingresses: StatefulTable::new(),
            network_policies: StatefulTable::new(),
            service_accounts: StatefulTable::new(),
            storage_classes: StatefulTable::new(),
            pvs: StatefulTable::new(),
            pvcs: StatefulTable::new(),
            events: StatefulTable::new(),
            roles: StatefulTable::new(),
            cluster_roles: StatefulTable::new(),
            role_bindings: StatefulTable::new(),
            cluster_role_bindings: StatefulTable::new(),
            hpa: StatefulTable::new(),
            endpoints: StatefulTable::new(),
            limit_ranges: StatefulTable::new(),
            resource_quotas: StatefulTable::new(),
            pdb: StatefulTable::new(),
            contexts: StatefulTable::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// App — main application state
// ---------------------------------------------------------------------------

pub struct App {
    pub should_quit: bool,
    pub route: Route,
    pub route_stack: Vec<Route>,
    pub resource_tab: ResourceTab,
    pub last_resource_tab: Option<ResourceTab>,

    pub context: String,
    pub selected_ns: String,
    pub contexts: Vec<String>,

    pub data: AppData,

    pub filter: FilterState,
    pub flash: Option<FlashMessage>,
    pub confirm_dialog: Option<ConfirmDialog>,

    pub logs: LogState,
    pub yaml: YamlState,
    pub describe: DescribeState,

    pub theme: crate::ui::theme::Theme,

    pub command_mode: bool,
    pub command_input: String,

    /// When true, the command prompt is used for entering a replica count for scaling.
    pub scale_mode: bool,
    /// The (resource_type, name, namespace) target of the pending scale operation.
    pub scale_target: (String, String, String),

    pub container_select_index: usize,

    pub help_scroll: usize,

    /// Whether the header (cluster info / key hints / logo) is visible.
    pub show_header: bool,

    pub tick_count: usize,

    /// Command history for `:` command mode (max 50 entries).
    pub command_history: Vec<String>,
    /// Current index into command_history when navigating with Up/Down.
    pub command_history_index: Option<usize>,

    /// When true, Ctrl-C does not quit the application (k9s `noExitOnCtrlC` config).
    pub no_exit_on_ctrl_c: bool,
    /// When true, destructive actions (delete, edit, scale, restart, force-kill, shell) are disabled.
    pub read_only: bool,

    /// Whether wide column mode is active (show additional columns).
    pub wide_mode: bool,
    /// Whether fault filter is active (show only unhealthy resources).
    pub fault_filter: bool,

    /// Cache for kubectl describe/yaml output (30s TTL).
    pub kubectl_cache: KubectlCache,
    /// Pending describe key for cache population when result arrives.
    pub pending_describe_key: Option<(String, String, String)>,
    /// Pending yaml key for cache population when result arrives.
    pub pending_yaml_key: Option<(String, String, String)>,
}

impl App {
    pub fn new(context: String, contexts: Vec<String>, namespace: String) -> Self {
        let (no_exit_on_ctrl_c, read_only) = Self::load_k9s_config();
        Self {
            should_quit: false,
            route: Route::Resources,
            route_stack: Vec::new(),
            resource_tab: ResourceTab::Pods,
            last_resource_tab: None,
            context,
            selected_ns: namespace,
            contexts,
            data: AppData::default(),
            filter: FilterState::default(),
            flash: None,
            confirm_dialog: None,
            logs: LogState::new(),
            yaml: YamlState::default(),
            describe: DescribeState::default(),
            theme: crate::ui::theme::Theme::load(),
            command_mode: false,
            command_input: String::new(),
            scale_mode: false,
            scale_target: (String::new(), String::new(), String::new()),
            container_select_index: 0,
            help_scroll: 0,
            show_header: true,
            tick_count: 0,
            command_history: Vec::new(),
            command_history_index: None,
            no_exit_on_ctrl_c,
            read_only,
            wide_mode: false,
            fault_filter: false,
            kubectl_cache: KubectlCache::new(Duration::from_secs(30)),
            pending_describe_key: None,
            pending_yaml_key: None,
        }
    }

    /// Load `noExitOnCtrlC` and `readOnly` settings from `~/.config/k9s/config.yaml`.
    /// Returns `(no_exit_on_ctrl_c, read_only)`. Defaults to `(false, false)` on any error.
    fn load_k9s_config() -> (bool, bool) {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return (false, false),
        };
        let config_path = std::path::Path::new(&home).join(".config/k9s/config.yaml");
        let content = match std::fs::read_to_string(&config_path) {
            Ok(c) => c,
            Err(_) => return (false, false),
        };
        let yaml: serde_yaml::Value = match serde_yaml::from_str(&content) {
            Ok(v) => v,
            Err(_) => return (false, false),
        };
        let k9s = yaml.get("k9s");
        let no_exit = k9s
            .and_then(|v| v.get("noExitOnCtrlC"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let read_only = k9s
            .and_then(|v| v.get("readOnly"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        (no_exit, read_only)
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

    /// Clear all resource table data. Used when switching namespaces so stale
    /// data from the old namespace doesn't persist.
    pub fn clear_data(&mut self) {
        self.data.pods.clear_data();
        self.data.deployments.clear_data();
        self.data.services.clear_data();
        self.data.nodes.clear_data();
        self.data.namespaces.clear_data();
        self.data.configmaps.clear_data();
        self.data.secrets.clear_data();
        self.data.statefulsets.clear_data();
        self.data.daemonsets.clear_data();
        self.data.jobs.clear_data();
        self.data.cronjobs.clear_data();
        self.data.replicasets.clear_data();
        self.data.ingresses.clear_data();
        self.data.network_policies.clear_data();
        self.data.service_accounts.clear_data();
        self.data.storage_classes.clear_data();
        self.data.pvs.clear_data();
        self.data.pvcs.clear_data();
        self.data.events.clear_data();
        self.data.roles.clear_data();
        self.data.cluster_roles.clear_data();
        self.data.role_bindings.clear_data();
        self.data.cluster_role_bindings.clear_data();
        self.data.hpa.clear_data();
        self.data.endpoints.clear_data();
        self.data.limit_ranges.clear_data();
        self.data.resource_quotas.clear_data();
        self.data.pdb.clear_data();
    }

    pub fn next_tab(&mut self) {
        let tabs = ResourceTab::all();
        let idx = self.resource_tab.index();
        self.resource_tab = ResourceTab::from_index((idx + 1) % tabs.len());
    }

    pub fn prev_tab(&mut self) {
        let tabs = ResourceTab::all();
        let idx = self.resource_tab.index();
        self.resource_tab = ResourceTab::from_index(if idx == 0 { tabs.len() - 1 } else { idx - 1 });
    }

    // Delegate navigation to the currently active table
    fn with_active_table<F: FnOnce(&mut dyn TableNav)>(&mut self, f: F) {
        match self.resource_tab {
            ResourceTab::Pods => f(&mut self.data.pods),
            ResourceTab::Deployments => f(&mut self.data.deployments),
            ResourceTab::Services => f(&mut self.data.services),
            ResourceTab::Nodes => f(&mut self.data.nodes),
            ResourceTab::Namespaces => f(&mut self.data.namespaces),
            ResourceTab::ConfigMaps => f(&mut self.data.configmaps),
            ResourceTab::Secrets => f(&mut self.data.secrets),
            ResourceTab::StatefulSets => f(&mut self.data.statefulsets),
            ResourceTab::DaemonSets => f(&mut self.data.daemonsets),
            ResourceTab::Jobs => f(&mut self.data.jobs),
            ResourceTab::CronJobs => f(&mut self.data.cronjobs),
            ResourceTab::ReplicaSets => f(&mut self.data.replicasets),
            ResourceTab::Ingresses => f(&mut self.data.ingresses),
            ResourceTab::NetworkPolicies => f(&mut self.data.network_policies),
            ResourceTab::ServiceAccounts => f(&mut self.data.service_accounts),
            ResourceTab::StorageClasses => f(&mut self.data.storage_classes),
            ResourceTab::Pvs => f(&mut self.data.pvs),
            ResourceTab::Pvcs => f(&mut self.data.pvcs),
            ResourceTab::Events => f(&mut self.data.events),
            ResourceTab::Roles => f(&mut self.data.roles),
            ResourceTab::ClusterRoles => f(&mut self.data.cluster_roles),
            ResourceTab::RoleBindings => f(&mut self.data.role_bindings),
            ResourceTab::ClusterRoleBindings => f(&mut self.data.cluster_role_bindings),
            ResourceTab::Hpa => f(&mut self.data.hpa),
            ResourceTab::Endpoints => f(&mut self.data.endpoints),
            ResourceTab::LimitRanges => f(&mut self.data.limit_ranges),
            ResourceTab::ResourceQuotas => f(&mut self.data.resource_quotas),
            ResourceTab::Pdb => f(&mut self.data.pdb),
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

    /// Toggle mark on the currently selected row, then advance to the next row.
    pub fn toggle_mark(&mut self) {
        self.with_active_table(|t| t.nav_toggle_mark());
    }

    pub fn apply_filter(&mut self, text: &str) {
        let t = text.to_lowercase();
        // Try to compile as regex; fall back to substring match
        let re = regex::Regex::new(&t).ok();
        macro_rules! apply {
            ($table:expr) => {{
                $table.filter_text = text.to_string();
                $table.apply_filter(|item| {
                    item.row().iter().any(|cell| {
                        let lower = cell.to_lowercase();
                        if let Some(ref re) = re {
                            re.is_match(&lower)
                        } else {
                            lower.contains(&t)
                        }
                    })
                })
            }};
        }
        match self.resource_tab {
            ResourceTab::Pods => apply!(self.data.pods),
            ResourceTab::Deployments => apply!(self.data.deployments),
            ResourceTab::Services => apply!(self.data.services),
            ResourceTab::Nodes => apply!(self.data.nodes),
            ResourceTab::Namespaces => apply!(self.data.namespaces),
            ResourceTab::ConfigMaps => apply!(self.data.configmaps),
            ResourceTab::Secrets => apply!(self.data.secrets),
            ResourceTab::StatefulSets => apply!(self.data.statefulsets),
            ResourceTab::DaemonSets => apply!(self.data.daemonsets),
            ResourceTab::Jobs => apply!(self.data.jobs),
            ResourceTab::CronJobs => apply!(self.data.cronjobs),
            ResourceTab::ReplicaSets => apply!(self.data.replicasets),
            ResourceTab::Ingresses => apply!(self.data.ingresses),
            ResourceTab::NetworkPolicies => apply!(self.data.network_policies),
            ResourceTab::ServiceAccounts => apply!(self.data.service_accounts),
            ResourceTab::StorageClasses => apply!(self.data.storage_classes),
            ResourceTab::Pvs => apply!(self.data.pvs),
            ResourceTab::Pvcs => apply!(self.data.pvcs),
            ResourceTab::Events => apply!(self.data.events),
            ResourceTab::Roles => apply!(self.data.roles),
            ResourceTab::ClusterRoles => apply!(self.data.cluster_roles),
            ResourceTab::RoleBindings => apply!(self.data.role_bindings),
            ResourceTab::ClusterRoleBindings => apply!(self.data.cluster_role_bindings),
            ResourceTab::Hpa => apply!(self.data.hpa),
            ResourceTab::Endpoints => apply!(self.data.endpoints),
            ResourceTab::LimitRanges => apply!(self.data.limit_ranges),
            ResourceTab::ResourceQuotas => apply!(self.data.resource_quotas),
            ResourceTab::Pdb => apply!(self.data.pdb),
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
        macro_rules! sort {
            ($table:expr) => {{
                $table.sort_by_column(col);
            }};
        }
        match self.resource_tab {
            ResourceTab::Pods => sort!(self.data.pods),
            ResourceTab::Deployments => sort!(self.data.deployments),
            ResourceTab::Services => sort!(self.data.services),
            ResourceTab::Nodes => sort!(self.data.nodes),
            ResourceTab::Namespaces => sort!(self.data.namespaces),
            ResourceTab::ConfigMaps => sort!(self.data.configmaps),
            ResourceTab::Secrets => sort!(self.data.secrets),
            ResourceTab::StatefulSets => sort!(self.data.statefulsets),
            ResourceTab::DaemonSets => sort!(self.data.daemonsets),
            ResourceTab::Jobs => sort!(self.data.jobs),
            ResourceTab::CronJobs => sort!(self.data.cronjobs),
            ResourceTab::ReplicaSets => sort!(self.data.replicasets),
            ResourceTab::Ingresses => sort!(self.data.ingresses),
            ResourceTab::NetworkPolicies => sort!(self.data.network_policies),
            ResourceTab::ServiceAccounts => sort!(self.data.service_accounts),
            ResourceTab::StorageClasses => sort!(self.data.storage_classes),
            ResourceTab::Pvs => sort!(self.data.pvs),
            ResourceTab::Pvcs => sort!(self.data.pvcs),
            ResourceTab::Events => sort!(self.data.events),
            ResourceTab::Roles => sort!(self.data.roles),
            ResourceTab::ClusterRoles => sort!(self.data.cluster_roles),
            ResourceTab::RoleBindings => sort!(self.data.role_bindings),
            ResourceTab::ClusterRoleBindings => sort!(self.data.cluster_role_bindings),
            ResourceTab::Hpa => sort!(self.data.hpa),
            ResourceTab::Endpoints => sort!(self.data.endpoints),
            ResourceTab::LimitRanges => sort!(self.data.limit_ranges),
            ResourceTab::ResourceQuotas => sort!(self.data.resource_quotas),
            ResourceTab::Pdb => sort!(self.data.pdb),
        }
    }

    /// Toggle the sort direction on the active table's current sort column.
    /// Re-sorts with the same column index, which toggles asc/desc.
    pub fn toggle_sort_direction(&mut self) {
        macro_rules! resort {
            ($table:expr) => {{
                let col = $table.sort_column;
                $table.sort_by_column(col);
            }};
        }
        match self.resource_tab {
            ResourceTab::Pods => resort!(self.data.pods),
            ResourceTab::Deployments => resort!(self.data.deployments),
            ResourceTab::Services => resort!(self.data.services),
            ResourceTab::Nodes => resort!(self.data.nodes),
            ResourceTab::Namespaces => resort!(self.data.namespaces),
            ResourceTab::ConfigMaps => resort!(self.data.configmaps),
            ResourceTab::Secrets => resort!(self.data.secrets),
            ResourceTab::StatefulSets => resort!(self.data.statefulsets),
            ResourceTab::DaemonSets => resort!(self.data.daemonsets),
            ResourceTab::Jobs => resort!(self.data.jobs),
            ResourceTab::CronJobs => resort!(self.data.cronjobs),
            ResourceTab::ReplicaSets => resort!(self.data.replicasets),
            ResourceTab::Ingresses => resort!(self.data.ingresses),
            ResourceTab::NetworkPolicies => resort!(self.data.network_policies),
            ResourceTab::ServiceAccounts => resort!(self.data.service_accounts),
            ResourceTab::StorageClasses => resort!(self.data.storage_classes),
            ResourceTab::Pvs => resort!(self.data.pvs),
            ResourceTab::Pvcs => resort!(self.data.pvcs),
            ResourceTab::Events => resort!(self.data.events),
            ResourceTab::Roles => resort!(self.data.roles),
            ResourceTab::ClusterRoles => resort!(self.data.cluster_roles),
            ResourceTab::RoleBindings => resort!(self.data.role_bindings),
            ResourceTab::ClusterRoleBindings => resort!(self.data.cluster_role_bindings),
            ResourceTab::Hpa => resort!(self.data.hpa),
            ResourceTab::Endpoints => resort!(self.data.endpoints),
            ResourceTab::LimitRanges => resort!(self.data.limit_ranges),
            ResourceTab::ResourceQuotas => resort!(self.data.resource_quotas),
            ResourceTab::Pdb => resort!(self.data.pdb),
        }
    }

    pub fn tick(&mut self) {
        self.tick_count = self.tick_count.wrapping_add(1);
        if let Some(ref flash) = self.flash {
            if flash.is_expired() {
                self.flash = None;
            }
        }
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
        "alias", "aliases", "a",
    ];

    /// Build completion candidates dynamically based on command input.
    pub fn command_completions(&self) -> Vec<String> {
        let input = &self.command_input;

        // If input starts with "ns " or "namespace ", complete namespace names
        if input.starts_with("ns ") || input.starts_with("namespace ") {
            let cmd_prefix = if input.starts_with("ns ") { "ns " } else { "namespace " };
            let mut completions: Vec<String> = self.data.namespaces.items.iter()
                .map(|ns| format!("{}{}", cmd_prefix, ns.name()))
                .filter(|s| s.starts_with(input.as_str()))
                .collect();
            completions.sort();
            completions.dedup();
            return completions;
        }

        // If input starts with "ctx " or "context ", complete context names
        if input.starts_with("ctx ") || input.starts_with("context ") {
            let cmd_prefix = if input.starts_with("ctx ") { "ctx " } else { "context " };
            let mut completions: Vec<String> = self.contexts.iter()
                .map(|c| format!("{}{}", cmd_prefix, c))
                .filter(|s| s.starts_with(input.as_str()))
                .collect();
            completions.sort();
            completions.dedup();
            return completions;
        }

        // If input contains a space, the first word might be a resource type
        // and the second word is a namespace: "deploy kube-system"
        if let Some(space_pos) = input.find(' ') {
            let resource_part = &input[..space_pos];
            // Check if the resource part is a valid resource command
            if Self::RESOURCE_COMMANDS.iter().any(|&r| r == resource_part) {
                // Complete with namespace names
                let mut completions: Vec<String> = self.data.namespaces.items.iter()
                    .map(|ns| format!("{} {}", resource_part, ns.name()))
                    .filter(|s| s.starts_with(input.as_str()))
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
            .filter(|s| s.starts_with(input.as_str()))
            .collect();

        completions.sort();
        completions.dedup();
        completions
    }

    /// Returns the best (first) completion match, if any.
    pub fn best_completion(&self) -> Option<String> {
        if self.command_input.is_empty() { return None; }
        self.command_completions().into_iter().next()
    }

    /// Accept the current ghost-text completion into the command input.
    pub fn accept_completion(&mut self) {
        if let Some(completion) = self.best_completion() {
            self.command_input = completion;
        }
    }

    /// Returns (filtered_count, total_count) for the currently active resource table.
    pub fn active_table_items_count(&self) -> (usize, usize) {
        match self.resource_tab {
            ResourceTab::Pods => (self.data.pods.len(), self.data.pods.total()),
            ResourceTab::Deployments => (self.data.deployments.len(), self.data.deployments.total()),
            ResourceTab::Services => (self.data.services.len(), self.data.services.total()),
            ResourceTab::Nodes => (self.data.nodes.len(), self.data.nodes.total()),
            ResourceTab::Namespaces => (self.data.namespaces.len(), self.data.namespaces.total()),
            ResourceTab::ConfigMaps => (self.data.configmaps.len(), self.data.configmaps.total()),
            ResourceTab::Secrets => (self.data.secrets.len(), self.data.secrets.total()),
            ResourceTab::StatefulSets => (self.data.statefulsets.len(), self.data.statefulsets.total()),
            ResourceTab::DaemonSets => (self.data.daemonsets.len(), self.data.daemonsets.total()),
            ResourceTab::Jobs => (self.data.jobs.len(), self.data.jobs.total()),
            ResourceTab::CronJobs => (self.data.cronjobs.len(), self.data.cronjobs.total()),
            ResourceTab::ReplicaSets => (self.data.replicasets.len(), self.data.replicasets.total()),
            ResourceTab::Ingresses => (self.data.ingresses.len(), self.data.ingresses.total()),
            ResourceTab::NetworkPolicies => (self.data.network_policies.len(), self.data.network_policies.total()),
            ResourceTab::ServiceAccounts => (self.data.service_accounts.len(), self.data.service_accounts.total()),
            ResourceTab::StorageClasses => (self.data.storage_classes.len(), self.data.storage_classes.total()),
            ResourceTab::Pvs => (self.data.pvs.len(), self.data.pvs.total()),
            ResourceTab::Pvcs => (self.data.pvcs.len(), self.data.pvcs.total()),
            ResourceTab::Events => (self.data.events.len(), self.data.events.total()),
            ResourceTab::Roles => (self.data.roles.len(), self.data.roles.total()),
            ResourceTab::ClusterRoles => (self.data.cluster_roles.len(), self.data.cluster_roles.total()),
            ResourceTab::RoleBindings => (self.data.role_bindings.len(), self.data.role_bindings.total()),
            ResourceTab::ClusterRoleBindings => (self.data.cluster_role_bindings.len(), self.data.cluster_role_bindings.total()),
            ResourceTab::Hpa => (self.data.hpa.len(), self.data.hpa.total()),
            ResourceTab::Endpoints => (self.data.endpoints.len(), self.data.endpoints.total()),
            ResourceTab::LimitRanges => (self.data.limit_ranges.len(), self.data.limit_ranges.total()),
            ResourceTab::ResourceQuotas => (self.data.resource_quotas.len(), self.data.resource_quotas.total()),
            ResourceTab::Pdb => (self.data.pdb.len(), self.data.pdb.total()),
        }
    }

    /// Returns the active filter text for the currently active resource table.
    pub fn active_filter_text(&self) -> &str {
        match self.resource_tab {
            ResourceTab::Pods => &self.data.pods.filter_text,
            ResourceTab::Deployments => &self.data.deployments.filter_text,
            ResourceTab::Services => &self.data.services.filter_text,
            ResourceTab::Nodes => &self.data.nodes.filter_text,
            ResourceTab::Namespaces => &self.data.namespaces.filter_text,
            ResourceTab::ConfigMaps => &self.data.configmaps.filter_text,
            ResourceTab::Secrets => &self.data.secrets.filter_text,
            ResourceTab::StatefulSets => &self.data.statefulsets.filter_text,
            ResourceTab::DaemonSets => &self.data.daemonsets.filter_text,
            ResourceTab::Jobs => &self.data.jobs.filter_text,
            ResourceTab::CronJobs => &self.data.cronjobs.filter_text,
            ResourceTab::ReplicaSets => &self.data.replicasets.filter_text,
            ResourceTab::Ingresses => &self.data.ingresses.filter_text,
            ResourceTab::NetworkPolicies => &self.data.network_policies.filter_text,
            ResourceTab::ServiceAccounts => &self.data.service_accounts.filter_text,
            ResourceTab::StorageClasses => &self.data.storage_classes.filter_text,
            ResourceTab::Pvs => &self.data.pvs.filter_text,
            ResourceTab::Pvcs => &self.data.pvcs.filter_text,
            ResourceTab::Events => &self.data.events.filter_text,
            ResourceTab::Roles => &self.data.roles.filter_text,
            ResourceTab::ClusterRoles => &self.data.cluster_roles.filter_text,
            ResourceTab::RoleBindings => &self.data.role_bindings.filter_text,
            ResourceTab::ClusterRoleBindings => &self.data.cluster_role_bindings.filter_text,
            ResourceTab::Hpa => &self.data.hpa.filter_text,
            ResourceTab::Endpoints => &self.data.endpoints.filter_text,
            ResourceTab::LimitRanges => &self.data.limit_ranges.filter_text,
            ResourceTab::ResourceQuotas => &self.data.resource_quotas.filter_text,
            ResourceTab::Pdb => &self.data.pdb.filter_text,
        }
    }
}

// ---------------------------------------------------------------------------
// TableNav trait — allows App to dispatch navigation to any StatefulTable
// ---------------------------------------------------------------------------

trait TableNav {
    fn nav_next(&mut self);
    fn nav_prev(&mut self);
    fn nav_page_up(&mut self);
    fn nav_page_down(&mut self);
    fn nav_home(&mut self);
    fn nav_end(&mut self);
    fn nav_clear_filter(&mut self);
    fn nav_reset(&mut self);
    fn nav_toggle_mark(&mut self);
}

impl<T: Clone> TableNav for StatefulTable<T> {
    fn nav_next(&mut self) { self.next(); }
    fn nav_prev(&mut self) { self.previous(); }
    fn nav_page_up(&mut self) { self.page_up(); }
    fn nav_page_down(&mut self) { self.page_down(); }
    fn nav_home(&mut self) { self.home(); }
    fn nav_end(&mut self) { self.end(); }
    fn nav_clear_filter(&mut self) { self.clear_filter(); }
    fn nav_reset(&mut self) { self.clear_data(); }
    fn nav_toggle_mark(&mut self) {
        if !self.filtered_indices.is_empty() {
            let real_idx = self.filtered_indices[self.selected];
            if self.marked.contains(&real_idx) {
                self.marked.remove(&real_idx);
            } else {
                self.marked.insert(real_idx);
            }
            self.next(); // move to next row after marking (like k9s)
        }
    }
}

// ---------------------------------------------------------------------------
// StatefulTable
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatefulTable<T: Clone> {
    pub items: Vec<T>,
    pub filtered_indices: Vec<usize>,
    pub selected: usize,
    pub offset: usize,
    pub sort_column: usize,
    pub sort_ascending: bool,
    pub page_size: usize,
    pub filter_text: String,
    /// Whether this table has ever received non-empty data from the watcher.
    /// Used to distinguish "loading" from "empty" in the UI.
    pub has_data: bool,
    /// Set of marked/selected row indices (real indices into `items`).
    pub marked: HashSet<usize>,
}

impl<T: Clone> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            filtered_indices: Vec::new(),
            selected: 0,
            offset: 0,
            sort_column: 0,
            sort_ascending: true,
            page_size: 40,
            filter_text: String::new(),
            has_data: false,
            marked: HashSet::new(),
        }
    }
}

impl<T: Clone> StatefulTable<T> {
    pub fn new() -> Self { Self::default() }

    pub fn len(&self) -> usize { self.filtered_indices.len() }
    pub fn is_empty(&self) -> bool { self.filtered_indices.is_empty() }
    pub fn total(&self) -> usize { self.items.len() }

    pub fn next(&mut self) {
        if !self.filtered_indices.is_empty() && self.selected + 1 < self.filtered_indices.len() {
            self.selected += 1;
        }
        self.adjust_offset();
    }

    pub fn previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
        self.adjust_offset();
    }

    pub fn page_up(&mut self) {
        self.selected = self.selected.saturating_sub(self.page_size);
        self.adjust_offset();
    }

    pub fn page_down(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.selected = (self.selected + self.page_size).min(self.filtered_indices.len() - 1);
        }
        self.adjust_offset();
    }

    pub fn home(&mut self) {
        self.selected = 0;
        self.offset = 0;
    }

    pub fn end(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        if !items.is_empty() {
            self.has_data = true;
        }
        self.items = items;
        self.marked.clear();
        self.filtered_indices = (0..self.items.len()).collect();
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    pub fn apply_filter<F: Fn(&T) -> bool>(&mut self, pred: F) {
        self.filtered_indices = self.items.iter().enumerate()
            .filter(|(_, item)| pred(item))
            .map(|(i, _)| i)
            .collect();
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    /// Clear all data and reset `has_data` to false.
    pub fn clear_data(&mut self) {
        self.items.clear();
        self.filtered_indices.clear();
        self.selected = 0;
        self.offset = 0;
        self.has_data = false;
        self.marked.clear();
    }

    pub fn clear_filter(&mut self) {
        self.filter_text.clear();
        self.filtered_indices = (0..self.items.len()).collect();
        if self.selected >= self.filtered_indices.len() && !self.filtered_indices.is_empty() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    pub fn selected_item(&self) -> Option<&T> {
        let idx = *self.filtered_indices.get(self.selected)?;
        self.items.get(idx)
    }

    pub fn visible_items(&self) -> Vec<&T> {
        let end = (self.offset + self.page_size).min(self.filtered_indices.len());
        if self.offset >= self.filtered_indices.len() {
            return Vec::new();
        }
        self.filtered_indices[self.offset..end]
            .iter()
            .filter_map(|&i| self.items.get(i))
            .collect()
    }

    fn adjust_offset(&mut self) {
        if self.page_size == 0 { return; }
        if self.selected < self.offset {
            self.offset = self.selected;
        }
        if self.selected >= self.offset + self.page_size {
            self.offset = self.selected - self.page_size + 1;
        }
    }
}

impl<T: Clone + KubeResource> StatefulTable<T> {
    /// Sort items by the given column index.
    /// If already sorted by this column, toggle ascending/descending.
    /// Tries numeric comparison first, falls back to lexicographic.
    pub fn sort_by_column(&mut self, col: usize) {
        if self.sort_column == col {
            self.sort_ascending = !self.sort_ascending;
        } else {
            self.sort_column = col;
            self.sort_ascending = true;
        }
        let asc = self.sort_ascending;
        self.items.sort_by(|a, b| {
            let a_row = a.row();
            let b_row = b.row();
            let a_val = a_row.get(col).map(|c| c.as_ref()).unwrap_or("");
            let b_val = b_row.get(col).map(|c| c.as_ref()).unwrap_or("");
            // Try numeric comparison first
            if let (Ok(a_num), Ok(b_num)) = (a_val.parse::<f64>(), b_val.parse::<f64>()) {
                let ord = a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal);
                return if asc { ord } else { ord.reverse() };
            }
            let ord = a_val.cmp(b_val);
            if asc { ord } else { ord.reverse() }
        });

        // Rebuild filtered indices preserving filter
        if !self.filter_text.is_empty() {
            let t = self.filter_text.to_lowercase();
            let re = regex::Regex::new(&t).ok();
            self.filtered_indices = self.items.iter().enumerate()
                .filter(|(_, item)| {
                    item.row().iter().any(|cell| {
                        let lower = cell.to_lowercase();
                        re.as_ref().map_or(lower.contains(&t), |r| r.is_match(&lower))
                    })
                })
                .map(|(i, _)| i)
                .collect();
        } else {
            self.filtered_indices = (0..self.items.len()).collect();
        }

        // Clamp selection
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.offset = 0;
        } else if self.selected >= self.filtered_indices.len() {
            self.selected = self.filtered_indices.len() - 1;
        }
        self.adjust_offset();
    }

    /// Sets items and re-applies the stored filter text if one is active.
    /// Preserves selection by resource identity (name + namespace) rather than
    /// by index, so the user's selection doesn't jump when items reorder.
    pub fn set_items_filtered(&mut self, items: Vec<T>) {
        // Guard: don't replace populated data with an empty snapshot
        // (happens during watcher re-init between Init and InitDone)
        if items.is_empty() && self.has_data && !self.items.is_empty() {
            return;
        }

        // Save current selection identity
        let prev_selection = self.selected_item().map(|item| {
            (item.name().to_string(), item.namespace().to_string())
        });

        if !items.is_empty() {
            self.has_data = true;
        }
        self.items = items;
        self.marked.clear();

        // Re-apply filter (supports regex)
        if !self.filter_text.is_empty() {
            let t = self.filter_text.to_lowercase();
            let re = regex::Regex::new(&t).ok();
            self.filtered_indices = self.items.iter().enumerate()
                .filter(|(_, item)| {
                    item.row().iter().any(|cell| {
                        let lower = cell.to_lowercase();
                        if let Some(ref re) = re {
                            re.is_match(&lower)
                        } else {
                            lower.contains(&t)
                        }
                    })
                })
                .map(|(i, _)| i)
                .collect();
        } else {
            self.filtered_indices = (0..self.items.len()).collect();
        }

        // Restore selection by identity
        if let Some((prev_name, prev_ns)) = prev_selection {
            if let Some(pos) = self.filtered_indices.iter().position(|&i| {
                self.items[i].name() == prev_name && self.items[i].namespace() == prev_ns
            }) {
                self.selected = pos;
            } else {
                // Item no longer exists, clamp
                if self.filtered_indices.is_empty() {
                    self.selected = 0;
                    self.offset = 0;
                } else if self.selected >= self.filtered_indices.len() {
                    self.selected = self.filtered_indices.len() - 1;
                }
            }
        } else {
            if self.filtered_indices.is_empty() {
                self.selected = 0;
                self.offset = 0;
            } else if self.selected >= self.filtered_indices.len() {
                self.selected = self.filtered_indices.len() - 1;
            }
        }

        self.adjust_offset();
    }
}
