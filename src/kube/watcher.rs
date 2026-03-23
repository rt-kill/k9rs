use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::{
    apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
    autoscaling::v2::HorizontalPodAutoscaler,
    batch::v1::{CronJob, Job},
    core::v1::{
        ConfigMap, Endpoints, Event, LimitRange, Namespace, Node, PersistentVolume,
        PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
    },
    networking::v1::{Ingress, NetworkPolicy},
    policy::v1::PodDisruptionBudget,
    rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
    storage::v1::StorageClass,
};
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::app::ResourceTab;
use crate::event::ResourceUpdate;
use crate::kube::resources::{
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

/// The resource types the watcher knows about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ResourceType {
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

/// Maps a UI tab to the corresponding watcher resource type.
fn resource_type_for_tab(tab: ResourceTab) -> ResourceType {
    match tab {
        ResourceTab::Pods => ResourceType::Pods,
        ResourceTab::Deployments => ResourceType::Deployments,
        ResourceTab::Services => ResourceType::Services,
        ResourceTab::StatefulSets => ResourceType::StatefulSets,
        ResourceTab::DaemonSets => ResourceType::DaemonSets,
        ResourceTab::Jobs => ResourceType::Jobs,
        ResourceTab::CronJobs => ResourceType::CronJobs,
        ResourceTab::ConfigMaps => ResourceType::ConfigMaps,
        ResourceTab::Secrets => ResourceType::Secrets,
        ResourceTab::Nodes => ResourceType::Nodes,
        ResourceTab::Namespaces => ResourceType::Namespaces,
        ResourceTab::Ingresses => ResourceType::Ingresses,
        ResourceTab::ReplicaSets => ResourceType::ReplicaSets,
        ResourceTab::Pvs => ResourceType::Pvs,
        ResourceTab::Pvcs => ResourceType::Pvcs,
        ResourceTab::StorageClasses => ResourceType::StorageClasses,
        ResourceTab::ServiceAccounts => ResourceType::ServiceAccounts,
        ResourceTab::NetworkPolicies => ResourceType::NetworkPolicies,
        ResourceTab::Events => ResourceType::Events,
        ResourceTab::Roles => ResourceType::Roles,
        ResourceTab::ClusterRoles => ResourceType::ClusterRoles,
        ResourceTab::RoleBindings => ResourceType::RoleBindings,
        ResourceTab::ClusterRoleBindings => ResourceType::ClusterRoleBindings,
        ResourceTab::Hpa => ResourceType::Hpa,
        ResourceTab::Endpoints => ResourceType::Endpoints,
        ResourceTab::LimitRanges => ResourceType::LimitRanges,
        ResourceTab::ResourceQuotas => ResourceType::ResourceQuotas,
        ResourceTab::Pdb => ResourceType::Pdb,
    }
}

/// Always-watched resource types that provide essential context.
const ALWAYS_WATCHED: &[ResourceType] = &[
    ResourceType::Namespaces,
    ResourceType::Nodes,
    ResourceType::Events,
];

/// Manages background watcher tasks for Kubernetes resource types.
///
/// Spawns tokio tasks that use `kube::runtime::watcher` to stream resource events,
/// convert them into the app's internal types, and send updates through a channel.
///
/// Only watches the always-needed types (Namespaces, Nodes, Events) plus the
/// currently active tab's resource type, to avoid unnecessary API load.
pub struct ResourceWatcher {
    client: Client,
    namespace: String,
    tx: mpsc::Sender<ResourceUpdate>,
    /// Handles for always-running watchers (Namespaces, Nodes, Events).
    core_handles: Vec<JoinHandle<()>>,
    /// Handles for the active tab's watcher(s).
    tab_handles: Vec<JoinHandle<()>>,
    /// The resource type currently being watched for the active tab.
    active_tab_type: Option<ResourceType>,
}

impl ResourceWatcher {
    /// Starts watchers for the core resource types plus the given tab.
    ///
    /// `namespace` can be `""` or `"all"` to watch across all namespaces,
    /// or a specific namespace name to scope namespace-scoped resources.
    ///
    /// `initial_tab` determines which resource type to watch first, avoiding
    /// a spurious Pods watcher when `--command` specifies a different resource.
    pub async fn start(
        client: Client,
        tx: mpsc::Sender<ResourceUpdate>,
        namespace: String,
        initial_tab: ResourceTab,
    ) -> Self {
        let mut watcher = Self {
            client: client.clone(),
            namespace: namespace.clone(),
            tx,
            core_handles: Vec::new(),
            tab_handles: Vec::new(),
            active_tab_type: None,
        };

        watcher.spawn_core_watchers();
        let initial_type = resource_type_for_tab(initial_tab);
        watcher.watch_resource_types(&[initial_type]);
        watcher.active_tab_type = Some(initial_type);
        watcher
    }

    /// Stops all current watchers and restarts them for the new namespace.
    pub async fn switch_namespace(&mut self, namespace: &str) {
        self.stop().await;
        self.namespace = namespace.to_string();
        self.spawn_core_watchers();
        // Re-watch the previously active tab type.
        if let Some(rt) = self.active_tab_type {
            self.watch_resource_types(&[rt]);
        }
    }

    /// Switch the active tab: stop old tab watchers, start new ones.
    pub async fn switch_tab(&mut self, tab: ResourceTab) {
        let new_type = resource_type_for_tab(tab);

        // If already watching this type (or it is an always-watched type), nothing to do.
        if self.active_tab_type == Some(new_type) {
            return;
        }
        if ALWAYS_WATCHED.contains(&new_type) {
            // Already covered by core watchers; just clear the tab handles.
            self.stop_tab_watchers().await;
            self.active_tab_type = Some(new_type);
            return;
        }

        self.stop_tab_watchers().await;
        self.watch_resource_types(&[new_type]);
    }

    /// Force-refresh the active tab: stop and restart even if same type.
    pub async fn switch_tab_force(&mut self, tab: ResourceTab) {
        let rt = resource_type_for_tab(tab);
        self.stop_tab_watchers().await;
        self.active_tab_type = Some(rt);
        self.watch_resource_types(&[rt]);
    }

    /// Replaces the internal client, stops all watchers, and restarts them
    /// with the new client. Used when switching Kubernetes contexts.
    pub async fn replace_client(&mut self, new_client: Client, namespace: String) {
        self.stop().await;
        self.client = new_client;
        self.namespace = namespace;
        self.spawn_core_watchers();
        if let Some(rt) = self.active_tab_type {
            self.watch_resource_types(&[rt]);
        }
    }

    /// Cancels all running watcher tasks (core + tab).
    pub async fn stop(&mut self) {
        for handle in self.core_handles.drain(..) {
            handle.abort();
        }
        self.stop_tab_watchers().await;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Spawn the always-on core watchers (Namespaces, Nodes, Events).
    fn spawn_core_watchers(&mut self) {
        let client = self.client.clone();
        let ns = self.namespace.clone();
        let tx = self.tx.clone();

        // Namespaces and Nodes are cluster-scoped.
        self.core_handles.push(tokio::spawn(Self::watch_namespaces(
            client.clone(),
            tx.clone(),
        )));
        self.core_handles.push(tokio::spawn(Self::watch_nodes(
            client.clone(),
            tx.clone(),
        )));
        // Events are namespace-scoped.
        self.core_handles.push(tokio::spawn(Self::watch_events(
            client.clone(),
            ns,
            tx.clone(),
        )));
    }

    /// Start watchers for the given resource types (used for tab switching).
    fn watch_resource_types(&mut self, types: &[ResourceType]) {
        let client = self.client.clone();
        let ns = self.namespace.clone();
        let tx = self.tx.clone();

        for rt in types {
            // Skip if already covered by core watchers.
            if ALWAYS_WATCHED.contains(rt) {
                self.active_tab_type = Some(*rt);
                continue;
            }

            let handle = match rt {
                ResourceType::Pods => {
                    tokio::spawn(Self::watch_pods(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Deployments => {
                    tokio::spawn(Self::watch_deployments(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Services => {
                    tokio::spawn(Self::watch_services(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::StatefulSets => {
                    tokio::spawn(Self::watch_stateful_sets(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::DaemonSets => {
                    tokio::spawn(Self::watch_daemon_sets(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Jobs => {
                    tokio::spawn(Self::watch_jobs(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::CronJobs => {
                    tokio::spawn(Self::watch_cron_jobs(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::ConfigMaps => {
                    tokio::spawn(Self::watch_config_maps(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Secrets => {
                    tokio::spawn(Self::watch_secrets(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Ingresses => {
                    tokio::spawn(Self::watch_ingresses(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::ReplicaSets => {
                    tokio::spawn(Self::watch_replica_sets(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Pvs => {
                    tokio::spawn(Self::watch_pvs(client.clone(), tx.clone()))
                }
                ResourceType::Pvcs => {
                    tokio::spawn(Self::watch_pvcs(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::StorageClasses => {
                    tokio::spawn(Self::watch_storage_classes(client.clone(), tx.clone()))
                }
                ResourceType::ServiceAccounts => {
                    tokio::spawn(Self::watch_service_accounts(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::NetworkPolicies => {
                    tokio::spawn(Self::watch_network_policies(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Roles => {
                    tokio::spawn(Self::watch_roles(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::ClusterRoles => {
                    tokio::spawn(Self::watch_cluster_roles(client.clone(), tx.clone()))
                }
                ResourceType::RoleBindings => {
                    tokio::spawn(Self::watch_role_bindings(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::ClusterRoleBindings => {
                    tokio::spawn(Self::watch_cluster_role_bindings(client.clone(), tx.clone()))
                }
                ResourceType::Hpa => {
                    tokio::spawn(Self::watch_hpa(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Endpoints => {
                    tokio::spawn(Self::watch_endpoints(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::LimitRanges => {
                    tokio::spawn(Self::watch_limit_ranges(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::ResourceQuotas => {
                    tokio::spawn(Self::watch_resource_quotas(client.clone(), ns.clone(), tx.clone()))
                }
                ResourceType::Pdb => {
                    tokio::spawn(Self::watch_pdb(client.clone(), ns.clone(), tx.clone()))
                }
                // Core types are handled above; listed here for exhaustiveness.
                ResourceType::Namespaces | ResourceType::Nodes | ResourceType::Events => continue,
            };

            self.tab_handles.push(handle);
            self.active_tab_type = Some(*rt);
        }
    }

    /// Stop only the tab-specific watchers (not core watchers).
    async fn stop_tab_watchers(&mut self) {
        for handle in self.tab_handles.drain(..) {
            handle.abort();
        }
    }

    // ── Cluster-scoped watchers ───────────────────────────────────────────────

    async fn watch_nodes(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Node> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Nodes(items.into_iter().map(KubeNode::from).collect())
        })
        .await;
    }

    async fn watch_namespaces(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Namespace> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Namespaces(items.into_iter().map(KubeNamespace::from).collect())
        })
        .await;
    }

    async fn watch_pvs(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<PersistentVolume> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Pvs(items.into_iter().map(KubePv::from).collect())
        })
        .await;
    }

    async fn watch_storage_classes(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<StorageClass> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::StorageClasses(
                items.into_iter().map(KubeStorageClass::from).collect(),
            )
        })
        .await;
    }

    async fn watch_cluster_roles(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<ClusterRole> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ClusterRoles(items.into_iter().map(KubeClusterRole::from).collect())
        })
        .await;
    }

    async fn watch_cluster_role_bindings(client: Client, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<ClusterRoleBinding> = Api::all(client);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ClusterRoleBindings(
                items
                    .into_iter()
                    .map(KubeClusterRoleBinding::from)
                    .collect(),
            )
        })
        .await;
    }

    // ── Namespace-scoped watchers ─────────────────────────────────────────────

    async fn watch_pods(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Pod> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Pods(items.into_iter().map(KubePod::from).collect())
        })
        .await;
    }

    async fn watch_deployments(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Deployment> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Deployments(items.into_iter().map(KubeDeployment::from).collect())
        })
        .await;
    }

    async fn watch_services(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Service> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Services(items.into_iter().map(KubeService::from).collect())
        })
        .await;
    }

    async fn watch_config_maps(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<ConfigMap> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ConfigMaps(items.into_iter().map(KubeConfigMap::from).collect())
        })
        .await;
    }

    async fn watch_secrets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Secret> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Secrets(items.into_iter().map(KubeSecret::from).collect())
        })
        .await;
    }

    async fn watch_stateful_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<StatefulSet> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::StatefulSets(items.into_iter().map(KubeStatefulSet::from).collect())
        })
        .await;
    }

    async fn watch_daemon_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<DaemonSet> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::DaemonSets(items.into_iter().map(KubeDaemonSet::from).collect())
        })
        .await;
    }

    async fn watch_jobs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Job> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Jobs(items.into_iter().map(KubeJob::from).collect())
        })
        .await;
    }

    async fn watch_cron_jobs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<CronJob> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::CronJobs(items.into_iter().map(KubeCronJob::from).collect())
        })
        .await;
    }

    async fn watch_replica_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<ReplicaSet> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ReplicaSets(items.into_iter().map(KubeReplicaSet::from).collect())
        })
        .await;
    }

    async fn watch_ingresses(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Ingress> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Ingresses(items.into_iter().map(KubeIngress::from).collect())
        })
        .await;
    }

    async fn watch_network_policies(
        client: Client,
        ns: String,
        tx: mpsc::Sender<ResourceUpdate>,
    ) {
        let api: Api<NetworkPolicy> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::NetworkPolicies(
                items.into_iter().map(KubeNetworkPolicy::from).collect(),
            )
        })
        .await;
    }

    async fn watch_service_accounts(
        client: Client,
        ns: String,
        tx: mpsc::Sender<ResourceUpdate>,
    ) {
        let api: Api<ServiceAccount> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ServiceAccounts(
                items.into_iter().map(KubeServiceAccount::from).collect(),
            )
        })
        .await;
    }

    async fn watch_pvcs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<PersistentVolumeClaim> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Pvcs(items.into_iter().map(KubePvc::from).collect())
        })
        .await;
    }

    async fn watch_events(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Event> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Events(items.into_iter().map(KubeEvent::from).collect())
        })
        .await;
    }

    async fn watch_roles(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Role> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Roles(items.into_iter().map(KubeRole::from).collect())
        })
        .await;
    }

    async fn watch_role_bindings(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<RoleBinding> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::RoleBindings(items.into_iter().map(KubeRoleBinding::from).collect())
        })
        .await;
    }

    async fn watch_hpa(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<HorizontalPodAutoscaler> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Hpa(items.into_iter().map(KubeHpa::from).collect())
        })
        .await;
    }

    async fn watch_endpoints(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<Endpoints> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Endpoints(items.into_iter().map(KubeEndpoints::from).collect())
        })
        .await;
    }

    async fn watch_limit_ranges(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<LimitRange> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::LimitRanges(items.into_iter().map(KubeLimitRange::from).collect())
        })
        .await;
    }

    async fn watch_resource_quotas(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<ResourceQuota> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::ResourceQuotas(
                items.into_iter().map(KubeResourceQuota::from).collect(),
            )
        })
        .await;
    }

    async fn watch_pdb(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>) {
        let api: Api<PodDisruptionBudget> = ns_api(&client, &ns);
        run_watcher(api, tx, |items| {
            ResourceUpdate::Pdb(items.into_iter().map(KubePdb::from).collect())
        })
        .await;
    }
}

// ── Helper: build a namespaced or cluster-wide Api ────────────────────────────

/// Returns `Api::all()` when `ns` is empty or `"all"`, otherwise `Api::namespaced()`.
fn ns_api<K>(client: &Client, ns: &str) -> Api<K>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as Resource>::DynamicType: Default,
{
    if ns.is_empty() || ns == "all" {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), ns)
    }
}

// ── Generic watcher loop with debounce ────────────────────────────────────────

/// Unique key for a Kubernetes object: (namespace, name).
type ObjKey = (String, String);

/// Extracts the `(namespace, name)` key from a Kubernetes resource.
fn obj_key<K: Resource<DynamicType = ()>>(obj: &K) -> ObjKey {
    let meta = obj.meta();
    (
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Runs a `kube::runtime::watcher` stream, maintaining a local cache of objects.
///
/// Events are debounced: after any change, a "dirty" flag is set and a background
/// timer sends at most one snapshot every 100ms, avoiding a flood of updates.
async fn run_watcher<K, F>(api: Api<K>, tx: mpsc::Sender<ResourceUpdate>, make_update: F)
where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    F: Fn(Vec<K>) -> ResourceUpdate + Send + 'static,
{
    let watcher_config = watcher::Config::default();
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjKey, K> = HashMap::new();
    let dirty = Arc::new(AtomicBool::new(false));

    // Shared channel to pass snapshots from the debounce task.
    let (snap_tx, mut snap_rx) = mpsc::channel::<Vec<K>>(1);

    // Spawn the debounce flusher: checks every 100ms, sends if dirty.
    let dirty_flag = dirty.clone();
    let debounce_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            // Signal the main loop to flush if dirty.
            if dirty_flag.swap(false, Ordering::AcqRel) {
                // Send an empty vec as a "flush" signal; the main loop
                // will use its own store to build the actual snapshot.
                // We use a oneshot-style ping here.
                if snap_tx.send(Vec::new()).await.is_err() {
                    break;
                }
            }
        }
    });

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Apply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, obj);
                            }
                            WatcherEvent::Delete(obj) => {
                                let key = obj_key(&obj);
                                store.remove(&key);
                            }
                            WatcherEvent::Init => {
                                store.clear();
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, obj);
                            }
                            WatcherEvent::InitDone => {
                                debug!("initial list complete, {} items", store.len());
                            }
                        }
                        // Mark dirty; the debounce task will trigger a flush.
                        dirty.store(true, Ordering::Release);
                    }
                    Ok(None) => {
                        debug!("watcher stream ended");
                        break;
                    }
                    Err(e) => {
                        warn!("watcher error: {}", e);
                    }
                }
            }
            _ = snap_rx.recv() => {
                // Debounce timer fired and store is dirty — send a snapshot.
                let mut items: Vec<K> = store.values().cloned().collect();
                items.sort_by(|a, b| {
                    let a_ns = a.meta().namespace.as_deref().unwrap_or("");
                    let b_ns = b.meta().namespace.as_deref().unwrap_or("");
                    let a_name = a.meta().name.as_deref().unwrap_or("");
                    let b_name = b.meta().name.as_deref().unwrap_or("");
                    (a_ns, a_name).cmp(&(b_ns, b_name))
                });
                let update = make_update(items);
                if tx.send(update).await.is_err() {
                    debug!("channel closed, stopping watcher");
                    break;
                }
            }
        }
    }

    debounce_handle.abort();
}
