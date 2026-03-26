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
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::app::ResourceTab;
use crate::event::ResourceUpdate;
use crate::kube::resources::{
    configmaps::KubeConfigMap,
    crds::{KubeCrd, DynamicKubeResource},
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
    Crds,
    DynamicResource,
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
        ResourceTab::Crds => ResourceType::Crds,
        ResourceTab::DynamicResource => ResourceType::DynamicResource,
    }
}

/// Always-watched resource types that provide essential context.
/// Events excluded — can be huge on active clusters, lazy-loaded on tab switch.
const ALWAYS_WATCHED: &[ResourceType] = &[
    ResourceType::Namespaces,
    ResourceType::Nodes,
];

/// Manages background watcher tasks for Kubernetes resource types.
///
/// Spawns tokio tasks that use `kube::runtime::watcher` to stream resource events,
/// convert them into the app's internal types, and send updates through a channel.
///
/// Only watches the always-needed types (Namespaces, Nodes) plus the
/// currently active tab's resource type, to avoid unnecessary API load.
pub struct ResourceWatcher {
    client: Client,
    namespace: String,
    tx: mpsc::Sender<ResourceUpdate>,
    /// Handles for always-running watchers (Namespaces, Nodes).
    core_handles: Vec<JoinHandle<()>>,
    /// Handles for the active tab's watcher(s).
    tab_handles: Vec<JoinHandle<()>>,
    /// The resource type currently being watched for the active tab.
    active_tab_type: Option<ResourceType>,
    /// When true, watchers skip incremental flushing during InitApply
    /// and wait for InitDone to send the complete snapshot at once.
    pub full_fetch: Arc<AtomicBool>,
    /// The GVK for dynamic CRD instance browsing, if any.
    dynamic_gvk: Option<GroupVersionKind>,
    /// The plural resource name from the CRD spec (e.g. "certificates").
    dynamic_plural: String,
    /// The scope of the dynamic CRD ("Namespaced" or "Cluster").
    dynamic_scope: String,
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
            full_fetch: Arc::new(AtomicBool::new(false)),
            dynamic_gvk: None,
            dynamic_plural: String::new(),
            dynamic_scope: String::new(),
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
            if rt == ResourceType::DynamicResource {
                // Dynamic resources need special handling — reconstruct from stored GVK.
                if let Some(ref gvk) = self.dynamic_gvk.clone() {
                    let plural = self.dynamic_plural.clone();
                    let scope = self.dynamic_scope.clone();
                    let client = self.client.clone();
                    let ns = self.namespace.clone();
                    let tx = self.tx.clone();
                    let handle = tokio::spawn(Self::watch_dynamic_resource(
                        client, ns, tx, gvk.clone(), plural, scope,
                    ));
                    self.tab_handles.push(handle);
                }
            } else {
                self.watch_resource_types(&[rt]);
            }
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

    /// Start watching a dynamic CRD resource type.
    pub async fn watch_dynamic(&mut self, group: String, version: String, kind: String, plural: String, scope: String) {
        self.stop_tab_watchers().await;
        let gvk = GroupVersionKind::gvk(&group, &version, &kind);
        self.dynamic_gvk = Some(gvk.clone());
        self.dynamic_plural = plural.clone();
        self.dynamic_scope = scope.clone();
        self.active_tab_type = Some(ResourceType::DynamicResource);

        let client = self.client.clone();
        let ns = self.namespace.clone();
        let tx = self.tx.clone();
        let handle = tokio::spawn(Self::watch_dynamic_resource(client, ns, tx, gvk, plural, scope));
        self.tab_handles.push(handle);
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

    /// Spawn the always-on core watchers (Namespaces, Nodes).
    fn spawn_core_watchers(&mut self) {
        let client = self.client.clone();
        let tx = self.tx.clone();

        // Namespaces and Nodes are cluster-scoped.
        self.core_handles.push(tokio::spawn(Self::watch_namespaces(
            client.clone(),
            tx.clone(),
            self.full_fetch.clone(),
        )));
        self.core_handles.push(tokio::spawn(Self::watch_nodes(
            client.clone(),
            tx.clone(),
            self.full_fetch.clone(),
        )));
        // Events are NOT always-watched — they can be huge on active clusters.
        // They are lazy-loaded when the user switches to the Events tab.
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
                    tokio::spawn(Self::watch_pods(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Deployments => {
                    tokio::spawn(Self::watch_deployments(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Services => {
                    tokio::spawn(Self::watch_services(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::StatefulSets => {
                    tokio::spawn(Self::watch_stateful_sets(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::DaemonSets => {
                    tokio::spawn(Self::watch_daemon_sets(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Jobs => {
                    tokio::spawn(Self::watch_jobs(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::CronJobs => {
                    tokio::spawn(Self::watch_cron_jobs(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ConfigMaps => {
                    tokio::spawn(Self::watch_config_maps(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Secrets => {
                    tokio::spawn(Self::watch_secrets(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Ingresses => {
                    tokio::spawn(Self::watch_ingresses(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ReplicaSets => {
                    tokio::spawn(Self::watch_replica_sets(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Pvs => {
                    tokio::spawn(Self::watch_pvs(client.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Pvcs => {
                    tokio::spawn(Self::watch_pvcs(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::StorageClasses => {
                    tokio::spawn(Self::watch_storage_classes(client.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ServiceAccounts => {
                    tokio::spawn(Self::watch_service_accounts(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::NetworkPolicies => {
                    tokio::spawn(Self::watch_network_policies(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Roles => {
                    tokio::spawn(Self::watch_roles(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ClusterRoles => {
                    tokio::spawn(Self::watch_cluster_roles(client.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::RoleBindings => {
                    tokio::spawn(Self::watch_role_bindings(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ClusterRoleBindings => {
                    tokio::spawn(Self::watch_cluster_role_bindings(client.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Hpa => {
                    tokio::spawn(Self::watch_hpa(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Endpoints => {
                    tokio::spawn(Self::watch_endpoints(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::LimitRanges => {
                    tokio::spawn(Self::watch_limit_ranges(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::ResourceQuotas => {
                    tokio::spawn(Self::watch_resource_quotas(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Pdb => {
                    tokio::spawn(Self::watch_pdb(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Crds => {
                    tokio::spawn(Self::watch_crds(client.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::Events => {
                    tokio::spawn(Self::watch_events(client.clone(), ns.clone(), tx.clone(), self.full_fetch.clone()))
                }
                ResourceType::DynamicResource => {
                    // Dynamic resource requires a GVK; if not set, skip.
                    if let Some(ref gvk) = self.dynamic_gvk {
                        tokio::spawn(Self::watch_dynamic_resource(
                            client.clone(), ns.clone(), tx.clone(),
                            gvk.clone(), self.dynamic_plural.clone(), self.dynamic_scope.clone(),
                        ))
                    } else {
                        continue;
                    }
                }
                // Core types are handled by spawn_core_watchers; skip here.
                ResourceType::Namespaces | ResourceType::Nodes => continue,
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

    async fn watch_nodes(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Node> = Api::all(client);
        run_watcher(api, tx, KubeNode::from, |items| ResourceUpdate::Nodes(items), full_fetch)
        .await;
    }

    async fn watch_namespaces(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Namespace> = Api::all(client);
        run_watcher(api, tx, KubeNamespace::from, |items| ResourceUpdate::Namespaces(items), full_fetch)
        .await;
    }

    async fn watch_pvs(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<PersistentVolume> = Api::all(client);
        run_watcher(api, tx, KubePv::from, |items| ResourceUpdate::Pvs(items), full_fetch)
        .await;
    }

    async fn watch_storage_classes(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<StorageClass> = Api::all(client);
        run_watcher(api, tx, KubeStorageClass::from, |items| ResourceUpdate::StorageClasses(items), full_fetch)
        .await;
    }

    async fn watch_cluster_roles(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<ClusterRole> = Api::all(client);
        run_watcher(api, tx, KubeClusterRole::from, |items| ResourceUpdate::ClusterRoles(items), full_fetch)
        .await;
    }

    async fn watch_cluster_role_bindings(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<ClusterRoleBinding> = Api::all(client);
        run_watcher(api, tx, KubeClusterRoleBinding::from, |items| ResourceUpdate::ClusterRoleBindings(items), full_fetch)
        .await;
    }

    // ── Namespace-scoped watchers ─────────────────────────────────────────────

    async fn watch_pods(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Pod> = ns_api(&client, &ns);
        run_watcher(api, tx, KubePod::from, |items| ResourceUpdate::Pods(items), full_fetch)
        .await;
    }

    async fn watch_deployments(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Deployment> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeDeployment::from, |items| ResourceUpdate::Deployments(items), full_fetch)
        .await;
    }

    async fn watch_services(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Service> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeService::from, |items| ResourceUpdate::Services(items), full_fetch)
        .await;
    }

    async fn watch_config_maps(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<ConfigMap> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeConfigMap::from, |items| ResourceUpdate::ConfigMaps(items), full_fetch)
        .await;
    }

    async fn watch_secrets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Secret> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeSecret::from, |items| ResourceUpdate::Secrets(items), full_fetch)
        .await;
    }

    async fn watch_stateful_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<StatefulSet> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeStatefulSet::from, |items| ResourceUpdate::StatefulSets(items), full_fetch)
        .await;
    }

    async fn watch_daemon_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<DaemonSet> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeDaemonSet::from, |items| ResourceUpdate::DaemonSets(items), full_fetch)
        .await;
    }

    async fn watch_jobs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Job> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeJob::from, |items| ResourceUpdate::Jobs(items), full_fetch)
        .await;
    }

    async fn watch_cron_jobs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<CronJob> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeCronJob::from, |items| ResourceUpdate::CronJobs(items), full_fetch)
        .await;
    }

    async fn watch_replica_sets(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<ReplicaSet> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeReplicaSet::from, |items| ResourceUpdate::ReplicaSets(items), full_fetch)
        .await;
    }

    async fn watch_ingresses(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Ingress> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeIngress::from, |items| ResourceUpdate::Ingresses(items), full_fetch)
        .await;
    }

    async fn watch_network_policies(
        client: Client,
        ns: String,
        tx: mpsc::Sender<ResourceUpdate>,
        full_fetch: Arc<AtomicBool>,
    ) {
        let api: Api<NetworkPolicy> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeNetworkPolicy::from, |items| ResourceUpdate::NetworkPolicies(items), full_fetch)
        .await;
    }

    async fn watch_service_accounts(
        client: Client,
        ns: String,
        tx: mpsc::Sender<ResourceUpdate>,
        full_fetch: Arc<AtomicBool>,
    ) {
        let api: Api<ServiceAccount> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeServiceAccount::from, |items| ResourceUpdate::ServiceAccounts(items), full_fetch)
        .await;
    }

    async fn watch_pvcs(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<PersistentVolumeClaim> = ns_api(&client, &ns);
        run_watcher(api, tx, KubePvc::from, |items| ResourceUpdate::Pvcs(items), full_fetch)
        .await;
    }

    async fn watch_events(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Event> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeEvent::from, |items| ResourceUpdate::Events(items), full_fetch)
        .await;
    }

    async fn watch_roles(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Role> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeRole::from, |items| ResourceUpdate::Roles(items), full_fetch)
        .await;
    }

    async fn watch_role_bindings(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<RoleBinding> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeRoleBinding::from, |items| ResourceUpdate::RoleBindings(items), full_fetch)
        .await;
    }

    async fn watch_hpa(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<HorizontalPodAutoscaler> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeHpa::from, |items| ResourceUpdate::Hpa(items), full_fetch)
        .await;
    }

    async fn watch_endpoints(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<Endpoints> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeEndpoints::from, |items| ResourceUpdate::Endpoints(items), full_fetch)
        .await;
    }

    async fn watch_limit_ranges(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<LimitRange> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeLimitRange::from, |items| ResourceUpdate::LimitRanges(items), full_fetch)
        .await;
    }

    async fn watch_resource_quotas(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<ResourceQuota> = ns_api(&client, &ns);
        run_watcher(api, tx, KubeResourceQuota::from, |items| ResourceUpdate::ResourceQuotas(items), full_fetch)
        .await;
    }

    async fn watch_pdb(client: Client, ns: String, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<PodDisruptionBudget> = ns_api(&client, &ns);
        run_watcher(api, tx, KubePdb::from, |items| ResourceUpdate::Pdb(items), full_fetch)
        .await;
    }

    async fn watch_crds(client: Client, tx: mpsc::Sender<ResourceUpdate>, full_fetch: Arc<AtomicBool>) {
        let api: Api<CustomResourceDefinition> = Api::all(client);
        run_watcher(api, tx, |crd: CustomResourceDefinition| {
            let meta = crd.metadata;
            let spec = crd.spec;
            let name = meta.name.unwrap_or_default();
            let group = spec.group;
            let version = spec.versions.first().map(|v| v.name.clone()).unwrap_or_default();
            let kind = spec.names.kind;
            let plural = spec.names.plural;
            let scope = format!("{:?}", spec.scope).trim_matches('"').to_string();
            let age = meta.creation_timestamp.map(|ts| ts.0);
            KubeCrd { name, group, version, kind, plural, scope, age }
        }, |items| ResourceUpdate::Crds(items), full_fetch)
        .await;
    }

    async fn watch_dynamic_resource(
        client: Client,
        ns: String,
        tx: mpsc::Sender<ResourceUpdate>,
        gvk: GroupVersionKind,
        plural: String,
        scope: String,
    ) {
        // Use from_gvk_with_plural to get the correct API endpoint.
        // from_gvk() guesses the plural (kind + "s") which is wrong for many CRDs.
        let ar = if plural.is_empty() {
            ApiResource::from_gvk(&gvk)
        } else {
            ApiResource::from_gvk_with_plural(&gvk, &plural)
        };
        let api: Api<DynamicObject> = if scope == "Namespaced" {
            if ns.is_empty() || ns == "all" {
                Api::all_with(client, &ar)
            } else {
                Api::namespaced_with(client, &ns, &ar)
            }
        } else {
            Api::all_with(client, &ar)
        };

        run_dynamic_watcher(api, tx).await;
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
async fn run_watcher<K, T, C, F>(
    api: Api<K>,
    tx: mpsc::Sender<ResourceUpdate>,
    convert: C,
    make_update: F,
    full_fetch: Arc<AtomicBool>,
)
where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    T: Clone + Send + 'static + crate::kube::resources::KubeResource,
    C: Fn(K) -> T + Send + 'static,
    F: Fn(Vec<T>) -> ResourceUpdate + Send + 'static,
{
    let watcher_config = watcher::Config::default();
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    // Store converted (lightweight) types instead of raw k8s objects.
    let mut store: HashMap<ObjKey, T> = HashMap::new();
    let mut init_apply_count: usize = 0;
    let mut next_flush_at: usize = 50;

    // Channel for sending snapshots — both the debounce timer and immediate
    // flushes use this to signal the snapshot builder below.
    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    // Debounce timer: only used during InitApply bursts (large initial lists).
    // For Apply/Delete/InitDone, we send immediately — no reason to delay.
    let debounce_dirty = Arc::new(AtomicBool::new(false));
    let dirty_flag = debounce_dirty.clone();
    let debounce_snap_tx = snap_tx.clone();
    let debounce_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            if dirty_flag.swap(false, Ordering::AcqRel) {
                if debounce_snap_tx.send(()).await.is_err() {
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
                            WatcherEvent::Init => {
                                store.clear();
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                // Batch InitApply via debounce — large lists would
                                // flood the UI with partial snapshots otherwise.
                                if !full_fetch.load(Ordering::Acquire) {
                                    init_apply_count += 1;
                                    if init_apply_count >= next_flush_at {
                                        debounce_dirty.store(true, Ordering::Release);
                                        next_flush_at += 100;
                                    }
                                }
                            }
                            WatcherEvent::InitDone => {
                                debug!("initial list complete, {} items", store.len());
                                init_apply_count = 0;
                                next_flush_at = 50;
                                // Send snapshot IMMEDIATELY — no debounce delay
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                // Immediate flush for live updates
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                let key = obj_key(&obj);
                                store.remove(&key);
                                let _ = snap_tx.try_send(());
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("watcher stream ended");
                        break;
                    }
                    Err(e) => {
                        warn!("watcher error: {}", e);
                        // Back off to prevent tight error loops when the API
                        // keeps returning errors (auth expired, network flap).
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
            _ = snap_rx.recv() => {
                // Debounce timer fired and store is dirty — send a snapshot.
                // Items are already converted to lightweight types at insert time,
                // so this clone is cheap (~400 bytes per item, not ~8KB).
                let mut items: Vec<T> = store.values().cloned().collect();
                // Sort by namespace+name for stable, deterministic ordering.
                // Without this, HashMap iteration order is random and the
                // table rows would jump around on every update.
                items.sort_by(|a, b| {
                    let key_a = (a.namespace(), a.name());
                    let key_b = (b.namespace(), b.name());
                    key_a.cmp(&key_b)
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

/// Runs a watcher for DynamicObject, converting results to DynamicKubeResource.
async fn run_dynamic_watcher(
    api: Api<DynamicObject>,
    tx: mpsc::Sender<ResourceUpdate>,
) {
    use std::collections::BTreeMap;

    let watcher_config = watcher::Config::default();
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjKey, DynamicObject> = HashMap::new();
    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    // No debounce for dynamic watcher — lists are typically small.
    // Send snapshot immediately on any change.

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                store.clear();
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                            }
                            WatcherEvent::InitDone => {
                                debug!("dynamic watcher initial list complete, {} items", store.len());
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                let key = dyn_obj_key(&obj);
                                store.remove(&key);
                                let _ = snap_tx.try_send(());
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("dynamic watcher stream ended");
                        break;
                    }
                    Err(e) => {
                        warn!("dynamic watcher error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
            _ = snap_rx.recv() => {
                let mut items: Vec<DynamicKubeResource> = store.values()
                    .map(|obj| {
                        let meta = &obj.metadata;
                        let namespace = meta.namespace.clone().unwrap_or_default();
                        let name = meta.name.clone().unwrap_or_default();
                        let age = meta.creation_timestamp.as_ref().map(|ts| ts.0);
                        let mut data = BTreeMap::new();

                        // Extract some commonly useful fields from the dynamic data
                        if let Some(status) = obj.data.get("status") {
                            if let Some(phase) = status.get("phase") {
                                if let Some(s) = phase.as_str() {
                                    data.insert("status".to_string(), s.to_string());
                                }
                            }
                        }

                        DynamicKubeResource {
                            namespace,
                            name,
                            data,
                            age,
                        }
                    })
                    .collect();
                items.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
                let update = ResourceUpdate::DynamicResources(items);
                if tx.send(update).await.is_err() {
                    debug!("channel closed, stopping dynamic watcher");
                    break;
                }
            }
        }
    }
}

/// Extracts the `(namespace, name)` key from a DynamicObject.
fn dyn_obj_key(obj: &DynamicObject) -> ObjKey {
    let meta = &obj.metadata;
    (
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}
