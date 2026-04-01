//! Live query system for watching Kubernetes resources.
//!
//! A `LiveQuery` represents a running watcher for a specific resource type.
//! It wraps the kube watch stream and produces typed `ResourceUpdate` values
//! via a `tokio::sync::watch` channel.
//!
//! `WatcherCache` manages a per-process cache of live queries using `Weak`
//! references so watchers die naturally when no `Subscription` holds them.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};

use dashmap::DashMap;

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
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::event::ResourceUpdate;
use crate::kube::protocol::ResourceScope;
use crate::kube::resources::{
    configmaps::KubeConfigMap,
    crds::{DynamicKubeResource, KubeCrd},
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
    KubeResource,
};

// ---------------------------------------------------------------------------
// QueryKey
// ---------------------------------------------------------------------------

/// Key for looking up a live query in the cache.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QueryKey {
    pub context: String,
    pub namespace: String,
    pub resource_type: String,
}

// ---------------------------------------------------------------------------
// LiveQuery
// ---------------------------------------------------------------------------

/// A running watcher that produces typed `ResourceUpdate` snapshots.
/// When all `Subscription` handles are dropped and the `Arc` strong count
/// reaches zero, the watcher task is aborted via the `Drop` impl.
pub struct LiveQuery {
    /// Latest typed snapshot.
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
    /// The watcher task handle — aborted on drop.
    task: JoinHandle<()>,
    /// What this query watches.
    pub key: QueryKey,
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// What callers hold. Clone-able. Provides access to the latest snapshot.
/// The watcher stays alive as long as any Subscription exists (Arc refcount > 0).
#[derive(Clone)]
pub struct Subscription {
    /// The query key identifying which resource stream this subscription is for.
    pub key: QueryKey,
    /// Receive typed snapshot updates.
    pub snapshot_rx: watch::Receiver<Option<ResourceUpdate>>,
    /// Prevents the LiveQuery from being dropped.
    _keepalive: Arc<LiveQuery>,
}

/// How long a watcher stays alive after the last subscriber drops.
const GRACE_PERIOD_SECS: u64 = 300;
/// Page size for the initial LIST request. Each page is a single HTTP response;
/// too large and the connection drops mid-transfer, too small and continue tokens
/// expire before all pages are fetched. 1000 items ≈ 3MB per page.
const WATCHER_PAGE_SIZE: u32 = 1000;
/// Interval for flushing intermediate snapshots during the initial list.
const INIT_FLUSH_INTERVAL_MS: u64 = 200;
/// Sleep duration between watcher retries on error (seconds).
const RETRY_SLEEP_SECS: u64 = 1;

impl Drop for Subscription {
    fn drop(&mut self) {
        // Keep the watcher alive for a grace period after this subscriber drops.
        // Arc handles refcounting — if other subscribers or grace tasks still hold
        // clones, the watcher stays alive beyond this task's sleep. The watcher only
        // dies when ALL strong refs (subscribers + grace tasks) are gone.
        let arc = self._keepalive.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
            drop(arc);
        });
    }
}

impl Subscription {
    /// Get the current snapshot (None if no data yet).
    pub fn current(&mut self) -> Option<ResourceUpdate> {
        self.snapshot_rx.borrow_and_update().clone()
    }

    /// Wait for the next snapshot update.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }
}

// ---------------------------------------------------------------------------
// WatcherCache
// ---------------------------------------------------------------------------

/// Per-process cache of live queries. Stores `Weak` references so watchers
/// die naturally when no one holds a `Subscription`.
pub struct WatcherCache {
    entries: DashMap<QueryKey, Weak<LiveQuery>>,
}

impl WatcherCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Subscribe to a resource type. Returns a `Subscription` handle.
    /// If a watcher is already running (`Weak` upgrades), reuses it.
    /// Otherwise starts a new watcher.
    /// Try to reuse an existing watcher from the cache. Returns `None` if
    /// no live watcher exists for this key (Weak is dead or absent).
    /// Does NOT create a new watcher — use `subscribe` for that.
    pub fn try_get(&self, key: &QueryKey) -> Option<Subscription> {
        let weak = self.entries.get(key)?;
        let arc = weak.upgrade()?;
        tracing::info!("WatcherCache: reusing existing watcher for {:?}", key);
        Some(Subscription {
            key: key.clone(),
            snapshot_rx: arc.snapshot_tx.subscribe(),
            _keepalive: arc,
        })
    }

    /// Subscribe to a resource type. Reuses an existing watcher if the Weak
    /// upgrades, otherwise creates a new one. Uses `DashMap::entry()` for
    /// atomic check-and-insert so two concurrent callers don't create
    /// duplicate watchers.
    pub fn subscribe(
        &self,
        key: QueryKey,
        client: &Client,
    ) -> Subscription {
        // Atomic check-and-insert via DashMap entry API.
        use dashmap::mapref::entry::Entry;

        // Fast path: try existing entry.
        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                tracing::info!("WatcherCache: reusing existing watcher for {:?}", key);
                return Subscription {
                    key,
                    snapshot_rx: arc.snapshot_tx.subscribe(),
                    _keepalive: arc,
                };
            }
        }

        // Slow path: create new watcher. Use entry() for atomicity.
        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                // Another thread might have inserted between our get() and entry().
                if let Some(arc) = e.get().upgrade() {
                    tracing::info!("WatcherCache: reusing watcher (race winner) for {:?}", key);
                    return Subscription {
                        key,
                        snapshot_rx: arc.snapshot_tx.subscribe(),
                        _keepalive: arc,
                    };
                }
                // Dead Weak — replace with new watcher.
                tracing::info!("WatcherCache: creating new watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_watcher(&key, client);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
            Entry::Vacant(e) => {
                tracing::info!("WatcherCache: creating new watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_watcher(&key, client);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
        }
    }

    /// Internal: spawn a watcher task and return the LiveQuery + initial receiver.
    fn create_watcher(key: &QueryKey, client: &Client) -> (Arc<LiveQuery>, watch::Receiver<Option<ResourceUpdate>>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let task_client = client.clone();
        let task_key = key.clone();
        let task_tx = snapshot_tx.clone();
        let task = tokio::spawn(async move {
            run_live_watcher(task_client, task_key, task_tx).await;
        });
        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
        });
        (live_query, snapshot_rx)
    }

    /// Remove the cache entry for a key. Used by `handle_refresh` for dynamic
    /// resources so the next `subscribe_dynamic` creates a fresh watcher.
    pub fn remove(&self, key: &QueryKey) {
        self.entries.remove(key);
    }

    /// Force-subscribe: removes any existing Weak entry for the key and creates
    /// a new watcher unconditionally. The old watcher's grace task still holds
    /// its Arc, so it will live out its grace period — that's fine. The cache
    /// now points to the new watcher.
    pub fn subscribe_force(
        &self,
        key: QueryKey,
        client: &Client,
    ) -> Subscription {
        // Remove the existing (possibly still-alive) Weak entry so we
        // unconditionally create a fresh watcher below.
        self.entries.remove(&key);

        // Start a new watcher (same logic as subscribe but without reuse).
        let (snapshot_tx, snapshot_rx) = watch::channel(None);

        let task_client = client.clone();
        let task_key = key.clone();
        let task_tx = snapshot_tx.clone();

        let task = tokio::spawn(async move {
            run_live_watcher(task_client, task_key, task_tx).await;
        });

        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
        });

        self.entries.insert(key.clone(), Arc::downgrade(&live_query));

        Subscription {
            key,
            snapshot_rx,
            _keepalive: live_query,
        }
    }
}

// ---------------------------------------------------------------------------
// run_live_watcher — dispatches on resource_type string
// ---------------------------------------------------------------------------

/// Generates a match that dispatches each resource type string to a typed
/// `Api<K>` + `run_typed_watcher` call, then returns from the enclosing function.
///
/// - `ns`: namespaced resources — uses `ns_api` (handles "all" namespaces).
/// - `cluster`: cluster-scoped resources — uses `Api::all`.
///
/// Each entry is `pattern => K8sType, DomainType, ResourceUpdate::Variant`.
/// Patterns can include alternatives separated by `|`.
macro_rules! watch_resource {
    ($rt:expr, $client:expr, $ns:expr, $snapshot_tx:expr,
     $(ns: $($np:pat_param)|+ => $nk:ty, $nd:ty, $nv:path),+,
     $(cluster: $($cp:pat_param)|+ => $ck:ty, $cd:ty, $cv:path),+ $(,)?
    ) => {
        match $rt {
            $(
                $($np)|+ => {
                    let api: Api<$nk> = ns_api(&$client, &$ns);
                    run_typed_watcher(api, $snapshot_tx, <$nd>::from, $nv, $rt).await;
                    return;
                }
            )+
            $(
                $($cp)|+ => {
                    let api: Api<$ck> = Api::all($client);
                    run_typed_watcher(api, $snapshot_tx, <$cd>::from, $cv, $rt).await;
                    return;
                }
            )+
            _ => {}
        }
    };
}

/// Dispatches to a typed watcher based on the `resource_type` in the query key.
async fn run_live_watcher(
    client: Client,
    key: QueryKey,
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
) {
    let ns = &key.namespace;
    let rt = key.resource_type.as_str();

    // CRDs have a custom conversion closure and can't use the simple From-based
    // macro, so handle them separately.
    if rt == "customresourcedefinitions" || rt == "crds" {
        let api: Api<CustomResourceDefinition> = Api::all(client);
        run_typed_watcher(
            api,
            snapshot_tx,
            |crd: CustomResourceDefinition| {
                let meta = crd.metadata;
                let spec = crd.spec;
                let name = meta.name.unwrap_or_default();
                let group = spec.group;
                let version = spec
                    .versions
                    .first()
                    .map(|v| v.name.clone())
                    .unwrap_or_default();
                let kind = spec.names.kind;
                let plural = spec.names.plural;
                let scope = format!("{:?}", spec.scope).trim_matches('"').to_string();
                let age = meta.creation_timestamp.map(|ts| ts.0);
                KubeCrd {
                    name,
                    group,
                    version,
                    kind,
                    plural,
                    scope,
                    age,
                }
            },
            ResourceUpdate::Crds,
            rt,
        )
        .await;
        return;
    }

    // Each arm dispatches to run_typed_watcher and returns.
    watch_resource!(rt, client, ns, snapshot_tx,
        // Namespaced resources
        ns: "pods"                                  => Pod, KubePod, ResourceUpdate::Pods,
        ns: "deployments"                           => Deployment, KubeDeployment, ResourceUpdate::Deployments,
        ns: "services"                              => Service, KubeService, ResourceUpdate::Services,
        ns: "statefulsets"                          => StatefulSet, KubeStatefulSet, ResourceUpdate::StatefulSets,
        ns: "daemonsets"                            => DaemonSet, KubeDaemonSet, ResourceUpdate::DaemonSets,
        ns: "jobs"                                  => Job, KubeJob, ResourceUpdate::Jobs,
        ns: "cronjobs"                              => CronJob, KubeCronJob, ResourceUpdate::CronJobs,
        ns: "configmaps"                            => ConfigMap, KubeConfigMap, ResourceUpdate::ConfigMaps,
        ns: "secrets"                               => Secret, KubeSecret, ResourceUpdate::Secrets,
        ns: "ingresses"                             => Ingress, KubeIngress, ResourceUpdate::Ingresses,
        ns: "replicasets"                           => ReplicaSet, KubeReplicaSet, ResourceUpdate::ReplicaSets,
        ns: "persistentvolumeclaims" | "pvcs"       => PersistentVolumeClaim, KubePvc, ResourceUpdate::Pvcs,
        ns: "serviceaccounts"                       => ServiceAccount, KubeServiceAccount, ResourceUpdate::ServiceAccounts,
        ns: "networkpolicies"                       => NetworkPolicy, KubeNetworkPolicy, ResourceUpdate::NetworkPolicies,
        ns: "events"                                => Event, KubeEvent, ResourceUpdate::Events,
        ns: "roles"                                 => Role, KubeRole, ResourceUpdate::Roles,
        ns: "rolebindings"                          => RoleBinding, KubeRoleBinding, ResourceUpdate::RoleBindings,
        ns: "horizontalpodautoscalers" | "hpa"      => HorizontalPodAutoscaler, KubeHpa, ResourceUpdate::Hpa,
        ns: "endpoints"                             => Endpoints, KubeEndpoints, ResourceUpdate::Endpoints,
        ns: "limitranges"                           => LimitRange, KubeLimitRange, ResourceUpdate::LimitRanges,
        ns: "resourcequotas"                        => ResourceQuota, KubeResourceQuota, ResourceUpdate::ResourceQuotas,
        ns: "poddisruptionbudgets" | "pdb"          => PodDisruptionBudget, KubePdb, ResourceUpdate::Pdb,
        // Cluster-scoped resources
        cluster: "nodes"                            => Node, KubeNode, ResourceUpdate::Nodes,
        cluster: "namespaces"                       => Namespace, KubeNamespace, ResourceUpdate::Namespaces,
        cluster: "persistentvolumes" | "pvs"        => PersistentVolume, KubePv, ResourceUpdate::Pvs,
        cluster: "storageclasses"                   => StorageClass, KubeStorageClass, ResourceUpdate::StorageClasses,
        cluster: "clusterroles"                     => ClusterRole, KubeClusterRole, ResourceUpdate::ClusterRoles,
        cluster: "clusterrolebindings"              => ClusterRoleBinding, KubeClusterRoleBinding, ResourceUpdate::ClusterRoleBindings,
    );

    // Only reached if no arm matched.
    warn!("live_query: unknown resource type '{}', ignoring", rt);
}

// ---------------------------------------------------------------------------
// Dynamic CRD instance watcher
// ---------------------------------------------------------------------------

impl WatcherCache {
    /// Subscribe to a dynamic CRD resource type. Reuses an existing watcher if
    /// the Weak upgrades, otherwise creates a new one. Uses `DashMap::entry()`
    /// for atomic check-and-insert so two concurrent callers don't create
    /// duplicate watchers.
    pub fn subscribe_dynamic(
        &self,
        key: QueryKey,
        client: &Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
    ) -> Subscription {
        use dashmap::mapref::entry::Entry;

        // Fast path: try existing entry.
        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                tracing::info!("WatcherCache: reusing existing dynamic watcher for {:?}", key);
                return Subscription {
                    key,
                    snapshot_rx: arc.snapshot_tx.subscribe(),
                    _keepalive: arc,
                };
            }
        }

        // Slow path: create new watcher. Use entry() for atomicity.
        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    tracing::info!("WatcherCache: reusing dynamic watcher (race winner) for {:?}", key);
                    return Subscription {
                        key,
                        snapshot_rx: arc.snapshot_tx.subscribe(),
                        _keepalive: arc,
                    };
                }
                // Dead Weak — replace with new dynamic watcher.
                tracing::info!("WatcherCache: creating new dynamic watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_dynamic_watcher(&key, client, gvk, plural, scope);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
            Entry::Vacant(e) => {
                tracing::info!("WatcherCache: creating new dynamic watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_dynamic_watcher(&key, client, gvk, plural, scope);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
        }
    }

    /// Internal: spawn a dynamic watcher task and return the LiveQuery + initial receiver.
    fn create_dynamic_watcher(
        key: &QueryKey,
        client: &Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
    ) -> (Arc<LiveQuery>, watch::Receiver<Option<ResourceUpdate>>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let task_client = client.clone();
        let task_ns = key.namespace.clone();
        let task_tx = snapshot_tx.clone();
        let task = tokio::spawn(async move {
            run_dynamic_live_watcher(task_client, task_ns, task_tx, gvk, plural, scope).await;
        });
        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
        });
        (live_query, snapshot_rx)
    }
}

/// Watcher loop for dynamic CRD instances (DynamicObject -> DynamicKubeResource).
async fn run_dynamic_live_watcher(
    client: Client,
    ns: String,
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
    gvk: GroupVersionKind,
    plural: String,
    scope: ResourceScope,
) {
    let ar = if plural.is_empty() {
        ApiResource::from_gvk(&gvk)
    } else {
        ApiResource::from_gvk_with_plural(&gvk, &plural)
    };
    let api: Api<DynamicObject> = if scope == ResourceScope::Namespaced {
        if ns.is_empty() || ns == "all" {
            Api::all_with(client, &ar)
        } else {
            Api::namespaced_with(client, &ns, &ar)
        }
    } else {
        Api::all_with(client, &ar)
    };

    let watcher_config = watcher::Config::default()
        .page_size(WATCHER_PAGE_SIZE)
        .any_semantic();      // serve from API server cache (resourceVersion=0), much faster
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    type ObjKey = (String, String);
    let mut store: HashMap<ObjKey, DynamicObject> = HashMap::new();
    let mut error_count: u32 = 0;
    let mut init_dirty = false;

    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    let mut init_flush = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    init_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                                init_dirty = true;
                            }
                            WatcherEvent::InitDone => {
                                error_count = 0;
                                debug!("live_query dynamic: initial list complete, {} items", store.len());
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                error_count = 0;
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                error_count = 0;
                                let key = dyn_obj_key(&obj);
                                store.remove(&key);
                                let _ = snap_tx.try_send(());
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("live_query dynamic: stream ended");
                        break;
                    }
                    Err(e) => {
                        error_count += 1;
                        if error_count >= MAX_CONSECUTIVE_ERRORS {
                            warn!("live_query dynamic: watcher failed {} times consecutively, giving up", error_count);
                            break;
                        }
                        warn!("live_query dynamic: watcher error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(RETRY_SLEEP_SECS)).await;
                    }
                }
            }
            _ = snap_rx.recv() => {
                let items = build_dynamic_snapshot(&store);

                // Update the watch channel with typed data. Ignore closed
                // errors — means all subscribers dropped and we'll be
                // aborted shortly.
                let _ = snapshot_tx.send(Some(ResourceUpdate::DynamicResources(items)));
            }
            _ = init_flush.tick() => {
                if init_dirty && !store.is_empty() {
                    init_dirty = false;
                    let items = build_dynamic_snapshot(&store);
                    let _ = snapshot_tx.send(Some(ResourceUpdate::DynamicResources(items)));
                }
            }
        }
    }

    // Signal that the watcher has died so the bridge detects it.
    let _ = snapshot_tx.send(None);
}

/// Build a sorted snapshot of DynamicKubeResource from the store.
fn build_dynamic_snapshot(store: &HashMap<(String, String), DynamicObject>) -> Vec<DynamicKubeResource> {
    let mut items: Vec<DynamicKubeResource> = store
        .values()
        .map(|obj| {
            let meta = &obj.metadata;
            let namespace = meta.namespace.clone().unwrap_or_default();
            let name = meta.name.clone().unwrap_or_default();
            let age = meta.creation_timestamp.as_ref().map(|ts| ts.0);
            let mut data = BTreeMap::new();
            if let Some(status) = obj.data.get("status") {
                if let Some(phase) = status.get("phase") {
                    if let Some(s) = phase.as_str() {
                        data.insert("status".to_string(), s.to_string());
                    }
                }
            }
            DynamicKubeResource { namespace, name, data, age }
        })
        .collect();
    items.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    items
}

// ---------------------------------------------------------------------------
// ns_api — namespaced or cluster-wide Api
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Generic typed watcher loop with debounce
// ---------------------------------------------------------------------------

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

/// Extracts the `(namespace, name)` key from a DynamicObject.
fn dyn_obj_key(obj: &DynamicObject) -> ObjKey {
    let meta = &obj.metadata;
    (
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Maximum consecutive watcher errors before giving up. With a 2-second sleep
/// between retries this gives ~60 seconds of retries.
const MAX_CONSECUTIVE_ERRORS: u32 = 30;

/// Runs a typed `kube::runtime::watcher` stream, maintaining a local cache.
///
/// Events are debounced during `InitApply` bursts (every 100ms). `Apply`,
/// `Delete`, and `InitDone` flush immediately. Snapshots are wrapped into a
/// `ResourceUpdate` variant via `wrap` and sent through the `watch::Sender`.
async fn run_typed_watcher<K, T, C, W>(
    api: Api<K>,
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
    convert: C,
    wrap: W,
    resource_type: &str,
) where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    T: Clone + Send + 'static + KubeResource,
    C: Fn(K) -> T + Send + 'static,
    W: Fn(Vec<T>) -> ResourceUpdate + Send + 'static,
{
    let watcher_config = watcher::Config::default()
        .page_size(WATCHER_PAGE_SIZE)
        .any_semantic();      // serve from API server cache (resourceVersion=0), much faster
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjKey, T> = HashMap::new();
    let mut error_count: u32 = 0;
    let mut init_dirty = false; // tracks whether we have unsent InitApply items

    // Channel for signalling that a snapshot should be built.
    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    // Timer for flushing intermediate snapshots during initial list.
    // Fires every 200ms so data appears progressively on large clusters.
    let mut init_flush = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    init_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rt = resource_type.to_string();

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                store.clear();
                                info!("live_query: starting initial list for {}", rt);
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                init_dirty = true;
                            }
                            WatcherEvent::InitDone => {
                                error_count = 0;
                                info!("live_query: initial list complete for {}, {} items", rt, store.len());
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                error_count = 0;
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                error_count = 0;
                                let key = obj_key(&obj);
                                store.remove(&key);
                                let _ = snap_tx.try_send(());
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("live_query: stream ended for {}", rt);
                        break;
                    }
                    Err(e) => {
                        error_count += 1;
                        if error_count >= MAX_CONSECUTIVE_ERRORS {
                            warn!("live_query: watcher for {} failed {} times consecutively, giving up", rt, error_count);
                            break;
                        }
                        warn!("live_query: watcher error for {}: {}", rt, e);
                        tokio::time::sleep(std::time::Duration::from_secs(RETRY_SLEEP_SECS)).await;
                    }
                }
            }
            _ = snap_rx.recv() => {
                // Build a sorted snapshot and wrap into a ResourceUpdate.
                let mut items: Vec<T> = store.values().cloned().collect();
                items.sort_by(|a, b| {
                    let key_a = (a.namespace(), a.name());
                    let key_b = (b.namespace(), b.name());
                    key_a.cmp(&key_b)
                });

                // Update the watch channel with typed data.
                let _ = snapshot_tx.send(Some(wrap(items)));
            }
            _ = init_flush.tick() => {
                // Flush intermediate snapshots during initial list so data
                // appears progressively instead of waiting for InitDone.
                if init_dirty && !store.is_empty() {
                    info!("live_query: flushing intermediate snapshot for {} ({} items)", rt, store.len());
                    init_dirty = false;
                    let mut items: Vec<T> = store.values().cloned().collect();
                    items.sort_by(|a, b| {
                        let key_a = (a.namespace(), a.name());
                        let key_b = (b.namespace(), b.name());
                        key_a.cmp(&key_b)
                    });
                    let _ = snapshot_tx.send(Some(wrap(items)));
                }
            }
        }
    }

    // Signal that the watcher has died so the bridge detects it.
    let _ = snapshot_tx.send(None);
}
