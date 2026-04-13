//! Live query system for watching Kubernetes resources.
//!
//! A `LiveQuery` represents a running watcher for a specific resource type.
//! It wraps the kube watch stream and produces typed `ResourceUpdate` values
//! via a `tokio::sync::watch` channel.
//!
//! `WatcherCache` manages a per-process cache of live queries using `Weak`
//! references so watchers die naturally when no `Subscription` holds them.

use std::collections::HashMap;
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
use kube::api::GroupVersionKind;
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::event::ResourceUpdate;
use crate::kube::cache::PrinterColumn;
use crate::kube::protocol::ResourceScope;
use crate::kube::resources::KubeResource;
use crate::kube::resources::converters::*;

// ---------------------------------------------------------------------------
// QueryKey
// ---------------------------------------------------------------------------

/// Key for looking up a live query in the cache.
/// Uses `ContextId` so watchers are shared by actual cluster endpoint,
/// not by context name (which can collide across kubeconfig files).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QueryKey {
    pub context: crate::kube::protocol::ContextId,
    pub namespace: crate::kube::protocol::Namespace,
    pub resource: crate::kube::protocol::ResourceId,
    /// Server-side filter. A filtered subscription creates a separate watcher
    /// from an unfiltered one (different API queries to K8s).
    pub filter: Option<crate::kube::protocol::SubscriptionFilter>,
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
    /// The last error seen by the watcher task before it exited. Written by
    /// the watcher just before sending `None` on the snapshot channel. Read
    /// by the bridge to include the actual K8s error in the StreamEvent::Error
    /// sent to the TUI, rather than a generic "Watcher failed" message.
    last_error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
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
pub(crate) const WATCHER_PAGE_SIZE: u32 = 1000;
/// Interval for flushing intermediate snapshots during the initial list.
pub(crate) const INIT_FLUSH_INTERVAL_MS: u64 = 200;
/// Initial backoff duration for watcher retries (milliseconds).
pub(crate) const INITIAL_BACKOFF_MS: u64 = 300;
/// Maximum single-sleep backoff cap (milliseconds).
pub(crate) const MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum total elapsed time before giving up on retries (milliseconds).
pub(crate) const MAX_ELAPSED_MS: u64 = 120_000;

impl Drop for Subscription {
    fn drop(&mut self) {
        // Only keep the watcher alive via grace period if it's still running.
        // Dead watchers should be dropped immediately so the cache entry is
        // cleaned up and a fresh watcher can be created on next subscribe.
        if self._keepalive.task.is_finished() {
            // Watcher task already died — drop immediately, no grace period.
            return;
        }
        let arc = self._keepalive.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
            drop(arc);
        });
    }
}

impl Subscription {
    /// Get the current snapshot (None if no data yet, or if the watcher died).
    pub fn current(&mut self) -> Option<ResourceUpdate> {
        self.snapshot_rx.borrow_and_update().clone()
    }

    /// Wait for the next snapshot update.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }

    /// Check if the underlying watcher task is still alive.
    pub fn is_alive(&self) -> bool {
        !self._keepalive.task.is_finished()
    }

    /// The last error message from the watcher task (if it died with an error).
    pub fn last_error(&self) -> Option<String> {
        self._keepalive.last_error.lock().unwrap().clone()
    }

    /// Eagerly drop the keepalive Arc without spawning a grace period task.
    /// Use this when you know the watcher is dead or you want immediate cleanup.
    pub fn eager_drop(self) {
        // Just drops self without the grace period (Drop impl checks is_finished).
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
        // Create a new receiver. The receiver from subscribe() has the current
        // value available via borrow(), so the bridge's current() call will
        // return the latest snapshot without waiting for a new update.
        let rx = arc.snapshot_tx.subscribe();
        Some(Subscription {
            key: key.clone(),
            snapshot_rx: rx,
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
        server_headers: Vec<String>,
    ) -> Subscription {
        // Atomic check-and-insert via DashMap entry API.
        use dashmap::mapref::entry::Entry;

        // Fast path: try existing entry.
        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                if !arc.task.is_finished() {
                    tracing::info!("WatcherCache: reusing existing watcher for {:?}", key);
                    return Subscription {
                        key,
                        snapshot_rx: arc.snapshot_tx.subscribe(),
                        _keepalive: arc,
                    };
                }
                // Task is dead — fall through to create a new watcher.
                tracing::info!("WatcherCache: existing watcher is dead, replacing for {:?}", key);
            }
        }

        // Slow path: create new watcher. Use entry() for atomicity.
        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                // Another thread might have inserted between our get() and entry().
                if let Some(arc) = e.get().upgrade() {
                    if !arc.task.is_finished() {
                        tracing::info!("WatcherCache: reusing watcher (race winner) for {:?}", key);
                        return Subscription {
                            key,
                            snapshot_rx: arc.snapshot_tx.subscribe(),
                            _keepalive: arc,
                        };
                    }
                }
                // Dead Weak or dead task — replace with new watcher.
                tracing::info!("WatcherCache: creating new watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_watcher(&key, client, server_headers);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
            Entry::Vacant(e) => {
                tracing::info!("WatcherCache: creating new watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_watcher(&key, client, server_headers);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
        }
    }

    /// Internal: spawn a watcher task and return the LiveQuery + initial receiver.
    fn create_watcher(key: &QueryKey, client: &Client, server_headers: Vec<String>) -> (Arc<LiveQuery>, watch::Receiver<Option<ResourceUpdate>>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let last_error: std::sync::Arc<std::sync::Mutex<Option<String>>> = Default::default();
        let task_client = client.clone();
        let task_key = key.clone();
        let task_tx = snapshot_tx.clone();
        let task_error = last_error.clone();
        let task = tokio::spawn(async move {
            run_live_watcher(task_client, task_key, task_tx, server_headers, task_error).await;
        });
        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
            last_error,
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
        server_headers: Vec<String>,
    ) -> Subscription {
        // Remove the existing (possibly still-alive) Weak entry so we
        // unconditionally create a fresh watcher below.
        self.entries.remove(&key);

        // Start a new watcher (same logic as subscribe but without reuse).
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let last_error: std::sync::Arc<std::sync::Mutex<Option<String>>> = Default::default();

        let task_client = client.clone();
        let task_key = key.clone();
        let task_tx = snapshot_tx.clone();
        let task_error = last_error.clone();

        let task = tokio::spawn(async move {
            run_live_watcher(task_client, task_key, task_tx, server_headers, task_error).await;
        });

        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
            last_error,
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

/// Dispatches to a typed watcher based on the `resource_type` in the query key.
///
/// `server_headers` are column headers fetched from the K8s Table API. If
/// non-empty they are used instead of the hardcoded fallback headers. For
/// resources with client-only columns (pods: CPU/MEM, nodes: CPU/MEMORY) the
/// client columns are appended after the server columns.
async fn run_live_watcher(
    client: Client,
    key: QueryKey,
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
    server_headers: Vec<String>,
    last_error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
) {
    let ns = &key.namespace;
    let rt = key.resource.plural.as_str();
    let watcher_filter = key.filter.clone();

    /// Pick server-provided headers if available, otherwise use the hardcoded fallback.
    fn pick_headers(server: &[String], fallback: Vec<String>) -> Vec<String> {
        if server.is_empty() { fallback } else { server.to_vec() }
    }

    /// Like `pick_headers` but appends client-only columns that the server
    /// doesn't know about (e.g. CPU/MEM from metrics-server overlay).
    fn pick_headers_with_extra(server: &[String], fallback: Vec<String>, extra: &[&str]) -> Vec<String> {
        if server.is_empty() {
            fallback
        } else {
            let mut h = server.to_vec();
            for col in extra {
                let upper = col.to_uppercase();
                if !h.iter().any(|c| c == &upper) {
                    h.push(upper);
                }
            }
            h
        }
    }

    let ns_str = ns.display();

    // ConfigMaps: migrated to the unified ResourceRow model.
    if rt == "configmaps" {
        let api: Api<ConfigMap> = ns_api(&client, ns);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("configmaps").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAMESPACE".into(), "NAME".into(), "DATA".into(), "LABELS".into(), "AGE".into()]);
        run_typed_watcher(
            api, snapshot_tx, configmap_to_row,
            move |rows| ResourceUpdate::Rows {
                resource: resource_id.clone(), headers: headers.clone(), rows,
            },
            rt, ns_str, watcher_filter.clone(), last_error,
        ).await;
        return;
    }

    macro_rules! unified_ns {
        ($rt_match:expr, $alias:expr, $api_type:ty, $conv:expr, $fallback:expr) => {
            if rt == $rt_match {
                let api: Api<$api_type> = ns_api(&client, ns);
                let resource_id = crate::kube::protocol::ResourceId::from_alias($alias).unwrap();
                let headers = pick_headers(&server_headers, $fallback);
                run_typed_watcher(api, snapshot_tx, $conv, move |rows| ResourceUpdate::Rows {
                    resource: resource_id.clone(), headers: headers.clone(), rows,
                }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
                return;
            }
        };
    }
    macro_rules! unified_cluster {
        ($rt_match:expr, $alias:expr, $api_type:ty, $conv:expr, $fallback:expr) => {
            if rt == $rt_match {
                let api: Api<$api_type> = Api::all(client.clone());
                let resource_id = crate::kube::protocol::ResourceId::from_alias($alias).unwrap();
                let headers = pick_headers(&server_headers, $fallback);
                run_typed_watcher(api, snapshot_tx, $conv, move |rows| ResourceUpdate::Rows {
                    resource: resource_id.clone(), headers: headers.clone(), rows,
                }, rt, "all", watcher_filter.clone(), last_error.clone()).await;
                return;
            }
        };
    }

    unified_ns!("secrets", "secrets", Secret, secret_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "TYPE".into(), "DATA".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("ingresses", "ingresses", Ingress, ingress_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "CLASS".into(), "HOSTS".into(), "ADDRESS".into(), "PORTS".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("networkpolicies", "networkpolicies", NetworkPolicy, network_policy_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "POD-SELECTOR".into(), "POLICY TYPES".into(), "AGE".into()]);
    unified_ns!("serviceaccounts", "serviceaccounts", ServiceAccount, service_account_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "SECRETS".into(), "AGE".into()]);
    unified_ns!("persistentvolumeclaims", "persistentvolumeclaims", PersistentVolumeClaim, pvc_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "STATUS".into(), "VOLUME".into(), "CAPACITY".into(), "ACCESS MODES".into(), "STORAGECLASS".into(), "AGE".into()]);
    if rt == "pvcs" {
        let api: Api<PersistentVolumeClaim> = ns_api(&client, ns);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("persistentvolumeclaims").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAMESPACE".into(), "NAME".into(), "STATUS".into(), "VOLUME".into(), "CAPACITY".into(), "ACCESS MODES".into(), "STORAGECLASS".into(), "AGE".into()]);
        run_typed_watcher(api, snapshot_tx, pvc_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(),
            headers: headers.clone(),
            rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }
    unified_ns!("events", "events", Event, event_to_row,
        vec!["NAMESPACE".into(), "TYPE".into(), "REASON".into(), "OBJECT".into(), "MESSAGE".into(), "SOURCE".into(), "COUNT".into(), "AGE".into()]);
    unified_ns!("roles", "roles", Role, role_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "RULES".into(), "AGE".into()]);
    unified_ns!("rolebindings", "rolebindings", RoleBinding, role_binding_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "ROLE".into(), "SUBJECTS".into(), "AGE".into()]);
    unified_ns!("horizontalpodautoscalers", "horizontalpodautoscalers", HorizontalPodAutoscaler, hpa_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "REFERENCE".into(), "MIN".into(), "MAX".into(), "CURRENT".into(), "AGE".into()]);
    if rt == "hpa" {
        let api: Api<HorizontalPodAutoscaler> = ns_api(&client, ns);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("horizontalpodautoscalers").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAMESPACE".into(), "NAME".into(), "REFERENCE".into(), "MIN".into(), "MAX".into(), "CURRENT".into(), "AGE".into()]);
        run_typed_watcher(api, snapshot_tx, hpa_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(),
            headers: headers.clone(),
            rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }
    unified_ns!("endpoints", "endpoints", Endpoints, endpoints_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "ENDPOINTS".into(), "AGE".into()]);
    unified_ns!("limitranges", "limitranges", LimitRange, limit_range_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "TYPE".into(), "AGE".into()]);
    unified_ns!("resourcequotas", "resourcequotas", ResourceQuota, resource_quota_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "HARD".into(), "USED".into(), "AGE".into()]);
    unified_ns!("poddisruptionbudgets", "poddisruptionbudgets", PodDisruptionBudget, pdb_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "MIN AVAILABLE".into(), "MAX UNAVAILABLE".into(), "ALLOWED DISRUPTIONS".into(), "AGE".into()]);
    if rt == "pdb" {
        let api: Api<PodDisruptionBudget> = ns_api(&client, ns);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("poddisruptionbudgets").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAMESPACE".into(), "NAME".into(), "MIN AVAILABLE".into(), "MAX UNAVAILABLE".into(), "ALLOWED DISRUPTIONS".into(), "AGE".into()]);
        run_typed_watcher(api, snapshot_tx, pdb_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(),
            headers: headers.clone(),
            rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }

    // Tier 2 workload resources — migrated to unified ResourceRow with extra metadata.
    unified_ns!("deployments", "deployments", Deployment, deployment_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "READY".into(), "UP-TO-DATE".into(), "AVAILABLE".into(), "CONTAINERS".into(), "IMAGES".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("statefulsets", "statefulsets", StatefulSet, statefulset_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "READY".into(), "SERVICE".into(), "CONTAINERS".into(), "IMAGES".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("daemonsets", "daemonsets", DaemonSet, daemonset_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "DESIRED".into(), "CURRENT".into(), "READY".into(), "UP-TO-DATE".into(), "AVAILABLE".into(), "NODE SELECTOR".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("replicasets", "replicasets", ReplicaSet, replicaset_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "DESIRED".into(), "CURRENT".into(), "READY".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("jobs", "jobs", Job, job_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "COMPLETIONS".into(), "DURATION".into(), "CONTAINERS".into(), "IMAGES".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("cronjobs", "cronjobs", CronJob, cronjob_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "SCHEDULE".into(), "SUSPEND".into(), "ACTIVE".into(), "LAST SCHEDULE".into(), "CONTAINERS".into(), "IMAGES".into(), "LABELS".into(), "AGE".into()]);
    unified_ns!("services", "services", Service, service_to_row,
        vec!["NAMESPACE".into(), "NAME".into(), "TYPE".into(), "CLUSTER-IP".into(), "EXTERNAL-IP".into(), "SELECTOR".into(), "PORT(S)".into(), "LABELS".into(), "AGE".into()]);

    // Unified ResourceRow migrations — cluster-scoped resources.
    unified_cluster!("storageclasses", "storageclasses", StorageClass, storage_class_to_row,
        vec!["NAME".into(), "PROVISIONER".into(), "RECLAIM POLICY".into(), "VOLUME BINDING MODE".into(), "EXPANSION".into(), "AGE".into()]);
    unified_cluster!("persistentvolumes", "persistentvolumes", PersistentVolume, pv_to_row,
        vec!["NAME".into(), "CAPACITY".into(), "ACCESS MODES".into(), "RECLAIM POLICY".into(), "STATUS".into(), "CLAIM".into(), "STORAGECLASS".into(), "AGE".into()]);
    if rt == "pvs" {
        let api: Api<PersistentVolume> = Api::all(client.clone());
        let resource_id = crate::kube::protocol::ResourceId::from_alias("persistentvolumes").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAME".into(), "CAPACITY".into(), "ACCESS MODES".into(), "RECLAIM POLICY".into(), "STATUS".into(), "CLAIM".into(), "STORAGECLASS".into(), "AGE".into()]);
        run_typed_watcher(api, snapshot_tx, pv_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(),
            headers: headers.clone(),
            rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }
    unified_cluster!("clusterroles", "clusterroles", ClusterRole, cluster_role_to_row,
        vec!["NAME".into(), "RULES".into(), "AGE".into()]);
    unified_cluster!("clusterrolebindings", "clusterrolebindings", ClusterRoleBinding, cluster_role_binding_to_row,
        vec!["NAME".into(), "ROLE".into(), "SUBJECTS".into(), "AGE".into()]);
    unified_cluster!("namespaces", "namespaces", Namespace, namespace_to_row,
        vec!["NAME".into(), "STATUS".into(), "AGE".into()]);

    // CRDs — migrated to unified ResourceRow with extra metadata for drill-down.
    if rt == "customresourcedefinitions" || rt == "crds" {
        let api: Api<CustomResourceDefinition> = Api::all(client);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("customresourcedefinitions").unwrap();
        let headers = pick_headers(&server_headers,
            vec!["NAME".into(), "GROUP".into(), "VERSION".into(), "KIND".into(), "SCOPE".into(), "AGE".into()]);
        run_typed_watcher(api, snapshot_tx, crd_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(),
            headers: headers.clone(),
            rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }

    // Nodes — server columns + client-only CPU/MEMORY columns from metrics overlay.
    if rt == "nodes" {
        let api: Api<Node> = Api::all(client.clone());
        let resource_id = crate::kube::protocol::ResourceId::from_alias("nodes").unwrap();
        let headers = pick_headers_with_extra(&server_headers,
            vec!["NAME".into(), "STATUS".into(), "ROLES".into(), "TAINTS".into(), "VERSION".into(), "INTERNAL-IP".into(), "EXTERNAL-IP".into(), "PODS".into(), "CPU%".into(), "MEM%".into(), "ARCH".into(), "LABELS".into(), "AGE".into()],
            &["CPU%", "MEM%"]);
        run_typed_watcher(api, snapshot_tx, node_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(), headers: headers.clone(), rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }

    // Pods — server columns + client-only CPU/MEM columns from metrics overlay.
    if rt == "pods" {
        let api: Api<Pod> = ns_api(&client, ns);
        let resource_id = crate::kube::protocol::ResourceId::from_alias("pods").unwrap();
        let headers = pick_headers_with_extra(&server_headers,
            vec!["NAMESPACE".into(), "NAME".into(), "READY".into(), "STATUS".into(), "RESTARTS".into(), "LAST RESTART".into(), "CPU".into(), "MEM".into(), "IP".into(), "NODE".into(), "QOS".into(), "SERVICE-ACCOUNT".into(), "READINESS GATES".into(), "LABELS".into(), "AGE".into()],
            &["CPU", "MEM"]);
        run_typed_watcher(api, snapshot_tx, pod_to_row, move |rows| ResourceUpdate::Rows {
            resource: resource_id.clone(), headers: headers.clone(), rows,
        }, rt, ns_str, watcher_filter.clone(), last_error.clone()).await;
        return;
    }

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
        printer_columns: Vec<PrinterColumn>,
    ) -> Subscription {
        use dashmap::mapref::entry::Entry;

        // Fast path: try existing entry.
        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                if !arc.task.is_finished() {
                    tracing::info!("WatcherCache: reusing existing dynamic watcher for {:?}", key);
                    return Subscription {
                        key,
                        snapshot_rx: arc.snapshot_tx.subscribe(),
                        _keepalive: arc,
                    };
                }
                tracing::info!("WatcherCache: existing dynamic watcher is dead, replacing for {:?}", key);
            }
        }

        // Slow path: create new watcher. Use entry() for atomicity.
        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    if !arc.task.is_finished() {
                        tracing::info!("WatcherCache: reusing dynamic watcher (race winner) for {:?}", key);
                        return Subscription {
                            key,
                            snapshot_rx: arc.snapshot_tx.subscribe(),
                            _keepalive: arc,
                        };
                    }
                }
                // Dead Weak or dead task — replace with new dynamic watcher.
                tracing::info!("WatcherCache: creating new dynamic watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_dynamic_watcher(&key, client, gvk, plural, scope, printer_columns);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
            Entry::Vacant(e) => {
                tracing::info!("WatcherCache: creating new dynamic watcher for {:?}", key);
                let (live_query, snapshot_rx) = Self::create_dynamic_watcher(&key, client, gvk, plural, scope, printer_columns);
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
        printer_columns: Vec<PrinterColumn>,
    ) -> (Arc<LiveQuery>, watch::Receiver<Option<ResourceUpdate>>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let last_error: std::sync::Arc<std::sync::Mutex<Option<String>>> = Default::default();
        let task_client = client.clone();
        let task_ns = key.namespace.clone();
        let task_tx = snapshot_tx.clone();
        let task_error = last_error.clone();
        let task = tokio::spawn(async move {
            crate::kube::live_query_dynamic::run_dynamic_live_watcher(task_client, task_ns, task_tx, gvk, plural, scope, printer_columns, task_error).await;
        });
        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
            last_error,
        });
        (live_query, snapshot_rx)
    }
}

// ---------------------------------------------------------------------------
// ns_api — namespaced or cluster-wide Api
// ---------------------------------------------------------------------------

/// Returns `Api::all()` for `Namespace::All`, otherwise `Api::namespaced()`.
fn ns_api<K>(client: &Client, ns: &crate::kube::protocol::Namespace) -> Api<K>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as Resource>::DynamicType: Default,
{
    match ns {
        crate::kube::protocol::Namespace::All => Api::all(client.clone()),
        crate::kube::protocol::Namespace::Named(name) => Api::namespaced(client.clone(), name),
    }
}

// ---------------------------------------------------------------------------
// Generic typed watcher loop with debounce
// ---------------------------------------------------------------------------

use crate::kube::protocol::ObjectKey;

/// Extracts the `ObjectKey` from a Kubernetes resource.
fn obj_key<K: Resource<DynamicType = ()>>(obj: &K) -> ObjectKey {
    let meta = obj.meta();
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}



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
    namespace: &str,
    filter: Option<crate::kube::protocol::SubscriptionFilter>,
    last_error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
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
    let mut watcher_config = watcher::Config::default()
        .page_size(WATCHER_PAGE_SIZE)
        .any_semantic();      // serve from API server cache (resourceVersion=0), much faster
    // Apply server-side filters to the watcher (pushed to K8s API).
    // OwnerUid is NOT applied here — it's post-filtered after snapshot creation
    // because the K8s API doesn't support filtering by ownerReference.
    match &filter {
        Some(crate::kube::protocol::SubscriptionFilter::Labels(map)) => {
            let sel = crate::kube::protocol::SubscriptionFilter::labels_to_selector(map);
            watcher_config = watcher_config.labels(&sel);
        }
        Some(crate::kube::protocol::SubscriptionFilter::Field(f)) => {
            watcher_config = watcher_config.fields(f);
        }
        Some(crate::kube::protocol::SubscriptionFilter::OwnerUid(_)) | None => {}
    }
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjectKey, T> = HashMap::new();
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;
    let mut backoff_start = std::time::Instant::now();
    // Track whether we've ever received data successfully. If the initial
    // LIST fails (RBAC, wrong resource, etc.), fail immediately instead of
    // retrying for 2 minutes — it's almost certainly a permanent error.
    let mut had_success = false;
    let mut init_dirty = false; // tracks whether we have unsent InitApply items

    // Channel for signalling that a snapshot should be built.
    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    // Timer for flushing intermediate snapshots during initial list.
    // Fires every 200ms so data appears progressively on large clusters.
    let mut init_flush = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    init_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rt = resource_type.to_string();
    let ns_label = if namespace.is_empty() || namespace == "all" {
        "all".to_string()
    } else {
        namespace.to_string()
    };

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                store.clear();
                                backoff_start = std::time::Instant::now();
                                info!("live_query: starting initial list for {}({})", rt, ns_label);
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                init_dirty = true;
                            }
                            WatcherEvent::InitDone => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                info!("live_query: initial list complete for {}({}), {} items", rt, ns_label, store.len());
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
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
                        if !had_success {
                            // Initial LIST failed — almost certainly permanent
                            // (RBAC, unknown resource, etc.). Fail immediately.
                            warn!("live_query: initial load failed for {}: {}", rt, e);
                            *last_error.lock().unwrap() = Some(format!("{}", e));
                            break;
                        }
                        if backoff_start.elapsed().as_millis() as u64 > MAX_ELAPSED_MS {
                            warn!("live_query: watcher for {} failed for over 2 minutes, giving up: {}", rt, e);
                            *last_error.lock().unwrap() = Some(format!("{}", e));
                            break;
                        }
                        warn!("live_query: watcher error for {}: {}, retrying in {}ms", rt, e, backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
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
                    info!("live_query: flushing intermediate snapshot for {}({}) ({} items)", rt, ns_label, store.len());
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
