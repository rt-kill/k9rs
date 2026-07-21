//! Log streaming, discovery fetching, and metrics polling.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::{Api, Client};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::kube::cache::CachedCrd;
use crate::kube::metrics::{parse_node_metrics_usage, parse_pod_metrics_usage, MetricsSnapshot};
use crate::kube::protocol::{self, SessionEvent};

use super::{ServerSession, SessionSharedState};

/// How often the background refresher re-runs discovery. 5 minutes is a
/// balance between catching new namespaces / CRDs promptly and not hammering
/// the API server — discovery is cheap but not free (two full-cluster lists).
fn discovery_refresh_interval() -> Duration {
    Duration::from_secs(crate::kube::daemon_config::daemon_config().discovery_refresh_secs)
}

impl ServerSession {
    // -----------------------------------------------------------------------
    // Log streaming
    // -----------------------------------------------------------------------

    // Log streaming is handled via yamux substreams now.
    // handle_stop_logs is a no-op (log_task is always None).

    // -----------------------------------------------------------------------
    // Discovery
    // -----------------------------------------------------------------------

    pub(super) fn handle_get_discovery_async(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let shared = self.shared.clone();
        let context = self.context.clone();
        self.track_task(async move {
            run_discovery_once(&client, &tx, &shared, &context).await;
        });
    }

    /// Subscribe this session to the cluster's shared discovery poller and
    /// bridge periodic refreshes into the event stream. The poll itself is
    /// shared across every session on the same `ContextId` (one 5-min loop per
    /// cluster, not per session); only this lightweight forwarder is per
    /// session. The eager `handle_get_discovery_async` in `run_session_inner`
    /// already delivered this session's t=0 discovery, so the bridge forwards
    /// only *updates* (changed-first), injecting this session's own context
    /// display name — sessions aliasing one cluster share the poller but keep
    /// distinct names.
    pub(super) fn spawn_discovery_refresher(&mut self) {
        let tx = self.event_tx.clone();
        let ctx_name = self.context.name.clone();
        let task_client = self.client.clone();
        let task_shared = self.shared.clone();
        let task_context = self.context.clone();
        let mut sub = self.shared.discovery_pollers.subscribe(
            self.context.clone(),
            // The bridge forwards only *updates* (changed-first; the eager
            // one-shot delivers this session's initial discovery), so the watch's
            // seed value is never read — `default` is correct, not just a stand-in.
            DiscoverySnapshot::default(),
            move |snapshot_tx| spawn_discovery_task(task_client, task_shared, task_context, snapshot_tx),
        );
        let handle = tokio::spawn(async move {
            loop {
                if sub.changed().await.is_err() { break; }
                let snap = sub.current();
                if tx.send(SessionEvent::Discovery {
                    context: ctx_name.clone(),
                    namespaces: snap.namespaces,
                    crds: snap.crds,
                }).await.is_err() { break; }
            }
        });
        self.discovery_refresher_task = Some(handle);
    }

    // -----------------------------------------------------------------------
    // Metrics polling
    // -----------------------------------------------------------------------

    /// Subscribe this session to the cluster's shared metrics poller and bridge
    /// its snapshots into the event stream. The 30s poll is shared across every
    /// session on the same `ContextId` (see [`crate::kube::shared_poller`]);
    /// only this forwarder is per session.
    pub(super) fn spawn_metrics_poller(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let mut sub = self.shared.metrics_pollers.subscribe(
            self.context.clone(),
            MetricsSnapshot::default(),
            move |snapshot_tx| spawn_metrics_task(client, snapshot_tx),
        );
        let handle = tokio::spawn(async move {
            // Forward the current snapshot first — a session joining a cluster
            // that's already being polled gets the latest metrics immediately —
            // then every subsequent poll. Unlike discovery there's no eager
            // one-shot, so this initial forward is how a late joiner is seeded.
            loop {
                let snap = sub.current();
                if tx.send(SessionEvent::PodMetrics(snap.pods)).await.is_err() { break; }
                if tx.send(SessionEvent::NodeMetrics(snap.nodes)).await.is_err() { break; }
                if sub.changed().await.is_err() { break; }
            }
        });
        self.metrics_task = Some(handle);
    }
}

/// The shared metrics poll loop: lists pod + node usage from metrics-server and
/// republishes a [`MetricsSnapshot`] on the `watch` channel. A failed sub-fetch
/// keeps that map's prior value (last-known), so a metrics-server blip never
/// blanks one half of the display. Publishes even with zero receivers (the
/// watch retains the value for the next subscriber) and never self-terminates —
/// the task's lifetime is owned by the [`crate::kube::shared_poller::SharedPoller`]
/// drop, not by any one session's channel.
fn spawn_metrics_task(client: Client, snapshot_tx: watch::Sender<MetricsSnapshot>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Brief stagger so metrics don't pile onto the connect burst (discovery
        // + watcher lists) the instant the first session on a cluster arrives.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Per-metric consecutive-failure tracker drives the log discipline
        // (warn once per failure run, info on recovery) and feeds the backoff.
        let mut pod_fails = ConsecutiveFails::default();
        let mut node_fails = ConsecutiveFails::default();
        // Running snapshot — only the successfully-fetched half is replaced each
        // cycle, so a transient failure leaves last-known data in place.
        let mut snapshot = MetricsSnapshot::default();

        loop {
            let pod_ar = ApiResource::from_gvk_with_plural(
                &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                "pods",
            );
            let pod_api: Api<DynamicObject> = Api::all_with(client.clone(), &pod_ar);
            match pod_api.list(&ListParams::default()).await {
                Ok(list) => {
                    pod_fails.record_success("pod metrics");
                    let mut pods = HashMap::new();
                    for item in &list.items {
                        let ns = item.metadata.namespace.clone().unwrap_or_default();
                        let name = item.metadata.name.clone().unwrap_or_default();
                        pods.insert(protocol::ObjectKey::new(ns, name), parse_pod_metrics_usage(&item.data));
                    }
                    snapshot.pods = pods;
                }
                Err(e) => pod_fails.record_failure("pod metrics", &e), // keep last-known pods
            }

            let node_ar = ApiResource::from_gvk_with_plural(
                &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics"),
                "nodes",
            );
            let node_api: Api<DynamicObject> = Api::all_with(client.clone(), &node_ar);
            match node_api.list(&ListParams::default()).await {
                Ok(list) => {
                    node_fails.record_success("node metrics");
                    let mut nodes = HashMap::new();
                    for item in &list.items {
                        let name = item.metadata.name.clone().unwrap_or_default();
                        nodes.insert(name.into(), parse_node_metrics_usage(&item.data));
                    }
                    snapshot.nodes = nodes;
                }
                Err(e) => node_fails.record_failure("node metrics", &e), // keep last-known nodes
            }

            let _ = snapshot_tx.send(snapshot.clone());

            // Back off when both metrics are down — on a cluster without
            // metrics-server installed this avoids hammering the API every 30s
            // for no reason. The interval grows 30s → 5min.
            let worst = pod_fails.count.max(node_fails.count);
            tokio::time::sleep(metrics_backoff(worst)).await;
        }
    })
}

/// Tracks consecutive failures so we warn-log once per failure run instead of
/// every 30s, and emit an info log the moment the metric recovers.
#[derive(Default)]
struct ConsecutiveFails {
    count: u32,
}

impl ConsecutiveFails {
    fn record_failure(&mut self, label: &str, err: &dyn std::fmt::Display) {
        self.count += 1;
        if self.count == 1 {
            warn!(
                "{}: fetch failed ({}); is metrics-server installed and accessible?",
                label, err
            );
        }
    }

    fn record_success(&mut self, label: &str) {
        if self.count > 0 {
            info!("{}: recovered after {} consecutive failure(s)", label, self.count);
        }
        self.count = 0;
    }
}

/// Sleep interval between metrics polls. 30s when everything's healthy;
/// exponential growth up to 5 minutes while the API is rejecting us, so a
/// cluster without metrics-server doesn't get pummeled every 30s forever.
fn metrics_backoff(consecutive_fails: u32) -> Duration {
    match consecutive_fails {
        0 => Duration::from_secs(30),
        1 => Duration::from_secs(60),
        2 => Duration::from_secs(120),
        _ => Duration::from_secs(300),
    }
}

// ---------------------------------------------------------------------------
// Discovery fetch — shared body for the eager call and the refresher loop
// ---------------------------------------------------------------------------

/// One discovery poll's namespaces + CRDs, fanned out to every session on a
/// cluster via the shared discovery poller. Carries no context *name*: sessions
/// aliasing one cluster (same `ContextId`, different kubeconfig context names)
/// share the poller, and each injects its own display name when forwarding.
#[derive(Clone, Default)]
pub(super) struct DiscoverySnapshot {
    namespaces: Vec<String>,
    crds: Vec<CachedCrd>,
}

/// The shared discovery poll loop: every refresh interval, re-fetch + re-cache
/// and publish a snapshot. Skips t=0 — each session's eager
/// `handle_get_discovery_async` already covers the initial fetch. Runs until the
/// [`crate::kube::shared_poller::SharedPoller`] is dropped.
fn spawn_discovery_task(
    client: Client,
    shared: Arc<SessionSharedState>,
    context: protocol::ContextId,
    snapshot_tx: watch::Sender<DiscoverySnapshot>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(discovery_refresh_interval()).await;
            let _ = snapshot_tx.send(run_discovery_poll(&client, &shared, &context).await);
        }
    })
}

/// Eager / on-demand discovery: poll once and emit a [`SessionEvent::Discovery`]
/// to this one session. The *periodic* refresh rides the shared poller instead
/// (see [`ServerSession::spawn_discovery_refresher`]).
async fn run_discovery_once(
    client: &Client,
    tx: &mpsc::Sender<SessionEvent>,
    shared: &Arc<SessionSharedState>,
    context: &protocol::ContextId,
) {
    let snap = run_discovery_poll(client, shared, context).await;
    let _ = tx.send(SessionEvent::Discovery {
        context: context.name.clone(),
        namespaces: snap.namespaces,
        crds: snap.crds,
    }).await;
}

/// Fetch namespaces + CRDs from the cluster, update the per-resource cache on
/// each success independently, and return the snapshot.
///
/// The cache is written resource-by-resource, gated on each fetch's success
/// independently. A failed fetch leaves the prior cached value untouched —
/// partial-success poisoning is structurally impossible under
/// [`crate::kube::cache::DiscoveryCache`].
async fn run_discovery_poll(
    client: &Client,
    shared: &Arc<SessionSharedState>,
    context: &protocol::ContextId,
) -> DiscoverySnapshot {
    let namespaces: Vec<String> = match Api::<Namespace>::all(client.clone())
        .list(&ListParams::default())
        .await
    {
        Ok(list) => {
            let ns: Vec<String> = list.items.iter()
                .filter_map(|ns| ns.metadata.name.clone())
                .collect();
            shared.discovery_cache.set_namespaces(context.clone(), ns.clone());
            ns
        }
        Err(e) => {
            warn!("Discovery: failed to list namespaces: {}", e);
            // Fall back to last-known-good (if any) so the client still gets
            // a usable list when we hit a transient blip.
            shared.discovery_cache.namespaces(context).unwrap_or_default()
        }
    };

    let crds: Vec<CachedCrd> = match Api::<CustomResourceDefinition>::all(client.clone())
        .list(&ListParams::default())
        .await
    {
        Ok(list) => {
            let crds: Vec<CachedCrd> = list.items.iter().filter_map(CachedCrd::from_k8s).collect();
            shared.discovery_cache.set_crds(context.clone(), crds.clone());
            crds
        }
        Err(e) => {
            warn!("Discovery: failed to list CRDs: {}", e);
            shared.discovery_cache.crds(context).unwrap_or_default()
        }
    };

    DiscoverySnapshot { namespaces, crds }
}
