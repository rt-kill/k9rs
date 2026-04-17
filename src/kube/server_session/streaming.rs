//! Log streaming, discovery fetching, and metrics polling.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::{Api, Client};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::kube::cache::CachedCrd;
use crate::kube::metrics::{parse_node_metrics_usage, parse_pod_metrics_usage};
use crate::kube::protocol::{self, SessionEvent};

use super::{ServerSession, SessionSharedState};

/// How often the background refresher re-runs discovery. 5 minutes is a
/// balance between catching new namespaces / CRDs promptly and not hammering
/// the API server — discovery is cheap but not free (two full-cluster lists).
const DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

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

    /// Background loop: re-runs discovery every [`DISCOVERY_REFRESH_INTERVAL`]
    /// so new namespaces / new CRDs reach the client without a reconnect.
    /// Skips the first tick — the eager call in `run_session_inner` already
    /// covers t=0.
    pub(super) fn spawn_discovery_refresher(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let shared = self.shared.clone();
        let context = self.context.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(DISCOVERY_REFRESH_INTERVAL).await;
                run_discovery_once(&client, &tx, &shared, &context).await;
            }
        });
        self.discovery_refresher_task = Some(handle);
    }

    // -----------------------------------------------------------------------
    // Metrics polling
    // -----------------------------------------------------------------------

    pub(super) fn spawn_metrics_poller(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Per-metric consecutive-failure tracker drives the log discipline
            // (warn once per failure run, info on recovery) and feeds the
            // shared backoff below.
            let mut pod_fails = ConsecutiveFails::default();
            let mut node_fails = ConsecutiveFails::default();

            loop {
                let pod_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                    "pods",
                );
                let pod_api: Api<DynamicObject> = Api::all_with(client.clone(), &pod_ar);
                match pod_api.list(&ListParams::default()).await {
                    Ok(list) => {
                        pod_fails.record_success("pod metrics");
                        let mut metrics: HashMap<protocol::ObjectKey, protocol::MetricsUsage> = HashMap::new();
                        for item in &list.items {
                            let ns = item.metadata.namespace.clone().unwrap_or_default();
                            let name = item.metadata.name.clone().unwrap_or_default();
                            let usage = parse_pod_metrics_usage(&item.data);
                            metrics.insert(protocol::ObjectKey::new(ns, name), usage);
                        }
                        if tx.send(SessionEvent::PodMetrics(metrics)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => pod_fails.record_failure("pod metrics", &e),
                }

                let node_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics"),
                    "nodes",
                );
                let node_api: Api<DynamicObject> = Api::all_with(client.clone(), &node_ar);
                match node_api.list(&ListParams::default()).await {
                    Ok(list) => {
                        node_fails.record_success("node metrics");
                        let mut metrics: HashMap<protocol::NodeName, protocol::MetricsUsage> = HashMap::new();
                        for item in &list.items {
                            let name = item.metadata.name.clone().unwrap_or_default();
                            let usage = parse_node_metrics_usage(&item.data);
                            metrics.insert(name.into(), usage);
                        }
                        if tx.send(SessionEvent::NodeMetrics(metrics)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => node_fails.record_failure("node metrics", &e),
                }

                // Back off when both metrics are down — on a cluster without
                // metrics-server installed this avoids hammering the API
                // every 30s for no reason. The interval grows 30s → 5min.
                let worst = pod_fails.count.max(node_fails.count);
                tokio::time::sleep(metrics_backoff(worst)).await;
            }
        });
        self.metrics_task = Some(handle);
    }
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

/// Fetch namespaces + CRDs from the cluster, update the per-resource cache
/// on each success independently, and emit a [`SessionEvent::Discovery`]
/// frame to the client.
///
/// The cache is written resource-by-resource, gated on each fetch's success
/// independently. A failed fetch leaves the prior cached value untouched —
/// partial-success poisoning is structurally impossible under
/// [`crate::kube::cache::DiscoveryCache`].
async fn run_discovery_once(
    client: &Client,
    tx: &mpsc::Sender<SessionEvent>,
    shared: &Arc<SessionSharedState>,
    context: &protocol::ContextId,
) {
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
            let crds: Vec<CachedCrd> = list.items.iter().filter_map(|crd| {
                let name = crd.metadata.name.clone()?;
                let spec = &crd.spec;
                let version = spec.versions.iter()
                    .find(|v| v.served)
                    .or(spec.versions.first())
                    .map(|v| v.name.clone())
                    .unwrap_or_default();
                let printer_columns: Vec<crate::kube::cache::PrinterColumn> = spec.versions.iter()
                    .find(|v| v.served)
                    .or(spec.versions.first())
                    .and_then(|v| v.additional_printer_columns.as_ref())
                    .map(|cols| cols.iter().map(|c| crate::kube::cache::PrinterColumn {
                        name: c.name.clone(),
                        json_path: c.json_path.clone(),
                        column_type: crate::kube::cache::PrinterColumnType::from_k8s(&c.type_),
                    }).collect())
                    .unwrap_or_default();
                let scope = crate::kube::protocol::ResourceScope::from_k8s_spec(&spec.scope);
                Some(CachedCrd {
                    name,
                    gvr: crate::kube::protocol::CrdRef::new(
                        spec.group.clone(),
                        version,
                        spec.names.kind.clone(),
                        spec.names.plural.clone(),
                        scope,
                    ),
                    printer_columns,
                })
            }).collect();
            shared.discovery_cache.set_crds(context.clone(), crds.clone());
            crds
        }
        Err(e) => {
            warn!("Discovery: failed to list CRDs: {}", e);
            shared.discovery_cache.crds(context).unwrap_or_default()
        }
    };

    let _ = tx.send(SessionEvent::Discovery {
        context: context.name.clone(),
        namespaces,
        crds,
    }).await;
}
