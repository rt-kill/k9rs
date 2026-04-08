//! Log streaming, discovery fetching, and metrics polling.

use std::collections::HashMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::Api;
use tracing::warn;

use crate::kube::cache::CachedCrd;
use crate::kube::metrics::{parse_node_metrics_usage, parse_pod_metrics_usage};
use crate::kube::protocol::{self, SessionEvent};

use super::ServerSession;

impl ServerSession {
    // -----------------------------------------------------------------------
    // Log streaming
    // -----------------------------------------------------------------------

    pub(super) async fn handle_stream_logs(
        &mut self,
        pod: &str,
        namespace: &str,
        container: &str,
        follow: bool,
        tail: Option<u64>,
        since: Option<String>,
        previous: bool,
    ) {
        self.handle_stop_logs();

        let log_tx = self.event_tx.clone();
        let pod = pod.to_string();
        let namespace = namespace.to_string();
        let container = container.to_string();
        let context = self.context.name.clone();

        let log_handle = tokio::spawn(async move {
            use tokio::io::BufReader as TokioBufReader;
            use tokio::io::AsyncBufReadExt;
            use tokio::process::Command;

            let mut cmd = Command::new("kubectl");
            cmd.arg("logs");

            if follow { cmd.arg("-f"); }
            if previous { cmd.arg("--previous"); }
            cmd.arg(&pod);
            if !namespace.is_empty() { cmd.arg("-n").arg(&namespace); }
            if !container.is_empty() && container != "all" {
                cmd.arg("-c").arg(&container);
            } else if container == "all" {
                cmd.arg("--all-containers=true");
            }
            if let Some(ref s) = since {
                cmd.arg(format!("--since={}", s));
            } else if let Some(t) = tail {
                cmd.arg("--tail").arg(t.to_string());
            }
            if !context.is_empty() { cmd.arg("--context").arg(&context); }

            cmd.stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let _ = log_tx.send(SessionEvent::SessionError(
                        format!("Failed to spawn kubectl logs: {}", e),
                    )).await;
                    let _ = log_tx.send(SessionEvent::LogEnd).await;
                    return;
                }
            };

            if let Some(stdout) = child.stdout.take() {
                let mut lines = TokioBufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if log_tx.send(SessionEvent::LogLine(line)).await.is_err() {
                        break;
                    }
                }
            }

            let _ = log_tx.send(SessionEvent::LogEnd).await;
        });

        self.log_task = Some(log_handle);
    }

    pub(super) fn handle_stop_logs(&mut self) {
        if let Some(handle) = self.log_task.take() {
            handle.abort();
        }
    }

    // -----------------------------------------------------------------------
    // Discovery
    // -----------------------------------------------------------------------

    pub(super) fn handle_get_discovery_async(&self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let shared = self.shared.clone();
        let context = self.context.clone();
        tokio::spawn(async move {
            if let Some(entry) = shared.discovery_cache.get(&context) {
                let (ns, crds) = entry.value().clone();
                let _ = tx.send(SessionEvent::Discovery { context: context.name.clone(), namespaces: ns, crds }).await;
                return;
            }

            let ns_api: Api<Namespace> = Api::all(client.clone());
            let mut ns_ok = false;
            let namespaces: Vec<String> = match ns_api.list(&ListParams::default()).await {
                Ok(list) => { ns_ok = true; list.items.iter().filter_map(|ns| ns.metadata.name.clone()).collect() }
                Err(e) => { warn!("Discovery: failed to list namespaces: {}", e); vec![] }
            };

            let crd_api: Api<CustomResourceDefinition> = Api::all(client);
            let mut crd_ok = false;
            let crds: Vec<CachedCrd> = match crd_api.list(&ListParams::default()).await {
                Ok(list) => { crd_ok = true; list.items.iter().filter_map(|crd| {
                    let name = crd.metadata.name.clone()?;
                    let spec = &crd.spec;
                    let version = spec.versions.iter()
                        .find(|v| v.served)
                        .or(spec.versions.first())
                        .map(|v| v.name.clone())
                        .unwrap_or_default();
                    // Extract printer columns from the served version's spec.
                    let printer_columns: Vec<crate::kube::cache::PrinterColumn> = spec.versions.iter()
                        .find(|v| v.served)
                        .or(spec.versions.first())
                        .and_then(|v| v.additional_printer_columns.as_ref())
                        .map(|cols| cols.iter().map(|c| crate::kube::cache::PrinterColumn {
                            name: c.name.clone(),
                            json_path: c.json_path.clone(),
                            column_type: c.type_.clone(),
                        }).collect())
                        .unwrap_or_default();
                    Some(CachedCrd {
                        name,
                        kind: spec.names.kind.clone(),
                        plural: spec.names.plural.clone(),
                        group: spec.group.clone(),
                        version,
                        scope: spec.scope.clone(),
                        printer_columns,
                    })
                }).collect() }
                Err(e) => { warn!("Discovery: failed to list CRDs: {}", e); vec![] }
            };

            if ns_ok || crd_ok {
                shared.discovery_cache.insert(context.clone(), (namespaces.clone(), crds.clone()));
            }

            let _ = tx.send(SessionEvent::Discovery { context: context.name, namespaces, crds }).await;
        });
    }

    // -----------------------------------------------------------------------
    // Metrics polling
    // -----------------------------------------------------------------------

    pub(super) fn spawn_metrics_poller(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;

            loop {
                // Fetch pod metrics.
                let pod_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                    "pods",
                );
                let pod_api: Api<DynamicObject> = Api::all_with(client.clone(), &pod_ar);
                if let Ok(list) = pod_api.list(&ListParams::default()).await {
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

                // Fetch node metrics.
                let node_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics"),
                    "nodes",
                );
                let node_api: Api<DynamicObject> = Api::all_with(client.clone(), &node_ar);
                if let Ok(list) = node_api.list(&ListParams::default()).await {
                    let mut metrics: HashMap<String, protocol::MetricsUsage> = HashMap::new();
                    for item in &list.items {
                        let name = item.metadata.name.clone().unwrap_or_default();
                        let usage = parse_node_metrics_usage(&item.data);
                        metrics.insert(name, usage);
                    }
                    if tx.send(SessionEvent::NodeMetrics(metrics)).await.is_err() {
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        self.metrics_task = Some(handle);
    }
}
