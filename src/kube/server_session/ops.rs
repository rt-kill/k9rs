//! Subscribe/unsubscribe, namespace/context switching, describe/YAML,
//! and mutating operations (delete, scale, restart, secret decode,
//! CronJob trigger/suspend).

use tracing::{info, warn};

use crate::kube::live_query::QueryKey;
use crate::kube::protocol::{self, SessionEvent};
use crate::kube::table;

use super::{InitParams, PendingClientResult, ServerSession};

impl ServerSession {
    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    pub(super) fn handle_subscribe_resource(&mut self, id: &protocol::ResourceId, force: bool) {
        if !force {
            if let Some((_sub, bridge)) = self.subscriptions.get(id) {
                if bridge.is_finished() {
                    info!("Removing dead subscription for '{}'", id);
                    self.subscriptions.remove(id);
                } else {
                    return;
                }
            }
        } else {
            if let Some((_sub, handle)) = self.subscriptions.remove(id) {
                handle.abort();
            }
        }

        let is_builtin = protocol::ResourceId::from_alias(&id.plural).is_some();

        let effective_ns = if id.scope == protocol::ResourceScope::Cluster {
            protocol::Namespace::All
        } else {
            self.namespace.clone()
        };
        let key = QueryKey {
            context: self.context.clone(),
            namespace: effective_ns,
            resource: id.clone(),
        };
        info!("handle_subscribe_resource: key=({}, {}, {}), server_url={}",
            self.context.name, self.namespace, key.resource, self.context.server_url);

        let watcher_client = self.watcher_client();

        let server_headers = self.shared.column_cache.get(id)
            .map(|v| v.clone())
            .unwrap_or_default();

        // Background column discovery (non-blocking).
        if server_headers.is_empty() && is_builtin {
            let client = self.client.clone();
            let id_clone = id.clone();
            let cache = self.shared.column_cache.clone();
            let ns_arg = match &self.namespace {
                protocol::Namespace::Named(n) => Some(n.clone()),
                protocol::Namespace::All => None,
            };
            tokio::spawn(async move {
                if let Ok(cols) = table::fetch_table_columns(
                    &client, &id_clone.group, &id_clone.version, &id_clone.plural,
                    ns_arg.as_deref(),
                ).await {
                    cache.insert(id_clone, table::columns_to_headers(&cols, false));
                }
            });
        }

        if is_builtin {
            // Built-in resources: subscribe immediately (no HTTP discovery needed).
            let sub = if force {
                self.shared.watcher_cache.subscribe_force(key, &watcher_client, server_headers)
            } else {
                self.shared.watcher_cache.subscribe(key, &watcher_client, server_headers)
            };
            let bridge = self.spawn_bridge(sub.clone(), id.plural.clone(), id.clone());
            self.subscriptions.insert(id.clone(), (sub, bridge));
        } else {
            // CRD: discovery may require HTTP calls — spawn as background task.
            // Track in pending_discovery so we can discard if unsubscribed before completion.
            self.pending_discovery.insert(id.clone());
            let client = self.client.clone();
            let shared = self.shared.clone();
            let context = self.context.clone();
            let id_clone = id.clone();
            let event_tx = self.event_tx.clone();
            let sub_ready_tx = self.sub_ready_tx.clone();
            let key_clone = key.clone();
            let force_flag = force;

            // Spawn the slow discovery + subscription as a background task.
            // When done, it sends the subscription through the sub_ready channel
            // so the main loop can spawn the bridge and track it.
            tokio::spawn(async move {
                let (gvk, plural, scope) = if id_clone.group.is_empty() && id_clone.version.is_empty() {
                    match crate::kube::describe::resolve_api_resource(&client, &id_clone.plural).await {
                        Ok((ar, resolved_scope)) => {
                            let gvk = kube::api::GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
                            // Notify TUI of the resolved identity so it can update
                            // its nav and table keys (scope, group, version may differ).
                            let resolved_rid = crate::kube::protocol::ResourceId::new(
                                ar.group.clone(), ar.version.clone(),
                                ar.kind.clone(), ar.plural.clone(), resolved_scope,
                            );
                            let _ = event_tx.send(SessionEvent::ResourceResolved {
                                original: id_clone.clone(),
                                resolved: resolved_rid,
                            }).await;
                            (gvk, ar.plural, resolved_scope)
                        }
                        Err(e) => {
                            warn!("Failed to discover resource '{}': {}", id_clone.plural, e);
                            let _ = event_tx.send(SessionEvent::SubscriptionError {
                                resource: id_clone.clone(),
                                message: format!("Unknown resource: {}", id_clone.plural),
                            }).await;
                            return;
                        }
                    }
                } else {
                    let gvk = kube::api::GroupVersionKind::gvk(&id_clone.group, &id_clone.version, &id_clone.kind);
                    (gvk, id_clone.plural.clone(), id_clone.scope)
                };

                let printer_columns = shared.discovery_cache.get(&context)
                    .and_then(|entry| {
                        let (_ns, crds) = entry.value();
                        crds.iter()
                            .find(|c| c.group == id_clone.group && c.plural == id_clone.plural)
                            .map(|c| c.printer_columns.clone())
                    })
                    .unwrap_or_default();

                if force_flag {
                    shared.watcher_cache.remove(&key_clone);
                }
                // Subscribe — the watcher starts streaming immediately.
                let sub = shared.watcher_cache.subscribe_dynamic(
                    key_clone,
                    &client,
                    gvk,
                    plural,
                    scope,
                    printer_columns,
                );
                // Send the subscription back to the main loop for bridging and tracking.
                let _ = sub_ready_tx.send((id_clone, sub)).await;
            });
        }
    }

    pub(super) fn handle_unsubscribe(&mut self, id: &protocol::ResourceId) {
        // Cancel pending CRD discovery — if the task completes after this,
        // the main loop will discard it because the ID is no longer in pending_discovery.
        self.pending_discovery.remove(id);
        if let Some((_sub, handle)) = self.subscriptions.remove(id) {
            handle.abort();
        }
        // Also clean up the watcher cache entry so the watcher's grace period
        // starts immediately rather than being kept alive by a stale Weak ref.
        let key = QueryKey {
            context: self.context.clone(),
            namespace: if id.scope == protocol::ResourceScope::Cluster {
                protocol::Namespace::All
            } else {
                self.namespace.clone()
            },
            resource: id.clone(),
        };
        self.shared.watcher_cache.remove(&key);
    }

    // -----------------------------------------------------------------------
    // Namespace / context switching
    // -----------------------------------------------------------------------

    pub(super) async fn handle_switch_namespace(&mut self, namespace: protocol::Namespace) {
        self.namespace = namespace;
        self.handle_stop_logs();
        // Only re-subscribe namespaced resources. Cluster-scoped resources
        // (nodes, pvs, clusterroles, etc.) are unaffected by namespace changes.
        let active_ids: Vec<protocol::ResourceId> = self.subscriptions.keys().cloned().collect();
        let namespaced_ids: Vec<protocol::ResourceId> = active_ids.iter()
            .filter(|id| id.scope == protocol::ResourceScope::Namespaced)
            .cloned().collect();
        // Stop only namespaced bridges. Keep cluster-scoped ones running.
        for id in &namespaced_ids {
            if let Some((_sub, handle)) = self.subscriptions.remove(id) {
                handle.abort();
            }
        }
        // Re-subscribe namespaced resources with the new namespace.
        for id in namespaced_ids {
            self.handle_subscribe_resource(&id, false);
        }
    }

    pub(super) async fn handle_switch_context_resolved(
        &mut self,
        context: &str,
        init: InitParams,
    ) -> anyhow::Result<()> {
        let active_ids: Vec<protocol::ResourceId> = self.subscriptions.keys().cloned().collect();
        self.handle_stop_logs();
        if let Some(h) = self.metrics_task.take() { h.abort(); }
        self.stop_all();

        // Remove discovery cache entries by server_url for the old context.
        self.shared.discovery_cache.remove(&self.context.clone());

        // Set context to new name with empty server_url/auth (will be filled when client is ready).
        self.context = protocol::ContextId::new(context.to_string(), String::new(), init.user_name.clone());
        self.namespace = protocol::Namespace::All;

        // Phase 1: Try cache hits immediately.
        let ns_id = protocol::ResourceId::from_alias("namespaces").unwrap();
        let nodes_id = protocol::ResourceId::from_alias("nodes").unwrap();
        let mut cache_misses: Vec<protocol::ResourceId> = Vec::new();
        let all_ids: Vec<protocol::ResourceId> = {
            let mut v = vec![ns_id, nodes_id.clone()];
            for id in &active_ids {
                if id.plural != "namespaces" && id.plural != "nodes" {
                    v.push(id.clone());
                }
            }
            v
        };

        for id in &all_ids {
            let key = QueryKey {
                context: self.context.clone(),
                namespace: self.namespace.clone(),
                resource: id.clone(),
            };
            if let Some(sub) = self.shared.watcher_cache.try_get(&key) {
                let bridge = self.spawn_bridge(sub.clone(), id.plural.clone(), id.clone());
                self.subscriptions.insert(id.clone(), (sub, bridge));
            } else {
                cache_misses.push(id.clone());
            }
        }

        // Phase 2+3: Create client in background.
        if let Some(h) = self.pending_client_task.take() { h.abort(); }
        let (client_tx, client_rx) = tokio::sync::oneshot::channel();
        let shared = self.shared.clone();
        let ctx = context.to_string();
        let user = init.user_name.clone();
        let task = tokio::spawn(async move {
            let result = Self::create_client_from_init(&init, &shared).await;
            let _ = client_tx.send(PendingClientResult {
                context_name: ctx,
                user_name: user,
                client: result,
                cache_misses,
            });
        });
        self.pending_client = Some(client_rx);
        self.pending_client_task = Some(task);

        // Don't send ok:true yet — the client hasn't been created.
        // The TUI will get the real ContextSwitched result when the
        // background client creation completes (or fails).
        self.send_event(&SessionEvent::CommandResult {
            ok: true,
            message: format!("Switching to context: {}...", context),
        })
        .await?;

        Ok(())
    }

    pub(super) async fn handle_pending_client_result(
        &mut self,
        result: Result<PendingClientResult, tokio::sync::oneshot::error::RecvError>,
    ) {
        self.pending_client = None;
        self.pending_client_task = None;
        match result {
            Ok(pending) => {
                if pending.context_name != self.context.name {
                    info!("Discarding stale client for context '{}' (current: '{}')", pending.context_name, self.context.name);
                } else {
                    match pending.client {
                        Ok((new_client, new_config)) => {
                            info!("Background client ready for context '{}'", pending.context_name);
                            self.context = protocol::ContextId::new(
                                pending.context_name.clone(),
                                new_config.cluster_url.to_string(),
                                pending.user_name,
                            );
                            self.client = new_client;
                            self.client_config = Some(new_config);
                            for id in pending.cache_misses {
                                self.handle_subscribe_resource(&id, false);
                            }
                            if let Some(h) = self.metrics_task.take() { h.abort(); }
                            self.spawn_metrics_poller();
                            self.handle_get_discovery_async();
                            let _ = self.send_event(&SessionEvent::ContextSwitched {
                                context: self.context.name.clone(),
                                ok: true,
                                message: format!("Switched to context: {}", self.context.name),
                            }).await;
                        }
                        Err(e) => {
                            warn!("Background client creation failed: {}", e);
                            let _ = self.send_event(&SessionEvent::ContextSwitched {
                                context: self.context.name.clone(),
                                ok: false,
                                message: format!("Client creation failed: {}", e),
                            }).await;
                        }
                    }
                }
            }
            Err(_) => {
                warn!("Background client task dropped without result");
                let _ = self.send_event(&SessionEvent::ContextSwitched {
                    context: self.context.name.clone(),
                    ok: false,
                    message: "Client creation task failed unexpectedly".to_string(),
                }).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Describe / YAML
    // -----------------------------------------------------------------------

    pub(super) fn handle_describe_async(&self, resource_type: &str, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_describe_native(&client, &rt, &n, &ns, &context).await;
            let _ = tx.send(SessionEvent::DescribeResult(content)).await;
        });
    }

    pub(super) fn handle_yaml_async(&self, resource_type: &str, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_yaml_native(&client, &rt, &n, &ns, &context).await;
            let _ = tx.send(SessionEvent::YamlResult(content)).await;
        });
    }

    // -----------------------------------------------------------------------
    // Mutating operations
    // -----------------------------------------------------------------------

    pub(super) fn reject_if_readonly(&self) -> bool {
        if !self.readonly { return false; }
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(SessionEvent::CommandResult {
                ok: false,
                message: "Session is read-only".to_string(),
            }).await;
        });
        true
    }

    pub(super) fn handle_delete_async(&self, resource_type: &str, name: &str, namespace: &str) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = crate::kube::ops::execute_delete(&client, &rt, &n, &ns, &context).await;
            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Deleted {}/{}", rt, n),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Delete failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    pub(super) fn handle_scale_async(&self, resource_type: &str, name: &str, namespace: &str, replicas: u32) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = async {
                use ::kube::api::{Api, DynamicObject, Patch, PatchParams};
                use crate::kube::protocol::ResourceScope;

                let (ar, scope) =
                    crate::kube::describe::resolve_api_resource(&client, &rt).await?;
                let api: Api<DynamicObject> = match scope {
                    ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
                    ResourceScope::Namespaced if ns.is_empty() => {
                        Api::all_with(client.clone(), &ar)
                    }
                    ResourceScope::Namespaced => {
                        Api::namespaced_with(client.clone(), &ns, &ar)
                    }
                };
                let patch = crate::kube::ops::ScalePatch {
                    spec: crate::kube::ops::ScalePatchSpec { replicas },
                };
                api.patch(&n, &PatchParams::apply("k9rs"), &Patch::Merge(&patch))
                    .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;

            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Scaled {}/{} to {} replicas", rt, n, replicas),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Scale failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    pub(super) fn handle_restart_async(&self, resource_type: &str, name: &str, namespace: &str) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = crate::kube::ops::restart_via_patch(&client, &rt, &n, &ns).await;
            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Restarted {}/{}", rt, n),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Restart failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    // -----------------------------------------------------------------------
    // Secret decode
    // -----------------------------------------------------------------------

    pub(super) fn handle_decode_secret_async(&self, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            use k8s_openapi::api::core::v1::Secret;
            let api: kube::Api<Secret> = if ns.is_empty() {
                kube::Api::default_namespaced(client)
            } else {
                kube::Api::namespaced(client, &ns)
            };
            match api.get(&n).await {
                Ok(secret) => {
                    let mut output = format!("Secret: {}/{}\n\n", ns, n);
                    if let Some(data) = &secret.data {
                        for (key, value) in data {
                            let decoded = String::from_utf8_lossy(&value.0);
                            output.push_str(&format!("{}:\n  {}\n\n", key, decoded));
                        }
                    }
                    if let Some(string_data) = &secret.string_data {
                        for (key, value) in string_data {
                            output.push_str(&format!("{}:\n  {}\n\n", key, value));
                        }
                    }
                    if output.ends_with("\n\n") {
                        output.truncate(output.len() - 1);
                    }
                    let _ = tx.send(SessionEvent::DescribeResult(output)).await;
                }
                Err(e) => {
                    let _ = tx.send(SessionEvent::DescribeResult(
                        format!("Failed to decode secret: {}", e),
                    )).await;
                }
            }
        });
    }

    // -----------------------------------------------------------------------
    // CronJob trigger / suspend
    // -----------------------------------------------------------------------

    pub(super) fn handle_trigger_cronjob_async(&self, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            use k8s_openapi::api::batch::v1::{CronJob, Job};
            let cj_api: kube::Api<CronJob> = kube::Api::namespaced(client.clone(), &ns);
            match cj_api.get(&n).await {
                Ok(cj) => {
                    let job_name = format!("{}-manual-{}", n, chrono::Utc::now().format("%s"));
                    let template = cj.spec.and_then(|s| Some(s.job_template));
                    let job_spec = template.and_then(|t| t.spec).unwrap_or_default();
                    let job = Job {
                        metadata: kube::api::ObjectMeta {
                            name: Some(job_name.clone()),
                            namespace: Some(ns.clone()),
                            ..Default::default()
                        },
                        spec: Some(job_spec),
                        ..Default::default()
                    };
                    let job_api: kube::Api<Job> = kube::Api::namespaced(client, &ns);
                    match job_api.create(&kube::api::PostParams::default(), &job).await {
                        Ok(_) => {
                            let _ = tx.send(SessionEvent::CommandResult {
                                ok: true,
                                message: format!("Triggered job: {}", job_name),
                            }).await;
                        }
                        Err(e) => {
                            let _ = tx.send(SessionEvent::CommandResult {
                                ok: false,
                                message: format!("Failed to trigger CronJob: {}", e),
                            }).await;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(SessionEvent::CommandResult {
                        ok: false,
                        message: format!("Failed to get CronJob: {}", e),
                    }).await;
                }
            }
        });
    }

    pub(super) fn handle_suspend_cronjob_async(&self, name: &str, namespace: &str, suspend: bool) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            use k8s_openapi::api::batch::v1::CronJob;
            let api: kube::Api<CronJob> = kube::Api::namespaced(client, &ns);
            let patch = crate::kube::ops::SuspendPatch {
                spec: crate::kube::ops::SuspendPatchSpec { suspend },
            };
            match api.patch(&n, &kube::api::PatchParams::default(), &kube::api::Patch::Merge(&patch)).await {
                Ok(_) => {
                    let action = if suspend { "Suspended" } else { "Resumed" };
                    let _ = tx.send(SessionEvent::CommandResult {
                        ok: true,
                        message: format!("{} CronJob: {}", action, n),
                    }).await;
                }
                Err(e) => {
                    let _ = tx.send(SessionEvent::CommandResult {
                        ok: false,
                        message: format!("Failed to update CronJob: {}", e),
                    }).await;
                }
            }
        });
    }
}
