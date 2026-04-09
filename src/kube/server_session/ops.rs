//! Subscribe/unsubscribe, namespace/context switching, describe/YAML,
//! and mutating operations (delete, scale, restart, secret decode,
//! CronJob trigger/suspend).

use std::sync::Arc;

use tracing::{info, warn};

use crate::kube::live_query::QueryKey;
use crate::kube::protocol::{self, SessionEvent};
use crate::kube::table;

use crate::kube::capabilities;
use super::{InitParams, PendingClientResult, ServerSession};

impl ServerSession {
    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    pub(super) fn handle_subscribe_resource(&mut self, id: &protocol::ResourceId, force: bool, _filter: Option<protocol::SubscriptionFilter>) {
        // Local (daemon-owned) resources go through a separate path — no
        // watcher cache, no filter negotiation, no column discovery.
        if id.is_local() {
            self.handle_subscribe_local_resource(id, force);
            return;
        }

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

        // `from_alias` will now also match local aliases (e.g. "pf"). Guard
        // against that: if the alias resolves to a local rid, it's not a
        // built-in K8s type from the perspective of column discovery.
        let is_builtin = protocol::ResourceId::from_alias(&id.plural)
            .map(|rid| !rid.is_local())
            .unwrap_or(false);

        let effective_ns = if id.scope == protocol::ResourceScope::Cluster {
            protocol::Namespace::All
        } else {
            self.namespace.clone()
        };
        let key = QueryKey {
            context: self.context.clone(),
            namespace: effective_ns,
            resource: id.clone(),
            filter: _filter.clone(),
        };
        info!("handle_subscribe_resource: key=({}, {}, {}, filter={:?}), server_url={}",
            self.context.name, self.namespace, key.resource, key.filter, self.context.server_url);

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
            let bridge = self.spawn_bridge(sub.clone(), id.plural.clone(), id.clone(), _filter.clone());
            self.subscriptions.insert(id.clone(), (sub, bridge));
            // Emit capabilities so the TUI knows which operations this resource supports.
            self.emit_capabilities(id);
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

    /// Build and send a `ResourceCapabilities` event for the given resource.
    /// Uses `ResourceTypeMeta` as the source of truth for built-in types;
    /// CRDs get the always-on operations only. Local resources emit their
    /// capabilities directly from their `LocalResourceSource` impl via
    /// `handle_subscribe_local_resource`, so this is a no-op for them.
    pub(super) fn emit_capabilities(&self, id: &protocol::ResourceId) {
        if id.is_local() {
            return;
        }
        let caps = capabilities::for_k8s(&id.plural);
        let tx = self.event_tx.clone();
        let resource = id.clone();
        tokio::spawn(async move {
            let _ = tx.send(SessionEvent::ResourceCapabilities { resource, capabilities: caps }).await;
        });
    }

    /// Subscribe to a local (daemon-owned) resource. Bypasses the watcher
    /// cache entirely — local sources are always alive and shared across
    /// sessions. Each session gets its own bridge task that forwards the
    /// source's snapshot stream to the client.
    pub(super) fn handle_subscribe_local_resource(
        &mut self,
        id: &protocol::ResourceId,
        force: bool,
    ) {
        // Reuse existing bridge if present.
        if !force {
            if let Some(handle) = self.local_subscriptions.get(id) {
                if !handle.is_finished() {
                    return;
                }
            }
        }
        if let Some(old) = self.local_subscriptions.remove(id) {
            old.abort();
        }

        let Some(source) = self.shared.local_registry.get(&self.context.name, id) else {
            let tx = self.event_tx.clone();
            let resource = id.clone();
            tokio::spawn(async move {
                let _ = tx.send(SessionEvent::SubscriptionError {
                    resource,
                    message: "Unknown local resource".into(),
                }).await;
            });
            return;
        };

        // Hand the bridge a strong `Arc` keepalive via `LocalSubscription`.
        // The subscription's `Drop` impl spawns a grace-period task that
        // holds the source for a few minutes, so a quick context-switch
        // away and back reuses the same source.
        let snapshot_rx = source.subscribe();
        let sub = crate::kube::local::LocalSubscription::new(
            id.clone(),
            snapshot_rx,
            Arc::clone(&source),
        );
        let tx = self.event_tx.clone();
        let rt = id.plural.clone();
        let bridge = tokio::spawn(async move {
            super::bridge_local_subscription_to_events(sub, rt, tx).await;
        });
        self.local_subscriptions.insert(id.clone(), bridge);

        // Emit the source's declared capabilities so the TUI knows which
        // operations this resource supports.
        let caps = source.capabilities();
        let tx = self.event_tx.clone();
        let resource = id.clone();
        tokio::spawn(async move {
            let _ = tx.send(SessionEvent::ResourceCapabilities { resource, capabilities: caps }).await;
        });
    }

    /// Delete a logical entry on a local resource. Looks up the source by
    /// resource id, dispatches to its `delete` method, and emits a
    /// `CommandResult`. No readonly check — local resources are per-daemon
    /// state, not cluster state, and readonly applies to K8s mutations.
    pub(super) fn handle_delete_local(&self, obj: &protocol::ObjectRef) {
        let tx = self.event_tx.clone();
        let Some(source) = self.shared.local_registry.get(&self.context.name, &obj.resource) else {
            tokio::spawn(async move {
                let _ = tx.send(SessionEvent::CommandResult {
                    ok: false,
                    message: "Unknown local resource".into(),
                }).await;
            });
            return;
        };
        let result = source.delete(&obj.name);
        let name = obj.name.clone();
        tokio::spawn(async move {
            let event = match result {
                Ok(()) => SessionEvent::CommandResult { ok: true, message: format!("Stopped {name}") },
                Err(e) => SessionEvent::CommandResult { ok: false, message: e },
            };
            let _ = tx.send(event).await;
        });
    }

    pub(super) fn handle_unsubscribe(&mut self, id: &protocol::ResourceId) {
        // Local unsubscribe: drop the bridge, source stays alive.
        if id.is_local() {
            if let Some(handle) = self.local_subscriptions.remove(id) {
                handle.abort();
            }
            return;
        }
        // Cancel pending CRD discovery — if the task completes after this,
        // the main loop will discard it because the ID is no longer in pending_discovery.
        self.pending_discovery.remove(id);
        if let Some((_sub, handle)) = self.subscriptions.remove(id) {
            handle.abort();
        }
        // Also clean up the watcher cache entry so the watcher's grace period
        // starts immediately rather than being kept alive by a stale Weak ref.
        // Remove both filtered and unfiltered cache entries for this resource.
        // TODO: if we track filters per subscription, remove the specific one.
        let key = QueryKey {
            context: self.context.clone(),
            namespace: if id.scope == protocol::ResourceScope::Cluster {
                protocol::Namespace::All
            } else {
                self.namespace.clone()
            },
            resource: id.clone(),
            filter: None,
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
            self.handle_subscribe_resource(&id, false, None);
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
                filter: None,
            };
            if let Some(sub) = self.shared.watcher_cache.try_get(&key) {
                let bridge = self.spawn_bridge(sub.clone(), id.plural.clone(), id.clone(), None);
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
                                self.handle_subscribe_resource(&id, false, None);
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

    pub(super) fn handle_describe_async(&self, target: &protocol::ObjectRef) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = target.resource.plural.clone();
        let n = target.name.clone();
        // `Namespace::All` (the cross-namespace TUI view) means "no -n flag";
        // pass the empty string to the downstream kubectl/native helper, which
        // interprets that as "default namespace for namespaced resources" or
        // "cluster-scoped — ignore the field". Avoids the historical bug
        // where `namespace.display()` returned the literal string "all".
        let ns = ns_for_kubectl(&target.namespace);
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_describe_native(&client, &rt, &n, &ns, &context).await;
            let _ = tx.send(SessionEvent::DescribeResult(content)).await;
        });
    }

    pub(super) fn handle_yaml_async(&self, target: &protocol::ObjectRef) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = target.resource.plural.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    pub(super) fn handle_delete_async(&self, target: &protocol::ObjectRef) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let rt = target.resource.plural.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    pub(super) fn handle_scale_async(&self, target: &protocol::ObjectRef, replicas: u32) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = target.resource.plural.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    pub(super) fn handle_restart_async(&self, target: &protocol::ObjectRef) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = target.resource.plural.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    pub(super) fn handle_decode_secret_async(&self, target: &protocol::ObjectRef) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    pub(super) fn handle_trigger_cronjob_async(&self, target: &protocol::ObjectRef) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
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

    /// Toggle the suspend state of a CronJob. The server reads the current
    /// state from K8s and flips it.
    pub(super) fn handle_toggle_suspend_cronjob_async(&self, target: &protocol::ObjectRef) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);
        tokio::spawn(async move {
            use k8s_openapi::api::batch::v1::CronJob;
            let api: kube::Api<CronJob> = kube::Api::namespaced(client, &ns);
            // Read current state.
            let current = match api.get(&n).await {
                Ok(cj) => cj.spec.and_then(|s| s.suspend).unwrap_or(false),
                Err(e) => {
                    let _ = tx.send(SessionEvent::CommandResult {
                        ok: false,
                        message: format!("Failed to read CronJob: {}", e),
                    }).await;
                    return;
                }
            };
            let new_state = !current;
            let patch = crate::kube::ops::SuspendPatch {
                spec: crate::kube::ops::SuspendPatchSpec { suspend: new_state },
            };
            match api.patch(&n, &kube::api::PatchParams::default(), &kube::api::Patch::Merge(&patch)).await {
                Ok(_) => {
                    let action = if new_state { "Suspended" } else { "Resumed" };
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

    // -----------------------------------------------------------------------
    // Port-forwarding
    // -----------------------------------------------------------------------

    /// Create a new port-forward by delegating to the shared
    /// `PortForwardSource`. State transitions and row snapshots flow through
    /// the source's `watch::Sender` — any TUI session subscribed to the
    /// port-forwards table sees them automatically via the normal
    /// Subscribe/Snapshot pipeline.
    pub(super) fn handle_port_forward(
        &mut self,
        target: &crate::kube::protocol::ObjectRef,
        local_port: u16,
        container_port: u16,
    ) {
        let kubectl_target = resolve_kubectl_pf_target(target);
        let namespace = target.namespace.display().to_string();

        // Get-or-create the per-context PortForwardSource. This may be the
        // very first PF on this context (constructs a fresh source) or
        // reuse one a sibling session is already keeping alive.
        let source = self.shared.local_registry.port_forwards_for(&self.context.name);
        let _pf_id = source.create(
            crate::kube::local::port_forward::PortForwardRequest {
                target: target.clone(),
                kubectl_target,
                namespace,
                local_port,
                remote_port: container_port,
            },
        );
    }
}

/// Map a typed `Namespace` to the string the downstream kubectl/native
/// helpers expect: empty string for `All` (the helpers interpret this as
/// "no -n flag" / "default namespace" / "cluster-scoped, ignore"), and
/// the named namespace otherwise. Replaces the historical
/// `namespace.display()` round-trip which leaked the literal string `"all"`
/// down into the kubectl call path.
fn ns_for_kubectl(ns: &crate::kube::protocol::Namespace) -> String {
    match ns.as_option() {
        Some(name) => name.to_string(),
        None => String::new(),
    }
}

/// Resolve an ObjectRef to a kubectl port-forward target string.
/// Maps resource types to their kubectl shorthand (e.g., "pod/name", "svc/name").
fn resolve_kubectl_pf_target(target: &crate::kube::protocol::ObjectRef) -> String {
    let prefix = match target.resource.plural.as_str() {
        "services" => "svc",
        "deployments" => "deploy",
        "statefulsets" => "sts",
        "pods" => "pod",
        "daemonsets" => "ds",
        "replicasets" => "rs",
        _ => &target.resource.plural,
    };
    format!("{}/{}", prefix, target.name)
}

