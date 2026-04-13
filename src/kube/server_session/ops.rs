//! Namespace switching, describe/YAML, and mutating operations (delete,
//! scale, restart, secret decode, CronJob trigger/suspend).


use crate::kube::protocol::{self, SessionEvent};

use super::ServerSession;

impl ServerSession {
    // handle_subscribe_resource, emit_capabilities, handle_subscribe_local_resource,
    // handle_unsubscribe — all deleted. Subscriptions are now per-yamux-substream.

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

    /// Apply edited YAML to a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`. Mirrors the K8s `Apply`
    /// path so the client can use one wire flow for both.
    pub(super) fn handle_apply_local(&self, obj: &protocol::ObjectRef, yaml: &str) {
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
        let result = source.apply_yaml(&obj.name, yaml);
        tokio::spawn(async move {
            let event = match result {
                Ok(message) => SessionEvent::CommandResult { ok: true, message },
                Err(e) => SessionEvent::CommandResult { ok: false, message: e },
            };
            let _ = tx.send(event).await;
        });
    }

    /// Apply edited YAML to a K8s resource via kube-rs server-side apply.
    /// Spawns the work so the session loop stays responsive.
    pub(super) fn handle_apply_async(&self, target: &protocol::ObjectRef, yaml: String) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let plural = target.resource.plural.clone();
        let name = target.name.clone();
        let ns = ns_for_kubectl(&target.namespace);

        tokio::spawn(async move {
            let result: anyhow::Result<()> = async {
                use ::kube::api::{Api, DynamicObject, Patch, PatchParams};
                use crate::kube::protocol::ResourceScope;

                // Resolve the API descriptor (group/version/kind/scope) so we
                // can build a `DynamicObject` API for any resource type —
                // built-in or CRD — without statically-typed wrappers.
                let (ar, scope) = crate::kube::describe::resolve_api_resource(&client, &plural).await?;
                let api: Api<DynamicObject> = match scope {
                    ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
                    ResourceScope::Namespaced if ns.is_empty() => {
                        Api::default_namespaced_with(client.clone(), &ar)
                    }
                    ResourceScope::Namespaced => {
                        Api::namespaced_with(client.clone(), &ns, &ar)
                    }
                };

                // Parse the user's YAML into an untyped `DynamicObject` so
                // server-side apply can handle any kind. The user is editing
                // the same shape kube-rs would have produced via
                // `kubectl get -o yaml`, so this round-trips cleanly.
                let parsed: DynamicObject = serde_yaml::from_str(&yaml)
                    .map_err(|e| anyhow::anyhow!("yaml parse error: {}", e))?;

                api.patch(&name, &PatchParams::apply("k9rs").force(), &Patch::Apply(&parsed))
                    .await?;
                Ok(())
            }
            .await;

            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Applied {}/{}", plural, name),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Apply failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    // -----------------------------------------------------------------------
    // Namespace / context switching
    // -----------------------------------------------------------------------

    pub(super) async fn handle_switch_namespace(&mut self, namespace: protocol::Namespace) {
        self.namespace = namespace;
        // With yamux substreams, the TUI manages subscription lifecycle.
        // The TUI closes its substreams (via `clear_namespaced_caches` +
        // dropping the nav step streams) and opens new ones for the new
        // namespace. The server's role here is just to record the namespace
        // so new subscription substreams use the right namespace.
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
                        Api::default_namespaced_with(client.clone(), &ar)
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
            let cj_api: kube::Api<CronJob> = if ns.is_empty() {
                kube::Api::default_namespaced(client.clone())
            } else {
                kube::Api::namespaced(client.clone(), &ns)
            };
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
                    let job_api: kube::Api<Job> = if ns.is_empty() {
                        kube::Api::default_namespaced(client)
                    } else {
                        kube::Api::namespaced(client, &ns)
                    };
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
            let api: kube::Api<CronJob> = if ns.is_empty() {
                kube::Api::default_namespaced(client)
            } else {
                kube::Api::namespaced(client, &ns)
            };
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
        "jobs" => "job",
        _ => &target.resource.plural,
    };
    format!("{}/{}", prefix, target.name)
}

