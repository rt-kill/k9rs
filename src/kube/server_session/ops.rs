//! Namespace switching, describe/YAML, and mutating operations (delete,
//! scale, restart, secret decode, CronJob trigger/suspend).

/// Field manager identity for server-side apply and strategic merge patches.
/// Passed to `PatchParams::apply(...)` so K8s attributes the change to k9rs.
const MANAGER_IDENTITY: &str = "k9rs";

use crate::kube::protocol::{self, SessionEvent};
use crate::kube::resource_def::BuiltInKind;

use super::ServerSession;

impl ServerSession {
    // handle_subscribe_resource, emit_capabilities, handle_subscribe_local_resource,
    // handle_unsubscribe — all deleted. Subscriptions are now per-yamux-substream.

    /// Delete a logical entry on a local resource. Looks up the source by
    /// resource id, dispatches to its `delete` method, and emits a
    /// `CommandResult`. Readonly is gated once at the top of
    /// `handle_command` via `SessionCommand::is_mutating`, so this handler
    /// (like every other mutating handler) doesn't re-check.
    pub(super) fn handle_delete_local(&mut self, obj: &protocol::ObjectRef) {
        let tx = self.event_tx.clone();
        let Some(source) = self.shared.local_registry.get(&self.context.name, &obj.resource) else {
            self.track_task(async move {
                let _ = tx.send(SessionEvent::CommandResult(Err(
                    "Unknown local resource".into(),
                ))).await;
            });
            return;
        };
        let result = source.delete(&obj.name);
        let name = obj.name.clone();
        self.track_task(async move {
            let event = match result {
                Ok(()) => SessionEvent::CommandResult(Ok(format!("Stopped {name}"))),
                Err(e) => SessionEvent::CommandResult(Err(e)),
            };
            let _ = tx.send(event).await;
        });
    }

    /// Apply edited YAML to a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`. Mirrors the K8s `Apply`
    /// path so the client can use one wire flow for both.
    pub(super) fn handle_apply_local(&mut self, obj: &protocol::ObjectRef, yaml: &str) {
        let tx = self.event_tx.clone();
        let Some(source) = self.shared.local_registry.get(&self.context.name, &obj.resource) else {
            self.track_task(async move {
                let _ = tx.send(SessionEvent::CommandResult(Err(
                    "Unknown local resource".into(),
                ))).await;
            });
            return;
        };
        let result = source.apply_yaml(&obj.name, yaml);
        self.track_task(async move {
            let event = match result {
                Ok(message) => SessionEvent::CommandResult(Ok(message)),
                Err(e) => SessionEvent::CommandResult(Err(e)),
            };
            let _ = tx.send(event).await;
        });
    }

    /// Apply edited YAML to a K8s resource via kube-rs server-side apply.
    /// Spawns the work so the session loop stays responsive. Routes through
    /// the typed `api_resource_for(target.resource)` so built-ins skip the
    /// discovery HTTP roundtrip and CRDs use their stored GVR if populated.
    pub(super) fn handle_apply_async(&mut self, target: &protocol::ObjectRef, yaml: String) {
        if self.reject_if_namespace_unresolved(target, "Apply") { return; }
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let target = target.clone();
        let display = target.kubectl_target();

        self.track_task(async move {
            let result: anyhow::Result<()> = async {
                use ::kube::api::{DynamicObject, Patch, PatchParams};

                let (ar, scope) =
                    crate::kube::describe::api_resource_for(&client, &target.resource).await?;
                let api = crate::kube::describe::dynamic_api_for(&client, &ar, scope, &target.namespace);

                // Parse the user's YAML into an untyped `DynamicObject` so
                // server-side apply can handle any kind. The user is editing
                // the same shape kube-rs would have produced via
                // `kubectl get -o yaml`, so this round-trips cleanly.
                let parsed: DynamicObject = serde_yaml::from_str(&yaml)
                    .map_err(|e| anyhow::anyhow!("yaml parse error: {}", e))?;

                api.patch(&target.name, &PatchParams::apply(MANAGER_IDENTITY).force(), &Patch::Apply(&parsed))
                    .await?;
                Ok(())
            }
            .await;

            let event = match result {
                Ok(()) => SessionEvent::CommandResult(Ok(format!("Applied {}", display))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Apply failed: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    // -----------------------------------------------------------------------
    // Describe / YAML
    // -----------------------------------------------------------------------

    pub(super) fn handle_describe_async(&mut self, target: &protocol::ObjectRef) {
        self.fetch_and_emit(
            target,
            |c, t, ctx| Box::pin(crate::kube::describe::fetch_describe(c, t, ctx)),
            |target, content| SessionEvent::DescribeResult { target, content },
        );
    }

    pub(super) fn handle_yaml_async(&mut self, target: &protocol::ObjectRef) {
        self.fetch_and_emit(
            target,
            |c, t, ctx| Box::pin(crate::kube::describe::fetch_yaml(c, t, ctx)),
            |target, content| SessionEvent::YamlResult { target, content },
        );
    }

    /// Shared helper for describe/yaml: clones the session state, spawns a
    /// task that calls `fetch_fn`, wraps the result via `make_event`, and
    /// sends it back on the event channel.
    fn fetch_and_emit<F, E>(
        &mut self,
        target: &protocol::ObjectRef,
        fetch_fn: F,
        make_event: E,
    )
    where
        F: for<'a> FnOnce(
                &'a kube::Client,
                &'a protocol::ObjectRef,
                &'a protocol::ContextName,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = String> + Send + 'a>>
            + Send
            + 'static,
        E: FnOnce(protocol::ObjectRef, String) -> SessionEvent + Send + 'static,
    {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let target = target.clone();
        self.track_task(async move {
            let content = fetch_fn(&client, &target, &context).await;
            let _ = tx.send(make_event(target, content)).await;
        });
    }

    // -----------------------------------------------------------------------
    // Mutating operations
    // -----------------------------------------------------------------------

    /// Refuse a mutating operation if the target lacks a resolved namespace.
    /// `Namespace::All` on a target that's supposed to be a specific object
    /// means the row lacked a namespace string — usually data corruption.
    /// Falling through would silently run the operation in the default
    /// namespace, mutating the wrong resource. All mutating handlers call
    /// this first, same pattern as `reject_if_readonly`.
    ///
    /// Cluster-scoped resources (Node, CRD, ClusterRole) legitimately have
    /// `Namespace::All`, so this helper only applies to namespaced targets.
    /// Handlers opt in by checking their own scope first.
    pub(super) fn reject_if_namespace_unresolved(
        &mut self,
        target: &protocol::ObjectRef,
        action: &str,
    ) -> bool {
        // Cluster-scoped targets are legitimately Namespace::All.
        if target.resource.is_cluster_scoped() {
            return false;
        }
        if target.namespace.as_option().is_none() {
            self.reject_async(format!(
                "{} refused: {} has no resolved namespace",
                action, target.kubectl_target(),
            ));
            return true;
        }
        false
    }

    /// Resolve the target's namespace for an op that NEEDS the string
    /// (typed `kube::Api::namespaced` constructors, `kubectl -n` args).
    /// Replaces the `reject_if_namespace_unresolved(...)` + later
    /// `target.namespace.as_option().expect("namespace already validated")`
    /// pair: instead of a runtime check followed by an `.expect` that trusts
    /// the check, we destructure once and bind the name or reject.
    pub(super) fn resolve_namespace_or_reject(
        &mut self,
        target: &protocol::ObjectRef,
        action: &str,
    ) -> Option<String> {
        // Cluster-scoped targets don't belong in handlers that need a
        // namespace (force-kill, decode, cron trigger/toggle) — surface
        // a specific message rather than "no resolved namespace".
        if target.resource.is_cluster_scoped() {
            self.reject_async(format!(
                "{} refused: {} is cluster-scoped",
                action, target.kubectl_target(),
            ));
            return None;
        }
        let protocol::Namespace::Named(n) = &target.namespace else {
            self.reject_async(format!(
                "{} refused: {} has no resolved namespace",
                action, target.kubectl_target(),
            ));
            return None;
        };
        Some(n.clone())
    }

    /// Spawn a fire-and-forget task that emits a failing `CommandResult`
    /// with the given message. Used by the synchronous capability gates in
    /// the mutating handlers (e.g. "this resource is not scaleable").
    pub(super) fn reject_async(&mut self, message: String) {
        let tx = self.event_tx.clone();
        self.track_task(async move {
            let _ = tx.send(reject(message)).await;
        });
    }

    /// Capability gate: extracts the target's [`BuiltInKind`] and asserts
    /// it supports the given [`OperationKind`]. On failure, emits a typed
    /// rejection and returns `None` so the caller can `return` cleanly.
    /// Shared between scale/restart/decode-secret/trigger/toggle handlers.
    pub(super) fn require_capability(
        &mut self,
        target: &protocol::ObjectRef,
        required_op: protocol::OperationKind,
        action: &str,
    ) -> Option<BuiltInKind> {
        let Some(kind) = target.resource.built_in_kind() else {
            self.reject_async(format!("{} not supported on {}", action, target.resource.plural()));
            return None;
        };
        let def = crate::kube::resource_defs::REGISTRY.by_kind(kind);
        if !def.operations().contains(&required_op) {
            self.reject_async(format!("{} not supported on {}", action, def.gvr().plural));
            return None;
        }
        Some(kind)
    }

    pub(super) fn handle_delete_async(&mut self, target: &protocol::ObjectRef) {
        if self.reject_if_namespace_unresolved(target, "Delete") { return; }
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let target = target.clone();
        let display = target.kubectl_target();
        self.track_task(async move {
            let result = crate::kube::ops::execute_delete(&client, &target, &context).await;
            let event = match result {
                Ok(()) => SessionEvent::CommandResult(Ok(format!("Deleted {}", display))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Delete failed: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    /// Force-delete a pod via the kube-rs API (grace_period_seconds=0,
    /// background propagation). **Pod-only** — the typed `kube::Api<Pod>`
    /// used below is load-bearing (any other workload kind would decode the
    /// wrong schema), so the kind check is a true precondition, not a
    /// capability flag. `ResourceDef::operations()` produces `ForceKill` from
    /// the same `BuiltInKind::Pod` check, so the server gate and the
    /// client-side capability manifest can't drift.
    pub(super) fn handle_force_kill_async(&mut self, target: &protocol::ObjectRef) {
        if target.resource.built_in_kind() != Some(BuiltInKind::Pod) {
            self.reject_async(format!("Force-kill is pod-only (got {})", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(target, "Force-kill") else { return; };
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let target = target.clone();
        let display = target.kubectl_target();
        let name = target.name.clone();
        self.track_task(async move {
            use ::kube::api::DeleteParams;
            let api: kube::Api<k8s_openapi::api::core::v1::Pod> = kube::Api::namespaced(client, &ns);
            let dp = DeleteParams {
                grace_period_seconds: Some(0),
                propagation_policy: Some(::kube::api::PropagationPolicy::Background),
                ..DeleteParams::default()
            };
            let event = match api.delete(&name, &dp).await {
                Ok(_) => SessionEvent::CommandResult(Ok(format!("Force-killed {}", display))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Force-kill failed: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    pub(super) fn handle_scale_async(&mut self, target: &protocol::ObjectRef, replicas: u32) {
        if self.reject_if_namespace_unresolved(target, "Scale") { return; }
        let Some(kind) = self.require_capability(
            target,
            protocol::OperationKind::Scale,
            "Scale",
        ) else { return; };
        let patch_body = ScalePatch { spec: ScaleSpec { replicas } };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let namespace = target.namespace.clone();
        let display = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().plural;
        self.track_task(async move {
            use ::kube::api::{Patch, PatchParams};
            let api = dynamic_api_for_kind(&client, kind, &namespace);
            let event = match api.patch(&n, &PatchParams::apply(MANAGER_IDENTITY), &Patch::Merge(&patch_body)).await {
                Ok(_) => SessionEvent::CommandResult(Ok(format!("Scaled {}/{} to {} replicas", display, n, replicas))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Scale failed: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    pub(super) fn handle_restart_async(&mut self, target: &protocol::ObjectRef) {
        if self.reject_if_namespace_unresolved(target, "Restart") { return; }

        let Some(kind) = self.require_capability(
            target,
            protocol::OperationKind::Restart,
            "Restart",
        ) else { return; };
        let patch_body = RestartPatch {
            spec: RestartSpec {
                template: RestartTemplate {
                    metadata: RestartMetadata {
                        annotations: RestartAnnotations {
                            restarted_at: chrono::Utc::now().to_rfc3339(),
                        },
                    },
                },
            },
        };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let namespace = target.namespace.clone();
        let display: &'static str = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().plural;
        self.track_task(async move {
            use ::kube::api::{Patch, PatchParams};
            let api = dynamic_api_for_kind(&client, kind, &namespace);
            let event = match api.patch(&n, &PatchParams::default(), &Patch::Merge(&patch_body)).await {
                Ok(_) => SessionEvent::CommandResult(Ok(format!("Restarted {}/{}", display, n))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Restart failed: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    // -----------------------------------------------------------------------
    // Secret decode
    // -----------------------------------------------------------------------

    pub(super) fn handle_decode_secret_async(&mut self, target: &protocol::ObjectRef) {
        // Typed gate: only built-ins can be `SecretLike` — non-Secret resources
        // get a friendly rejection in the describe panel.
        let target_owned = target.clone();
        let Some(kind) = target.resource.built_in_kind() else {
            let tx = self.event_tx.clone();
            let plural = target.resource.plural().to_string();
            self.track_task(async move {
                let _ = tx.send(SessionEvent::DescribeResult {
                    target: target_owned,
                    content: format!("Decode not supported on {}", plural),
                }).await;
            });
            return;
        };
        let def = crate::kube::resource_defs::REGISTRY.by_kind(kind);
        if !def.operations().contains(&protocol::OperationKind::DecodeSecret) {
            let tx = self.event_tx.clone();
            let plural = def.gvr().plural;
            self.track_task(async move {
                let _ = tx.send(SessionEvent::DescribeResult {
                    target: target_owned,
                    content: format!("{} is not a secret-like resource", plural),
                }).await;
            });
            return;
        }

        let Some(ns) = self.resolve_namespace_or_reject(target, "Decode") else { return; };
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        self.track_task(async move {
            use k8s_openapi::api::core::v1::Secret;
            let api: kube::Api<Secret> = kube::Api::namespaced(client, &ns);
            let content = match api.get(&n).await {
                Ok(secret) => decode_secret_to_text(&secret),
                Err(e) => format!("Failed to decode secret: {}", e),
            };
            let _ = tx.send(SessionEvent::DescribeResult {
                target: target_owned,
                content,
            }).await;
        });
    }

    // -----------------------------------------------------------------------
    // CronJob trigger / suspend
    // -----------------------------------------------------------------------

    pub(super) fn handle_trigger_cronjob_async(&mut self, target: &protocol::ObjectRef) {
        // Typed gate: CronJob-specific. The handler body uses
        // `kube::Api<CronJob>` / `kube::Api<Job>` directly, so the kind
        // check is load-bearing: a future workload kind that wants a
        // "manual trigger" would decode the wrong schema.
        if target.resource.built_in_kind() != Some(BuiltInKind::CronJob) {
            self.reject_async(format!("Trigger not supported on {}", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(target, "Trigger") else { return; };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        self.track_task(async move {
            use k8s_openapi::api::batch::v1::{CronJob, Job};

            let cj_api: kube::Api<CronJob> = kube::Api::namespaced(client.clone(), &ns);
            let cj = match cj_api.get(&n).await {
                Ok(cj) => cj,
                Err(e) => {
                    let _ = tx.send(reject(format!("Failed to get CronJob: {}", e))).await;
                    return;
                }
            };

            let job = build_trigger_job(&cj, &ns);
            let job_name = job.metadata.name.clone().unwrap_or_default();

            let job_api: kube::Api<Job> = kube::Api::namespaced(client, &ns);
            let event = match job_api.create(&kube::api::PostParams::default(), &job).await {
                Ok(_) => SessionEvent::CommandResult(Ok(format!("Triggered job: {}", job_name))),
                Err(e) => SessionEvent::CommandResult(Err(format!("Failed to trigger CronJob: {}", e))),
            };
            let _ = tx.send(event).await;
        });
    }

    /// Toggle the suspend state of a CronJob. The `toggle_cron_suspend`
    /// free function below reads the current state and produces both the
    /// next state and the merge-patch body. (Previously a `CronLike` trait
    /// method, but all cron-likes behave the same way so it lives inline.)
    pub(super) fn handle_toggle_suspend_cronjob_async(&mut self, target: &protocol::ObjectRef) {
        if target.resource.built_in_kind() != Some(BuiltInKind::CronJob) {
            self.reject_async(format!("Toggle suspend not supported on {}", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(target, "Toggle suspend") else { return; };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        self.track_task(async move {
            use k8s_openapi::api::batch::v1::CronJob;

            let api: kube::Api<CronJob> = kube::Api::namespaced(client, &ns);
            let cj = match api.get(&n).await {
                Ok(cj) => cj,
                Err(e) => {
                    let _ = tx.send(reject(format!("Failed to read CronJob: {}", e))).await;
                    return;
                }
            };

            let (new_state, patch_body) = toggle_cron_suspend(&cj);
            let event = match api.patch(&n, &kube::api::PatchParams::default(), &kube::api::Patch::Merge(&patch_body)).await {
                Ok(_) => {
                    let action = if new_state { "Suspended" } else { "Resumed" };
                    SessionEvent::CommandResult(Ok(format!("{} CronJob: {}", action, n)))
                }
                Err(e) => SessionEvent::CommandResult(Err(format!("Failed to update CronJob: {}", e))),
            };
            let _ = tx.send(event).await;
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
        if self.reject_if_namespace_unresolved(target, "Port-forward") { return; }
        let kubectl_target = target.kubectl_target();

        // Get-or-create the per-context PortForwardSource. This may be the
        // very first PF on this context (constructs a fresh source) or
        // reuse one a sibling session is already keeping alive.
        let source = self.shared.local_registry.port_forwards_for(&self.context.name);
        // Pass the typed `Namespace` straight through — no
        // `display().to_string()` round-trip that would leak the literal
        // "all" string down into kubectl.
        let _pf_id = source.create(
            crate::kube::local::port_forward::PortForwardRequest {
                target: target.clone(),
                kubectl_target,
                namespace: target.namespace.clone(),
                local_port,
                remote_port: container_port,
            },
        );
    }
}

/// Shorthand for a failing `CommandResult` event.
fn reject(message: String) -> SessionEvent {
    SessionEvent::CommandResult(Err(message))
}

/// Build the `kube::api::ApiResource` for a built-in kind directly from the
/// def's `&'static Gvr` — no discovery HTTP roundtrip, no string allocations.
/// All mutating handlers go through this so the typed `BuiltInKind` is the
/// single source of truth for how to talk to the K8s API.
fn built_in_api_resource(
    kind: crate::kube::resource_def::BuiltInKind,
) -> (kube::api::ApiResource, crate::kube::protocol::ResourceScope) {
    let g = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr();
    let gvk = kube::api::GroupVersionKind::gvk(g.group, g.version, g.kind);
    let ar = kube::api::ApiResource::from_gvk_with_plural(&gvk, g.plural);
    (ar, g.scope)
}

/// Build a `DynamicObject` API for a built-in kind in a given namespace.
/// Thin wrapper over the centralized `describe::dynamic_api_for`. Takes a
/// typed `&Namespace` so the handlers don't have to flatten to a string
/// and re-parse back at the boundary.
fn dynamic_api_for_kind(
    client: &kube::Client,
    kind: crate::kube::resource_def::BuiltInKind,
    namespace: &crate::kube::protocol::Namespace,
) -> kube::Api<kube::api::DynamicObject> {
    let (ar, scope) = built_in_api_resource(kind);
    crate::kube::describe::dynamic_api_for(client, &ar, scope, namespace)
}

/// Render a Secret as human-readable `key:\n  value` blocks. Walks both
/// `data` (base64-decoded by k8s-openapi) and `stringData`. Lives here as
/// a free function rather than a `SecretLike` trait method default that
/// nobody overrides.
fn decode_secret_to_text(secret: &k8s_openapi::api::core::v1::Secret) -> String {
    let ns = secret.metadata.namespace.as_deref().unwrap_or("");
    let name = secret.metadata.name.as_deref().unwrap_or("");
    let mut output = format!("Secret: {}/{}\n\n", ns, name);
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
    output
}

/// Build a one-off `Job` from a CronJob's template, used by the manual
/// trigger flow. Stamps a `{name}-manual-{yyyymmddhhmmss}-{nanos}` Job
/// name — epoch seconds alone let two triggers within the same second
/// collide with K8s 409 (name already exists).
fn build_trigger_job(
    cron: &k8s_openapi::api::batch::v1::CronJob,
    namespace: &str,
) -> k8s_openapi::api::batch::v1::Job {
    use k8s_openapi::api::batch::v1::Job;
    let base_name = cron.metadata.name.as_deref().unwrap_or("cronjob");
    let now = chrono::Utc::now();
    // Subsecond nanos disambiguate two triggers within the same second.
    // K8s names are limited to 253 chars and must be a DNS subdomain —
    // `base-manual-YYYYMMDDHHMMSS-NNNNNNNNN` easily fits.
    let job_name = format!(
        "{}-manual-{}-{:09}",
        base_name,
        now.format("%Y%m%d%H%M%S"),
        now.timestamp_subsec_nanos(),
    );
    let template = cron.spec.as_ref().map(|s| s.job_template.clone());
    let job_spec = template.and_then(|t| t.spec).unwrap_or_default();
    Job {
        metadata: kube::api::ObjectMeta {
            name: Some(job_name),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        spec: Some(job_spec),
        ..Default::default()
    }
}

/// Compute the new suspend state for a CronJob and the merge-patch body
/// that applies it. Returns `(new_state, patch)`.
fn toggle_cron_suspend(
    cron: &k8s_openapi::api::batch::v1::CronJob,
) -> (bool, SuspendPatch) {
    let current = cron.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false);
    let new_state = !current;
    (new_state, SuspendPatch { spec: SuspendSpec { suspend: new_state } })
}

// ---------------------------------------------------------------------------
// Typed merge-patch bodies
// ---------------------------------------------------------------------------
//
// The three mutating operations (scale, restart, toggle-suspend) send JSON
// merge-patches to the K8s API. Rather than construct these via
// `serde_json::json!(...)` and hand an untyped `Value` to `Patch::Merge`,
// the shapes below are typed structs: invalid patches can't be built, and
// the untyped→JSON conversion happens inside kube-rs/serde at the true API
// boundary instead of in our handler.

#[derive(Debug, serde::Serialize)]
struct ScalePatch { spec: ScaleSpec }
#[derive(Debug, serde::Serialize)]
struct ScaleSpec { replicas: u32 }

#[derive(Debug, serde::Serialize)]
struct RestartPatch { spec: RestartSpec }
#[derive(Debug, serde::Serialize)]
struct RestartSpec { template: RestartTemplate }
#[derive(Debug, serde::Serialize)]
struct RestartTemplate { metadata: RestartMetadata }
#[derive(Debug, serde::Serialize)]
struct RestartMetadata { annotations: RestartAnnotations }
#[derive(Debug, serde::Serialize)]
struct RestartAnnotations {
    #[serde(rename = "kubectl.kubernetes.io/restartedAt")]
    restarted_at: String,
}

#[derive(Debug, serde::Serialize)]
struct SuspendPatch { spec: SuspendSpec }
#[derive(Debug, serde::Serialize)]
struct SuspendSpec { suspend: bool }

