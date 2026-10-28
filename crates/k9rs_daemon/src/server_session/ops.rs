//! Namespace switching, describe/YAML, and mutating operations (delete,
//! scale, restart, secret decode, CronJob trigger/suspend).

// Patch strategy, two tiers: the full-YAML EDIT apply is server-side
// apply as field manager "k9rs" with `force()` — an interactive "make it
// exactly this" edit must win, and plain merge lost to resourceVersion
// races (rationale at `handle_apply_async`). The small targeted
// mutations (restart annotation, cron suspend) stay plain merge patches
// with NO field-manager claim, minimizing ownership friction with GitOps
// controllers (Pulumi, ArgoCD, Flux) for fields k9rs merely pokes.

use k9rs_proto::protocol::{self, SessionEvent};
use k9rs_proto::resource_def::BuiltInKind;
use k9rs_kube::session_env::SessionEnv;

use super::ServerSession;

impl ServerSession {
    // handle_subscribe_resource, emit_capabilities, handle_subscribe_local_resource,
    // handle_unsubscribe — all deleted. Subscriptions are now per-yamux-substream.

    /// Delete a logical entry on a local resource. Looks up the source by
    /// resource id, dispatches to its `delete` method, and emits an
    /// `OpResult` for the target. Readonly is gated once at the top of
    /// `handle_command` via `SessionCommand::is_mutating`, so this handler
    /// (like every other mutating handler) doesn't re-check.
    pub(super) fn handle_delete_local(&mut self, obj: &protocol::ObjectRef) {
        let tx = self.event_tx.clone();
        let Some(source) = self.locals.get(&obj.resource) else {
            self.reject_async(protocol::OperationKind::Delete, obj, "Unknown local resource".into());
            return;
        };
        let result = source.delete(&obj.name);
        let name = obj.name.clone();
        let target = obj.clone();
        self.track_task(async move {
            let result = match result {
                Ok(()) => Ok(format!("Stopped {name}")),
                Err(e) => Err(e),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Delete, target, result })
                .await;
        });
    }

    /// Apply edited YAML to a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`. Mirrors the K8s `Apply`
    /// path so the client can use one wire flow for both.
    pub(super) fn handle_apply_local(&mut self, obj: &protocol::ObjectRef, yaml: &str) {
        let tx = self.event_tx.clone();
        let Some(source) = self.locals.get(&obj.resource) else {
            self.reject_async(protocol::OperationKind::Apply, obj, "Unknown local resource".into());
            return;
        };
        let result = source.apply_yaml(&obj.name, yaml);
        let target = obj.clone();
        self.track_task(async move {
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Apply, target, result })
                .await;
        });
    }

    /// Apply edited YAML to a K8s resource via kube-rs server-side apply.
    /// Spawns the work so the session loop stays responsive. Routes through
    /// the typed `api_resource_for(target.resource)` so built-ins skip the
    /// discovery HTTP roundtrip and CRDs use their stored GVR if populated.
    pub(super) fn handle_apply_async(&mut self, target: &protocol::ObjectRef, yaml: String) {
        if self.reject_if_namespace_unresolved(protocol::OperationKind::Apply, target, "Apply") { return; }
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let target = target.clone();
        let display = target.kubectl_target();

        self.track_task(async move {
            let result: anyhow::Result<()> = async {
                use ::kube::api::{DynamicObject, Patch, PatchParams};

                let (ar, scope) =
                    k9rs_kube::describe::api_resource_for(&client, &target.resource).await?;
                let api = k9rs_kube::describe::dynamic_api_for(&client, &ar, scope, &target.namespace);

                // Parse the user's edited object and write it back with Server-Side
                // Apply. SSA is declarative — the object is reconciled to *exactly*
                // the applied shape. The old primitive, a JSON merge patch, can only
                // add or overwrite keys: a field the user DELETED is simply absent
                // from the patch, and "absent" means "leave untouched", so the stale
                // field survived on the server (deleting `strategy.rollingUpdate`
                // left it in place, and the apiserver then rejected it as invalid
                // under `type: Recreate`). SSA drops the omitted field and re-defaults
                // against the new object — what a full-object editor actually means.
                let mut parsed: DynamicObject = serde_yaml::from_str(&yaml)
                    .map_err(|e| anyhow::anyhow!("yaml parse error: {}", e))?;

                // Validate that the parsed YAML targets the expected resource.
                // Catches corrupted or mis-edited YAML before it reaches the API.
                if let Some(ref name) = parsed.metadata.name {
                    if name != &target.name {
                        anyhow::bail!(
                            "metadata.name mismatch: expected '{}', got '{}'",
                            target.name, name
                        );
                    }
                }

                // Two request-body fields must be cleared before SSA:
                //
                // - `managedFields`: SSA rejects a body that carries them
                //   ("metadata.managedFields must be nil"). The API-fetch edit path
                //   strips them, but the `kubectl get -o yaml` fallback
                //   (`describe::fetch_yaml_via_kubectl`) does not — clear here so the
                //   write is correct regardless of how the edited YAML was produced.
                //
                // - `resourceVersion`: dropping it removes the optimistic-concurrency
                //   PRECONDITION. Keeping it 409s on a churning object — a controller
                //   writing status/annotations while the user sits in $EDITOR bumps
                //   the RV, so the apply fails even when the field edit collides with
                //   nobody, and retry dead-ends (the overlay re-sends the same
                //   stale-RV YAML). k9s sidesteps this by shelling out to `kubectl
                //   edit`, whose diff-based patch carries no RV precondition; we match
                //   that behavior. Concurrency is now governed by SSA field OWNERSHIP:
                //   `force` makes this interactive "make it exactly this" edit
                //   authoritative over a field another manager owns (equivalent to
                //   `kubectl apply --server-side --force-conflicts`). Trade-off vs
                //   `kubectl edit`: we round-trip the whole object, so this is
                //   last-writer-wins on a genuinely concurrent edit rather than
                //   field-granular.
                parsed.metadata.managed_fields = None;
                parsed.metadata.resource_version = None;

                let params = PatchParams::apply("k9rs").force();
                api.patch(&target.name, &params, &Patch::Apply(&parsed))
                    .await?;
                Ok(())
            }
            .await;

            let result = match result {
                Ok(()) => Ok(format!("Applied {}", display)),
                Err(e) => Err(format!("Apply failed: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Apply, target, result })
                .await;
        });
    }

    // -----------------------------------------------------------------------
    // Describe / YAML
    // -----------------------------------------------------------------------

    pub(super) fn handle_describe_async(&mut self, target: &protocol::ObjectRef) {
        self.fetch_and_emit(
            target,
            |c, t, ctx, env| Box::pin(k9rs_kube::describe::fetch_describe(c, t, ctx, env)),
            |target, lines| SessionEvent::DescribeResult { target, lines },
        );
    }

    pub(super) fn handle_yaml_async(&mut self, target: &protocol::ObjectRef) {
        self.fetch_and_emit(
            target,
            |c, t, ctx, env| Box::pin(k9rs_kube::describe::fetch_yaml(c, t, ctx, env)),
            |target, content| SessionEvent::YamlResult { target, content },
        );
    }

    /// Shared helper for describe/yaml: clones the session state, spawns a
    /// task that calls `fetch_fn`, wraps the result via `make_event`, and
    /// sends it back on the event channel.
    fn fetch_and_emit<T, F, E>(
        &mut self,
        target: &protocol::ObjectRef,
        fetch_fn: F,
        make_event: E,
    )
    where
        T: Send + 'static,
        F: for<'a> FnOnce(
                &'a kube::Client,
                &'a protocol::ObjectRef,
                &'a protocol::ContextName,
                &'a SessionEnv,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
        E: FnOnce(protocol::ObjectRef, T) -> SessionEvent + Send + 'static,
    {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let session_env = self.session_env.clone();
        let target = target.clone();
        self.track_task(async move {
            let content = fetch_fn(&client, &target, &context, &session_env).await;
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
    /// namespace, mutating the wrong resource.
    ///
    /// The PERMISSIVE half of the pair: cluster-scoped resources (Node, CRD,
    /// ClusterRole) legitimately have `Namespace::All`, so they pass here —
    /// where [`Self::resolve_namespace_or_reject`] refuses them, because it
    /// exists to hand back a namespace STRING and a cluster-scoped target
    /// has none. Which handler may use which is not a free choice, and
    /// nothing records it: picking the permissive one for an operation that
    /// needs a namespace is how `:pf` came to have no gate at all.
    ///
    /// Callers, counted rather than claimed: `Apply`, `Delete`, `Scale`,
    /// `Restart`. Its strict sibling serves `ForceKill`, `DecodeSecret` and
    /// the two CronJob ops; `handle_port_forward` has its own typed guard.
    /// This doc used to say "all mutating handlers call this first, same
    /// pattern as `reject_if_readonly`" — of eleven mutating handlers four
    /// call it, and `reject_if_readonly` has never existed (the readonly
    /// gate is one `is_mutating` check in `handle_command`).
    pub(super) fn reject_if_namespace_unresolved(
        &mut self,
        op: protocol::OperationKind,
        target: &protocol::ObjectRef,
        action: &str,
    ) -> bool {
        // Cluster-scoped targets are legitimately Namespace::All — and this
        // one line IS the difference between the two gates.
        if target.resource.is_cluster_scoped() {
            return false;
        }
        match namespace_for_op(target, action) {
            Ok(_) => false,
            Err(message) => {
                self.reject_async(op, target, message);
                true
            }
        }
    }

    /// Resolve the target's namespace for an op that NEEDS the string
    /// (typed `kube::Api::namespaced` constructors, `kubectl -n` args).
    /// Replaces the `reject_if_namespace_unresolved(...)` + later
    /// `target.namespace.as_option().expect("namespace already validated")`
    /// pair: instead of a runtime check followed by an `.expect` that trusts
    /// the check, we destructure once and bind the name or reject.
    pub(super) fn resolve_namespace_or_reject(
        &mut self,
        op: protocol::OperationKind,
        target: &protocol::ObjectRef,
        action: &str,
    ) -> Option<String> {
        match namespace_for_op(target, action) {
            Ok(n) => Some(n.to_string()),
            Err(message) => {
                self.reject_async(op, target, message);
                None
            }
        }
    }

    /// Spawn a fire-and-forget task that emits a failing `OpResult` for
    /// the given target. Used by the synchronous capability gates in
    /// the mutating handlers (e.g. "this resource is not scaleable").
    pub(super) fn reject_async(
        &mut self,
        op: protocol::OperationKind,
        target: &protocol::ObjectRef,
        message: String,
    ) {
        let tx = self.event_tx.clone();
        let event = reject(op, target, message);
        self.track_task(async move {
            let _ = tx.send(event).await;
        });
    }

    /// The `Ok` twin of [`Self::reject_async`]: a succeeding `OpResult` for
    /// a handler whose work is already done (or already handed to an owner
    /// that will report its own state).
    ///
    /// The asynchronous handlers build their own `OpResult` inside the task
    /// that does the work, because that is where the outcome is known. This
    /// is for the synchronous ones, which had no way to answer at all.
    pub(super) fn ack_async(
        &mut self,
        op: protocol::OperationKind,
        target: &protocol::ObjectRef,
        message: String,
    ) {
        let tx = self.event_tx.clone();
        let event = SessionEvent::OpResult {
            op,
            target: target.clone(),
            result: Ok(message),
        };
        self.track_task(async move {
            let _ = tx.send(event).await;
        });
    }

    /// Spawn a fire-and-forget task that emits a failing `CommandResult`
    /// (no target). Only for rejections of a COMMAND as a whole — the
    /// centralized readonly gate, which fires before any target-specific
    /// handler runs. Target-specific gates use [`Self::reject_async`].
    pub(super) fn reject_command_async(&mut self, message: String) {
        let tx = self.event_tx.clone();
        self.track_task(async move {
            let _ = tx.send(SessionEvent::CommandResult(Err(message))).await;
        });
    }

    /// Capability gate: extracts the target's [`BuiltInKind`] and asserts
    /// it supports the given [`k9rs_proto::protocol::OperationKind`]. On failure, emits a typed
    /// rejection and returns `None` so the caller can `return` cleanly.
    ///
    /// Called by `handle_scale_async`, `handle_toggle_suspend_cronjob_async`
    /// and `handle_port_forward`. This doc used to claim "scale / restart /
    /// decode-secret / trigger / toggle" — five handlers, of which two
    /// actually called it, and port-forward (which did not) was the one with
    /// no capability check at all. Anything NOT in the list above reaches
    /// the API server and is refused there, or by a typed kind check inside
    /// the handler; a list is only worth writing if it is checked, so grep
    /// before trusting this one.
    pub(super) fn require_capability(
        &mut self,
        target: &protocol::ObjectRef,
        required_op: protocol::OperationKind,
        action: &str,
    ) -> Option<BuiltInKind> {
        match capability_check(target, &required_op, action) {
            Ok(kind) => Some(kind),
            Err(message) => {
                self.reject_async(required_op, target, message);
                None
            }
        }
    }

    pub(super) fn handle_delete_async(&mut self, target: &protocol::ObjectRef) {
        if self.reject_if_namespace_unresolved(protocol::OperationKind::Delete, target, "Delete") { return; }
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.name.clone();
        let session_env = self.session_env.clone();
        let target = target.clone();
        let display = target.kubectl_target();
        self.track_task(async move {
            let result = k9rs_kube::ops::execute_delete(&client, &target, &context, &session_env).await;
            let result = match result {
                Ok(()) => Ok(format!("Deleted {}", display)),
                Err(e) => Err(format!("Delete failed: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Delete, target, result })
                .await;
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
            self.reject_async(protocol::OperationKind::ForceKill, target, format!("Force-kill is pod-only (got {})", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(protocol::OperationKind::ForceKill, target, "Force-kill") else { return; };
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
            let result = match api.delete(&name, &dp).await {
                Ok(_) => Ok(format!("Force-killed {}", display)),
                Err(e) => Err(format!("Force-kill failed: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::ForceKill, target, result })
                .await;
        });
    }

    pub(super) fn handle_scale_async(&mut self, target: &protocol::ObjectRef, replicas: u32) {
        if self.reject_if_namespace_unresolved(protocol::OperationKind::Scale, target, "Scale") { return; }
        let Some(kind) = self.require_capability(
            target,
            protocol::OperationKind::Scale,
            "Scale",
        ) else { return; };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let namespace = target.namespace.clone();
        let target = target.clone();
        let display = kind.def().gvr().plural;
        self.track_task(async move {
            // Use the /scale subresource (same as kubectl scale / k9s).
            // This doesn't interact with SSA field management — no field
            // ownership is claimed on spec.replicas.
            // The one `BuiltInKind -> ApiResource` mapping, in the crate
            // that owns it. This was hand-rolled here, re-spelling the
            // `api_version` rule (`version` alone for core, `group/version`
            // otherwise) that `ApiResource::from_gvk_with_plural` already
            // implements — a third copy of a mapping that had just been
            // reduced from three to one everywhere else.
            let (ar, _scope) = k9rs_kube::describe::built_in_api_resource(kind);
            let api: kube::Api<kube::api::DynamicObject> = match &namespace {
                protocol::Namespace::Named(ns) => kube::Api::namespaced_with(client, ns, &ar),
                protocol::Namespace::All => kube::Api::all_with(client, &ar),
            };
            let result = match api.get_scale(&n).await {
                Ok(mut scale) => {
                    // The write must HAPPEN before it is reported. With an
                    // `if let` here, a Scale whose `spec` the apiserver did
                    // not populate skipped the assignment, PUT the object
                    // back unchanged, succeeded, and reported "Scaled …/… to
                    // N replicas" — a green flash naming a number that was
                    // never written. Likewise `unwrap_or_default()` on the
                    // serialize turned a failure into a PUT with an EMPTY
                    // body.
                    let prepared = match scale.spec.as_mut() {
                        Some(s) => {
                            s.replicas = Some(replicas as i32);
                            serde_json::to_vec(&scale)
                                .map_err(|e| format!("Scale failed: {e}"))
                        }
                        None => Err(
                            "Scale failed: the server returned no scale spec".to_string()
                        ),
                    };
                    match prepared {
                        Ok(body) => {
                            match api
                                .replace_scale(&n, &kube::api::PostParams::default(), body)
                                .await
                            {
                                Ok(_) => Ok(format!(
                                    "Scaled {}/{} to {} replicas",
                                    display, n, replicas
                                )),
                                Err(e) => Err(format!("Scale failed: {}", e)),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(format!("Scale failed: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Scale, target, result })
                .await;
        });
    }

    pub(super) fn handle_restart_async(&mut self, target: &protocol::ObjectRef) {
        if self.reject_if_namespace_unresolved(protocol::OperationKind::Restart, target, "Restart") { return; }

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
        let target = target.clone();
        let display: &'static str = kind.def().gvr().plural;
        self.track_task(async move {
            use ::kube::api::{Patch, PatchParams};
            let api = dynamic_api_for_kind(&client, kind, &namespace);
            let result = match api.patch(&n, &PatchParams::default(), &Patch::Merge(&patch_body)).await {
                Ok(_) => Ok(format!("Restarted {}/{}", display, n)),
                Err(e) => Err(format!("Restart failed: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::Restart, target, result })
                .await;
        });
    }

    // -----------------------------------------------------------------------
    // Secret decode
    // -----------------------------------------------------------------------

    /// Answer a decode — success or refusal — in the shape the command's
    /// own success has: lines in the describe panel.
    ///
    /// Every exit from `handle_decode_secret_async` goes through here or
    /// through the same event below it, because the client opens a content
    /// view on `DecodeSecret` and that view's ONLY exit is a
    /// `DescribeResult` for its target. An `OpResult` refusal is not an
    /// answer to it: the panel stays on "Loading…" while a flash goes by
    /// somewhere else. Two of the three refusals already knew this; the
    /// namespace one went through the general `OpResult` gate.
    fn answer_decode(&mut self, target: &protocol::ObjectRef, text: String) {
        let tx = self.event_tx.clone();
        let target = target.clone();
        self.track_task(async move {
            let _ = tx.send(SessionEvent::DescribeResult {
                target,
                lines: k9rs_kube::describe::describe_lines_from_text(&text),
            }).await;
        });
    }

    pub(super) fn handle_decode_secret_async(&mut self, target: &protocol::ObjectRef) {
        let ns = match plan_decode(target) {
            // Every refusal, in the panel. There is exactly one other exit
            // from this handler and it sends the same event.
            DecodePlan::Refused(text) => return self.answer_decode(target, text),
            DecodePlan::Fetch { namespace } => namespace,
        };
        let target_owned = target.clone();
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
                lines: k9rs_kube::describe::describe_lines_from_text(&content),
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
            self.reject_async(protocol::OperationKind::TriggerCronJob, target, format!("Trigger not supported on {}", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(protocol::OperationKind::TriggerCronJob, target, "Trigger") else { return; };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let target = target.clone();
        self.track_task(async move {
            use k8s_openapi::api::batch::v1::{CronJob, Job};

            let cj_api: kube::Api<CronJob> = kube::Api::namespaced(client.clone(), &ns);
            let cj = match cj_api.get(&n).await {
                Ok(cj) => cj,
                Err(e) => {
                    let _ = tx.send(reject(protocol::OperationKind::TriggerCronJob, &target, format!("Failed to get CronJob: {}", e))).await;
                    return;
                }
            };

            let job = build_trigger_job(&cj, &ns);
            let job_name = job.metadata.name.clone().unwrap_or_default();

            let job_api: kube::Api<Job> = kube::Api::namespaced(client, &ns);
            let result = match job_api.create(&kube::api::PostParams::default(), &job).await {
                Ok(_) => Ok(format!("Triggered job: {}", job_name)),
                Err(e) => Err(format!("Failed to trigger CronJob: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::TriggerCronJob, target, result })
                .await;
        });
    }

    /// Toggle the suspend state of a CronJob. The `toggle_cron_suspend`
    /// free function below reads the current state and produces both the
    /// next state and the merge-patch body. (Previously a `CronLike` trait
    /// method, but all cron-likes behave the same way so it lives inline.)
    pub(super) fn handle_toggle_suspend_cronjob_async(&mut self, target: &protocol::ObjectRef) {
        if target.resource.built_in_kind() != Some(BuiltInKind::CronJob) {
            self.reject_async(protocol::OperationKind::ToggleSuspendCronJob, target, format!("Toggle suspend not supported on {}", target.resource.plural()));
            return;
        }
        let Some(ns) = self.resolve_namespace_or_reject(protocol::OperationKind::ToggleSuspendCronJob, target, "Toggle suspend") else { return; };

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let n = target.name.clone();
        let target = target.clone();
        self.track_task(async move {
            use k8s_openapi::api::batch::v1::CronJob;

            let api: kube::Api<CronJob> = kube::Api::namespaced(client, &ns);
            let cj = match api.get(&n).await {
                Ok(cj) => cj,
                Err(e) => {
                    let _ = tx.send(reject(protocol::OperationKind::ToggleSuspendCronJob, &target, format!("Failed to read CronJob: {}", e))).await;
                    return;
                }
            };

            let (new_state, patch_body) = toggle_cron_suspend(&cj);
            let result = match api.patch(&n, &kube::api::PatchParams::default(), &kube::api::Patch::Merge(&patch_body)).await {
                Ok(_) => {
                    let action = if new_state { "Suspended" } else { "Resumed" };
                    Ok(format!("{} CronJob: {}", action, n))
                }
                Err(e) => Err(format!("Failed to update CronJob: {}", e)),
            };
            let _ = tx
                .send(SessionEvent::OpResult { op: protocol::OperationKind::ToggleSuspendCronJob, target, result })
                .await;
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
        target: &k9rs_proto::protocol::ObjectRef,
        local_port: u16,
        container_port: u16,
    ) {
        // The namespace guard, in the form the request's type asks for.
        //
        // Not `reject_if_namespace_unresolved` like its siblings: that helper
        // exempts cluster-scoped targets, which a port-forward target never
        // is, and it answers with a `bool` while
        // `k9rs_kube::local::port_forward::PodNamespace` needs the VALUE. One
        // guard that produces what the request needs beats a guard followed
        // by an unwrap of the same question.
        let Some(namespace) =
            k9rs_kube::local::port_forward::PodNamespace::of(&target.namespace)
        else {
            self.reject_async(
                protocol::OperationKind::PortForward,
                target,
                format!(
                    "Port-forward refused: {} has no resolved namespace",
                    target.kubectl_target(),
                ),
            );
            return;
        };
        // The capability gate every other mutating handler has. Without it
        // `:pf` on a kind that declares no `PortForward` — a configmap, a
        // namespace — was accepted, and the forward then failed somewhere
        // inside the tunnel with nothing said on the wire.
        if self
            .require_capability(target, protocol::OperationKind::PortForward, "Port-forward")
            .is_none()
        {
            return;
        }
        let kubectl_target = target.kubectl_target();
        // Kept for the acknowledgement below; the request takes ownership.
        let display = kubectl_target.clone();

        // The per-context PortForwardSource, owned by this session's
        // ContextLocals slice (shared with any sibling session on the same
        // context).
        let source = self.locals.port_forwards();
        // Pass the typed `Namespace` straight through — no
        // `display().to_string()` round-trip that would leak the literal
        // "all" string into resolution.
        let _pf_id = source.create(
            k9rs_kube::local::port_forward::PortForwardRequest {
                target: target.clone(),
                // THIS session's context name, not the shared source's — a
                // source is per `ContextId`, which excludes the name.
                context: self.context.name.clone(),
                kubectl_target,
                namespace,
                local_port,
                remote_port: container_port,
                // The session's client: the native tunnel resolves and
                // authenticates as this session, and keeps working after
                // the session ends (refreshable creds ride in the client).
                client: self.client.clone(),
            },
        );

        // Say so. This handler sent NOTHING back — no `OpResult`, success or
        // failure — so a `:pf` the daemon accepted was indistinguishable
        // from one it dropped on the floor, and the TUI's only recourse was
        // for the user to go and look at the port-forwards table.
        //
        // Deliberately "starting", not "forwarding": `create` returns as
        // soon as the entry exists in `PortForwardState::Starting`, and the
        // tunnel's real outcome arrives as that entry's state. Claiming
        // success here would be the fake-success class.
        self.ack_async(
            protocol::OperationKind::PortForward,
            target,
            format!(
                "Port-forward starting: localhost:{} -> {}:{}",
                local_port, display, container_port
            ),
        );
    }
}

/// Whether `target`'s kind declares `required_op` — the whole of the
/// capability gate's DECISION, with none of its emitting.
///
/// Split out so it can be tested: the gate itself needs a live
/// `ServerSession` (an authenticated kube client, an event channel), which
/// is exactly what a unit test must not have, so the rule it enforces had no
/// test — and `handle_port_forward` went without the gate entirely.
fn capability_check(
    target: &protocol::ObjectRef,
    required_op: &protocol::OperationKind,
    action: &str,
) -> Result<BuiltInKind, String> {
    let Some(kind) = target.resource.built_in_kind() else {
        return Err(format!("{} not supported on {}", action, target.resource.plural()));
    };
    let def = kind.def();
    if !def.operations().contains(required_op) {
        return Err(format!("{} not supported on {}", action, def.gvr().plural));
    }
    Ok(kind)
}

/// Shorthand for a failing `OpResult` event for the given target.
fn reject(op: protocol::OperationKind, target: &protocol::ObjectRef, message: String) -> SessionEvent {
    SessionEvent::OpResult { op, target: target.clone(), result: Err(message) }
}

/// Build a `DynamicObject` API for a built-in kind in a given namespace.
/// Thin wrapper over the centralized `describe::dynamic_api_for`. Takes a
/// typed `&Namespace` so the handlers don't have to flatten to a string
/// and re-parse back at the boundary.
fn dynamic_api_for_kind(
    client: &kube::Client,
    kind: k9rs_proto::resource_def::BuiltInKind,
    namespace: &k9rs_proto::protocol::Namespace,
) -> kube::Api<kube::api::DynamicObject> {
    let (ar, scope) = k9rs_kube::describe::built_in_api_resource(kind);
    k9rs_kube::describe::dynamic_api_for(client, &ar, scope, namespace)
}

/// Render a Secret as human-readable `key:\n  value` blocks. Walks both
/// `data` (base64-decoded by k8s-openapi) and `stringData`. Lives here as
/// a free function rather than a `SecretLike` trait method default that
/// nobody overrides.
fn decode_secret_to_text(secret: &k8s_openapi::api::core::v1::Secret) -> String {
    let ns = secret.metadata.namespace.as_deref().unwrap_or("");
    let name = secret.metadata.name.as_deref().unwrap_or("");

    // `data` is base64 (decode it); `string_data` is already plaintext. Both
    // render as "key:\n  value\n\n". `.iter().flatten()` yields nothing when
    // the map is absent, so the two sources concatenate without branching.
    let data = secret.data.iter().flatten()
        .map(|(key, value)| format!("{}:\n  {}\n\n", key, String::from_utf8_lossy(&value.0)));
    let string_data = secret.string_data.iter().flatten()
        .map(|(key, value)| format!("{}:\n  {}\n\n", key, value));

    let body: String = data.chain(string_data).collect();
    let output = format!("Secret: {}/{}\n\n{}", ns, name, body);
    // Every section ends in a blank line; drop the final newline so the text
    // ends with a single newline rather than a trailing blank line.
    output.strip_suffix('\n').map(str::to_owned).unwrap_or(output)
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

/// What a `DecodeSecret` should do: fetch, or refuse with this text.
///
/// Three gates, extracted so the property that matters is checkable without
/// a live session — that EVERY refusal is one shape, the describe panel the
/// client already opened (see [`ServerSession::answer_decode`]). As three
/// inline early-returns it was not: two answered in the panel and the
/// namespace one took the general `OpResult` path, so a secret whose row had
/// lost its namespace left the panel loading for ever.
enum DecodePlan {
    Refused(String),
    Fetch { namespace: String },
}

fn plan_decode(target: &protocol::ObjectRef) -> DecodePlan {
    // Typed gate: only built-ins can be secret-like.
    let Some(kind) = target.resource.built_in_kind() else {
        return DecodePlan::Refused(format!(
            "Decode not supported on {}",
            target.resource.plural(),
        ));
    };
    let def = kind.def();
    if !def.operations().contains(&protocol::OperationKind::DecodeSecret) {
        return DecodePlan::Refused(format!(
            "{} is not a secret-like resource",
            def.gvr().plural,
        ));
    }
    match namespace_for_op(target, "Decode") {
        Ok(namespace) => DecodePlan::Fetch { namespace: namespace.to_string() },
        Err(message) => DecodePlan::Refused(message),
    }
}

/// The namespace a namespace-needing operation may use, or the refusal to
/// report — the DECISION, with no answer shape attached.
///
/// Pure and shared because the rule is one thing and the reply is another.
/// Three answer shapes want this same rule: most handlers refuse with an
/// `OpResult`, [`ServerSession::handle_decode_secret_async`] has to refuse
/// into the describe panel the client already opened for it, and
/// `handle_port_forward` wants a typed `PodNamespace`. The wording was
/// written out four times, so a fix to one refusal did not reach the others
/// — and `DecodeSecret` had drifted all the way to a different EVENT type.
///
/// (`handle_port_forward` keeps its own guard: `PodNamespace::of` produces
/// the value its request type needs, and routing through here would add an
/// impossible `else` for a case a pod target cannot be in. Its sentence is
/// deliberately the same one.)
fn namespace_for_op<'a>(
    target: &'a protocol::ObjectRef,
    action: &str,
) -> Result<&'a str, String> {
    // Cluster-scoped targets don't belong in handlers that need a namespace
    // (force-kill, decode, cron trigger/toggle) — a specific message rather
    // than "no resolved namespace", which would be true but misleading.
    if target.resource.is_cluster_scoped() {
        return Err(format!(
            "{action} refused: {} is cluster-scoped",
            target.kubectl_target(),
        ));
    }
    let protocol::Namespace::Named(n) = &target.namespace else {
        return Err(format!(
            "{action} refused: {} has no resolved namespace",
            target.kubectl_target(),
        ));
    };
    Ok(n)
}

#[cfg(test)]
#[path = "../tests/ops.rs"]
mod tests;
