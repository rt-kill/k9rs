//! What the app can be told, and what each telling does to it.
//!
//! The vocabulary ([`AppEvent`]) and its interpreter ([`apply_event`]) are one
//! file on purpose: `apply_event` is total over the enum, so a new variant is
//! a compile error here rather than an event that is produced and silently
//! never applied.
//!
//! The interpreter lived in `session` for a while, declared there as "the
//! apply path for daemon events". It does no I/O: no socket, no task, no
//! `.await`, no client — it is `(&mut App, AppEvent)` and nothing else, which
//! is why it belongs with the model and not with the loop that happens to
//! call it. `session::actions` is the real counterpart to `app::actions`,
//! because executing an action DOES reach the daemon.
//!
//! Living here also means the one way to drive `App` into a given state is
//! reachable from everything above `app`: the paint tests set up through
//! `apply_event` instead of hand-rolling the same three mutations, which is
//! what a renderer test depending on the session layer used to cost.

use std::collections::HashMap;


use crate::{App, FlashMessage};

/// Top-level event type for the application event loop.
///
/// Input events (keystrokes and ticks — k9rs is keyboard-only) are handled
/// directly by the main event loop via crossterm's `EventStream`. Only
/// resource updates, errors, and flash messages flow through this channel.
pub enum AppEvent {
    /// A table-stream event, carrying its DESTINATION: the bridge that
    /// produced it holds the `Arc<RowStore>` it was handed at subscribe
    /// time, so there is no routing and nothing to mis-route — an event
    /// for a popped element lands in an unreferenced store and frees it
    /// when the queue drains. The event loop is the sole applier (the
    /// store's single-writer convention).
    Store(StoreEvent),
    /// A content-view update (yaml / describe / log line).
    ResourceUpdate(ResourceUpdate),
    /// A server-sent `CommandResult` — a management/command-level
    /// acknowledgment with no resource target (e.g. the daemon's readonly
    /// gate refusing a whole command). Flash-only.
    ///
    /// `Result<String, String>` rather than `{ ok, message }` so readers
    /// have to branch on success/failure before touching the message —
    /// the struct shape let convention-based code leak.
    CommandResult(Result<String, String>),
    /// A server-sent `OpResult` — the response to a mutating operation on
    /// a specific target (Apply, Delete, Scale, Restart, Trigger, Toggle,
    /// local stop/apply). The target correlates the result back to its
    /// consumer: the edit flow reacts only to its own apply, a
    /// [`crate::BatchTracker`] consumes its items' results (unmark
    /// per-success, retain marks on failure, ONE aggregate flash), and
    /// anything unclaimed flashes directly.
    OpResult {
        /// WHICH operation produced this result — target alone was
        /// ambiguous (an edit-apply and a batch op on the same object
        /// could claim each other's outcomes).
        op: k9rs_proto::protocol::OperationKind,
        target: k9rs_proto::protocol::ObjectRef,
        result: Result<String, String>,
    },
    /// A temporary flash message shown in the status bar. Produced by
    /// purely local TUI events (key bindings, nav, info messages).
    Flash(FlashMessage),
    /// Pod metrics from the metrics-server.
    PodMetrics(HashMap<k9rs_proto::protocol::ObjectKey, k9rs_proto::protocol::MetricsUsage>),
    /// Node metrics from the metrics-server.
    NodeMetrics(HashMap<k9rs_proto::protocol::NodeName, k9rs_proto::protocol::MetricsUsage>),
    /// A log-stream event for its destination line store — same
    /// destination-rides-the-event shape as [`AppEvent::Store`].
    Log(LogEvent),
    /// The server resolved an unknown resource to its true identity.
    /// The TUI should update its nav and table keys.
    ResourceResolved {
        original: k9rs_proto::protocol::ResourceId,
        resolved: k9rs_proto::protocol::ResourceId,
    },
    /// A subscription failed for a specific resource. FLASH-ONLY: the
    /// failing bridge separately delivers `StorePayload::Failed` to its
    /// own store (epoch-gated), which is what flips the element's state —
    /// this event just surfaces the message regardless of which view is
    /// active.
    SubscriptionFailed {
        resource: k9rs_proto::protocol::ResourceId,
        message: String,
    },
    /// Cached discovery data from the daemon (namespace names + CRDs),
    /// typed — the event loop converts to rows and seeds the app-level
    /// core stores (completion / picker sources).
    Discovery {
        namespaces: Vec<String>,
        crds: Vec<k9rs_kube::cache::CachedCrd>,
    },
    /// Raw terminal bytes from an exec session (daemon → TUI).
    /// Buffered during Connecting, written directly to stdout during
    /// bridge mode.
    ExecData(Vec<u8>),
    /// The exec stream ended (daemon closed the substream).
    ExecEnded,
    /// The daemon connection was lost. TUI should exit gracefully.
    DaemonDisconnected,
    /// The daemon handshake completed and the session is ready. The TUI should
    /// populate context/cluster/user info and trigger any initial subscriptions.
    ConnectionEstablished {
        context: k9rs_proto::protocol::ContextName,
        identity: k9rs_proto::protocol::ClusterIdentity,
        namespaces: Vec<String>,
    },
    /// The daemon handshake failed. The TUI should exit with this error.
    ConnectionFailed(String),
    /// The kubeconfig was read in the background. Lets the TUI populate the
    /// contexts panel and `:ctx <tab>` completion before the daemon answers.
    KubeconfigLoaded {
        contexts: Vec<crate::KubeContext>,
        current_context: Option<k9rs_proto::protocol::ContextName>,
        current_identity: k9rs_proto::protocol::ClusterIdentity,
    },
    /// The kubeconfig is fine but names no context to connect to (none on
    /// the command line, `current-context` absent or blank). NOT a
    /// `ConnectionFailed`: nothing failed and nothing should be retried —
    /// there is simply no target until the user picks one, which is what
    /// the contexts picker is for.
    NoContextConfigured,
}

/// A table-stream event plus its destination store. CLIENT-INTERNAL:
/// deliberately NOT serde-derived (the wire carries
/// [`k9rs_proto::protocol::StreamEvent`]; the bridge maps into this).
/// The `epoch` stamps which stream instance produced it — the store's
/// epoch floor drops events from superseded streams targeting the same
/// store (refresh / reconnect succession).
pub struct StoreEvent {
    pub store: std::sync::Arc<crate::store::RowStore>,
    pub epoch: u64,
    pub payload: crate::store::StorePayload,
}

impl std::fmt::Debug for StoreEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreEvent").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

/// A log-stream event plus its destination line store. CLIENT-INTERNAL.
pub struct LogEvent {
    pub store: std::sync::Arc<crate::store::LineStore>,
    pub epoch: u64,
    pub payload: LogPayload,
}

pub enum LogPayload {
    /// One typed log line (ANSI passthrough; the renderer parses SGR).
    Line(k9rs_proto::protocol::LogLine),
    /// The daemon closed the log substream.
    Ended,
}

impl std::fmt::Debug for LogEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogEvent").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

/// A content-view update (yaml / describe / log line).
///
/// CLIENT-INTERNAL: deliberately NOT serde-derived. The missing serde
/// derives are the compile-time guarantee this enum can never sneak back
/// onto the wire.
#[derive(Debug, Clone)]
pub enum ResourceUpdate {
    /// Response to `SessionCommand::Yaml(target)`. Carries the originating
    /// `ObjectRef` so the apply path can gate on "does the current route
    /// target match". Without this, rapid navigation A→B can deliver A's
    /// slower yaml fetch to B's edit state machine and write A's content
    /// to B's temp file.
    Yaml { target: k9rs_proto::protocol::ObjectRef, content: String },
    /// Response to `SessionCommand::Describe(target)`. Same gating rules
    /// as `Yaml` above. Typed lines (not a flat `String`) so the UI renders by
    /// the producer's role tags instead of re-inferring structure from text.
    Describe { target: k9rs_proto::protocol::ObjectRef, lines: Vec<k9rs_proto::protocol::DescribeLine> },
}

/// Handle a single AppEvent (resource update, error, or flash).
pub fn apply_event(app: &mut App, event: AppEvent) {
    match event {
        // The destination rides the event: apply and done. Routing bugs
        // (an event landing in the wrong table) are unrepresentable —
        // there is no lookup. A popped element's queued events land in an
        // unreferenced store and free it when the queue drains.
        AppEvent::Store(ev) => ev.store.apply(ev.epoch, ev.payload),
        AppEvent::ResourceUpdate(update) => apply_resource_update(app, update),
        AppEvent::Flash(flash) => {
            // Purely local flashes: just show them. Do NOT pop the edit
            // route — that's driven by the target-gated `OpResult` below.
            app.ui.flash = Some(flash);
        }
        AppEvent::CommandResult(result) => {
            // Management/command-level acknowledgment (no target): flash.
            app.ui.flash = Some(match result {
                Ok(msg) => crate::FlashMessage::info(msg),
                Err(msg) => crate::FlashMessage::error(msg),
            });
        }
        AppEvent::OpResult { op, target, result } => {
            apply_op_result(app, op, target, result);
        }
        AppEvent::ResourceResolved { original, resolved } => {
            // The server discovered the true identity of a resource we
            // subscribed to with incomplete info (e.g., `:nodeclaims` →
            // karpenter.sh/v1/NodeClaim/Cluster). The owning elements
            // update themselves in place — the element IS the identity,
            // so there are no global maps to rekey.
            app.nav.apply_resolved(&original, &resolved);
            // If the resolved resource is cluster-scoped but we're in a
            // specific namespace, auto-switch the SELECTOR to All (a
            // display-only correction for future root constructions; the
            // element's own query already used the server-resolved scope).
            if app.nav.resource_id() == Some(&resolved)
                && resolved.is_cluster_scoped()
                && !app.kube.selected_ns.is_all()
            {
                app.kube.selected_ns = k9rs_proto::protocol::Namespace::All;
            }
        }
        AppEvent::SubscriptionFailed { resource, message } => {
            // FLASH-ONLY: the failing bridge separately delivered
            // `StorePayload::Failed` to its own store (epoch-gated), which
            // is what flips the owning element's state. This surfaces the
            // message regardless of which view is active.
            app.ui.flash = Some(crate::FlashMessage::error(
                format!("{}: {}", resource.short_label(), message)
            ));
        }
        AppEvent::PodMetrics(metrics) => {
            // Elements bound to the hub overlay these at derive time —
            // the version bump invalidates their view memos; nothing to
            // route, nothing to re-apply.
            app.kube.metrics.set_pods(metrics);
        }
        AppEvent::NodeMetrics(metrics) => {
            app.kube.metrics.set_nodes(metrics);
        }
        AppEvent::Discovery { namespaces, crds } => {
            // Cached discovery data seeds the app-level core stores
            // (completion / picker sources) at the SEED epoch — always
            // weaker than live data, so a stale cache can never clobber a
            // live core stream.
            use k9rs_proto::resource_def::BuiltInKind;
            if !namespaces.is_empty() {
                let rows = k9rs_kube::resources::namespaces::cached_namespaces_to_rows(&namespaces);
                app.core.seed(BuiltInKind::Namespace, rows);
            }
            if !crds.is_empty() {
                let rows = k9rs_kube::resources::crds::cached_crds_to_rows(&crds);
                app.core.seed(BuiltInKind::CustomResourceDefinition, rows);
            }
        }
        AppEvent::Log(ev) => {
            // Destination rides the event — apply into the line store.
            match ev.payload {
                crate::event::LogPayload::Line(line) => ev.store.push(ev.epoch, line),
                crate::event::LogPayload::Ended => ev.store.mark_ended(ev.epoch),
            }
        }
        AppEvent::ExecData(bytes) => {
            if let Some(crate::TakeoverOp::Shell(shell)) = app.ui.takeover_mut() {
                shell.connect_state = crate::ShellConnectState::Connected;
                // Buffer output until the main loop enters bridge mode.
                // These bytes (typically the initial shell prompt) will be
                // flushed to stdout when the TUI suspends.
                shell.pending_output.extend_from_slice(&bytes);
            }
        }
        AppEvent::ExecEnded => {
            // Pre-bridge ExecEnded. During bridge mode this event is consumed
            // directly by the bridge loop (it never reaches this handler), so
            // arriving here means the shell ended in the one-turn window
            // before the bridge took over — and the overlay's own state says
            // which story that is: still `Connecting` = the connection FAILED
            // (error flash); already `Connected` = the shell came up and
            // exited immediately (fast command / instant exit — a normal end,
            // not a failure).
            if let Some(crate::TakeoverOp::Shell(ref shell)) = app.ui.takeover() {
                app.ui.flash = Some(match shell.connect_state {
                    crate::ShellConnectState::Connecting => {
                        crate::FlashMessage::error("Shell connection failed".to_string())
                    }
                    crate::ShellConnectState::Connected => {
                        crate::FlashMessage::info("Shell session ended".to_string())
                    }
                });
                app.ui.close_modal();
            }
        }
        AppEvent::DaemonDisconnected => {
            // Don't quit — trigger auto-reconnection instead (the main
            // loop drops the old session and builds a new one, same as
            // context switching; the user stays in the TUI) and show the
            // connecting screen NOW (this frame), not stale rows.
            app.conn.disconnected();
            // A batch's remaining results die with the connection; fold
            // its partial tally into the message instead of dropping it
            // silently (single flash slot).
            let batch_note = app
                .pending_batch
                .take()
                .filter(|b| !b.is_done())
                .map(|b| format!(" ({})", b.interrupted_summary().message))
                .unwrap_or_default();
            app.ui.flash = Some(crate::FlashMessage::warn(
                format!("Connection lost — reconnecting...{}", batch_note)
            ));
        }
        AppEvent::ConnectionEstablished { context, identity, namespaces } => {
            // Daemon's view is authoritative — overwrite whatever the
            // KubeconfigLoaded stage put there.
            app.kube.context = Some(context);
            app.kube.identity = identity;
            // Confirmed — the candidate has served its purpose.
            app.kube.connecting = None;
            if !namespaces.is_empty() {
                let ns_rows = k9rs_kube::resources::namespaces::cached_namespaces_to_rows(&namespaces);
                app.core.seed(k9rs_proto::resource_def::BuiltInKind::Namespace, ns_rows);
            }
            // The connection is LIVE — the render gate stops showing the
            // connecting screen, future failures are reconnects, and the
            // backoff resets.
            app.conn.established();
        }
        AppEvent::ConnectionFailed(message) => {
            let in_flight_target = match &app.kube.context_switch {
                crate::ContextSwitchState::InFlight(t) => Some(t.clone()),
                // `Requested` is NOT the switch failing: its session hasn't
                // been built yet (the main loop takes the request at the top
                // of its next turn), so a failure arriving in that state
                // belongs to a PRIOR attempt — fall through to the reconnect
                // arms and leave the queued switch to proceed. (`target()`
                // deliberately spans both states; this edge needs the
                // narrower question.)
                // NAMED, not `_`. A future `Cancelling`/`Retrying` variant
                // falling in here would take the reconnect-backoff branch
                // instead of settling the switch: `is_stable()` would stay
                // false and every later `:context` would be refused forever.
                // That is the 2026-07-30 context-lock bug, and `types.rs`
                // already warns that outcome edges must match `InFlight`.
                crate::ContextSwitchState::Stable
                | crate::ContextSwitchState::Requested(_) => None,
            };
            if let Some(target) = in_flight_target {
                // A context SWITCH failed: the target we asked for is
                // unreachable. The daemon itself is fine — this is NOT a
                // daemon disconnect (which would fire from a `Stable` state),
                // so it must NOT be treated as a reconnect-to-the-target, or
                // the loop would hammer an unreachable context forever while
                // the switch stayed stuck InFlight and locked out every later
                // `:context`. Settle the switch (unlock) and fall back to the
                // last CONFIRMED context — still in `app.kube.context`, since
                // a switch no longer clobbers it — by requesting an immediate
                // reconnect (`reconnect_at = None`; the daemon is up, no need
                // to back off). If that fallback itself fails, we're now
                // `Stable` again so it lands in the steady-state arm below and
                // backs off normally — no lock either way.
                app.kube.context_switch.settle();
                app.conn.switch_failed_fallback();
                app.ui.flash = Some(crate::FlashMessage::error(
                    format!("Couldn't reach context {}: {}", target, message)
                ));
            } else if app.conn.has_ever_connected() {
                // A steady-state RECONNECT failed (the daemon died and hasn't
                // come back yet). Stay in the TUI and retry with backoff —
                // quitting on the first failed re-connect contradicts the
                // DaemonDisconnected "the user stays in the TUI" contract.
                // The user can Ctrl-C to leave.
                app.conn.reconnect_failed_backoff(std::time::Instant::now());
                app.ui.flash = Some(crate::FlashMessage::warn(
                    format!("Reconnecting to daemon... ({})", message)
                ));
            } else {
                // The INITIAL connection failed — nothing to fall back to.
                app.exit_reason = Some(crate::ExitReason::Error(message));
                app.should_quit = true;
            }
        }
        AppEvent::KubeconfigLoaded {
            contexts, current_context, current_identity,
        } => {
            // A CANDIDATE, not a confirmation: this fires before the
            // handshake and names the context this session is aiming at. It
            // must never touch `context`/`identity`, which mean "last
            // CONFIRMED" and are what a failed switch falls back to.
            app.kube.connecting = current_context.map(|c| (c, current_identity));
            // One store, re-seeded in place: a contexts view that is already
            // open sees the new rows on its next derive, exactly as a
            // resource table sees a fresh baseline. No second copy to sync.
            app.core.seed_contexts(&contexts);
        }
        AppEvent::NoContextConfigured => {
            // Nothing to connect to, so don't pretend to be connecting: the
            // link goes to `NoContext` (no retry plan — a retry with no
            // target is just a loop) and the picker becomes the whole UI.
            // `KubeconfigLoaded` always precedes this event, so the contexts
            // are already loaded.
            app.conn.no_context();
            // RESET, not push: everything below belongs to a context we do
            // not have. Making it the ROOT is also what makes it
            // un-escapable — `NavStack::pop` refuses at depth 1, so Esc has
            // nothing to fall back to and needs no special case.
            if let Some(root) = app.core.client_root_element(
                &app.kube.metrics,
                &k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id(),
                k9rs_proto::protocol::Namespace::All,
            ) {
                app.nav.reset(root);
            }
            app.ui.flash = Some(crate::FlashMessage::info(
                "No current-context set — select one with Enter (or start with --context)".to_string(),
            ));
        }
    }
}

/// Route a target-ed operation result to its consumer, in priority order:
/// the edit-apply flow (overlay in `Applying` for THIS target), then the
/// in-flight batch tracker, then a plain flash. (op, target) correlation
/// is what makes each consumer take only its own results — the old
/// target-less `CommandResult` let any concurrent result pop the edit
/// overlay, and the v9 target-only shape still let an edit-apply and a
/// batch op on the SAME object claim each other's outcomes.
fn apply_op_result(
    app: &mut App,
    op: k9rs_proto::protocol::OperationKind,
    target: k9rs_proto::protocol::ObjectRef,
    result: Result<String, String>,
) {
    // 1. Edit flow: terminal state of an apply. Only take the overlay out
    // if this result IS an apply for the edited object.
    let is_applying_this = op == k9rs_proto::protocol::OperationKind::Apply
        && matches!(
            app.ui.takeover(),
            Some(crate::TakeoverOp::Edit {
                target: ref t,
                state: crate::EditState::Applying { .. },
            }) if *t == target
        );
    if is_applying_this {
        // Move the overlay out so we own TempFile (not clone).
        if let Some(crate::TakeoverOp::Edit {
            target: edit_target,
            state: crate::EditState::Applying { temp_file, original },
        }) = app.ui.take_modal().into_takeover() {
            match &result {
                Ok(_) => {
                    drop(temp_file);
                    app.kube.kubectl_cache.clear();
                }
                Err(msg) => {
                    let current = std::fs::read_to_string(temp_file.path()).unwrap_or_default();
                    let with_error = format!(
                        "# k9rs: Error from server:\n# k9rs: {}\n# k9rs: Save to retry, :cq to abort.\n#\n{}",
                        msg, current,
                    );
                    let _ = std::fs::write(temp_file.path(), &with_error);
                    app.ui.open(crate::Modal::Takeover(crate::TakeoverOp::Edit {
                        target: edit_target,
                        state: crate::EditState::EditorReady { temp_file, original },
                    }));
                }
            }
        }
        app.ui.flash = Some(match result {
            Ok(msg) => crate::FlashMessage::info(msg),
            Err(msg) => crate::FlashMessage::error(msg),
        });
        return;
    }

    // 2. Batch tracker: consume our items' results silently; ONE summary
    // flash when the last lands.
    if let Some(tracker) = app.pending_batch.as_mut() {
        if tracker.consume(&op, &target, &result) {
            if tracker.is_done() {
                app.ui.flash = Some(tracker.summary());
                app.pending_batch = None;
            }
            return;
        }
    }

    // 3. Ordinary single-op result: flash it.
    app.ui.flash = Some(match result {
        Ok(msg) => crate::FlashMessage::info(msg),
        Err(msg) => crate::FlashMessage::error(msg),
    });
}

fn apply_resource_update(
    app: &mut App,
    update: ResourceUpdate,
) {
    match update {
        ResourceUpdate::Yaml { target: response_target, content } => {
            // Two consumers:
            //   1. Every `ContentView` showing this target's YAML — routed
            //      by content identity, WHEREVER it sits in the stack
            //      (covered views fill while hidden, so a pop-reveal shows
            //      content instead of an orphaned spinner).
            //   2. The Edit overlay in `AwaitingYaml` — write the temp
            //      file and hand off to the main loop's editor poll. A
            //      matched view takes priority (the fetch was the view's).
            use crate::element::{ContentPhase, ContentSpec};
            let nav = &mut app.nav;
            let cache = &mut app.kube.kubectl_cache;
            let mut view_took_it = false;
            nav.for_each_content_view(|cv| {
                if let ContentSpec::Yaml(ref target) = cv.kind {
                    if *target == response_target {
                        if cv.phase == ContentPhase::Fetching {
                            cache.insert(
                                target.clone(),
                                crate::ContentKind::Yaml,
                                content.clone(),
                            );
                        }
                        cv.phase = ContentPhase::Ready;
                        cv.state.set_content(content.clone());
                        view_took_it = true;
                    }
                }
            });
            if view_took_it {
                return;
            }
            if let Some(crate::TakeoverOp::Edit { target, state }) = app.ui.takeover_mut() {
                if *target != response_target { return; }
                if matches!(state, crate::EditState::AwaitingYaml) {
                    match write_edit_temp_file(target, &content) {
                        Ok(temp_path) => {
                            *state = crate::EditState::EditorReady {
                                temp_file: crate::TempFile(temp_path),
                                original: content.clone(),
                            };
                        }
                        Err(e) => {
                            // Couldn't write the temp file — abort the edit.
                            app.ui.flash = Some(crate::FlashMessage::error(
                                format!("Edit failed: {}", e)
                            ));
                            app.ui.close_modal();
                        }
                    }
                }
            }
        }
        ResourceUpdate::Describe { target: response_target, mut lines } => {
            // Describe text (and Secret-decode output, which rides the same
            // event) is cluster-controlled and renders through the
            // Paragraph path, which does NOT strip control chars. Sanitize
            // once here, at ingest — before the cache and the view both see
            // it — so no annotation value or decoded byte can smuggle a
            // terminal escape (OSC 52 clipboard, cursor-report stdin
            // injection) onto the screen.
            for line in &mut lines {
                if line.text.chars().any(|c| c.is_control() && c != '\t') {
                    line.text = k9rs_util::text::sanitize_terminal(&line.text);
                }
            }
            // Delivery walk, same shape as Yaml above. `DecodedSecret`
            // views ride the same wire event by design — they receive the
            // lines but NEVER cache them (decoded secret bytes must not
            // become the target's cached describe text; that mistake is
            // what the distinct spec kind exists to prevent). Residual
            // wire-level ambiguity: a describe view and a decode view for
            // the SAME secret both match this event and both display it —
            // disambiguating needs an op discriminant on the wire event.
            use crate::element::{ContentPhase, ContentSpec};
            let nav = &mut app.nav;
            let cache = &mut app.kube.kubectl_cache;
            // Kind axis EXHAUSTIVE; the target comparison is the only guard.
            //
            // Both arms used to carry the `if *target == …` in the PATTERN,
            // which forced a trailing wildcard — and that wildcard was doing
            // two jobs. Absorbing "same kind, wrong target" is right;
            // absorbing "a kind this walk has never heard of" is how a future
            // `ContentSpec` delivered over this same event would sit at
            // `Fetching` forever, animating "Loading…" with no request behind
            // it. That is precisely the state `ContentPhase` replaced an
            // `awaiting_response: bool` to make unrepresentable.
            nav.for_each_content_view(|cv| {
                match cv.kind {
                    ContentSpec::Describe(ref target) => {
                        if *target != response_target {
                            return;
                        }
                        if cv.phase == ContentPhase::Fetching {
                            cache.insert_describe(target.clone(), lines.clone());
                        }
                        cv.phase = ContentPhase::Ready;
                        cv.state.set_describe_lines(lines.clone());
                    }
                    ContentSpec::DecodedSecret(ref target) => {
                        if *target != response_target {
                            return;
                        }
                        cv.phase = ContentPhase::Ready;
                        cv.state.set_describe_lines(lines.clone());
                    }
                    // Delivered over their own events, not this one.
                    ContentSpec::Yaml(_) => {}
                }
            });
        }
    }
}


/// Write the YAML returned by the server to a temp file the editor can
/// open. Goes through [`k9rs_util::temp::safe_write_temp`] which puts the file
/// in our per-process `0700` dir and uses `O_CREAT | O_EXCL` so a planted
/// symlink can't divert the write to an attacker-chosen location. The
/// filename still embeds the resource type + name so editors that show
/// the filename in their title give the user useful context.
fn write_edit_temp_file(
    target: &k9rs_proto::protocol::ObjectRef,
    yaml: &str,
) -> std::io::Result<std::path::PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
    let filename = format!(
        "edit-{}-{}-{}.yaml",
        safe(target.resource.plural()),
        safe(&target.name),
        n,
    );
    k9rs_util::temp::safe_write_temp(&filename, yaml.as_bytes())
}

#[cfg(test)]
#[path = "tests/event.rs"]
mod tests;
