use crate::app::App;

use crate::event::{AppEvent, ResourceUpdate};

/// Handle a single AppEvent (resource update, error, or flash).
pub(crate) fn apply_event(
    app: &mut App,
    event: AppEvent,
) {
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
                Ok(msg) => crate::app::FlashMessage::info(msg),
                Err(msg) => crate::app::FlashMessage::error(msg),
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
                app.kube.selected_ns = crate::kube::protocol::Namespace::All;
            }
        }
        AppEvent::SubscriptionFailed { resource, message } => {
            // FLASH-ONLY: the failing bridge separately delivered
            // `StorePayload::Failed` to its own store (epoch-gated), which
            // is what flips the owning element's state. This surfaces the
            // message regardless of which view is active.
            app.ui.flash = Some(crate::app::FlashMessage::error(
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
            use crate::kube::resource_def::BuiltInKind;
            if !namespaces.is_empty() {
                let rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                app.core.seed(BuiltInKind::Namespace, rows);
            }
            if !crds.is_empty() {
                let rows = crate::kube::cache::cached_crds_to_rows(&crds);
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
            if let Some(crate::app::Overlay::Shell(ref mut shell)) = app.ui.overlay {
                shell.connect_state = crate::app::ShellConnectState::Connected;
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
            if let Some(crate::app::Overlay::Shell(ref shell)) = app.ui.overlay {
                app.ui.flash = Some(match shell.connect_state {
                    crate::app::ShellConnectState::Connecting => {
                        crate::app::FlashMessage::error("Shell connection failed".to_string())
                    }
                    crate::app::ShellConnectState::Connected => {
                        crate::app::FlashMessage::info("Shell session ended".to_string())
                    }
                });
                app.ui.overlay = None;
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
            app.ui.flash = Some(crate::app::FlashMessage::warn(
                format!("Connection lost — reconnecting...{}", batch_note)
            ));
        }
        AppEvent::ConnectionEstablished { context, identity, namespaces } => {
            // Daemon's view is authoritative — overwrite whatever the
            // KubeconfigLoaded stage put there.
            app.kube.context = Some(context);
            app.kube.identity = identity;
            if !namespaces.is_empty() {
                let ns_rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                app.core.seed(crate::kube::resource_def::BuiltInKind::Namespace, ns_rows);
            }
            // The connection is LIVE — the render gate stops showing the
            // connecting screen, future failures are reconnects, and the
            // backoff resets.
            app.conn.established();
        }
        AppEvent::ConnectionFailed(message) => {
            let in_flight_target = match &app.kube.context_switch {
                crate::app::ContextSwitchState::InFlight(t) => Some(t.clone()),
                // `Requested` is NOT the switch failing: its session hasn't
                // been built yet (the main loop takes the request at the top
                // of its next turn), so a failure arriving in that state
                // belongs to a PRIOR attempt — fall through to the reconnect
                // arms and leave the queued switch to proceed. (`target()`
                // deliberately spans both states; this edge needs the
                // narrower question.)
                _ => None,
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
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("Couldn't reach context {}: {}", target, message)
                ));
            } else if app.conn.has_ever_connected() {
                // A steady-state RECONNECT failed (the daemon died and hasn't
                // come back yet). Stay in the TUI and retry with backoff —
                // quitting on the first failed re-connect contradicts the
                // DaemonDisconnected "the user stays in the TUI" contract.
                // The user can Ctrl-C to leave.
                app.conn.reconnect_failed_backoff(std::time::Instant::now());
                app.ui.flash = Some(crate::app::FlashMessage::warn(
                    format!("Reconnecting to daemon... ({})", message)
                ));
            } else {
                // The INITIAL connection failed — nothing to fall back to.
                app.exit_reason = Some(crate::app::ExitReason::Error(message));
                app.should_quit = true;
            }
        }
        AppEvent::KubeconfigLoaded {
            contexts, current_context, current_identity,
        } => {
            // Adopt the kubeconfig's view only if the daemon hasn't already
            // published its own (authoritative) values via ConnectionEstablished.
            // In the normal startup order KubeconfigLoaded arrives first and
            // ConnectionEstablished arrives later, so this branch is taken.
            if app.kube.context.is_none() {
                app.kube.context = current_context;
                app.kube.identity = current_identity;
            }
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
                &crate::kube::local::LocalResourceKind::Context.to_resource_id(),
                crate::kube::protocol::Namespace::All,
            ) {
                app.nav.reset(root);
            }
            app.ui.flash = Some(crate::app::FlashMessage::info(
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
    op: crate::kube::protocol::OperationKind,
    target: crate::kube::protocol::ObjectRef,
    result: Result<String, String>,
) {
    // 1. Edit flow: terminal state of an apply. Only take the overlay out
    // if this result IS an apply for the edited object.
    let is_applying_this = op == crate::kube::protocol::OperationKind::Apply
        && matches!(
            app.ui.overlay,
            Some(crate::app::Overlay::Edit {
                target: ref t,
                state: crate::app::EditState::Applying { .. },
            }) if *t == target
        );
    if is_applying_this {
        // Move the overlay out so we own TempFile (not clone).
        if let Some(crate::app::Overlay::Edit {
            target: edit_target,
            state: crate::app::EditState::Applying { temp_file, original },
        }) = app.ui.overlay.take() {
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
                    app.ui.overlay = Some(crate::app::Overlay::Edit {
                        target: edit_target,
                        state: crate::app::EditState::EditorReady { temp_file, original },
                    });
                }
            }
        }
        app.ui.flash = Some(match result {
            Ok(msg) => crate::app::FlashMessage::info(msg),
            Err(msg) => crate::app::FlashMessage::error(msg),
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
        Ok(msg) => crate::app::FlashMessage::info(msg),
        Err(msg) => crate::app::FlashMessage::error(msg),
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
            use crate::app::element::{ContentPhase, ContentSpec};
            let nav = &mut app.nav;
            let cache = &mut app.kube.kubectl_cache;
            let mut view_took_it = false;
            nav.for_each_content_view(|cv| {
                if let ContentSpec::Yaml(ref target) = cv.kind {
                    if *target == response_target {
                        if cv.phase == ContentPhase::Fetching {
                            cache.insert(
                                target.clone(),
                                crate::app::ContentKind::Yaml,
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
            if let Some(crate::app::Overlay::Edit { ref target, ref mut state }) = app.ui.overlay {
                if *target != response_target { return; }
                if matches!(state, crate::app::EditState::AwaitingYaml) {
                    match write_edit_temp_file(target, &content) {
                        Ok(temp_path) => {
                            *state = crate::app::EditState::EditorReady {
                                temp_file: crate::app::TempFile(temp_path),
                                original: content.clone(),
                            };
                        }
                        Err(e) => {
                            // Couldn't write the temp file — abort the edit.
                            app.ui.flash = Some(crate::app::FlashMessage::error(
                                format!("Edit failed: {}", e)
                            ));
                            app.ui.overlay = None;
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
                    line.text = crate::util::sanitize_terminal(&line.text);
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
            use crate::app::element::{ContentPhase, ContentSpec};
            let nav = &mut app.nav;
            let cache = &mut app.kube.kubectl_cache;
            nav.for_each_content_view(|cv| {
                match cv.kind {
                    ContentSpec::Describe(ref target) if *target == response_target => {
                        if cv.phase == ContentPhase::Fetching {
                            cache.insert_describe(target.clone(), lines.clone());
                        }
                        cv.phase = ContentPhase::Ready;
                        cv.state.set_describe_lines(lines.clone());
                    }
                    ContentSpec::DecodedSecret(ref target) if *target == response_target => {
                        cv.phase = ContentPhase::Ready;
                        cv.state.set_describe_lines(lines.clone());
                    }
                    _ => {}
                }
            });
        }
    }
}


/// Write the YAML returned by the server to a temp file the editor can
/// open. Goes through [`crate::util::safe_write_temp`] which puts the file
/// in our per-process `0700` dir and uses `O_CREAT | O_EXCL` so a planted
/// symlink can't divert the write to an attacker-chosen location. The
/// filename still embeds the resource type + name so editors that show
/// the filename in their title give the user useful context.
fn write_edit_temp_file(
    target: &crate::kube::protocol::ObjectRef,
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
    crate::util::safe_write_temp(&filename, yaml.as_bytes())
}

#[cfg(test)]
#[path = "../tests/kube/session_events.rs"]
mod tests;
