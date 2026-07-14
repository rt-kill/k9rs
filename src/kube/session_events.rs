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
            // route — that's driven by `CommandResult` below.
            app.ui.flash = Some(flash);
        }
        AppEvent::CommandResult(result) => {
            // Terminal state of the unified edit flow. Only take the
            // overlay out if we're actually in Edit::Applying — an
            // unrelated CommandResult must not disturb whatever dialog
            // is up.
            let is_applying = matches!(
                app.ui.overlay,
                Some(crate::app::Overlay::Edit {
                    state: crate::app::EditState::Applying { .. }, ..
                })
            );
            if is_applying {
                // Move the overlay out so we own TempFile (not clone).
                if let Some(crate::app::Overlay::Edit {
                    target,
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
                                target,
                                state: crate::app::EditState::EditorReady { temp_file, original },
                            });
                        }
                    }
                }
            }
            app.ui.flash = Some(match result {
                Ok(msg) => crate::app::FlashMessage::info(msg),
                Err(msg) => crate::app::FlashMessage::error(msg),
            });
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
            // During the Connecting phase, ExecEnded means the connection
            // failed before we entered bridge mode. Clear the overlay and
            // flash. During bridge mode this event is consumed directly by
            // the bridge loop (it never reaches this handler).
            if matches!(app.ui.overlay, Some(crate::app::Overlay::Shell(_))) {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    "Shell connection failed".to_string()
                ));
                app.ui.overlay = None;
            }
        }
        AppEvent::DaemonDisconnected => {
            // Don't quit — trigger auto-reconnection instead. The main
            // loop will drop the old session and create a new one, same
            // as context switching. The user stays in the TUI.
            app.reconnect_requested = true;
            app.ui.flash = Some(crate::app::FlashMessage::warn(
                "Connection lost — reconnecting...".to_string()
            ));
        }
        AppEvent::ConnectionEstablished { context, identity, namespaces } => {
            // Daemon's view is authoritative — overwrite whatever the
            // KubeconfigLoaded stage put there.
            app.kube.context = context;
            app.kube.identity = identity;
            if !namespaces.is_empty() {
                let ns_rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                app.core.seed(crate::kube::resource_def::BuiltInKind::Namespace, ns_rows);
            }
        }
        AppEvent::ConnectionFailed(message) => {
            app.exit_reason = Some(crate::app::ExitReason::Error(message));
            app.should_quit = true;
        }
        AppEvent::KubeconfigLoaded {
            contexts, current_context, current_identity,
        } => {
            // Adopt the kubeconfig's view only if the daemon hasn't already
            // published its own (authoritative) values via ConnectionEstablished.
            // In the normal startup order KubeconfigLoaded arrives first and
            // ConnectionEstablished arrives later, so this branch is taken.
            if app.kube.context.is_empty() {
                app.kube.context = current_context;
                app.kube.identity = current_identity;
            }
            app.data.contexts.set_items(contexts.clone());
            // A showing context picker refreshes in place (top-only touch).
            if let crate::app::element::Element::ContextList(c) = app.nav.top_mut() {
                c.table.set_items(contexts);
            }
        }
    }
}

fn apply_resource_update(
    app: &mut App,
    update: ResourceUpdate,
) {
    match update {
        ResourceUpdate::Yaml { target: response_target, content } => {
            // Two consumers:
            //   1. A `ContentView` element showing this target's YAML —
            //      delivery is peek-top-and-match (a response for a view
            //      the user already left is dropped; re-entering re-fetches).
            //   2. The Edit overlay in `AwaitingYaml` — write the temp
            //      file and hand off to the main loop's editor poll.
            use crate::app::element::{ContentSpec, Element};
            if let Element::ContentView(cv) = app.nav.top_mut() {
                if let ContentSpec::Yaml(ref target) = cv.kind {
                    if *target == response_target {
                        if cv.awaiting_response {
                            app.kube.kubectl_cache.insert(
                                target.clone(),
                                crate::app::ContentKind::Yaml,
                                content.clone(),
                            );
                            cv.awaiting_response = false;
                        }
                        cv.state.set_content(content);
                        return;
                    }
                }
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
        ResourceUpdate::Describe { target: response_target, lines } => {
            use crate::app::element::{ContentSpec, Element};
            if let Element::ContentView(cv) = app.nav.top_mut() {
                if let ContentSpec::Describe(ref target) = cv.kind {
                    if *target == response_target {
                        if cv.awaiting_response {
                            app.kube.kubectl_cache.insert_describe(target.clone(), lines.clone());
                            cv.awaiting_response = false;
                        }
                        cv.state.set_describe_lines(lines);
                    }
                }
            }
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
