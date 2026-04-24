use crate::app::App;

use crate::kube::resource_def::MetricsKind;
use crate::event::{AppEvent, ResourceUpdate};

/// Look up the metrics overlay kind for a resource, if any, via the registry.
/// CRDs and locals never carry metrics overlays, so this returns `None` for them.
fn metrics_kind_for(rid: &crate::kube::protocol::ResourceId) -> Option<MetricsKind> {
    let kind = rid.built_in_kind()?;
    crate::kube::resource_defs::REGISTRY.by_kind(kind).metrics_kind()
}

/// Handle a single AppEvent (resource update, error, or flash).
pub(crate) fn apply_event(
    app: &mut App,
    event: AppEvent,
) {
    match event {
        AppEvent::ResourceUpdate(update) => apply_resource_update(app, update),
        AppEvent::Flash(flash) => {
            // Purely local flashes: just show them. Do NOT pop the edit
            // route — that's driven by `CommandResult` below.
            app.ui.flash = Some(flash);
        }
        AppEvent::CommandResult(result) => {
            // Terminal state of the unified edit flow. Only take the route
            // out if we're actually in EditingResource::Applying — don't
            // touch app.route if the user navigated elsewhere (avoids a
            // flicker from the replace/restore cycle).
            let is_applying = matches!(
                app.route,
                crate::app::Route::EditingResource {
                    state: crate::app::EditState::Applying { .. }, ..
                }
            );
            if is_applying {
                // Move the route out so we own TempFile (not clone).
                let old_route = std::mem::replace(&mut app.route, crate::app::Route::Resources);
                if let crate::app::Route::EditingResource {
                    target,
                    state: crate::app::EditState::Applying { temp_file, original },
                } = old_route {
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
                            app.route = crate::app::Route::EditingResource {
                                target,
                                state: crate::app::EditState::EditorReady { temp_file, original },
                            };
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
            // The server discovered the true identity of a resource we subscribed to
            // with incomplete info (e.g., `:nodeclaims` → karpenter.sh/v1/NodeClaim/Cluster).
            // Update the nav step's ResourceId and move the table entry so future
            // snapshots (keyed by resolved rid) find the right table.
            if *app.nav.resource_id() == original {
                app.nav.current_mut().resource = resolved.clone();
                // If the resolved resource is cluster-scoped but we're in a
                // specific namespace, auto-switch to All. The watcher already
                // uses Api::all_with (server resolved the scope), so this is
                // a display-only correction — same as the auto-switch that
                // fires when discovery is loaded (session_commands.rs:366).
                if resolved.is_cluster_scoped() && !app.kube.selected_ns.is_all() {
                    app.kube.selected_ns = crate::kube::protocol::Namespace::All;
                }
            }
            // Move the table entry and descriptor from old key to new key.
            // For globally-stored resources, this moves entries in the global
            // maps. For NavStep-owned resources, the step's `resource` field
            // was already updated above and the table/descriptor live on the
            // same step — no rekey needed (the find methods walk by identity).
            if crate::app::nav::is_globally_stored(&original) || crate::app::nav::is_globally_stored(&resolved) {
                if let Some(table) = app.data.tables.remove(&original) {
                    app.data.tables.insert(resolved.clone(), table);
                }
                if let Some(desc) = app.data.descriptors.remove(&original) {
                    app.data.descriptors.insert(resolved, desc);
                }
            }
        }
        AppEvent::SubscriptionFailed { resource, message } => {
            // Mark the table as errored so the UI shows the error instead of spinner.
            // Clear items so the error is visible even if data was previously loaded
            // (the rendering code only shows table.error when items is empty).
            // IMPORTANT: clear_data() first, THEN set error — clear_data() resets
            // error to None, so doing it after would wipe the error we just set.
            if crate::app::nav::is_globally_stored(&resource) {
                let table = app.data.tables.entry(resource.clone()).or_default();
                table.clear_data();
                table.error = Some(message.clone());
            } else if let Some(table) = app.nav.find_table_for_resource_mut(&resource) {
                table.clear_data();
                table.error = Some(message.clone());
            }
            // The bridge task behind the failing subscription has already
            // exited. Drop the stale `SubscriptionStream` handle sitting
            // in the nav stack so a later Esc pop-back past this rid
            // re-subscribes instead of thinking the dead handle is a
            // live owner.
            app.nav.clear_dead_subscription_for(&resource);
            // Flash the error so the user sees it regardless of which
            // view is active — table.error is only visible when viewing
            // that specific resource.
            app.ui.flash = Some(crate::app::FlashMessage::error(
                format!("{}: {}", resource.short_label(), message)
            ));
        }
        AppEvent::PodMetrics(metrics) => {
            app.kube.pod_metrics = metrics;
            app.apply_pod_metrics();
            // Metrics overlay changes cell values — if the user has a grep
            // filter active on CPU/MEM columns, re-filter so it reflects the
            // updated values. Only needed when viewing a metrics-overlay
            // resource (pods or nodes).
            if metrics_kind_for(app.nav.resource_id()).is_some() {
                app.reapply_nav_filters();
            }
        }
        AppEvent::NodeMetrics(metrics) => {
            app.kube.node_metrics = metrics;
            app.apply_node_metrics();
            if metrics_kind_for(app.nav.resource_id()).is_some() {
                app.reapply_nav_filters();
            }
        }
        AppEvent::LogStreamEnded => {
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                state.streaming = false;
            }
        }
        AppEvent::ExecData(bytes) => {
            if let crate::app::Route::Shell(ref mut shell) = app.route {
                shell.connected = true;
                shell.parser.process(&bytes);
            }
        }
        AppEvent::ExecEnded => {
            if matches!(app.route, crate::app::Route::Shell(_)) {
                app.ui.flash = Some(crate::app::FlashMessage::info("Shell session ended".to_string()));
                app.pop_route();
            }
        }
        AppEvent::DaemonDisconnected => {
            app.exit_reason = Some(crate::app::ExitReason::DaemonDisconnected);
            app.should_quit = true;
        }
        AppEvent::ConnectionEstablished { context, identity, namespaces } => {
            // Daemon's view is authoritative — overwrite whatever the
            // KubeconfigLoaded stage put there.
            app.kube.context = context;
            app.kube.identity = identity;
            if !namespaces.is_empty() {
                let ns_rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                let table = app.data.tables.entry(crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Namespace))
                    .or_default();
                table.set_items(ns_rows);
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
            app.data.contexts.set_items(contexts);
        }
    }
}

fn apply_resource_update(
    app: &mut App,
    update: ResourceUpdate,
) {
    match update {
        ResourceUpdate::Rows { resource, headers, rows } => {
            let num_cols = headers.len();
            let descriptor = crate::app::TableDescriptor { headers };
            if crate::app::nav::is_globally_stored(&resource) {
                // Set descriptor BEFORE populating table data so column-
                // restricted greps see the correct column count.
                app.data.descriptors.insert(resource.clone(), descriptor);
                let table = app.data.tables.entry(resource.clone()).or_default();
                app.ui.deltas.update(&rows);
                table.set_num_cols(num_cols);
                table.set_items_filtered(rows);
            } else {
                app.nav.set_descriptor_for_resource(&resource, descriptor);
                if let Some(table) = app.nav.find_table_for_resource_mut(&resource) {
                    app.ui.deltas.update(&rows);
                    table.set_num_cols(num_cols);
                    table.set_items_filtered(rows);
                } else {
                    return;
                }
            }
            // Apply metrics overlay whenever fresh data arrives. Dispatch
            // through the typed `MetricsKind` enum — no string match on
            // `resource.plural`. Pod and node metrics come from different
            // APIs and update different columns, hence the per-kind branch.
            match metrics_kind_for(&resource) {
                Some(MetricsKind::Pod) => app.apply_pod_metrics(),
                Some(MetricsKind::Node) => app.apply_node_metrics(),
                None => {}
            }
        }
        ResourceUpdate::Yaml { target: response_target, content } => {
            // Two routes consume `YamlResult`:
            //   1. `Route::ContentView { kind: Yaml }` — the read-only YAML viewer.
            //   2. `Route::EditingResource { state: AwaitingYaml }` — the
            //      first stage of the unified edit flow. We write the
            //      content to a temp file and transition to `EditorReady`,
            //      and the session main loop will pick it up on its next
            //      iteration to suspend + exec `$EDITOR`.
            //
            // Gate on `target == response_target`: if the user navigated
            // A→B while A's fetch was in flight, dropping the A response
            // prevents it from writing A's content under B's identity.
            if let crate::app::Route::ContentView {
                kind: crate::app::ContentViewKind::Yaml,
                target: Some(ref target),
                ref mut awaiting_response,
                ref mut state,
            } = app.route {
                if *target != response_target { return; }
                if *awaiting_response {
                    app.kube.kubectl_cache.insert(target.clone(), crate::app::ContentKind::Yaml, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
            } else if let crate::app::Route::EditingResource {
                ref target,
                ref mut state,
            } = app.route {
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
                            // Couldn't write the temp file — abort the edit
                            // and pop back to the previous route.
                            app.ui.flash = Some(crate::app::FlashMessage::error(
                                format!("Edit failed: {}", e)
                            ));
                            app.pop_route();
                        }
                    }
                }
            }
        }
        ResourceUpdate::Describe { target: response_target, content } => {
            if let crate::app::Route::ContentView {
                kind: crate::app::ContentViewKind::Describe,
                target: Some(ref target),
                ref mut awaiting_response,
                ref mut state,
            } = app.route {
                if *target != response_target { return; }
                if *awaiting_response {
                    app.kube.kubectl_cache.insert(target.clone(), crate::app::ContentKind::Describe, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
            }
        }
        ResourceUpdate::LogLine { generation, line } => {
            // Gate apply on a matching generation id. The previous log
            // stream's bridge may have been aborted, but events already in
            // the channel queue still arrive after the route changed —
            // those carry the OLD generation and must be dropped.
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                if state.generation == generation {
                    state.push(line);
                }
            }
            return; // Log lines don't need nav filter reapply
        }
    }
    // Reapply nav stack filters after every table data update so that drill-down
    // and grep filters stay active as fresh snapshots arrive.
    app.reapply_nav_filters();

    // Expire old change highlights.
    app.ui.deltas.expire(crate::app::CHANGE_HIGHLIGHT_DURATION);
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
