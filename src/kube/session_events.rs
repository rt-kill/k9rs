use std::collections::HashMap;

use crate::app::App;

use crate::kube::protocol::ObjectKey;
use crate::event::{AppEvent, ResourceUpdate};

/// Handle a single AppEvent (resource update, error, or flash).
pub(crate) fn apply_event(
    app: &mut App,
    event: AppEvent,
) {
    match event {
        AppEvent::ResourceUpdate(update) => apply_resource_update(app, update),
        AppEvent::Flash(flash) => {
            // Terminal state of the unified edit flow: when we're waiting
            // for an `Apply` to come back, the next `CommandResult`
            // (delivered as a `Flash` event) is *the* result. Pop the
            // edit route so the user lands back on the resource view,
            // and clear the kubectl cache since the resource may have
            // changed.
            if let crate::app::Route::EditingResource {
                state: crate::app::EditState::Applying,
                ..
            } = app.route {
                app.kubectl_cache.clear();
                app.pop_route();
            }
            app.flash = Some(flash);
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
                if resolved.is_cluster_scoped() && !app.selected_ns.is_all() {
                    app.selected_ns = crate::kube::protocol::Namespace::All;
                }
            }
            // Move the table entry from old key to new key.
            if let Some(table) = app.data.unified.remove(&original) {
                app.data.unified.insert(resolved.clone(), table);
            }
            if let Some(desc) = app.data.descriptors.remove(&original) {
                app.data.descriptors.insert(resolved, desc);
            }
        }
        AppEvent::SubscriptionFailed { resource, message } => {
            // Mark the table as errored so the UI shows the error instead of spinner.
            // Clear items so the error is visible even if data was previously loaded
            // (the rendering code only shows table.error when items is empty).
            let table = app.data.unified.entry(resource).or_insert_with(crate::app::StatefulTable::new);
            table.error = Some(message);
            table.clear_data();
        }
        AppEvent::PodMetrics(metrics) => {
            app.pod_metrics = metrics;
            app.apply_pod_metrics();
            // Metrics overlay changes cell values — if the user has a grep
            // filter active on CPU/MEM columns, re-filter so it reflects the
            // updated values. Only needed when viewing the affected resource.
            if app.nav.resource_id().plural == "pods" {
                app.reapply_nav_filters();
            }
        }
        AppEvent::NodeMetrics(metrics) => {
            app.node_metrics = metrics;
            app.apply_node_metrics();
            if app.nav.resource_id().plural == "nodes" {
                app.reapply_nav_filters();
            }
        }
        AppEvent::LogStreamEnded => {
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                state.streaming = false;
            }
        }
        AppEvent::ResourceCapabilities { resource, capabilities } => {
            app.capabilities.insert(resource, capabilities);
        }
        AppEvent::DaemonDisconnected => {
            app.exit_reason = Some(crate::app::ExitReason::DaemonDisconnected);
            app.should_quit = true;
        }
        AppEvent::ConnectionEstablished { context, cluster, user, namespaces } => {
            // Daemon's view is authoritative — overwrite whatever the
            // KubeconfigLoaded stage put there.
            app.context = context;
            app.cluster = cluster;
            app.user = user;
            if !namespaces.is_empty() {
                let ns_rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                let table = app.data.unified.entry(crate::app::nav::rid("namespaces"))
                    .or_insert_with(crate::app::StatefulTable::new);
                table.set_items(ns_rows);
            }
        }
        AppEvent::ConnectionFailed(message) => {
            app.exit_reason = Some(crate::app::ExitReason::Error(message));
            app.should_quit = true;
        }
        AppEvent::KubeconfigLoaded {
            contexts, current_context, current_cluster, current_user, ..
        } => {
            // Adopt the kubeconfig's view only if the daemon hasn't already
            // published its own (authoritative) values via ConnectionEstablished.
            // In the normal startup order KubeconfigLoaded arrives first and
            // ConnectionEstablished arrives later, so this branch is taken.
            if app.context.is_empty() {
                app.context = current_context;
                app.cluster = current_cluster;
                app.user = current_user;
            }
            app.contexts = contexts.iter().map(|c| c.name.clone()).collect();
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
            // The entry is guaranteed to exist for any actively-subscribed
            // resource: `apply_nav_change` calls `clear_resource()` (which
            // creates-or-clears) before sending Subscribe. A `None` here means
            // the snapshot is stale (the user navigated away and we
            // unsubscribed before the snapshot reached us); drop it.
            if !app.data.unified.contains_key(&resource) {
                return;
            }
            app.data.descriptors.insert(resource.clone(), crate::app::TableDescriptor { headers });
            track_deltas(app, &rows);
            // Safe: contains_key check above guarantees the entry exists.
            app.data.unified.get_mut(&resource).unwrap().set_items_filtered(rows);
            // Apply metrics overlay whenever fresh data arrives.
            if resource.plural == "nodes" {
                app.apply_node_metrics();
            }
            if resource.plural == "pods" {
                app.apply_pod_metrics();
            }
        }
        ResourceUpdate::Yaml(content) => {
            // Two routes consume `YamlResult`:
            //   1. `Route::Yaml` — the read-only YAML viewer.
            //   2. `Route::EditingResource { state: AwaitingYaml }` — the
            //      first stage of the unified edit flow. We write the
            //      content to a temp file and transition to `EditorReady`,
            //      and the session main loop will pick it up on its next
            //      iteration to suspend + exec `$EDITOR`.
            if let crate::app::Route::Yaml { ref target, ref mut awaiting_response, ref mut state } = app.route {
                if *awaiting_response {
                    app.kubectl_cache.insert(target.clone(), crate::app::ContentKind::Yaml, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
            } else if let crate::app::Route::EditingResource {
                ref target,
                ref mut state,
            } = app.route {
                if matches!(state, crate::app::EditState::AwaitingYaml) {
                    match write_edit_temp_file(target, &content) {
                        Ok(temp_path) => {
                            *state = crate::app::EditState::EditorReady { temp_path };
                        }
                        Err(e) => {
                            // Couldn't write the temp file — abort the edit
                            // and pop back to the previous route.
                            app.flash = Some(crate::app::FlashMessage::error(
                                format!("Edit failed: {}", e)
                            ));
                            app.pop_route();
                        }
                    }
                }
            }
        }
        ResourceUpdate::Describe(content) => {
            if let crate::app::Route::Describe { ref target, ref mut awaiting_response, ref mut state } = app.route {
                if *awaiting_response {
                    app.kubectl_cache.insert(target.clone(), crate::app::ContentKind::Describe, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
            }
        }
        ResourceUpdate::LogLine(line) => {
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                state.push(line);
            }
            return; // Log lines don't need nav filter reapply
        }
    }
    // Reapply nav stack filters after every table data update so that drill-down
    // and grep filters stay active as fresh snapshots arrive.
    app.reapply_nav_filters();

    // Expire old change highlights (older than 5 seconds).
    let now = std::time::Instant::now();
    let expired: Vec<_> = app.changed_rows.iter()
        .filter(|(_, ts)| now.duration_since(**ts).as_secs() >= crate::app::CHANGE_HIGHLIGHT_SECS)
        .map(|(k, _)| k.clone())
        .collect();
    for key in &expired {
        app.changed_rows.remove(key);
    }
}

/// Track which rows changed between the previous and current snapshot.
/// Populates `app.prev_rows` (per-row content hash) and `app.changed_rows`
/// (row-level timestamp, used by the table widget to flash recently-changed
/// rows). Storing a hash instead of the full `Vec<String>` removes the
/// per-row allocation that used to thrash the heap on large tables.
fn track_deltas(app: &mut App, rows: &[crate::kube::resources::row::ResourceRow]) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let now = std::time::Instant::now();
    let mut new_prev = HashMap::with_capacity(rows.len());

    for row in rows {
        let key = ObjectKey::new(row.namespace.clone().unwrap_or_default(), row.name.clone());

        // 64-bit content hash of the row cells. A collision would only
        // suppress the flash highlight on a single row, never miss data.
        let mut hasher = DefaultHasher::new();
        for cell in &row.cells {
            cell.hash(&mut hasher);
        }
        let new_hash = hasher.finish();

        if let Some(prev_hash) = app.prev_rows.get(&key) {
            if *prev_hash != new_hash {
                app.changed_rows.insert(key.clone(), now);
            }
        }

        new_prev.insert(key, new_hash);
    }
    app.prev_rows = new_prev;
}

/// Write the YAML returned by the server to a temp file the editor can
/// open. The filename embeds the resource type + name + a process-unique
/// counter, so concurrent edits in different sessions don't collide and
/// editors that show the filename in their title give the user useful
/// context. The session loop is responsible for deleting the file after
/// the editor exits.
fn write_edit_temp_file(
    target: &crate::kube::protocol::ObjectRef,
    yaml: &str,
) -> std::io::Result<std::path::PathBuf> {
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
    let filename = format!(
        "k9rs-edit-{}-{}-{}.yaml",
        safe(&target.resource.plural),
        safe(&target.name),
        n,
    );
    let path = std::env::temp_dir().join(filename);
    let mut f = std::fs::File::create(&path)?;
    f.write_all(yaml.as_bytes())?;
    Ok(path)
}
