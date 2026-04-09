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
            app.flash = Some(flash);
        }
        // ContextSwitchResult is handled in the main event loop before apply_event
        AppEvent::ContextSwitchResult { .. } => {}
        AppEvent::ResourceResolved { original, resolved } => {
            // The server discovered the true identity of a resource we subscribed to
            // with incomplete info (e.g., `:nodeclaims` → karpenter.sh/v1/NodeClaim/Cluster).
            // Update the nav step's ResourceId and move the table entry so future
            // snapshots (keyed by resolved rid) find the right table.
            if *app.nav.resource_id() == original {
                app.nav.current_mut().resource = resolved.clone();
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
            let table = app.data.unified.entry(resource).or_insert_with(crate::app::StatefulTable::new);
            table.error = Some(message);
        }
        AppEvent::PodMetrics(metrics) => {
            app.pod_metrics = metrics;
            app.apply_pod_metrics();
        }
        AppEvent::NodeMetrics(metrics) => {
            app.node_metrics = metrics;
            app.apply_node_metrics();
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
            contexts, current_context, current_cluster, current_user,
            kubeconfig_yaml, env_vars,
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
            // Cache the YAML/env so context switches don't have to read disk.
            app.kubeconfig_yaml = kubeconfig_yaml;
            app.kubeconfig_env = env_vars;
        }
    }
}

fn apply_resource_update(
    app: &mut App,
    update: ResourceUpdate,
) {
    match update {
        ResourceUpdate::Rows { resource, headers, rows } => {
            app.data.descriptors.insert(resource.clone(), crate::app::TableDescriptor { headers });
            track_deltas(app, &rows);
            // The entry is guaranteed to exist for any actively-subscribed
            // resource: `apply_nav_change` calls `clear_resource()` (which
            // creates-or-clears) before sending Subscribe. A `None` here means
            // the snapshot is stale (the user navigated away and we
            // unsubscribed before the snapshot reached us); drop it.
            let Some(table) = app.data.unified.get_mut(&resource) else {
                return;
            };
            table.set_items_filtered(rows);
            // Apply metrics overlay whenever fresh data arrives.
            if resource.plural == "nodes" {
                app.apply_node_metrics();
            }
            if resource.plural == "pods" {
                app.apply_pod_metrics();
            }
        }
        ResourceUpdate::Yaml(content) => {
            if let crate::app::Route::Yaml { ref target, ref mut awaiting_response, ref mut state } = app.route {
                if *awaiting_response {
                    app.kubectl_cache.insert(target.clone(), crate::app::ContentKind::Yaml, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
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
        let key = ObjectKey::new(row.namespace.clone(), row.name.clone());

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
