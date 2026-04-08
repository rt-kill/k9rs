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
        AppEvent::DaemonDisconnected => {
            app.exit_reason = Some(crate::app::ExitReason::DaemonDisconnected);
            app.should_quit = true;
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
            // Only update existing tables — don't create new ones from stale
            // snapshots arriving after the user navigated away. The table is
            // created by `clear_resource()` in `apply_nav_change` before
            // subscribing, so `get_mut()` succeeds for active resources.
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
                    app.kubectl_cache.insert(target.resource.display_label().to_string(), target.name.clone(), target.namespace.to_string(), crate::app::ContentKind::Yaml, content.clone());
                    *awaiting_response = false;
                }
                state.set_content(content);
            }
        }
        ResourceUpdate::Describe(content) => {
            if let crate::app::Route::Describe { ref target, ref mut awaiting_response, ref mut state } = app.route {
                if *awaiting_response {
                    app.kubectl_cache.insert(target.resource.display_label().to_string(), target.name.clone(), target.namespace.to_string(), crate::app::ContentKind::Describe, content.clone());
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
    app.changed_rows.retain(|_, ts| now.duration_since(*ts).as_secs() < crate::app::CHANGE_HIGHLIGHT_SECS);

}

/// Track which rows changed between the previous and current snapshot.
/// Updates `app.prev_rows` with current data and `app.changed_rows` with
/// newly detected changes.
fn track_deltas(app: &mut App, rows: &[crate::kube::resources::row::ResourceRow]) {
    let now = std::time::Instant::now();
    let mut new_prev = HashMap::with_capacity(rows.len());

    for row in rows {
        let key = ObjectKey::new(row.namespace.clone(), row.name.clone());

        if let Some(prev_row) = app.prev_rows.get(&key) {
            if *prev_row != row.cells {
                app.changed_rows.insert(key.clone(), now);
            }
        }

        new_prev.insert(key, row.cells.clone());
    }
    app.prev_rows = new_prev;
}
