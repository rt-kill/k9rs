use tokio::task::JoinHandle;

use crate::app::{App, InputMode};
use crate::app::nav::rid;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, apply_nav_change};

// ---------------------------------------------------------------------------
// Nav drill-down helpers
// ---------------------------------------------------------------------------

/// Drill down to pods filtered by label selector (deployment->pods, service->pods, etc.)
/// Uses CLIENT-SIDE label filtering via the NavStack, NOT a server-side label selector.
/// This means we subscribe to the regular "pods" watcher (which may already be running
/// and have data), and the NavStack's Labels filter handles the narrowing. Instant
/// results from cached data instead of a new slow LIST request.
pub(crate) fn drill_to_pods_by_labels(
    app: &mut App,
    data_source: &mut ClientSession,
    labels: std::collections::BTreeMap<String, String>,
    description: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};

    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep {
        resource: rid("pods"),
        filter: Some(NavFilter::Labels(labels)),
        saved_selected: 0,
        filter_input: crate::app::nav::FilterInputState::default(),
    });
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    app.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", description)));
}

/// Drill down to pods filtered by name prefix (fallback when no selector_labels).
pub(crate) fn drill_to_pods_by_grep(
    app: &mut App,
    data_source: &mut ClientSession,
    name: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};
    let filter = format!("{}-", regex::escape(name));
    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep {
        resource: rid("pods"),
        filter: Some(NavFilter::Grep(filter)),
        saved_selected: 0,
        filter_input: crate::app::nav::FilterInputState::default(),
    });
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    app.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
}

/// Drill down to pods owned by a resource (via ownerReferences chain).
/// This is 100% accurate regardless of naming conventions or label quirks.
pub(crate) fn drill_to_pods_by_owner(
    app: &mut App,
    data_source: &mut ClientSession,
    uid: &str,
    kind: &str,
    name: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};

    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep {
        resource: rid("pods"),
        filter: Some(NavFilter::OwnerChain {
            uid: uid.to_string(),
            kind: kind.to_string(),
            display_name: name.to_string(),
        }),
        saved_selected: 0,
        filter_input: crate::app::nav::FilterInputState::default(),
    });
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods for {}/{}",
        kind.to_lowercase(), name
    )));
}

/// Drill from Deployment to its pods via selector labels.
pub(crate) fn drill_deployment_to_pods(
    app: &mut App,
    data_source: &mut ClientSession,
    deploy_name: &str,
) {
    if let Some(row) = app.data.unified.get(&rid("deployments")).and_then(|t| t.selected_item()) {
        let labels = row.extra_map("selector_labels").cloned().unwrap_or_default();
        if !labels.is_empty() {
            drill_to_pods_by_labels(app, data_source, labels, &format!("deploy/{}", deploy_name));
            return;
        }
    }
    drill_to_pods_by_grep(app, data_source, deploy_name);
}

/// Begin a context switch. Immediately clears UI state and shows a flash message,
/// then delegates the slow client creation to `DataSource`. In local mode the
/// result arrives via `AppEvent::ContextSwitchResult`; in daemon mode it comes
/// through the event stream.
pub(crate) fn begin_context_switch(
    app: &mut App,
    data_source: &mut ClientSession,
    ctx_name: &str,
    log_task: &mut Option<JoinHandle<()>>,
    port_forward_task: &mut Option<JoinHandle<()>>,
) {
    // Guard against rapid context switches — if one is already in flight, reject.
    if app.context_switch_pending {
        app.flash = Some(crate::app::FlashMessage::error(
            "Context switch already in progress".to_string(),
        ));
        return;
    }
    app.context_switch_pending = true;

    // Cancel any active log stream — both local task and daemon-side.
    if let Some(handle) = log_task.take() {
        handle.abort();
    }
    ds_try!(app, data_source.stop_logs());
    // Cancel any active port-forward — it belongs to the old context
    if let Some(handle) = port_forward_task.take() {
        handle.abort();
    }
    // Immediate: clear data and show feedback — keeps TUI responsive.
    // Look up cluster/user from the in-memory contexts list (no disk I/O).
    app.context = ctx_name.to_string();
    app.selected_ns = crate::kube::protocol::Namespace::All;
    if let Some(ctx) = app.data.contexts.items.iter().find(|c| c.name == ctx_name) {
        app.cluster = ctx.cluster.clone();
        app.user = ctx.user.clone();
    } else {
        app.cluster = String::new();
        app.user = String::new();
    }
    app.clear_data();
    let root = app.nav.root_resource_id().clone();
    // Reset nav but DON'T subscribe yet — the server is creating a new client.
    // Subscription happens in apply_context_switch_result after the new client is ready.
    let _change = app.nav.reset(root);
    *app.nav.filter_input_mut() = Default::default();
    app.kubectl_cache.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Overview;
    app.confirm_dialog = None;
    app.port_forward_dialog = None;
    app.input_mode = InputMode::Normal;
    app.prev_rows.clear();
    app.changed_rows.clear();
    app.pod_metrics.clear();
    app.node_metrics.clear();
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...",
        ctx_name
    )));

    // Delegate background client creation to the session.
    ds_try!(app, data_source.switch_context(ctx_name));
}

/// Apply the result of a background context switch.
pub(crate) fn apply_context_switch_result(
    app: &mut App,
    data_source: &mut ClientSession,
    ctx_name: &str,
    result: Result<(), String>,
) {
    // Always clear the pending flag when a result arrives.
    app.context_switch_pending = false;

    // Discard stale result if user already switched to a different context
    if ctx_name != app.context {
        return;
    }
    match result {
        Ok(()) => {
            data_source.set_context_info(ctx_name);
            let updated: Vec<crate::app::KubeContext> = app
                .data.contexts.items
                .iter()
                .map(|ctx| crate::app::KubeContext {
                    name: ctx.name.clone(),
                    cluster: ctx.cluster.clone(),
                    user: ctx.user.clone(),
                    is_current: ctx.name == ctx_name,
                })
                .collect();
            app.data.contexts.set_items(updated);

            // Re-subscribe to the nav's current resource so data is ready
            // when the user navigates from Overview to a resource view.
            // Keep the route as Overview (set by begin_context_switch).
            let rid = app.nav.resource_id().clone();
            let change = crate::app::nav::NavChange {
                unsubscribe: None,
                subscribe: Some(rid),
            };
            super::session::apply_nav_change(app, data_source, change);

            app.flash = Some(crate::app::FlashMessage::info(format!(
                "Switched to context: {}",
                ctx_name
            )));
        }
        Err(e) => {
            // Restore context/cluster to match the still-running data source,
            // so the app state is consistent with the actual active client.
            let active_ctx = {
                let ds_ctx = data_source.context_name();
                if ds_ctx.is_empty() { app.context.clone() } else { ds_ctx.to_string() }
            };
            // Look up cluster/user from the in-memory contexts list (no disk I/O).
            let (cluster, user) = app.data.contexts.items.iter()
                .find(|c| c.name == active_ctx)
                .map(|c| (c.cluster.clone(), c.user.clone()))
                .unwrap_or_default();
            app.context = active_ctx.clone();
            app.cluster = cluster.clone();
            app.user = user;
            data_source.set_context_info(&active_ctx);
            // Data was already cleared by begin_context_switch. Re-subscribe
            // via apply_nav_change so the table is properly initialized.
            app.route = crate::app::Route::Resources;
            let rid = app.nav.resource_id().clone();
            let change = crate::app::nav::NavChange {
                unsubscribe: None,
                subscribe: Some(rid),
            };
            super::session::apply_nav_change(app, data_source, change);
            app.flash = Some(crate::app::FlashMessage::error(format!(
                "Context switch failed: {}",
                e
            )));
        }
    }
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
pub(crate) fn do_switch_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
    log_task: &mut Option<JoinHandle<()>>,
) {
    // Record the namespace change globally (affects future namespaced subscriptions).
    app.selected_ns = ns.clone();

    // If the current view is cluster-scoped, just record the ns for later — nothing
    // to re-subscribe or clear since cluster-scoped resources ignore namespaces.
    if app.current_tab_is_cluster_scoped() {
        ds_try!(app, data_source.switch_namespace(ns.display()));
        return;
    }

    // Cancel any active log stream — both local task and daemon-side.
    if let Some(handle) = log_task.take() {
        handle.abort();
    }
    ds_try!(app, data_source.stop_logs());
    // Clear the current table so "Loading..." shows while fresh data arrives.
    // Note: stale data from the old namespace may briefly appear if already queued,
    // but will be overwritten by fresh data from the new namespace's watcher.
    let current_rid = app.nav.resource_id().clone();
    app.clear_resource(&current_rid);
    app.kubectl_cache.clear();
    app.prev_rows.clear();
    app.changed_rows.clear();
    ds_try!(app, data_source.switch_namespace(ns.display()));
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns.display()
    )));
}
