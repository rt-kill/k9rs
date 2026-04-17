use crate::app::{App, InputMode};
use crate::app::nav::rid;
use crate::kube::client_session::ClientSession;
use crate::kube::resource_def::BuiltInKind;
use crate::kube::session::apply_nav_change;

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
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::Labels(labels)),
    ));
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
    use crate::app::nav::{CompiledGrep, NavFilter, NavStep};
    let filter = format!("{}-", regex::escape(name));
    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::Grep(CompiledGrep::new(filter))),
    ));
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
    kind: BuiltInKind,
    name: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};

    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::OwnerChain {
            uid: uid.to_string(),
            kind,
            display_name: name.to_string(),
        }),
    ));
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    let kind_lower = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind.to_lowercase();
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods for {}/{}",
        kind_lower, name
    )));
}

/// Begin a context switch. Immediately clears UI state and transitions
/// `app.context_switch` to `Requested(ctx_name)` so the main loop drops
/// the current session and creates a new one. One socket = one context
/// = one session.
pub(crate) fn begin_context_switch(
    app: &mut App,
    _data_source: &mut ClientSession,
    ctx_name: &crate::kube::protocol::ContextName,
    log_stream: &mut Option<crate::kube::client_session::LogStream>,
) {
    // Guard against rapid context switches — only `Stable` accepts a new
    // switch; `Requested` and `InFlight` reject with a user-visible error.
    if !app.context_switch.is_stable() {
        app.flash = Some(crate::app::FlashMessage::error(
            "Context switch already in progress".to_string(),
        ));
        return;
    }

    // Cancel any active log stream — drop closes the substream.
    *log_stream = None;
    // Drop all subscription substreams (core + nav).
    app.core_streams.clear();
    app.nav.current_mut().stream = None;
    // Reset UI state immediately so the user sees a clean slate. Data
    // clearing happens in session_main's `context_switch.take_requested()`
    // block, AFTER the old session is dropped and stale events are
    // discarded.
    app.context = ctx_name.clone();
    app.selected_ns = crate::kube::protocol::Namespace::All;
    app.identity = app.data.contexts.items.iter()
        .find(|c| c.name == *ctx_name)
        .map(|ctx| ctx.identity.clone())
        .unwrap_or_default();
    let root = app.nav.root_resource_id().clone();
    let _change = app.nav.reset(root);
    *app.nav.filter_input_mut() = Default::default();
    app.kubectl_cache.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Overview;
    app.confirm_dialog = None;
    app.form_dialog = None;
    app.input_mode = InputMode::Normal;
    app.deltas.clear();
    app.pod_metrics.clear();
    app.node_metrics.clear();
    // Capabilities are no longer cached — `current_capabilities()` reads
    // straight off the typed rid via `ResourceId::capabilities()`, so
    // there's nothing to clear here.

    // Signal the main loop to drop the entire ClientSession and create
    // a new one for the target context. One socket = one context = one
    // session. The old session's socket close causes the daemon to exit
    // the session (all watchers enter grace period). The new session
    // connects fresh with the new kubeconfig.
    //
    // Transition: Stable → Requested(ctx_name). The main loop's
    // `take_requested()` will atomically move it to `InFlight` at the
    // top of the next iteration.
    app.context_switch = crate::app::ContextSwitchState::Requested(ctx_name.clone());
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...",
        ctx_name
    )));
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
pub(crate) fn do_switch_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
    log_stream: &mut Option<crate::kube::client_session::LogStream>,
) {
    // Record the namespace change globally (affects future namespaced subscriptions).
    app.selected_ns = ns.clone();

    // If the current view is cluster-scoped, just record the ns locally — nothing
    // to re-subscribe or clear since cluster-scoped resources ignore namespaces.
    // No daemon notification needed; every subscription carries its own
    // namespace in `SubscriptionInit`.
    if app.current_tab_is_cluster_scoped() {
        return;
    }

    // Cancel any active log stream — drop closes the substream.
    *log_stream = None;
    // Pop any stacked routes (Yaml, Describe, Logs) — they reference
    // resources from the old namespace and would show stale content on Back.
    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    // Dismiss any open confirm/form dialogs — they hold ObjectRefs from
    // the old namespace. Confirming after a switch could mutate the wrong
    // resource.
    app.confirm_dialog = None;
    app.form_dialog = None;
    app.input_mode = InputMode::Normal;
    // Wipe every namespaced table so cached rows from the old namespace
    // can't bleed into the new namespace's view.
    app.clear_namespaced_caches();
    app.kubectl_cache.clear();
    app.deltas.clear();

    // Reset nav to root resource — any drill-down filters (owner UIDs,
    // labels) reference old-namespace resources and are now invalid.
    // steps.clear() inside reset() drops every NavStep, which drops their
    // subscription streams via SubscriptionStream::drop → AbortHandle::abort.
    // This is the structural guarantee: no old-namespace subscription can
    // deliver data after this point.
    let root_rid = app.nav.root_resource_id().clone();
    let _change = app.nav.reset(root_rid.clone());
    *app.nav.filter_input_mut() = Default::default();

    // No daemon notification needed — each subscription carries its own
    // namespace in SubscriptionInit, so the server doesn't track session-
    // level namespace state. (The previous `SwitchNamespace` wire command
    // was server-side logging only; removed for simplicity.)

    // Open a fresh subscription substream for the root resource in the
    // new namespace. Same drop+recreate pattern as apply_nav_change and
    // the Refresh action — structural ownership guarantees isolation.
    app.clear_resource(&root_rid);
    let stream = data_source.subscribe_stream(root_rid, ns.clone(), None);
    app.nav.current_mut().stream = Some(stream);

    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns.display()
    )));
}
