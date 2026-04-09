use tokio::task::JoinHandle;

use crate::app::{App, ContainerRef};
use crate::app::nav::rid;
use crate::kube::protocol::ObjectRef;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, ActionResult, apply_nav_change};
use crate::kube::session_nav::{
    drill_to_pods_by_labels, drill_to_pods_by_grep, drill_to_pods_by_owner,
    do_switch_namespace, begin_context_switch,
};

pub(crate) fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
) -> ActionResult {
    use crate::app::Route;
    use crate::kube::protocol::ResourceId;

    // Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name, log_task);
        }
        return ActionResult::None;
    }

    // Handle ContainerSelect: open logs or shell for the selected container.
    if let Route::ContainerSelect { ref pod, ref namespace, selected, for_shell } = app.route {
        let pod_name = pod.clone();
        let pod_ns = namespace.clone();
        let container_name = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.items.iter().find(|p| p.name == pod_name && p.namespace == pod_ns))
            .and_then(|p| p.containers.get(selected).map(|ci| ci.real_name.clone()))
            .unwrap_or_default();

        if for_shell {
            // Shell mode: return ActionResult::Shell so the main loop runs it
            app.route = app.route_stack.pop().unwrap_or(Route::Resources);
            return ActionResult::Shell {
                pod: pod_name,
                namespace: pod_ns,
                container: container_name,
                context: app.context.clone(),
            };
        }

        app.push_route(app.route.clone());
        let mut log_state = crate::app::LogState::new();
        log_state.streaming = true;
        let tail = Some(log_state.tail_lines);
        app.route = Route::Logs {
            target: ContainerRef::new(pod_name.clone(), pod_ns.clone(), container_name.clone()),
            state: Box::new(log_state),
        };

        // Cancel any previous log stream
        if let Some(handle) = log_task.take() {
            handle.abort();
        }

        ds_try!(app, data_source.stream_logs(
            &pod_name,
            &pod_ns,
            &container_name,
            true,  // follow
            tail,
            None,
            false, // not previous
        ));
        return ActionResult::None;
    }

    // Handle Enter: read the row's drill_target (set by the server-side
    // converter) and act on it. The client has NO K8s knowledge here —
    // it just blindly executes the server's instructions.
    use crate::kube::resources::row::DrillTarget;
    let current_rid = app.nav.resource_id().clone();
    let row_data = app.data.unified.get(&current_rid).and_then(|t| t.selected_item()).cloned();

    let Some(row) = row_data else {
        handle_describe(app, data_source);
        return ActionResult::None;
    };

    match row.drill_target.clone() {
        Some(DrillTarget::SwitchNamespace(ns)) => {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::from(ns.as_str()), log_task);
        }
        Some(DrillTarget::BrowseCrd { group, version, kind, plural, scope }) => {
            let crd_rid = ResourceId::new(group, version, kind.clone(), plural, scope);
            let sel = app.data.unified.get(&current_rid).map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep {
                resource: crd_rid,
                filter: None,
                saved_selected: 0,
                filter_input: crate::app::nav::FilterInputState::default(),
            });
            apply_nav_change(app, data_source, change);
            app.flash = Some(crate::app::FlashMessage::info(format!("Browsing CRD: {}", kind)));
        }
        Some(DrillTarget::PodsByLabels { labels, breadcrumb }) => {
            drill_to_pods_by_labels(app, data_source, labels, &breadcrumb);
        }
        Some(DrillTarget::PodsByOwner { uid, kind, name }) => {
            drill_to_pods_by_owner(app, data_source, &uid, &kind, &name);
        }
        Some(DrillTarget::PodsByField { field, value, breadcrumb }) => {
            let sel = app.data.unified.get(&current_rid).map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep {
                resource: rid("pods"),
                filter: Some(crate::app::nav::NavFilter::Field { field, value }),
                saved_selected: 0,
                filter_input: crate::app::nav::FilterInputState::default(),
            });
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", breadcrumb)));
        }
        Some(DrillTarget::PodsByNameGrep(name)) => {
            drill_to_pods_by_grep(app, data_source, &name);
        }
        None => {
            handle_describe(app, data_source);
        }
    }
    ActionResult::None
}

pub(crate) fn handle_describe(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    // Local resources route through the LocalResourceSource trait server-side
    // (see ServerSession::describe_local) — same wire command, server picks
    // the right backend. No client-side branching needed.
    if let Some(info) = get_selected_resource_info(app) {
        // Check cache first — keyed on the typed ObjectRef so different CRDs
        // sharing a Kind name across groups never collide.
        if let Some(cached) = app.kubectl_cache.get(&info, crate::app::ContentKind::Describe) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.push_route(app.route.clone());
            app.route = Route::Describe {
                target: info,
                awaiting_response: false,
                state,
            };
            return;
        }

        app.push_route(app.route.clone());
        ds_try!(app, data_source.describe(&info));
        app.route = Route::Describe {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        };
    }
}

pub(crate) fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    // Local resources route through LocalResourceSource server-side (see
    // ServerSession::yaml_local) — same wire command, server picks the
    // right backend.
    if let Some(info) = get_selected_resource_info(app) {
        if let Some(cached) = app.kubectl_cache.get(&info, crate::app::ContentKind::Yaml) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.push_route(app.route.clone());
            app.route = Route::Yaml {
                target: info,
                awaiting_response: false,
                state,
            };
            return;
        }

        app.push_route(app.route.clone());
        ds_try!(app, data_source.yaml(&info));
        app.route = Route::Yaml {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        };
    }
}

pub(crate) fn handle_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
) {
    use crate::app::Route;

    let Some(info) = get_selected_resource_info(app) else { return; };
    let resource_type = info.resource.display_label().to_string();
    let name = info.name;
    let namespace = info.namespace.to_string();

    // The server returns an error or empty stream if there are no pods —
    // the client doesn't need to gate on deployment "available" state.

    // Detect "is this a pod?" from the current nav resource id directly.
    // We can't rely on `capabilities().supports(Shell)` because capabilities
    // arrive asynchronously from the server and may not be set yet when the
    // user hits `l` immediately after navigating.
    let is_pod = app.nav.resource_id().plural == "pods";

    // For multi-container pods, open ContainerSelect instead of going straight to logs
    if is_pod {
        let container_count = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .map(|p| p.containers.len())
            .unwrap_or(0);
        if container_count > 1 {
            app.push_route(app.route.clone());
            app.route = crate::app::Route::ContainerSelect {
                pod: name.clone(),
                namespace: namespace.clone(),
                selected: 0,
                for_shell: false,
            };
            return;
        }
    }

    app.push_route(app.route.clone());

    // Build the kubectl target and container depending on the resource type.
    // For pods, target the pod directly with -c <container>.
    // For workloads (deployments, statefulsets, etc.), use "type/name" with --all-containers.
    let (log_target, route_pod, route_container) = if is_pod {
        let container = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .and_then(|p| p.containers.first().map(|ci| ci.real_name.clone()))
            .unwrap_or_default();
        (name.clone(), name.clone(), container)
    } else {
        let target = format!("{}/{}", resource_type, name);
        (target.clone(), target, "all".to_string())
    };

    let mut log_state = crate::app::LogState::new();
    log_state.streaming = true;
    let tail = Some(log_state.tail_lines);

    app.route = Route::Logs {
        target: ContainerRef::new(route_pod, namespace.clone(), route_container.clone()),
        state: Box::new(log_state),
    };

    // Cancel any previous log stream
    if let Some(handle) = log_task.take() {
        handle.abort();
    }

    ds_try!(app, data_source.stream_logs(
        &log_target,
        &namespace,
        &route_container,
        true,  // follow
        tail,
        None,
        false, // not previous
    ));
}

pub(crate) fn handle_previous_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
) {
    use crate::app::Route;

    let Some(info) = get_selected_resource_info(app) else { return; };
    let resource_type = info.resource.display_label().to_string();
    let name = info.name;
    let namespace = info.namespace.to_string();

    app.push_route(app.route.clone());

    // Build the kubectl target and container depending on the resource type.
    let is_pod = app.nav.resource_id().plural == "pods";
    let (log_target, route_pod, route_container) = if is_pod {
        let container = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .and_then(|p| p.containers.first().map(|ci| ci.real_name.clone()))
            .unwrap_or_default();
        (name.clone(), name.clone(), container)
    } else {
        let target = format!("{}/{}", resource_type, name);
        (target.clone(), target, "all".to_string())
    };

    let mut log_state = crate::app::LogState::new();
    log_state.follow = false; // previous logs are static
    log_state.streaming = true;
    let tail_lines = log_state.tail_lines;

    app.route = Route::Logs {
        target: ContainerRef::new(route_pod, namespace.clone(), route_container.clone()),
        state: Box::new(log_state),
    };

    // Cancel any previous log stream
    if let Some(handle) = log_task.take() {
        handle.abort();
    }

    ds_try!(app, data_source.stream_logs(
        &log_target,
        &namespace,
        &route_container,
        false, // no follow for previous logs
        Some(tail_lines), // always tail for previous logs
        None,  // no --since for previous logs
        true,  // --previous
    ));
}

/// Build a tab-separated text dump of the currently visible resource table.
/// Respects column visibility (column_level) so the dump matches what the user sees.
pub(crate) fn build_table_dump(app: &App) -> String {
    let current_rid = app.nav.resource_id();
    if let Some(table) = app.data.unified.get(current_rid) {
        let ns = app.selected_ns.display();
        let skip_ns = ns != "all" && !ns.is_empty();
        let visible = app.data.descriptors.get(current_rid)
            .map(|d| d.visible_columns(app.column_level, skip_ns))
            .unwrap_or_default();
        let visible_indices: Vec<usize> = visible.iter().map(|&(i, _)| i).collect();
        let headers: String = visible.iter().map(|&(_, name)| name).collect::<Vec<_>>().join("\t");
        let mut lines = vec![headers];
        for &i in &table.filtered_indices {
            if let Some(item) = table.items.get(i) {
                let row: String = visible_indices.iter()
                    .map(|&ci| item.cells.get(ci).map(|s| s.as_str()).unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join("\t");
                lines.push(row);
            }
        }
        lines.join("\n")
    } else {
        String::new()
    }
}

/// Build a port-forward dialog for the currently selected resource.
/// Build a Scale `FormDialog` for the given target. The form has a single
/// `replicas` Number field; the user types the desired count and submits.
/// We don't seed the field with the current replica count — the legacy
/// `ScaleDialog` didn't either, and it kept the user from accidentally
/// confirming "scale to current = no-op" by hitting Enter.
pub(crate) fn build_scale_form(target: ObjectRef) -> crate::app::FormDialog {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};
    use crate::kube::protocol::OperationKind;

    let title = format!("Scale: {}/{}", target.resource.display_label(), target.name);
    let subtitle = if target.namespace.display().is_empty() {
        String::new()
    } else {
        format!("namespace: {}", target.namespace.display())
    };
    FormDialog {
        kind: OperationKind::Scale,
        title,
        subtitle,
        target,
        fields: vec![FormFieldState {
            name: "replicas".into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 1_000_000 },
            value: String::new(),
        }],
        focused: 0,
    }
}

/// Build a port-forward `FormDialog` for the currently-selected row.
/// Returns `None` if the current resource type doesn't support
/// port-forwarding or if no row is selected.
///
/// The schema declared in `ResourceCapabilities` for `PortForward` is the
/// starting point — we read the field names/types from there and then
/// augment them with row-specific data: `container_port` becomes a Select
/// over `row.pf_ports` (if non-empty), and both ports default to the first
/// available container port.
pub(crate) fn resolve_port_forward_dialog(app: &App) -> Option<crate::app::FormDialog> {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};
    use crate::kube::protocol::{Namespace, OperationKind};

    // Capability gate: only resources flagged as port-forwardable qualify.
    if !app.current_capabilities().supports(OperationKind::PortForward) {
        return None;
    }

    let rid_key = app.nav.resource_id().clone();
    let row = app.data.unified.get(&rid_key)?.selected_item()?;

    // Ports come from the typed `pf_ports` field set by the converter.
    let ports: Vec<u16> = row.pf_ports.clone();
    let first_port = ports.first().copied().unwrap_or(8080);

    let short = rid_key.short_label().to_lowercase();
    let title = format!("Port forward: {}/{}", short, row.name);
    let ns = row.namespace.clone();
    let subtitle = if ns.is_empty() {
        String::new()
    } else {
        format!("namespace: {}", ns)
    };
    let target_ref = ObjectRef::new(rid_key, row.name.clone(), Namespace::from(ns.as_str()));

    // container_port: Select if we know the available ports, plain Port
    // input otherwise. The initial value is the index of the first option.
    let container_field = if ports.is_empty() {
        FormFieldState {
            name: "container_port".into(),
            label: "Container port".into(),
            kind: FormFieldKind::Port,
            value: first_port.to_string(),
        }
    } else {
        let options: Vec<(String, String)> = ports
            .iter()
            .map(|p| (p.to_string(), p.to_string()))
            .collect();
        FormFieldState {
            name: "container_port".into(),
            label: "Container port".into(),
            kind: FormFieldKind::Select { options },
            value: "0".into(),
        }
    };

    let local_field = FormFieldState {
        name: "local_port".into(),
        label: "Local port".into(),
        kind: FormFieldKind::Port,
        value: first_port.to_string(),
    };

    Some(FormDialog {
        kind: OperationKind::PortForward,
        title,
        subtitle,
        target: target_ref,
        fields: vec![container_field, local_field],
        focused: 0,
    })
}

pub(crate) fn get_selected_resource_info(app: &App) -> Option<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::{Namespace, ObjectRef};

    let current_rid = app.nav.resource_id().clone();
    let table = app.data.unified.get(&current_rid)?;
    let item = table.selected_item()?;
    Some(ObjectRef::new(
        current_rid,
        item.name().to_string(),
        Namespace::from(item.namespace()),
    ))
}

/// Get resource info for all marked items in the active table.
/// Returns empty Vec if no items are marked.
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::{Namespace, ObjectRef};

    let current_rid = app.nav.resource_id().clone();
    let mut result = Vec::new();
    if let Some(table) = app.data.unified.get(&current_rid) {
        for item in &table.items {
            let key = crate::kube::protocol::ObjectKey::new(item.namespace(), item.name());
            if table.marked.contains(&key) {
                result.push(ObjectRef::new(
                    current_rid.clone(),
                    item.name().to_string(),
                    Namespace::from(item.namespace()),
                ));
            }
        }
    }
    result
}
