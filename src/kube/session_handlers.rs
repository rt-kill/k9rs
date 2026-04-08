use tokio::task::JoinHandle;

use crate::app::{App, ContainerRef};
use crate::app::nav::rid;
use crate::kube::protocol::ObjectRef;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, ActionResult, apply_nav_change};
use crate::kube::session_nav::{
    drill_to_pods_by_labels, drill_to_pods_by_grep, drill_to_pods_by_owner,
    drill_deployment_to_pods, do_switch_namespace, begin_context_switch,
};

pub(crate) fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
    port_forward_task: &mut Option<JoinHandle<()>>,
) -> ActionResult {
    use crate::app::Route;
    use crate::kube::protocol::ResourceId;

    // Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name, log_task, port_forward_task);
        }
        return ActionResult::None;
    }

    // Handle ContainerSelect: open logs or shell for the selected container.
    if let Route::ContainerSelect { ref pod, ref namespace, selected, for_shell } = app.route {
        let pod_name = pod.clone();
        let pod_ns = namespace.clone();
        let container_name = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.items.iter().find(|p| p.name == pod_name && p.namespace == pod_ns))
            .and_then(|p| p.extra_containers())
            .and_then(|c| c.get(selected).map(|ci| ci.real_name.clone()))
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

    // Handle Enter: data-driven drill-down based on row metadata.
    // Instead of hardcoding which resource types drill down how, we check
    // what metadata the selected row carries and act accordingly.
    let current_rid = app.nav.resource_id().clone();
    let row_data = app.data.unified.get(&current_rid).and_then(|t| t.selected_item()).cloned();

    if let Some(row) = row_data {
        // CRD definitions -> navigate to instances
        if row.extra_str("group").is_some() && row.extra_str("plural").is_some() && row.extra_str("scope").is_some() {
            let group = row.extra_str("group").unwrap().to_string();
            let version = row.extra_str("version").unwrap_or("v1").to_string();
            let kind = row.extra_str("kind").unwrap_or("").to_string();
            let plural = row.extra_str("plural").unwrap().to_string();
            let scope_str = row.extra_str("scope").unwrap().to_string();
            let crd_rid = ResourceId::new(
                group, version, kind.clone(), plural,
                crate::kube::protocol::ResourceScope::from_scope_str(&scope_str),
            );
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
        // Namespaces -> switch to that namespace
        else if current_rid.plural == "namespaces" {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::from(row.name.as_str()), log_task);
        }
        // Nodes -> show pods on this node (field filter)
        else if current_rid.plural == "nodes" {
            let sel = app.data.unified.get(&current_rid).map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep {
                resource: rid("pods"),
                filter: Some(crate::app::nav::NavFilter::Field {
                    field: "node".to_string(),
                    value: row.name.clone(),
                }),
                saved_selected: 0,
                filter_input: crate::app::nav::FilterInputState::default(),
            });
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.flash = Some(crate::app::FlashMessage::info(format!("Pods on node: {}", row.name)));
        }
        // Deployments -> drill through replicasets to pods
        else if current_rid.plural == "deployments" {
            drill_deployment_to_pods(app, data_source, &row.name);
        }
        // Resources with uid -> drill to pods by ownerReferences, fallback to labels, fallback to grep
        else if row.extra_str("uid").is_some() {
            let uid = row.extra_str("uid").unwrap().to_string();
            let kind = current_rid.kind.clone();
            if !uid.is_empty() {
                drill_to_pods_by_owner(app, data_source, &uid, &kind, &row.name);
            } else {
                drill_to_pods_by_grep(app, data_source, &row.name);
            }
        }
        // Resources with selector/selector_labels -> drill to pods by labels
        else if let Some(labels) = row.extra_map("selector").or_else(|| row.extra_map("selector_labels")).cloned() {
            if !labels.is_empty() {
                let label = format!("{}/{}", current_rid.short_label(), row.name);
                drill_to_pods_by_labels(app, data_source, labels, &label);
            } else {
                drill_to_pods_by_grep(app, data_source, &row.name);
            }
        }
        // Resources that support logs -> drill to pods by name grep
        else if current_rid.supports_logs() {
            drill_to_pods_by_grep(app, data_source, &row.name);
        }
        // Everything else -> describe
        else {
            handle_describe(app, data_source);
        }
    } else {
        handle_describe(app, data_source);
    }
    ActionResult::None
}

pub(crate) fn handle_describe(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    if let Some(info) = get_selected_resource_info(app) {
        let resource_str = info.resource.display_label().to_string();
        let name_str = info.name.clone();
        let ns_str = info.namespace.to_string();
        // Check cache first
        if let Some(cached) = app.kubectl_cache.get(&resource_str, &name_str, &ns_str, crate::app::ContentKind::Describe) {
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
        app.route = Route::Describe {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        };

        ds_try!(app, data_source.describe(&resource_str, &name_str, &ns_str));
    }
}

pub(crate) fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    if let Some(info) = get_selected_resource_info(app) {
        let resource_str = info.resource.display_label().to_string();
        let name_str = info.name.clone();
        let ns_str = info.namespace.to_string();
        // Check cache first
        if let Some(cached) = app.kubectl_cache.get(&resource_str, &name_str, &ns_str, crate::app::ContentKind::Yaml) {
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
        app.route = Route::Yaml {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        };

        ds_try!(app, data_source.yaml(&resource_str, &name_str, &ns_str));
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

    // For deployments, check if there are available pods before spawning
    if app.nav.resource_id().plural == "deployments" {
        if let Some(row) = app.data.unified.get(&rid("deployments")).and_then(|t| t.selected_item()) {
            let available: i32 = row.extra_str("available").and_then(|s| s.parse().ok()).unwrap_or(0);
            if available == 0 {
                app.flash = Some(crate::app::FlashMessage::warn(format!(
                    "No pods available for {}", row.name
                )));
                return;
            }
        }
    }

    // For multi-container pods, open ContainerSelect instead of going straight to logs
    if app.nav.resource_id().supports_shell() {
        let container_count = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .and_then(|p| p.extra_containers())
            .map(|c| c.len())
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
    let is_pod = app.nav.resource_id().supports_shell();
    let (log_target, route_pod, route_container) = if is_pod {
        let container = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .and_then(|p| p.extra_containers())
            .and_then(|c| c.first().map(|ci| ci.real_name.clone()))
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
    let is_pod = app.nav.resource_id().supports_shell();
    let (log_target, route_pod, route_container) = if is_pod {
        let container = app.data.unified.get(&rid("pods"))
            .and_then(|t| t.selected_item())
            .and_then(|p| p.extra_containers())
            .and_then(|c| c.first().map(|ci| ci.real_name.clone()))
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

/// Resolve the port-forward target for the currently selected resource.
/// Returns (kubectl_target, namespace, default_port_string, available_ports).
/// The default_port_string uses the first detected port, or is empty if none found.
pub(crate) fn resolve_port_forward_target(app: &App) -> Option<(String, String, String, Vec<u16>)> {
    let plural = app.nav.resource_id().plural.as_str();
    let rid_key = app.nav.resource_id().clone();
    let (target, ns, ports) = match plural {
        "services" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let svc_ports: Vec<u16> = row.extra_str("ports").unwrap_or("").split(',')
                .filter_map(|s| s.split('/').next()?.trim().parse().ok())
                .collect();
            (format!("svc/{}", row.name), row.namespace.clone(), svc_ports)
        }
        "deployments" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let ports = row.extra_ports("container_ports").unwrap_or(&[]).to_vec();
            (format!("deploy/{}", row.name), row.namespace.clone(), ports)
        }
        "statefulsets" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let ports = row.extra_ports("container_ports").unwrap_or(&[]).to_vec();
            (format!("sts/{}", row.name), row.namespace.clone(), ports)
        }
        "pods" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let ports: Vec<u16> = row.extra_containers().unwrap_or(&[]).iter().flat_map(|c| c.ports.iter().copied()).collect();
            (format!("pod/{}", row.name), row.namespace.clone(), ports)
        }
        "daemonsets" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let ports = row.extra_ports("container_ports").unwrap_or(&[]).to_vec();
            (format!("ds/{}", row.name), row.namespace.clone(), ports)
        }
        "replicasets" => {
            let row = app.data.unified.get(&rid_key)?.selected_item()?;
            let uid = row.extra_str("uid").unwrap_or("").to_string();
            (format!("rs/{}", row.name), row.namespace.clone(), find_container_ports_by_owner(app, &uid))
        }
        _ => return None,
    };
    // Default port string from detected ports. Falls back to 8080 if none detected.
    let first_port = ports.first().copied().unwrap_or(8080);
    let default = format!("{}:{}", first_port, first_port);
    Some((target, ns, default, ports))
}

/// Find all container ports from a pod owned by the given UID.
fn find_container_ports_by_owner(app: &App, owner_uid: &str) -> Vec<u16> {
    app.data.unified.get(&rid("pods"))
        .and_then(|t| t.items.iter().find(|p| {
            p.extra_owner_refs().unwrap_or(&[]).iter().any(|or| or.uid == owner_uid)
        }))
        .and_then(|p| p.extra_containers())
        .map(|c| c.iter().flat_map(|ci| ci.ports.iter().copied()).collect())
        .unwrap_or_default()
}

pub(crate) fn get_selected_resource_info(app: &App) -> Option<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::{Namespace, ObjectRef};

    let current_rid = app.nav.resource_id().clone();
    // Determine the resource type string for ObjectRef construction.
    // For built-in types, use the canonical name. For CRDs, use "plural.group".
    let resource_type = current_rid.meta()
        .map(|m| m.name.to_string())
        .unwrap_or_else(|| {
            if current_rid.group.is_empty() {
                current_rid.plural.clone()
            } else {
                format!("{}.{}", current_rid.plural, current_rid.group)
            }
        });

    if let Some(table) = app.data.unified.get(&current_rid) {
        if let Some(item) = table.selected_item() {
            return Some(ObjectRef::from_parts(
                &resource_type,
                item.name().to_string(),
                Namespace::from(item.namespace()),
            ));
        }
    }
    None
}

/// Get resource info for all marked items in the active table.
/// Returns empty Vec if no items are marked.
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::{Namespace, ObjectRef};

    let current_rid = app.nav.resource_id().clone();
    let resource_type = current_rid.meta()
        .map(|m| m.name.to_string())
        .unwrap_or_else(|| {
            if current_rid.group.is_empty() {
                current_rid.plural.clone()
            } else {
                format!("{}.{}", current_rid.plural, current_rid.group)
            }
        });

    let mut result = Vec::new();
    if let Some(table) = app.data.unified.get(&current_rid) {
        for &idx in &table.marked {
            if let Some(item) = table.items.get(idx) {
                result.push(ObjectRef::from_parts(
                    &resource_type,
                    item.name().to_string(),
                    Namespace::from(item.namespace()),
                ));
            }
        }
    }
    result
}
