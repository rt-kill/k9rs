
use crate::app::{App, ContainerRef};
use crate::app::nav::rid;
use crate::kube::protocol::{self, ObjectRef};
use crate::kube::client_session::ClientSession;
use crate::kube::resource_def::BuiltInKind;
use crate::kube::session::{ds_try, ActionResult, apply_nav_change};

/// Map a container name string from a row to the typed `LogContainer`.
/// Empty → `Default` (let kubectl pick); a real container name → `Named`.
/// There's deliberately no `"all"` branch — multi-container streaming is
/// requested explicitly by the workload-not-Pod path that constructs
/// `LogContainer::All` directly, never by passing a literal "all" through
/// here.
fn log_container_from_str(name: &str) -> protocol::LogContainer {
    if name.is_empty() {
        protocol::LogContainer::Default
    } else {
        protocol::LogContainer::Named(name.to_string())
    }
}
use crate::kube::session_nav::{
    drill_to_pods_by_labels, drill_to_pods_by_grep, drill_to_pods_by_owner,
    do_switch_namespace, begin_context_switch,
};

pub(crate) fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,

) -> ActionResult {
    use crate::app::Route;
    use crate::kube::protocol::ResourceId;

    // Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name);
        }
        return ActionResult::None;
    }

    // Handle ContainerSelect: open logs or shell for the selected container.
    if let Route::ContainerSelect { ref target, selected, action } = app.route {
        let target = target.clone();
        // Find the selected container on the live pod row. Lookup by the
        // typed `ObjectRef`'s (resource, name, namespace) triple — no
        // flat string re-parse.
        let pod_ns_str = target.namespace.display().to_string();
        // Look up the pod in the table — check both global and nav stack.
        let container_name = {
            let table = if crate::app::nav::is_globally_stored(&target.resource) {
                app.data.unified.get(&target.resource)
            } else {
                app.nav.find_table_for_resource(&target.resource)
            };
            table
                .and_then(|t| t.items.iter().find(|p| {
                    p.name == target.name && p.namespace.as_deref() == target.namespace.as_option()
                }))
                .and_then(|p| p.containers.get(selected).map(|ci| ci.name.clone()))
        };

        // If the pod disappeared mid-dialog or the index is out of range,
        // refuse rather than sending an empty container name (which the
        // server interprets as "default container" — the user almost
        // certainly didn't want logs from a different container than the
        // one they selected).
        let container_name = match container_name {
            Some(n) => n,
            None => {
                app.flash = Some(crate::app::FlashMessage::error(
                    format!("Pod {}/{} no longer has a container at index {}", pod_ns_str, target.name, selected)
                ));
                app.route = app.route_stack.pop().unwrap_or(Route::Resources);
                return ActionResult::None;
            }
        };

        if matches!(action, crate::app::ContainerAction::Shell) {
            // Shell mode: return ActionResult::Shell so the main loop runs it.
            app.route = app.route_stack.pop().unwrap_or(Route::Resources);
            return ActionResult::Shell(crate::kube::session::ExecTarget {
                pod: target.name,
                namespace: pod_ns_str,
                container: container_name,
            });
        }

        let previous = matches!(action, crate::app::ContainerAction::PreviousLogs);

        let mut log_state = crate::app::LogState::new();
        log_state.follow = !previous;
        log_state.streaming = true;
        let tail = Some(log_state.tail_lines);

        // Create the stream and stamp its generation on the state.
        let new_stream = data_source.stream_log_substream(crate::kube::protocol::LogInit {
            pod: target.name.clone(),
            namespace: target.namespace.clone(),
            container: log_container_from_str(&container_name),
            follow: !previous,
            tail,
            since: None,
            previous,
        });
        log_state.generation = new_stream.generation;

        // The log stream is owned by the Route — when the route is
        // dropped (Back, tab switch, navigate away), the stream dies.
        app.navigate_to(Route::Logs {
            target: ContainerRef::new(
                target.name.clone(),
                pod_ns_str,
                log_container_from_str(&container_name),
            ),
            state: Box::new(log_state),
            stream: Some(new_stream),
        });
        return ActionResult::None;
    }

    // Handle Enter: read the row's drill_target (set by the server-side
    // converter) and act on it. The client has NO K8s knowledge here —
    // it just blindly executes the server's instructions.
    use crate::kube::resources::row::DrillTarget;
    let row_data = app.active_view_table().and_then(|t| t.selected_item()).cloned();

    let Some(row) = row_data else {
        handle_describe(app, data_source);
        return ActionResult::None;
    };

    match row.drill_target.clone() {
        Some(DrillTarget::SwitchNamespace(ns)) => {
            do_switch_namespace(app, data_source, ns);
        }
        Some(DrillTarget::BrowseCrd(crd_ref)) => {
            let kind_label = crd_ref.kind.clone();
            let sel = app.active_view_table().map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep::new(
                ResourceId::Crd(crd_ref),
                None,
            ));
            apply_nav_change(app, data_source, change);
            app.flash = Some(crate::app::FlashMessage::info(format!("Browsing CRD: {}", kind_label)));
        }
        Some(DrillTarget::PodsByLabels { labels, breadcrumb }) => {
            drill_to_pods_by_labels(app, data_source, labels, &breadcrumb);
        }
        Some(DrillTarget::PodsByOwner { uid, kind, name }) => {
            drill_to_pods_by_owner(app, data_source, &uid, kind, &name);
        }
        Some(DrillTarget::PodsByField(selector)) => {
            let breadcrumb = selector.breadcrumb();
            let sel = app.active_view_table().map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep::new(
                rid(BuiltInKind::Pod),
                Some(crate::app::nav::NavFilter::Field(selector)),
            ));
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.flash = Some(crate::app::FlashMessage::info(format!("Pods filtered by {}", breadcrumb)));
        }
        Some(DrillTarget::PodsByNameGrep(name)) => {
            drill_to_pods_by_grep(app, data_source, &name);
        }
        Some(DrillTarget::JobsByOwner { uid, kind, name }) => {
            // Drill into jobs filtered by ownerReference UID. The
            // parent `kind` comes from the server — client doesn't
            // assume it's `CronJob` (even though it is today).
            use crate::app::nav::{NavFilter, NavStep};
            let sel = app.active_view_table().map(|t| t.selected).unwrap_or(0);
            app.nav.save_selected(sel);
            let kind_lower = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind.to_lowercase();
            let change = app.nav.push(NavStep::new(
                rid(BuiltInKind::Job),
                Some(NavFilter::OwnerChain {
                    uid,
                    kind,
                    display_name: name.clone(),
                }),
            ));
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.flash = Some(crate::app::FlashMessage::info(format!("Jobs for {}/{}", kind_lower, name)));
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
            app.navigate_to(Route::Describe {
                target: info,
                awaiting_response: false,
                state,
            });
            return;
        }

        ds_try!(app, data_source.describe(&info));
        app.navigate_to(Route::Describe {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        });
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
            app.navigate_to(Route::Yaml {
                target: info,
                awaiting_response: false,
                state,
            });
            return;
        }

        ds_try!(app, data_source.yaml(&info));
        app.navigate_to(Route::Yaml {
            target: info,
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        });
    }
}

pub(crate) fn handle_logs(
    app: &mut App,
    data_source: &mut ClientSession,

) {
    open_logs(app, data_source, false);
}

pub(crate) fn handle_previous_logs(
    app: &mut App,
    data_source: &mut ClientSession,

) {
    open_logs(app, data_source, true);
}

/// Core log-open flow shared by live and previous-logs actions. `previous=false`
/// opens a follow-mode stream, `previous=true` opens a static tail of the
/// previous container incarnation. Multi-container pods route through
/// [`Route::ContainerSelect`] with a matching [`crate::app::ContainerAction`]
/// variant so the picker remembers which flow the user chose.
fn open_logs(
    app: &mut App,
    data_source: &mut ClientSession,

    previous: bool,
) {
    use crate::app::Route;

    let Some(info) = get_selected_resource_info(app) else { return; };
    let name = info.name.clone();
    // `info.namespace` is the typed `Namespace`. Keep it typed for `LogInit`;
    // separately materialize a display string for `ContainerRef` (which
    // stores a *location* string, not a selection).
    let namespace_typed = info.namespace.clone();
    let namespace_display = namespace_typed.display().to_string();

    // Get containers from the selected row — resource-type-agnostic.
    let containers = app.active_view_table()
        .and_then(|t| t.selected_item())
        .map(|row| row.containers.clone())
        .unwrap_or_default();

    // Multi-container resources route through the container picker. The
    // picker re-enters this flow via the `Route::ContainerSelect` branch of
    // `handle_enter_key` which threads `previous` through from the
    // ContainerAction variant.
    if containers.len() > 1 {
        app.navigate_to(Route::ContainerSelect {
            target: info.clone(),
            selected: 0,
            action: if previous {
                crate::app::ContainerAction::PreviousLogs
            } else {
                crate::app::ContainerAction::Logs
            },
        });
        return;
    }

    // Single container: log it directly. No containers (workload-level):
    // use the typed `kubectl_target()` ("deployment/foo") and stream all.
    let (log_target, route_pod, route_container) = if !containers.is_empty() {
        let container = containers.first().map(|ci| ci.name.clone()).unwrap_or_default();
        (name.clone(), name.clone(), log_container_from_str(&container))
    } else {
        let target = info.kubectl_target();
        (target.clone(), target, protocol::LogContainer::All)
    };

    let mut log_state = crate::app::LogState::new();
    log_state.follow = !previous;
    log_state.streaming = true;
    let tail = Some(log_state.tail_lines);

    let new_stream = data_source.stream_log_substream(protocol::LogInit {
        pod: log_target.clone(),
        namespace: namespace_typed,
        container: route_container.clone(),
        follow: !previous,
        tail,
        since: None,
        previous,
    });
    log_state.generation = new_stream.generation;

    app.navigate_to(Route::Logs {
        target: ContainerRef::new(route_pod, namespace_display, route_container),
        state: Box::new(log_state),
        stream: Some(new_stream),
    });
}

/// Build a tab-separated text dump of the currently visible resource table.
/// Respects column visibility (column_level) so the dump matches what the user sees.
pub(crate) fn build_table_dump(app: &App) -> String {
    let current_rid = app.nav.resource_id();
    if let Some(table) = app.active_view_table() {
        let skip_ns = !app.selected_ns.is_all();
        let visible = app.active_view_descriptor()
            .map(|d| d.visible_columns(current_rid, app.column_level, skip_ns))
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
pub(crate) fn build_scale_form(target: ObjectRef, current_replicas: Option<&str>) -> crate::app::FormDialog {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};

    let title = format!("Scale: {}/{}", target.resource.display_label(), target.name);
    let subtitle = format!("namespace: {}", target.namespace.display());
    // Default to the current desired replica count so the user can adjust
    // from the existing value instead of typing from scratch.
    let default_value = current_replicas.unwrap_or("").to_string();
    FormDialog {
        submit: crate::app::FormSubmit::Scale,
        title,
        subtitle,
        target,
        fields: vec![FormFieldState {
            name: crate::kube::protocol::form_field_name::REPLICAS.into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 1_000_000 },
            value: default_value,
        }],
        focused: 0,
    }
}

/// Build a port-forward `FormDialog` for the currently-selected row.
/// Returns `None` if the current resource type doesn't support
/// port-forwarding or if no row is selected.
///
/// Field shapes (`container_port`, `local_port`) are decided here from the
/// row context: `container_port` is a Select over `row.pf_ports` when the
/// pod exposes ports, both fields default to the first available port.
/// Field name strings come from the `form_field_name` constants so the
/// submit dispatcher and the form builder share a single source of truth.
pub(crate) fn resolve_port_forward_dialog(app: &App) -> Option<crate::app::FormDialog> {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};
    use crate::kube::protocol::{Namespace, OperationKind};

    // Capability gate: only resources flagged as port-forwardable qualify.
    if !app.current_capabilities().supports(OperationKind::PortForward) {
        return None;
    }

    let rid_key = app.nav.resource_id().clone();
    let row = app.active_view_table()?.selected_item()?;

    // Ports come from the typed `pf_ports` field set by the converter.
    let ports: Vec<u16> = row.pf_ports.clone();
    let first_port = ports.first().copied().unwrap_or(8080);

    let short = rid_key.short_label().to_lowercase();
    let title = format!("Port forward: {}/{}", short, row.name);
    let ns = row.namespace.clone().unwrap_or_default();
    let subtitle = if ns.is_empty() {
        String::new()
    } else {
        format!("namespace: {}", ns)
    };
    let target_ref = ObjectRef::new(rid_key, row.name.clone(), Namespace::from_row(ns.as_str()));

    // container_port: Select if we know the available ports, plain Port
    // input otherwise. The initial value is the index of the first option.
    let container_field = if ports.is_empty() {
        FormFieldState {
            name: crate::kube::protocol::form_field_name::CONTAINER_PORT.into(),
            label: "Container port".into(),
            kind: FormFieldKind::Port,
            value: first_port.to_string(),
        }
    } else {
        let options: Vec<crate::app::SelectOption> = ports
            .iter()
            .map(|p| crate::app::SelectOption::new(p.to_string(), p.to_string()))
            .collect();
        FormFieldState {
            name: crate::kube::protocol::form_field_name::CONTAINER_PORT.into(),
            label: "Container port".into(),
            kind: FormFieldKind::Select { options },
            value: "0".into(),
        }
    };

    let local_field = FormFieldState {
        name: crate::kube::protocol::form_field_name::LOCAL_PORT.into(),
        label: "Local port".into(),
        kind: FormFieldKind::Port,
        value: first_port.to_string(),
    };

    Some(FormDialog {
        submit: crate::app::FormSubmit::PortForward,
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
    let table = app.active_view_table()?;
    let item = table.selected_item()?;
    Some(ObjectRef::new(
        current_rid,
        item.name().to_string(),
        Namespace::from_row(item.namespace()),
    ))
}

/// Get resource info for all marked items in the active table.
/// Returns empty Vec if no items are marked.
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::{Namespace, ObjectRef};

    let current_rid = app.nav.resource_id().clone();
    let mut result = Vec::new();
    if let Some(table) = app.active_view_table() {
        for item in &table.items {
            let key = crate::kube::protocol::ObjectKey::new(item.namespace(), item.name());
            if table.marked.contains(&key) {
                result.push(ObjectRef::new(
                    current_rid.clone(),
                    item.name().to_string(),
                    Namespace::from_row(item.namespace()),
                ));
            }
        }
    }
    result
}
