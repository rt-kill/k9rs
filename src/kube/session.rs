use std::time::Duration;

use anyhow::Result;
use crossterm::{
    cursor::SetCursorStyle,
    event::{Event as CtEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::app::{App, ResourceTab};
use crate::event::{AppEvent, ResourceUpdate};
use crate::kube::client_session::ClientSession;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum events to drain per iteration to prevent UI freezes during bursts.
const EVENT_DRAIN_CAP: usize = 200;
/// Maximum number of entries in the command history ring.
const COMMAND_HISTORY_LIMIT: usize = 50;
/// Number of lines to scroll per PageUp/PageDown in content views.
const PAGE_SCROLL_LINES: usize = 40;
/// Number of lines to scroll per PageUp/PageDown in the help view.
const HELP_PAGE_SCROLL_LINES: usize = 10;
/// Scroll offset when jumping to a search match (lines of context above match).
const SEARCH_SCROLL_CONTEXT: usize = 10;
/// Default terminal height when `crossterm::terminal::size()` is unavailable.
const DEFAULT_TERMINAL_HEIGHT: usize = 24;
/// Lines reserved for chrome (borders, status bar, etc.) when computing the
/// visible viewport height from the terminal height.
const CHROME_LINES: usize = 5;
/// Lines reserved for chrome in the log follow/toggle view (borders + bars).
const LOG_CHROME_LINES: usize = 4;
/// Regex pattern for the fault filter (matches common error/unhealthy states).
const FAULT_FILTER_PATTERN: &str = "error|failed|crashloop|pending|imagepull|oom|evicted|init";
/// Substring used to identify the fault filter grep in the nav stack.
const FAULT_FILTER_ID: &str = "crashloop";

/// Result returned from `handle_action` to signal the main loop about actions
/// that require terminal access (suspend/resume TUI for interactive commands).
enum ActionResult {
    /// No special handling needed.
    None,
    /// Suspend the TUI and run `kubectl exec -it` into a pod shell.
    Shell {
        pod: String,
        namespace: String,
        container: String,
        context: String,
    },
    /// Suspend the TUI and run `kubectl edit` on a resource.
    Edit {
        resource: String,
        name: String,
        namespace: String,
        context: String,
    },
}

pub async fn session_main(
    mut app: App,
    mut data_source: ClientSession,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    event_tx: mpsc::Sender<AppEvent>,
    mut event_rx: mpsc::Receiver<AppEvent>,
    mut input_rx: mpsc::Receiver<CtEvent>,
    tick_rate: Duration,
) -> Result<()> {
    // Track active log streaming task so we can cancel on Back
    let mut log_task: Option<JoinHandle<()>> = None;
    // Track active port-forward task so we can cancel it
    let mut port_forward_task: Option<JoinHandle<()>> = None;

    let mut tick_interval = tokio::time::interval(tick_rate);

    // Main event loop — only redraw when state changes
    let mut needs_redraw = true; // draw the first frame immediately
    loop {
        if needs_redraw {
            terminal.draw(|f| {
                crate::ui::draw(f, &mut app);
            })?;

            // Set cursor style based on input mode: bar for text input, block otherwise
            let in_input_mode = app.command_mode || app.scale_mode || app.port_forward_mode
                || app.filter_input.active
                || app.yaml.search_input_active || app.describe.search_input_active;
            if in_input_mode {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBar)?;
            } else {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBlock)?;
            }

            needs_redraw = false;
        }

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(ct_event) = input_rx.recv() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Capture input in command mode before normal dispatch
                        if app.command_mode {
                            match key.code {
                                KeyCode::Esc => {
                                    app.command_mode = false;
                                    app.command_input.clear();
                                }
                                KeyCode::Enter => {
                                    let raw_cmd = app.command_input.trim().to_string();
                                    let cmd = raw_cmd.to_lowercase();
                                    app.command_mode = false;
                                    // Push to command history before clearing
                                    if !cmd.is_empty() {
                                        app.command_history.push(raw_cmd.clone());
                                        if app.command_history.len() > COMMAND_HISTORY_LIMIT {
                                            app.command_history.remove(0);
                                        }
                                    }
                                    app.command_history_index = None;
                                    app.command_input.clear();
                                    // Handle command
                                    if cmd.is_empty() {
                                        // Empty command — just close the prompt
                                    } else if cmd == "q" || cmd == "quit" || cmd == "exit" || cmd == "q!" {
                                        app.should_quit = true;
                                    } else if cmd == "help" || cmd == "h" || cmd == "?" {
                                        app.push_route(app.route.clone());
                                        app.route = crate::app::Route::Help;
                                    } else if cmd == "alias" || cmd == "aliases" || cmd == "a" {
                                        // Show aliases view
                                        let action = crate::app::actions::Action::ShowAliases;
                                        handle_action(
                                            &mut app, action, &event_tx,
                                            &mut data_source,
                                            &mut log_task,
                                            &mut port_forward_task,
                                        ).await;
                                    } else if cmd == "ctx" || cmd == "context" || cmd == "contexts" {
                                        app.push_route(app.route.clone());
                                        app.route = crate::app::Route::Contexts;
                                    } else if cmd.starts_with("ctx ") || cmd.starts_with("context ") {
                                        let ctx_name = if cmd.starts_with("ctx ") { &raw_cmd[4..] } else { &raw_cmd[8..] }.trim().to_string();
                                        begin_context_switch(&mut app, &mut data_source, &ctx_name, &mut log_task, &mut port_forward_task).await;
                                    } else if cmd.starts_with("ns ") || cmd.starts_with("namespace ") {
                                        let ns = if cmd.starts_with("ns ") { &raw_cmd[3..] } else { &raw_cmd[10..] }.trim().to_string();
                                        do_switch_namespace(&mut app, &mut data_source, &ns, &mut log_task).await;
                                    } else if let Some(tab) = parse_resource_command(&cmd) {
                                        app.route = crate::app::Route::Resources;
                                        app.last_resource_tab = Some(app.resource_tab);
                                        app.resource_tab = tab;
                                        app.nav.reset(tab);
                                        app.filter_input = Default::default();
                                        data_source.switch_tab(tab).await;
                                    } else if let Some((tab, ns)) = parse_resource_ns_command(&cmd) {
                                        // :deploy kube-system — switch resource AND namespace
                                        app.route = crate::app::Route::Resources;
                                        app.last_resource_tab = Some(app.resource_tab);
                                        app.resource_tab = tab;
                                        app.nav.reset(tab);
                                        app.filter_input = Default::default();
                                        if ns != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut data_source, &ns, &mut log_task).await;
                                        }
                                        data_source.switch_tab(tab).await;
                                        app.flash = Some(crate::app::FlashMessage::info(format!(
                                            "{}({})", tab.label(), ns
                                        )));
                                    } else if let Some((tab, filter)) = parse_resource_filter_command(&cmd) {
                                        // :deploy /nginx — switch resource AND apply filter
                                        app.route = crate::app::Route::Resources;
                                        app.resource_tab = tab;
                                        app.nav.reset(tab);
                                        app.nav.push(crate::app::nav::NavStep {
                                            tab,
                                            filter: Some(crate::app::nav::NavFilter::Grep(filter.clone())),
                                            dynamic_spec: None,
                                            saved_selected: 0,
                                        });
                                        app.filter_input = Default::default();
                                        data_source.switch_tab(tab).await;
                                        app.reapply_nav_filters();
                                    } else if let Some((crd, ns)) = parse_crd_ns_command(&cmd, &app) {
                                        // :clickhouseinstallation prod — CRD + namespace
                                        app.route = crate::app::Route::Resources;
                                        if ns != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut data_source, &ns, &mut log_task).await;
                                        }
                                        let spec = crate::app::nav::DynamicSpec {
                                            group: crd.group.clone(), version: crd.version.clone(),
                                            kind: crd.kind.clone(), plural: crd.plural.clone(),
                                            scope: crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                                        };
                                        app.nav.reset_dynamic(spec);
                                        app.dynamic_resource_name = crd.kind.clone();
                                        app.dynamic_resource_api_resource = crd.name.clone();
                                        app.resource_tab = ResourceTab::DynamicResource;
                                        data_source.watch_dynamic(
                                            crd.group.clone(), crd.version.clone(),
                                            crd.kind.clone(), crd.plural.clone(),
                                            crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                                        ).await;
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Browsing CRD: {}({})", crd.kind, ns)
                                        ));
                                    } else if cmd == "overview" || cmd == "home" {
                                        app.route = crate::app::Route::Overview;
                                    } else if let Some(crd) = app.find_crd_by_name(&cmd) {
                                        app.route = crate::app::Route::Resources;
                                        let spec = crate::app::nav::DynamicSpec {
                                            group: crd.group.clone(), version: crd.version.clone(),
                                            kind: crd.kind.clone(), plural: crd.plural.clone(),
                                            scope: crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                                        };
                                        app.nav.reset_dynamic(spec);
                                        app.dynamic_resource_name = crd.kind.clone();
                                        app.dynamic_resource_api_resource = crd.name.clone();
                                        app.resource_tab = ResourceTab::DynamicResource;
                                        data_source.watch_dynamic(
                                            crd.group.clone(), crd.version.clone(),
                                            crd.kind.clone(), crd.plural.clone(),
                                            crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                                        ).await;
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Browsing CRD: {}", crd.kind)
                                        ));
                                    } else {
                                        app.flash = Some(crate::app::FlashMessage::warn(format!("Unknown command: {}", cmd)));
                                    }
                                }
                                KeyCode::Tab => {
                                    app.accept_completion();
                                }
                                KeyCode::Up => {
                                    if !app.command_history.is_empty() {
                                        let idx = match app.command_history_index {
                                            None => app.command_history.len() - 1,
                                            Some(i) => i.saturating_sub(1),
                                        };
                                        app.command_history_index = Some(idx);
                                        app.command_input = app.command_history[idx].clone();
                                    }
                                }
                                KeyCode::Down => {
                                    if let Some(idx) = app.command_history_index {
                                        if idx + 1 < app.command_history.len() {
                                            app.command_history_index = Some(idx + 1);
                                            app.command_input = app.command_history[idx + 1].clone();
                                        } else {
                                            app.command_history_index = None;
                                            app.command_input.clear();
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.command_input.pop();
                                }
                                KeyCode::Char(c) => {
                                    app.command_input.push(c);
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue; // skip normal key handling
                        }

                        // Scale mode: capture numeric input for replica count
                        if app.scale_mode {
                            match key.code {
                                KeyCode::Esc => {
                                    app.scale_mode = false;
                                    app.command_input.clear();
                                    app.scale_target = (String::new(), String::new(), String::new());
                                }
                                KeyCode::Enter => {
                                    let replica_str = app.command_input.trim().to_string();
                                    let target = app.scale_target.clone();
                                    app.scale_mode = false;
                                    app.command_input.clear();
                                    app.scale_target = (String::new(), String::new(), String::new());

                                    if let Ok(replicas) = replica_str.parse::<u32>() {
                                        app.kubectl_cache.clear();
                                        let (resource, name, namespace) = target;
                                        let _ = data_source.scale(&resource, &name, &namespace, replicas).await;
                                    } else {
                                        app.flash = Some(crate::app::FlashMessage::warn(
                                            format!("Invalid replica count: {}", replica_str)
                                        ));
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.command_input.pop();
                                }
                                KeyCode::Char(c) if c.is_ascii_digit() => {
                                    app.command_input.push(c);
                                }
                                _ => {} // Ignore non-digit input in scale mode
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Port-forward mode: capture port input (local:remote)
                        if app.port_forward_mode {
                            match key.code {
                                KeyCode::Esc => {
                                    app.port_forward_mode = false;
                                    app.command_input.clear();
                                    app.port_forward_target = (String::new(), String::new());
                                }
                                KeyCode::Enter => {
                                    let ports_str = app.command_input.trim().to_string();
                                    let (pod_name, pod_ns) = app.port_forward_target.clone();
                                    app.port_forward_mode = false;
                                    app.command_input.clear();
                                    app.port_forward_target = (String::new(), String::new());

                                    // Validate format: should contain a colon separating local:remote
                                    let parts: Vec<&str> = ports_str.split(':').collect();
                                    if parts.len() == 2
                                        && parts[0].parse::<u16>().is_ok()
                                        && parts[1].parse::<u16>().is_ok()
                                    {
                                        let context = app.context.clone();
                                        let tx = event_tx.clone();
                                        let ports = ports_str.clone();
                                        let pn = pod_name.clone();
                                        // Abort any existing port-forward before starting a new one
                                        if let Some(h) = port_forward_task.take() { h.abort(); }
                                        port_forward_task = Some(tokio::spawn(async move {
                                            let mut cmd = tokio::process::Command::new("kubectl");
                                            cmd.arg("port-forward").arg(&pn).arg(&ports);
                                            if !pod_ns.is_empty() { cmd.arg("-n").arg(&pod_ns); }
                                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                            // Use spawn + wait instead of output() because
                                            // kubectl port-forward runs indefinitely and
                                            // output() buffers all stdout/stderr into memory.
                                            match cmd.kill_on_drop(true).spawn() {
                                                Ok(mut child) => {
                                                    match child.wait().await {
                                                        Ok(_) => {
                                                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                                                format!("Port-forward ended for {}", pn)
                                                            ))).await;
                                                        }
                                                        Err(e) => {
                                                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                                                format!("Port-forward failed: {}", e)
                                                            ))).await;
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                                        format!("Port-forward failed: {}", e)
                                                    ))).await;
                                                }
                                            }
                                        }));
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Port-forwarding pod/{} on {}", pod_name, ports_str)
                                        ));
                                    } else {
                                        app.flash = Some(crate::app::FlashMessage::warn(
                                            format!("Invalid port format: {} (expected local:remote, e.g. 8080:80)", ports_str)
                                        ));
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.command_input.pop();
                                }
                                KeyCode::Char(c) if c.is_ascii_digit() || c == ':' => {
                                    app.command_input.push(c);
                                }
                                _ => {} // Ignore non-port input in port-forward mode
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Capture input in filter mode before normal dispatch
                        if app.filter_input.active {
                            match key.code {
                                KeyCode::Esc => {
                                    // Discard uncommitted text, reapply existing nav filters
                                    app.filter_input.text.clear();
                                    app.filter_input.active = false;
                                    app.reapply_nav_filters();
                                }
                                KeyCode::Enter => {
                                    // Commit: push grep onto nav stack
                                    let text = std::mem::take(&mut app.filter_input.text);
                                    app.filter_input.active = false;
                                    if !text.is_empty() {
                                        app.nav.push(crate::app::nav::NavStep {
                                            tab: app.nav.tab(),
                                            filter: Some(crate::app::nav::NavFilter::Grep(text)),
                                            dynamic_spec: None,
                                            saved_selected: 0,
                                        });
                                    }
                                    app.reapply_nav_filters();
                                }
                                KeyCode::Backspace => {
                                    app.filter_input.text.pop();
                                    // Live preview
                                    app.reapply_nav_filters();
                                }
                                KeyCode::Char(c) => {
                                    app.filter_input.text.push(c);
                                    // Live preview
                                    app.reapply_nav_filters();
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Capture search input in YAML/describe views
                        if app.yaml.search_input_active && matches!(app.route, crate::app::Route::Yaml { .. }) {
                            match key.code {
                                KeyCode::Esc => {
                                    app.yaml.search_input_active = false;
                                    app.yaml.search_input.clear();
                                }
                                KeyCode::Enter => {
                                    let term = app.yaml.search_input.clone();
                                    app.yaml.search_input_active = false;
                                    if term.is_empty() {
                                        app.yaml.clear_search();
                                    } else {
                                        app.yaml.search = Some(term);
                                        app.yaml.update_search();
                                        // Jump to first match
                                        if !app.yaml.search_matches.is_empty() {
                                            app.yaml.current_match = 0;
                                            let target = app.yaml.search_matches[0];
                                            app.yaml.scroll = target.saturating_sub(SEARCH_SCROLL_CONTEXT);
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.yaml.search_input.pop();
                                }
                                KeyCode::Char(c) => {
                                    app.yaml.search_input.push(c);
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue;
                        }
                        if app.describe.search_input_active && matches!(app.route, crate::app::Route::Describe { .. } | crate::app::Route::Aliases) {
                            match key.code {
                                KeyCode::Esc => {
                                    app.describe.search_input_active = false;
                                    app.describe.search_input.clear();
                                }
                                KeyCode::Enter => {
                                    let term = app.describe.search_input.clone();
                                    app.describe.search_input_active = false;
                                    if term.is_empty() {
                                        app.describe.clear_search();
                                    } else {
                                        app.describe.search = Some(term);
                                        app.describe.update_search();
                                        // Jump to first match
                                        if !app.describe.search_matches.is_empty() {
                                            app.describe.current_match = 0;
                                            let target = app.describe.search_matches[0];
                                            app.describe.scroll = target.saturating_sub(SEARCH_SCROLL_CONTEXT);
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    app.describe.search_input.pop();
                                }
                                KeyCode::Char(c) => {
                                    app.describe.search_input.push(c);
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue;
                        }

                        needs_redraw = true;
                        if let Some(action) = crate::event::handler::handle_key_event(&app, key) {
                            let result = handle_action(
                                &mut app,
                                action,
                                &event_tx,
                                &mut data_source,
                                &mut log_task,
                                &mut port_forward_task,
                            ).await;
                            match result {
                                ActionResult::Shell { pod, namespace, container, context } => {
                                    let args = build_shell_args(&pod, &namespace, &container, &context);
                                    run_interactive_local(
                                        &mut terminal, &mut app, "kubectl", &args,
                                    ).await?;
                                }
                                ActionResult::Edit { resource, name, namespace, context } => {
                                    let args = build_edit_args(&resource, &name, &namespace, &context);
                                    run_interactive_local(
                                        &mut terminal, &mut app, "kubectl", &args,
                                    ).await?;
                                    // Clear cache after edit (resource may have changed)
                                    app.kubectl_cache.clear();
                                }
                                ActionResult::None => {
                                    // Check for pending shell from ContainerSelect
                                    if let Some((pod, ns, container)) = app.pending_shell.take() {
                                        let args = build_shell_args(&pod, &ns, &container, &app.context.clone());
                                        run_interactive_local(
                                            &mut terminal, &mut app, "kubectl", &args,
                                        ).await?;
                                    }
                                }
                            }
                        }
                    }
                    CtEvent::Mouse(_mouse) => {
                        // TODO: mouse handling
                    }
                    CtEvent::Resize(_, _) => {
                        needs_redraw = true;
                    }
                    _ => {}
                }
            }
            Some(event) = event_rx.recv() => {
                let mut cache_flags = CacheUpdateFlags::default();
                if let AppEvent::ContextSwitchResult { context, result } = event {
                    apply_context_switch_result(
                        &mut app, &mut data_source,
                        &context, result,
                    ).await;
                    app.pod_metrics.clear();
                    app.node_metrics.clear();
                } else {
                    cache_flags.merge(apply_event(&mut app, event));
                }
                // Drain pending events before redrawing to batch log lines,
                // but cap at EVENT_DRAIN_CAP to prevent UI freezes during massive bursts.
                let mut drained = 0;
                while drained < EVENT_DRAIN_CAP {
                    match event_rx.try_recv() {
                        Ok(event) => {
                            if let AppEvent::ContextSwitchResult { context, result } = event {
                                apply_context_switch_result(
                                    &mut app, &mut data_source,
                                    &context, result,
                                ).await;
                                app.pod_metrics.clear();
                                app.node_metrics.clear();
                            } else {
                                cache_flags.merge(apply_event(&mut app, event));
                            }
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }
                // Persist discovery cache when namespaces or CRDs are updated.
                // Send to daemon (shared) via the persistent writer, or save to disk.
                if cache_flags.namespaces || cache_flags.crds {
                    let ns_names: Vec<String> = app.data.namespaces.items
                        .iter()
                        .map(|ns| ns.name.clone())
                        .collect();
                    let cached_crds = crate::kube::cache::domain_crds_to_cached(&app.discovered_crds);
                    data_source.put_discovery(&ns_names, &cached_crds);
                }
                needs_redraw = true;
            }
            _ = tick_interval.tick() => {
                if app.tick() {
                    needs_redraw = true;
                }
                // Check if the log streaming task has finished
                if let Some(ref handle) = log_task {
                    if handle.is_finished() {
                        log_task.take();
                        app.logs.streaming = false;
                        needs_redraw = true;
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Cleanup
    if let Some(handle) = log_task.take() {
        handle.abort();
    }
    if let Some(handle) = port_forward_task.take() {
        handle.abort();
    }
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        SetCursorStyle::DefaultUserShape,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    Ok(())
}

async fn handle_action(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
    port_forward_task: &mut Option<JoinHandle<()>>,
) -> ActionResult {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Quit => app.should_quit = true,
        Action::Back => {
            // Cancel any active log stream
            if let Some(handle) = log_task.take() {
                handle.abort();
                app.logs.streaming = false;
            }
            // Clear logs only when leaving a log/shell view
            if matches!(app.route, Route::Logs { .. } | Route::Shell { .. }) {
                let _ = data_source.stop_logs().await;
                app.logs.clear();
            }
            // Reset shell_mode_container_select when leaving ContainerSelect
            if matches!(app.route, Route::ContainerSelect { .. }) {
                app.shell_mode_container_select = false;
            }
            // Clear describe/yaml content and search state to free memory on navigation
            if matches!(app.route, Route::Describe { .. } | Route::Aliases) {
                app.describe.content.clear();
                app.describe.clear_search();
            }
            if matches!(app.route, Route::Yaml { .. }) {
                app.yaml.content.clear();
                app.yaml.clear_search();
            }
            // Pop the route stack to go back
            if let Some(route) = app.route_stack.pop() {
                app.route = route;
            }
        }
        Action::Help => {
            if app.route == Route::Help {
                // Toggle: pressing ? while in help goes back
                if let Some(route) = app.route_stack.pop() {
                    app.route = route;
                } else {
                    app.route = Route::Resources;
                }
            } else {
                app.push_route(app.route.clone());
                app.route = Route::Help;
            }
        }
        Action::NextTab => {
            if matches!(app.route, crate::app::Route::Overview) {
                app.route = crate::app::Route::Resources;
            }
            app.last_resource_tab = Some(app.resource_tab);
            app.next_tab();
            app.nav.reset(app.resource_tab);
            app.filter_input = Default::default();
            data_source.switch_tab(app.resource_tab).await;
        }
        Action::PrevTab => {
            if matches!(app.route, crate::app::Route::Overview) {
                app.route = crate::app::Route::Resources;
            }
            app.last_resource_tab = Some(app.resource_tab);
            app.prev_tab();
            app.nav.reset(app.resource_tab);
            app.filter_input = Default::default();
            data_source.switch_tab(app.resource_tab).await;
        }
        Action::NextItem => {
            match &app.route {
                Route::Yaml { .. } => {
                    let max = app.yaml.content.lines().count().saturating_sub(1);
                    app.yaml.scroll = (app.yaml.scroll + 1).min(max);
                }
                Route::Describe { .. } | Route::Aliases => {
                    let max = app.describe.content.lines().count().saturating_sub(1);
                    app.describe.scroll = (app.describe.scroll + 1).min(max);
                }
                Route::ContainerSelect { ref pod, ref namespace } => {
                    let container_count = app.data.pods.items.iter()
                        .find(|p| p.name == *pod && p.namespace == *namespace)
                        .map(|p| p.containers.len())
                        .unwrap_or(0);
                    if container_count > 0 && app.container_select_index + 1 < container_count {
                        app.container_select_index += 1;
                    }
                }
                Route::Help => {
                    app.help_scroll += 1;
                }
                _ => app.select_next(),
            }
        }
        Action::PrevItem => {
            match &app.route {
                Route::Yaml { .. } => app.yaml.scroll = app.yaml.scroll.saturating_sub(1),
                Route::Describe { .. } | Route::Aliases => app.describe.scroll = app.describe.scroll.saturating_sub(1),
                Route::ContainerSelect { .. } => {
                    app.container_select_index = app.container_select_index.saturating_sub(1);
                }
                Route::Help => {
                    app.help_scroll = app.help_scroll.saturating_sub(1);
                }
                _ => app.select_prev(),
            }
        }
        Action::PageUp => {
            match &app.route {
                Route::Logs { .. } | Route::Shell { .. } => {
                    app.logs.follow = false;
                    app.logs.scroll = app.logs.scroll.saturating_sub(PAGE_SCROLL_LINES);
                }
                Route::Yaml { .. } => app.yaml.scroll = app.yaml.scroll.saturating_sub(PAGE_SCROLL_LINES),
                Route::Describe { .. } | Route::Aliases => app.describe.scroll = app.describe.scroll.saturating_sub(PAGE_SCROLL_LINES),
                Route::Help => {
                    app.help_scroll = app.help_scroll.saturating_sub(HELP_PAGE_SCROLL_LINES);
                }
                _ => app.page_up(),
            }
        }
        Action::PageDown => {
            match &app.route {
                Route::Logs { .. } | Route::Shell { .. } => {
                    app.logs.follow = false;
                    let total = app.logs.lines.len();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    app.logs.scroll = (app.logs.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Yaml { .. } => {
                    let max = app.yaml.content.lines().count().saturating_sub(1);
                    app.yaml.scroll = (app.yaml.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Describe { .. } | Route::Aliases => {
                    let max = app.describe.content.lines().count().saturating_sub(1);
                    app.describe.scroll = (app.describe.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Help => {
                    app.help_scroll += HELP_PAGE_SCROLL_LINES;
                }
                _ => app.page_down(),
            }
        }
        Action::Home => {
            match &app.route {
                Route::Logs { .. } | Route::Shell { .. } => {
                    app.logs.follow = false;
                    app.logs.scroll = 0;
                }
                Route::Yaml { .. } => app.yaml.scroll = 0,
                Route::Describe { .. } | Route::Aliases => app.describe.scroll = 0,
                Route::Help => app.help_scroll = 0,
                _ => app.go_home(),
            }
        }
        Action::End => {
            match &app.route {
                Route::Logs { .. } | Route::Shell { .. } => {
                    let total = app.logs.lines.len();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    app.logs.scroll = total.saturating_sub(visible);
                    app.logs.follow = true;
                }
                Route::Yaml { .. } => {
                    app.yaml.scroll = app.yaml.content.lines().count().saturating_sub(1);
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.scroll = app.describe.content.lines().count().saturating_sub(1);
                }
                Route::Help => {
                    app.help_scroll = usize::MAX; // clamped in render
                }
                _ => app.go_end(),
            }
        }
        Action::Enter => handle_enter(app, data_source, log_task, port_forward_task).await,
        Action::Describe => handle_describe(app, data_source).await,
        Action::Yaml => handle_yaml(app, data_source).await,
        Action::Logs => handle_logs(app, data_source, log_task).await,
        Action::Shell => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    if pod.containers.len() > 1 {
                        // Multi-container pod: show container selector
                        let pod_name = pod.name.clone();
                        let pod_ns = pod.namespace.clone();
                        app.container_select_index = 0;
                        app.push_route(app.route.clone());
                        app.route = Route::ContainerSelect {
                            pod: pod_name,
                            namespace: pod_ns,
                        };
                        // Store a flag so ContainerSelect Enter knows to shell, not log
                        app.shell_mode_container_select = true;
                    } else {
                        let pod_name = pod.name.clone();
                        let pod_ns = pod.namespace.clone();
                        let container = pod.containers.first().map(|c| c.real_name.clone()).unwrap_or_default();
                        let context = app.context.clone();
                        return ActionResult::Shell {
                            pod: pod_name,
                            namespace: pod_ns,
                            container,
                            context,
                        };
                    }
                }
            }
        }
        Action::Delete => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                let resource = marked[0].0.clone();
                app.pending_batch_delete = marked;
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {} {}s?", count, resource),
                    action: Action::Delete,
                    yes_selected: false,
                });
            } else {
                let (resource, name, _namespace) = get_selected_resource_info(app);
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {}/{}?", resource, name),
                    action: Action::Delete,
                    yes_selected: false,
                });
            }
        }
        Action::Edit => {
            let (resource, name, namespace) = get_selected_resource_info(app);
            if !name.is_empty() {
                let context = app.context.clone();
                return ActionResult::Edit {
                    resource,
                    name,
                    namespace,
                    context,
                };
            }
        }
        Action::Scale => {
            let (resource, name, namespace) = get_selected_resource_info(app);
            if !name.is_empty() {
                // Enter scale mode: show a prompt for the replica count
                app.scale_mode = true;
                app.scale_target = (resource.clone(), name.clone(), namespace);
                app.command_input.clear();
                app.flash = Some(crate::app::FlashMessage::info(
                    format!("Scale {}/{} - enter replica count:", resource, name)
                ));
            }
        }
        Action::Filter(_) => {
            app.filter_input.active = true;
            app.filter_input.text.clear();
        }
        Action::ClearFilter => {
            // Pop one nav level (Esc behavior)
            let old_tab = app.nav.tab();
            if let Some(popped) = app.nav.pop() {
                let new_tab = app.nav.tab();
                if new_tab != old_tab {
                    // Tab changed (was a drill-down) — switch subscription back
                    app.resource_tab = new_tab;
                    data_source.switch_tab(new_tab).await;
                    // Restore saved cursor position from the step we're returning to
                    let saved = app.nav.current().saved_selected;
                    app.select_in_active_table(saved);
                }
                app.reapply_nav_filters();
                // If we popped a fault filter toggle, reset the flag
                if app.fault_filter {
                    if let Some(crate::app::nav::NavFilter::Grep(ref t)) = popped.filter {
                        if t.contains(FAULT_FILTER_ID) {
                            app.fault_filter = false;
                        }
                    }
                }
            } else {
                // At root — just clear any existing filter
                app.clear_filter();
            }
        }
        Action::ToggleLogFollow => {
            app.logs.follow = !app.logs.follow;
            if app.logs.follow {
                app.logs.scroll = app.logs.lines.len().saturating_sub(1);
            } else {
                // Snapshot scroll to top-of-viewport so the view freezes
                // immediately instead of drifting while new lines arrive.
                // Use the terminal height as an approximation of the log
                // viewport; the renderer clamps via min() anyway.
                let (_, rows) = crossterm::terminal::size().unwrap_or((80, DEFAULT_TERMINAL_HEIGHT as u16));
                // Subtract borders (2) + indicator bar (1) + keybinding bar (1)
                let visible = (rows as usize).saturating_sub(LOG_CHROME_LINES);
                let total = app.logs.lines.len();
                app.logs.scroll = total.saturating_sub(visible);
            }
        }
        Action::ToggleLogWrap => {
            app.logs.wrap = !app.logs.wrap;
        }
        Action::ToggleLogTimestamps => {
            app.logs.show_timestamps = !app.logs.show_timestamps;
            app.flash = Some(crate::app::FlashMessage::info(
                if app.logs.show_timestamps { "Timestamps: on" } else { "Timestamps: off" }
            ));
        }
        Action::ClearLogs => {
            app.logs.clear();
            app.flash = Some(crate::app::FlashMessage::info("Logs cleared"));
        }
        Action::Restart => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                let resource = marked[0].0.clone();
                app.pending_batch_restart = marked;
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Restart {} {}s?", count, resource),
                    action: Action::Restart,
                    yes_selected: false,
                });
            } else {
                let (resource, name, _namespace) = get_selected_resource_info(app);
                if !name.is_empty() {
                    app.confirm_dialog = Some(crate::app::ConfirmDialog {
                        message: format!("Restart {}/{}?", resource, name),
                        action: Action::Restart,
                        yes_selected: false,
                    });
                }
            }
        }
        Action::ForceKill => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                app.pending_batch_force_kill = marked;
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Force-kill {} pods?", count),
                    action: Action::ForceKill,
                    yes_selected: false,
                });
            } else {
                let (resource, name, _namespace) = get_selected_resource_info(app);
                if !name.is_empty() {
                    app.confirm_dialog = Some(crate::app::ConfirmDialog {
                        message: format!("Force-kill {}/{}?", resource, name),
                        action: Action::ForceKill,
                        yes_selected: false,
                    });
                }
            }
        }
        Action::PortForward => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let pod_name = pod.name.clone();
                    let pod_ns = pod.namespace.clone();
                    // Enter port-forward mode: show a prompt for the ports
                    app.port_forward_mode = true;
                    app.port_forward_target = (pod_name.clone(), pod_ns);
                    app.command_input = "8080:8080".to_string();
                    app.flash = Some(crate::app::FlashMessage::info(
                        format!("Port-forward pod/{} - enter local:remote ports:", pod_name)
                    ));
                }
            } else {
                app.flash = Some(crate::app::FlashMessage::info("Port-forward: select a pod first"));
            }
        }
        Action::ToggleHeader => {
            app.show_header = !app.show_header;
        }
        Action::ToggleFullFetch => {
            // No-op: full-fetch mode is not implemented in the daemon architecture.
        }
        Action::ScrollUp(n) => {
            app.logs.follow = false;
            app.logs.scroll = app.logs.scroll.saturating_sub(n);
        }
        Action::ScrollDown(n) => {
            app.logs.follow = false;
            // Clamp to last valid viewport position (total - visible_height),
            // not total - 1. Otherwise scroll can overshoot and the render
            // path's min() causes a "catch-up" scroll effect as new lines arrive.
            let total = app.logs.lines.len();
            let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
            let max = total.saturating_sub(visible);
            app.logs.scroll = (app.logs.scroll + n).min(max);
        }
        Action::SwitchNamespace(ns) => {
            do_switch_namespace(app, data_source, &ns, log_task).await;
        }
        Action::SwitchContext(ctx) => {
            begin_context_switch(app, data_source, &ctx, log_task, port_forward_task).await;
        }
        Action::Refresh => {
            data_source.refresh_tab(app.resource_tab).await;
            app.flash = Some(crate::app::FlashMessage::info("Refreshed"));
        }
        Action::Copy => {
            let (text, label) = match &app.route {
                Route::Yaml { .. } => {
                    let c = &app.yaml.content;
                    if c.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = c.lines().count();
                        (c.clone(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                Route::Describe { .. } => {
                    let c = &app.describe.content;
                    if c.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = c.lines().count();
                        (c.clone(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                Route::Logs { .. } => {
                    if app.logs.lines.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let joined: String = app.logs.lines.iter().cloned().collect::<Vec<_>>().join("\n");
                        let count = app.logs.lines.len();
                        (joined, format!("Copied {} lines to clipboard", count))
                    }
                }
                _ => {
                    let (_, name, _) = get_selected_resource_info(app);
                    if name.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let label = format!("Copied: {}", name);
                        (name, label)
                    }
                }
            };
            if !text.is_empty() {
                let tx = event_tx.clone();
                tokio::spawn(async move {
                    let ok = tokio::task::spawn_blocking(move || {
                        crate::util::try_copy_to_clipboard(&text)
                    }).await.unwrap_or(false);
                    if ok {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(label))).await;
                    } else {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::warn(
                            "No clipboard tool found (install xclip, xsel, wl-copy, or pbcopy)"
                        ))).await;
                    }
                });
            }
        }
        Action::Confirm => {
            let confirmed_action = app.confirm_dialog.as_ref().map(|d| d.action.clone());
            app.confirm_dialog = None;

            if matches!(confirmed_action, Some(Action::Restart)) {
                app.kubectl_cache.clear();
                let batch = std::mem::take(&mut app.pending_batch_restart);
                if !batch.is_empty() {
                    // Batch restart: iterate through all marked items
                    app.clear_marks();
                    for (resource, name, namespace) in &batch {
                        let _ = data_source.restart(resource, name, namespace).await;
                    }
                } else {
                    // Single restart
                    let (resource, name, namespace) = get_selected_resource_info(app);
                    if !name.is_empty() {
                        let _ = data_source.restart(&resource, &name, &namespace).await;
                    }
                }
            } else if matches!(confirmed_action, Some(Action::ForceKill)) {
                app.kubectl_cache.clear();
                let batch = std::mem::take(&mut app.pending_batch_force_kill);
                if !batch.is_empty() {
                    // Batch force-kill: iterate through all marked items
                    let count = batch.len();
                    app.clear_marks();
                    let context = app.context.clone();
                    let tx = event_tx.clone();
                    tokio::spawn(async move {
                        let mut ok_count = 0usize;
                        let mut last_err = String::new();
                        for (_resource, name, namespace) in &batch {
                            let mut cmd = tokio::process::Command::new("kubectl");
                            cmd.arg("delete").arg("pod").arg(name).arg("--force").arg("--grace-period=0");
                            if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                            match cmd.output().await {
                                Ok(o) if o.status.success() => { ok_count += 1; }
                                Ok(o) => { last_err = String::from_utf8_lossy(&o.stderr).to_string(); }
                                Err(e) => { last_err = e.to_string(); }
                            }
                        }
                        if ok_count == count {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                format!("Force-killed {} pods", count)
                            ))).await;
                        } else {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                format!("Force-killed {}/{}, last error: {}", ok_count, count, last_err)
                            ))).await;
                        }
                    });
                } else {
                    // Single force-kill
                    let (_resource, name, namespace) = get_selected_resource_info(app);
                    if !name.is_empty() {
                        let context = app.context.clone();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let mut cmd = tokio::process::Command::new("kubectl");
                            cmd.arg("delete").arg("pod").arg(&name).arg("--force").arg("--grace-period=0");
                            if !namespace.is_empty() { cmd.arg("-n").arg(&namespace); }
                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                            match cmd.output().await {
                                Ok(o) if o.status.success() => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Force-killed pod/{}", name)))).await;
                                }
                                Ok(o) => {
                                    let err = String::from_utf8_lossy(&o.stderr);
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Force-kill failed: {}", err)))).await;
                                }
                                Err(e) => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Failed: {}", e)))).await;
                                }
                            }
                        });
                    }
                }
            } else if matches!(confirmed_action, Some(Action::Delete)) {
                // Execute the actual delete
                app.kubectl_cache.clear();
                let batch = std::mem::take(&mut app.pending_batch_delete);
                if !batch.is_empty() {
                    // Batch delete: iterate through all marked items
                    app.clear_marks();
                    for (resource, name, namespace) in &batch {
                        let _ = data_source.delete(resource, name, namespace).await;
                    }
                } else {
                    // Single delete
                    let (resource, name, namespace) = get_selected_resource_info(app);
                    if !name.is_empty() {
                        let _ = data_source.delete(&resource, &name, &namespace).await;
                    }
                }
            } else {
                app.flash = Some(crate::app::FlashMessage::info("Confirmed"));
            }
        }
        Action::Cancel => {
            app.confirm_dialog = None;
            app.pending_batch_delete.clear();
            app.pending_batch_restart.clear();
            app.pending_batch_force_kill.clear();
        }
        Action::CommandMode => {
            app.command_mode = true;
            app.command_input.clear();
        }
        Action::ToggleDialogButton => {
            if let Some(ref mut dialog) = app.confirm_dialog {
                dialog.yes_selected = !dialog.yes_selected;
            }
        }
        Action::Sort(col) => {
            app.sort_by(col);
        }
        Action::ToggleSortDirection => {
            app.toggle_sort_direction();
        }
        Action::SearchStart => {
            match &app.route {
                Route::Yaml { .. } => {
                    app.yaml.search_input_active = true;
                    app.yaml.search_input.clear();
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.search_input_active = true;
                    app.describe.search_input.clear();
                }
                _ => {}
            }
        }
        Action::SearchExec(term) => {
            match &app.route {
                Route::Yaml { .. } => {
                    app.yaml.search = Some(term.clone());
                    app.yaml.update_search();
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.search = Some(term.clone());
                    app.describe.update_search();
                }
                _ => {}
            }
        }
        Action::SearchNext => {
            match &app.route {
                Route::Yaml { .. } => {
                    // Use a reasonable default viewport size for centering
                    app.yaml.next_match(40);
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.next_match(40);
                }
                _ => {}
            }
        }
        Action::SearchPrev => {
            match &app.route {
                Route::Yaml { .. } => {
                    app.yaml.prev_match(40);
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.prev_match(40);
                }
                _ => {}
            }
        }
        Action::SearchClear => {
            match &app.route {
                Route::Yaml { .. } => {
                    app.yaml.clear_search();
                }
                Route::Describe { .. } | Route::Aliases => {
                    app.describe.clear_search();
                }
                _ => {}
            }
        }
        Action::PreviousLogs => {
            handle_previous_logs(app, data_source, log_task).await;
        }
        Action::ShowNode => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let node = pod.node.clone();
                    if !node.is_empty() {
                        app.nav.save_selected(app.data.pods.selected);
                        app.nav.push(crate::app::nav::NavStep {
                            tab: ResourceTab::Nodes,
                            filter: Some(crate::app::nav::NavFilter::Grep(node.clone())),
                            dynamic_spec: None,
                            saved_selected: 0,
                        });
                        app.resource_tab = ResourceTab::Nodes;
                        data_source.switch_tab(ResourceTab::Nodes).await;
                        app.reapply_nav_filters();
                    }
                }
            }
        }
        Action::ToggleLastView => {
            if let Some(last) = app.last_resource_tab.take() {
                let current = app.resource_tab;
                app.resource_tab = last;
                app.last_resource_tab = Some(current);
                app.nav.reset(last);
                app.filter_input = Default::default();
                data_source.switch_tab(app.resource_tab).await;
            }
        }
        Action::ToggleMark => {
            app.toggle_mark();
        }
        Action::SaveTable => {
            let filename = format!(
                "/tmp/k9rs-{}-{}.txt",
                app.resource_tab.label(),
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            );
            let content = build_table_dump(app);
            let fname = filename.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                match tokio::fs::write(&fname, &content).await {
                    Ok(_) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Saved to {}", fname)))).await;
                    }
                    Err(e) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Save failed: {}", e)))).await;
                    }
                }
            });
        }
        Action::ShowAliases => {
            let mut content = String::from("Resource Aliases\n================\n\n");
            content.push_str(&format!("  {:<45} {}\n", "ALIAS", "RESOURCE"));
            content.push_str(&format!("  {:<45} {}\n", "-----", "--------"));
            for meta in crate::kube::resource_types::RESOURCE_TYPES {
                let aliases = meta.aliases.join("/");
                content.push_str(&format!("  {:<45} {}\n", aliases, meta.kind));
            }
            content.push_str("\n\nSpecial Commands\n================\n\n");
            content.push_str("  :q / :quit / :exit         Quit\n");
            content.push_str("  :help / :h / :?            Show help\n");
            content.push_str("  :ctx / :context            Context selector\n");
            content.push_str("  :ctx <name>                Switch context\n");
            content.push_str("  :ns <name>                 Switch namespace\n");
            content.push_str("  :alias / :aliases / :a     This view\n");
            content.push_str("\n\nKey Bindings\n============\n\n");
            content.push_str("  Ctrl-a                     Aliases view\n");
            content.push_str("  Ctrl-c                     Quit\n");
            content.push_str("  Ctrl-r                     Refresh\n");
            content.push_str("  Ctrl-e                     Toggle header\n");
            content.push_str("  Ctrl-s                     Save table to file\n");
            app.describe.content = content;
            app.describe.scroll = 0;
            app.push_route(app.route.clone());
            app.route = Route::Aliases;
        }
        Action::LogSince(ref since) => {
            app.logs.since = since.clone();
            app.logs.clear();
            app.logs.follow = true;
            app.logs.streaming = true;
            let since_label = since.as_deref().unwrap_or("tail");
            app.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            // Cancel existing log task
            if let Some(handle) = log_task.take() { handle.abort(); }
            // Restart with new since/tail parameter
            if let Route::Logs { ref pod, ref container, ref namespace } = app.route {
                let tail = if since.is_none() { Some(app.logs.tail_lines) } else { None };
                let pod = pod.clone();
                let namespace = namespace.clone();
                let container = container.clone();
                let _ = data_source.stream_logs(
                    &pod,
                    &namespace,
                    &container,
                    true,  // follow
                    tail,
                    since.clone(),
                    false, // not previous
                ).await;
            }
        }
        Action::ToggleWide => {
            app.wide_mode = !app.wide_mode;
            app.flash = Some(crate::app::FlashMessage::info(
                if app.wide_mode { "Wide mode ON" } else { "Wide mode OFF" }
            ));
        }
        Action::ToggleFaultFilter => {
            app.fault_filter = !app.fault_filter;
            if app.fault_filter {
                app.nav.push(crate::app::nav::NavStep {
                    tab: app.nav.tab(),
                    filter: Some(crate::app::nav::NavFilter::Grep(
                        FAULT_FILTER_PATTERN.to_string(),
                    )),
                    dynamic_spec: None,
                    saved_selected: 0,
                });
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            } else {
                // Remove the fault filter grep specifically (not just the top step)
                app.nav.pop_grep_containing("crashloop");
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            }
        }
        Action::FlashInfo(msg) => {
            app.flash = Some(crate::app::FlashMessage::info(msg));
        }
    }
    ActionResult::None
}

async fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
    port_forward_task: &mut Option<JoinHandle<()>>,
) {
    use crate::app::Route;

    // Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name, log_task, port_forward_task).await;
        }
        return;
    }

    // Handle ContainerSelect: open logs or shell for the selected container.
    if let Route::ContainerSelect { ref pod, ref namespace } = app.route {
        let pod_name = pod.clone();
        let pod_ns = namespace.clone();
        let idx = app.container_select_index;
        let container_name = app.data.pods.items.iter()
            .find(|p| p.name == pod_name && p.namespace == pod_ns)
            .and_then(|p| p.containers.get(idx).map(|c| c.real_name.clone()))
            .unwrap_or_default();

        if app.shell_mode_container_select {
            // Shell mode: set pending shell info, the main loop will pick it up
            app.shell_mode_container_select = false;
            app.pending_shell = Some((pod_name, pod_ns, container_name));
            app.route = app.route_stack.pop().unwrap_or(Route::Resources);
            return;
        }

        app.push_route(app.route.clone());
        app.logs.clear();
        app.logs.follow = true;
        app.logs.wrap = false;
        app.logs.show_timestamps = true;
        app.logs.since = None; // Reset time range for new pod
        app.logs.streaming = true;
        app.route = Route::Logs {
            pod: pod_name.clone(),
            container: container_name.clone(),
            namespace: pod_ns.clone(),
        };

        // Cancel any previous log stream
        if let Some(handle) = log_task.take() {
            handle.abort();
        }

        app.logs.streaming = true;

        let tail = if app.logs.since.is_none() { Some(app.logs.tail_lines) } else { None };
        let _ = data_source.stream_logs(
            &pod_name,
            &pod_ns,
            &container_name,
            true,  // follow
            tail,
            app.logs.since.clone(),
            false, // not previous
        ).await;
        return;
    }

    // Handle resource tab-specific Enter behavior
    match app.resource_tab {
        ResourceTab::Pods => {
            // Enter on pods goes to describe (consistent with other resources).
            // Use `l` to view logs.
            handle_describe(app, data_source).await;
        }
        ResourceTab::Namespaces => {
            let ns_name = app
                .data
                .namespaces
                .selected_item()
                .map(|ns| ns.name.clone());
            if let Some(ns_name) = ns_name {
                do_switch_namespace(app, data_source, &ns_name, log_task).await;
            }
        }
        // Workload types: drill down to pods by label selector.
        ResourceTab::Deployments => {
            if let Some(dep) = app.data.deployments.selected_item() {
                let labels = dep.selector_labels.clone();
                let name = dep.name.clone();
                if !labels.is_empty() {
                    drill_to_pods_by_labels(app, data_source, labels, &format!("deploy/{}", name)).await;
                } else {
                    // Fallback: name prefix match (no selector_labels available)
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        ResourceTab::StatefulSets => {
            if let Some(sts) = app.data.statefulsets.selected_item() {
                let labels = sts.selector_labels.clone();
                let name = sts.name.clone();
                if !labels.is_empty() {
                    drill_to_pods_by_labels(app, data_source, labels, &format!("sts/{}", name)).await;
                } else {
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        ResourceTab::DaemonSets => {
            if let Some(ds) = app.data.daemonsets.selected_item() {
                let labels = ds.selector_labels.clone();
                let name = ds.name.clone();
                if !labels.is_empty() {
                    drill_to_pods_by_labels(app, data_source, labels, &format!("ds/{}", name)).await;
                } else {
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        ResourceTab::ReplicaSets => {
            if let Some(rs) = app.data.replicasets.selected_item() {
                let labels = rs.selector_labels.clone();
                let name = rs.name.clone();
                if !labels.is_empty() {
                    drill_to_pods_by_labels(app, data_source, labels, &format!("rs/{}", name)).await;
                } else {
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        ResourceTab::Jobs => {
            if let Some(job) = app.data.jobs.selected_item() {
                let labels = job.selector_labels.clone();
                let name = job.name.clone();
                if !labels.is_empty() {
                    drill_to_pods_by_labels(app, data_source, labels, &format!("job/{}", name)).await;
                } else {
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        ResourceTab::CronJobs => {
            let (_, name, _) = get_selected_resource_info(app);
            if !name.is_empty() {
                // CronJobs don't have selector_labels — use name prefix
                drill_to_pods_by_grep(app, data_source, &name).await;
            }
        }
        ResourceTab::Services => {
            if let Some(svc) = app.data.services.selected_item() {
                let selector = svc.selector.clone();
                let name = svc.name.clone();
                if !selector.is_empty() {
                    drill_to_pods_by_labels(app, data_source, selector, &format!("svc/{}", name)).await;
                } else {
                    drill_to_pods_by_grep(app, data_source, &name).await;
                }
            }
        }
        // Nodes: drill down to pods on that node.
        ResourceTab::Nodes => {
            let (_, name, _) = get_selected_resource_info(app);
            if !name.is_empty() {
                // Save selection and push drill-down step
                app.nav.save_selected(app.data.nodes.selected);
                app.nav.push(crate::app::nav::NavStep {
                    tab: ResourceTab::Pods,
                    filter: Some(crate::app::nav::NavFilter::Field {
                        field: "node".to_string(),
                        value: name.clone(),
                    }),
                    dynamic_spec: None,
                    saved_selected: 0,
                });
                app.resource_tab = ResourceTab::Pods;
                data_source.switch_tab(ResourceTab::Pods).await;
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info(format!("Pods on node: {}", name)));
            }
        }
        // CRDs: drill down to browse instances of the selected CRD.
        ResourceTab::Crds => {
            if let Some(crd) = app.data.crds.selected_item().cloned() {
                let spec = crate::app::nav::DynamicSpec {
                    group: crd.group.clone(),
                    version: crd.version.clone(),
                    kind: crd.kind.clone(),
                    plural: crd.plural.clone(),
                    scope: crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                };
                app.nav.save_selected(app.data.crds.selected);
                app.nav.push(crate::app::nav::NavStep {
                    tab: ResourceTab::DynamicResource,
                    filter: None,
                    dynamic_spec: Some(spec),
                    saved_selected: 0,
                });
                // These fields are read by get_selected_resource_info() for kubectl commands
                app.dynamic_resource_name = crd.kind.clone();
                app.dynamic_resource_api_resource = crd.name.clone();
                app.resource_tab = ResourceTab::DynamicResource;
                data_source.watch_dynamic(
                    crd.group.clone(),
                    crd.version.clone(),
                    crd.kind.clone(),
                    crd.plural.clone(),
                    crate::kube::protocol::ResourceScope::from_scope_str(&crd.scope),
                ).await;
                app.flash = Some(crate::app::FlashMessage::info(
                    format!("Browsing CRD: {}", crd.kind)
                ));
            }
        }
        _ => {
            // Config resources and everything else: describe
            handle_describe(app, data_source).await;
        }
    }
}

async fn handle_describe(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    let (resource, name, namespace) = get_selected_resource_info(app);
    if !name.is_empty() {
        // Check cache first
        if let Some(cached) = app.kubectl_cache.get(&resource, &name, &namespace, "describe") {
            let cached = cached.to_string();
            app.push_route(app.route.clone());
            app.describe.content = cached;
            app.describe.scroll = 0;
            app.describe.clear_search();
            app.route = Route::Describe {
                resource,
                name,
                namespace,
            };
            return;
        }

        app.push_route(app.route.clone());
        app.describe.content = format!("Loading describe for {}/{}...", resource, name);
        app.describe.scroll = 0;
        app.route = Route::Describe {
            resource: resource.clone(),
            name: name.clone(),
            namespace: namespace.clone(),
        };

        // Store pending key for cache population when result arrives
        app.pending_describe_key = Some((resource.clone(), name.clone(), namespace.clone()));

        let _ = data_source.describe(&resource, &name, &namespace).await;
    }
}

async fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    let (resource, name, namespace) = get_selected_resource_info(app);
    if !name.is_empty() {
        // Check cache first
        if let Some(cached) = app.kubectl_cache.get(&resource, &name, &namespace, "yaml") {
            let cached = cached.to_string();
            app.push_route(app.route.clone());
            app.yaml.content = cached;
            app.yaml.scroll = 0;
            app.yaml.clear_search();
            app.route = Route::Yaml {
                resource,
                name,
                namespace,
            };
            return;
        }

        app.push_route(app.route.clone());
        app.yaml.content = "Loading YAML...".to_string();
        app.yaml.scroll = 0;
        app.route = Route::Yaml {
            resource: resource.clone(),
            name: name.clone(),
            namespace: namespace.clone(),
        };

        // Store pending key for cache population when result arrives
        app.pending_yaml_key = Some((resource.clone(), name.clone(), namespace.clone()));

        let _ = data_source.yaml(&resource, &name, &namespace).await;
    }
}



async fn handle_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
) {
    use crate::app::Route;

    let (resource_type, name, namespace) = get_selected_resource_info(app);
    if name.is_empty() {
        return;
    }

    // For deployments, check if there are available pods before spawning kubectl
    if app.resource_tab == ResourceTab::Deployments {
        if let Some(dep) = app.data.deployments.selected_item() {
            if dep.available == 0 {
                app.flash = Some(crate::app::FlashMessage::warn(format!(
                    "No pods available for {}", dep.name
                )));
                return;
            }
        }
    }

    // For multi-container pods, open ContainerSelect instead of going straight to logs
    if app.resource_tab == ResourceTab::Pods {
        let container_count = app.data.pods.selected_item()
            .map(|p| p.containers.len())
            .unwrap_or(0);
        if container_count > 1 {
            app.push_route(app.route.clone());
            app.container_select_index = 0;
            app.route = crate::app::Route::ContainerSelect {
                pod: name.clone(),
                namespace: namespace.clone(),
            };
            return;
        }
    }

    app.push_route(app.route.clone());
    app.logs.clear();
    app.logs.follow = true;
    app.logs.wrap = false;
    app.logs.show_timestamps = true;
    app.logs.since = None; // Reset time range for new pod
    app.logs.streaming = true;

    // Build the kubectl target and container depending on the resource type.
    // For pods, target the pod directly with -c <container>.
    // For workloads (deployments, statefulsets, etc.), use "type/name" with --all-containers.
    let (log_target, route_pod, route_container) = match app.resource_tab {
        ResourceTab::Pods => {
            let container = app.data.pods.selected_item()
                .and_then(|p| p.containers.first().map(|c| c.real_name.clone()))
                .unwrap_or_default();
            (name.clone(), name.clone(), container)
        }
        _ => {
            let target = format!("{}/{}", resource_type, name);
            (target.clone(), target, "all".to_string())
        }
    };

    app.route = Route::Logs {
        pod: route_pod,
        container: route_container.clone(),
        namespace: namespace.clone(),
    };

    // Cancel any previous log stream
    if let Some(handle) = log_task.take() {
        handle.abort();
    }

    app.logs.streaming = true;

    let tail = if app.logs.since.is_none() { Some(app.logs.tail_lines) } else { None };

    let _ = data_source.stream_logs(
        &log_target,
        &namespace,
        &route_container,
        true,  // follow
        tail,
        app.logs.since.clone(),
        false, // not previous
    ).await;
}

async fn handle_previous_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
) {
    use crate::app::Route;

    let (resource_type, name, namespace) = get_selected_resource_info(app);
    if name.is_empty() {
        return;
    }

    app.push_route(app.route.clone());
    app.logs.clear();
    app.logs.follow = false; // previous logs are static, no follow
    app.logs.wrap = false;
    app.logs.show_timestamps = true;
    app.logs.streaming = true;

    // Build the kubectl target and container depending on the resource type.
    let (log_target, route_pod, route_container) = match app.resource_tab {
        ResourceTab::Pods => {
            let container = app.data.pods.selected_item()
                .and_then(|p| p.containers.first().map(|c| c.real_name.clone()))
                .unwrap_or_default();
            (name.clone(), name.clone(), container)
        }
        _ => {
            let target = format!("{}/{}", resource_type, name);
            (target.clone(), target, "all".to_string())
        }
    };

    app.route = Route::Logs {
        pod: route_pod,
        container: route_container.clone(),
        namespace: namespace.clone(),
    };

    // Cancel any previous log stream
    if let Some(handle) = log_task.take() {
        handle.abort();
    }

    app.logs.streaming = true;

    let _ = data_source.stream_logs(
        &log_target,
        &namespace,
        &route_container,
        false, // no follow for previous logs
        Some(app.logs.tail_lines), // always tail for previous logs
        None,  // no --since for previous logs
        true,  // --previous
    ).await;
}

/// Build a tab-separated text dump of the currently visible resource table.
fn build_table_dump(app: &App) -> String {
    use crate::kube::resources::KubeResource;

    macro_rules! dump_table {
        ($table:expr, $type:ty) => {{
            let headers = <$type>::headers();
            let mut lines = vec![headers.join("\t")];
            for &i in &$table.filtered_indices {
                if let Some(item) = $table.items.get(i) {
                    let row: Vec<String> = item.row().iter().map(|c| c.to_string()).collect();
                    lines.push(row.join("\t"));
                }
            }
            lines.join("\n")
        }};
    }

    match app.resource_tab {
        ResourceTab::Pods => dump_table!(app.data.pods, crate::kube::resources::pods::KubePod),
        ResourceTab::Deployments => dump_table!(app.data.deployments, crate::kube::resources::deployments::KubeDeployment),
        ResourceTab::Services => dump_table!(app.data.services, crate::kube::resources::services::KubeService),
        ResourceTab::Nodes => dump_table!(app.data.nodes, crate::kube::resources::nodes::KubeNode),
        ResourceTab::Namespaces => dump_table!(app.data.namespaces, crate::kube::resources::namespaces::KubeNamespace),
        ResourceTab::ConfigMaps => dump_table!(app.data.configmaps, crate::kube::resources::configmaps::KubeConfigMap),
        ResourceTab::Secrets => dump_table!(app.data.secrets, crate::kube::resources::secrets::KubeSecret),
        ResourceTab::StatefulSets => dump_table!(app.data.statefulsets, crate::kube::resources::statefulsets::KubeStatefulSet),
        ResourceTab::DaemonSets => dump_table!(app.data.daemonsets, crate::kube::resources::daemonsets::KubeDaemonSet),
        ResourceTab::Jobs => dump_table!(app.data.jobs, crate::kube::resources::jobs::KubeJob),
        ResourceTab::CronJobs => dump_table!(app.data.cronjobs, crate::kube::resources::cronjobs::KubeCronJob),
        ResourceTab::ReplicaSets => dump_table!(app.data.replicasets, crate::kube::resources::replicasets::KubeReplicaSet),
        ResourceTab::Ingresses => dump_table!(app.data.ingresses, crate::kube::resources::ingress::KubeIngress),
        ResourceTab::NetworkPolicies => dump_table!(app.data.network_policies, crate::kube::resources::networkpolicies::KubeNetworkPolicy),
        ResourceTab::ServiceAccounts => dump_table!(app.data.service_accounts, crate::kube::resources::serviceaccounts::KubeServiceAccount),
        ResourceTab::StorageClasses => dump_table!(app.data.storage_classes, crate::kube::resources::storageclasses::KubeStorageClass),
        ResourceTab::Pvs => dump_table!(app.data.pvs, crate::kube::resources::pvs::KubePv),
        ResourceTab::Pvcs => dump_table!(app.data.pvcs, crate::kube::resources::pvcs::KubePvc),
        ResourceTab::Events => dump_table!(app.data.events, crate::kube::resources::events::KubeEvent),
        ResourceTab::Roles => dump_table!(app.data.roles, crate::kube::resources::rbac::KubeRole),
        ResourceTab::ClusterRoles => dump_table!(app.data.cluster_roles, crate::kube::resources::rbac::KubeClusterRole),
        ResourceTab::RoleBindings => dump_table!(app.data.role_bindings, crate::kube::resources::rbac::KubeRoleBinding),
        ResourceTab::ClusterRoleBindings => dump_table!(app.data.cluster_role_bindings, crate::kube::resources::rbac::KubeClusterRoleBinding),
        ResourceTab::Hpa => dump_table!(app.data.hpa, crate::kube::resources::hpa::KubeHpa),
        ResourceTab::Endpoints => dump_table!(app.data.endpoints, crate::kube::resources::endpoints::KubeEndpoints),
        ResourceTab::LimitRanges => dump_table!(app.data.limit_ranges, crate::kube::resources::limitranges::KubeLimitRange),
        ResourceTab::ResourceQuotas => dump_table!(app.data.resource_quotas, crate::kube::resources::resourcequotas::KubeResourceQuota),
        ResourceTab::Pdb => dump_table!(app.data.pdb, crate::kube::resources::pdb::KubePdb),
        ResourceTab::Crds => dump_table!(app.data.crds, crate::kube::resources::crds::KubeCrd),
        ResourceTab::DynamicResource => dump_table!(app.data.dynamic_resources, crate::kube::resources::crds::DynamicKubeResource),
    }
}

fn get_selected_resource_info(app: &App) -> (String, String, String) {
    use crate::kube::resources::KubeResource;

    // Use the KubeResource trait methods (name(), namespace(), kind()) for
    // reliable extraction instead of positional row access.  This is correct
    // for every resource type regardless of its column layout.
    macro_rules! selected_info {
        ($table:expr, $kind:expr) => {
            if let Some(item) = $table.selected_item() {
                return (
                    $kind.to_string(),
                    item.name().to_string(),
                    item.namespace().to_string(),
                );
            }
        };
    }

    match app.resource_tab {
        ResourceTab::Pods => selected_info!(app.data.pods, "pod"),
        ResourceTab::Deployments => selected_info!(app.data.deployments, "deployment"),
        ResourceTab::Services => selected_info!(app.data.services, "service"),
        ResourceTab::Nodes => selected_info!(app.data.nodes, "node"),
        ResourceTab::Namespaces => selected_info!(app.data.namespaces, "namespace"),
        ResourceTab::ConfigMaps => selected_info!(app.data.configmaps, "configmap"),
        ResourceTab::Secrets => selected_info!(app.data.secrets, "secret"),
        ResourceTab::StatefulSets => selected_info!(app.data.statefulsets, "statefulset"),
        ResourceTab::DaemonSets => selected_info!(app.data.daemonsets, "daemonset"),
        ResourceTab::Jobs => selected_info!(app.data.jobs, "job"),
        ResourceTab::CronJobs => selected_info!(app.data.cronjobs, "cronjob"),
        ResourceTab::ReplicaSets => selected_info!(app.data.replicasets, "replicaset"),
        ResourceTab::Ingresses => selected_info!(app.data.ingresses, "ingress"),
        ResourceTab::Events => selected_info!(app.data.events, "event"),
        ResourceTab::Pvs => selected_info!(app.data.pvs, "pv"),
        ResourceTab::Pvcs => selected_info!(app.data.pvcs, "pvc"),
        ResourceTab::StorageClasses => selected_info!(app.data.storage_classes, "storageclass"),
        ResourceTab::NetworkPolicies => selected_info!(app.data.network_policies, "networkpolicy"),
        ResourceTab::ServiceAccounts => selected_info!(app.data.service_accounts, "serviceaccount"),
        ResourceTab::Roles => selected_info!(app.data.roles, "role"),
        ResourceTab::ClusterRoles => selected_info!(app.data.cluster_roles, "clusterrole"),
        ResourceTab::RoleBindings => selected_info!(app.data.role_bindings, "rolebinding"),
        ResourceTab::ClusterRoleBindings => selected_info!(app.data.cluster_role_bindings, "clusterrolebinding"),
        ResourceTab::Hpa => selected_info!(app.data.hpa, "hpa"),
        ResourceTab::Endpoints => selected_info!(app.data.endpoints, "endpoints"),
        ResourceTab::LimitRanges => selected_info!(app.data.limit_ranges, "limitrange"),
        ResourceTab::ResourceQuotas => selected_info!(app.data.resource_quotas, "resourcequota"),
        ResourceTab::Pdb => selected_info!(app.data.pdb, "poddisruptionbudget"),
        ResourceTab::Crds => selected_info!(app.data.crds, "customresourcedefinition"),
        ResourceTab::DynamicResource => {
            if let Some(item) = app.data.dynamic_resources.selected_item() {
                // Use the full CRD API resource name (e.g. "clickhouseinstallations.clickhouse.altinity.com")
                // so kubectl describe/get/delete commands work correctly.
                let resource_type = if app.dynamic_resource_api_resource.is_empty() {
                    app.dynamic_resource_name.to_lowercase()
                } else {
                    app.dynamic_resource_api_resource.clone()
                };
                return (
                    resource_type,
                    item.name().to_string(),
                    item.namespace().to_string(),
                );
            }
        }
    }
    (String::new(), String::new(), String::new())
}

/// Get resource info for all marked items in the active table.
/// Returns a Vec of (resource_type, name, namespace) tuples.
/// Returns empty Vec if no items are marked.
fn get_marked_resource_infos(app: &App) -> Vec<(String, String, String)> {
    use crate::kube::resources::KubeResource;

    macro_rules! marked_infos {
        ($table:expr, $kind:expr) => {{
            let mut result = Vec::new();
            for &idx in &$table.marked {
                if let Some(item) = $table.items.get(idx) {
                    result.push((
                        $kind.to_string(),
                        item.name().to_string(),
                        item.namespace().to_string(),
                    ));
                }
            }
            return result;
        }};
    }

    match app.resource_tab {
        ResourceTab::Pods => marked_infos!(app.data.pods, "pod"),
        ResourceTab::Deployments => marked_infos!(app.data.deployments, "deployment"),
        ResourceTab::Services => marked_infos!(app.data.services, "service"),
        ResourceTab::Nodes => marked_infos!(app.data.nodes, "node"),
        ResourceTab::Namespaces => marked_infos!(app.data.namespaces, "namespace"),
        ResourceTab::ConfigMaps => marked_infos!(app.data.configmaps, "configmap"),
        ResourceTab::Secrets => marked_infos!(app.data.secrets, "secret"),
        ResourceTab::StatefulSets => marked_infos!(app.data.statefulsets, "statefulset"),
        ResourceTab::DaemonSets => marked_infos!(app.data.daemonsets, "daemonset"),
        ResourceTab::Jobs => marked_infos!(app.data.jobs, "job"),
        ResourceTab::CronJobs => marked_infos!(app.data.cronjobs, "cronjob"),
        ResourceTab::ReplicaSets => marked_infos!(app.data.replicasets, "replicaset"),
        ResourceTab::Ingresses => marked_infos!(app.data.ingresses, "ingress"),
        ResourceTab::Events => marked_infos!(app.data.events, "event"),
        ResourceTab::Pvs => marked_infos!(app.data.pvs, "pv"),
        ResourceTab::Pvcs => marked_infos!(app.data.pvcs, "pvc"),
        ResourceTab::StorageClasses => marked_infos!(app.data.storage_classes, "storageclass"),
        ResourceTab::NetworkPolicies => marked_infos!(app.data.network_policies, "networkpolicy"),
        ResourceTab::ServiceAccounts => marked_infos!(app.data.service_accounts, "serviceaccount"),
        ResourceTab::Roles => marked_infos!(app.data.roles, "role"),
        ResourceTab::ClusterRoles => marked_infos!(app.data.cluster_roles, "clusterrole"),
        ResourceTab::RoleBindings => marked_infos!(app.data.role_bindings, "rolebinding"),
        ResourceTab::ClusterRoleBindings => marked_infos!(app.data.cluster_role_bindings, "clusterrolebinding"),
        ResourceTab::Hpa => marked_infos!(app.data.hpa, "hpa"),
        ResourceTab::Endpoints => marked_infos!(app.data.endpoints, "endpoints"),
        ResourceTab::LimitRanges => marked_infos!(app.data.limit_ranges, "limitrange"),
        ResourceTab::ResourceQuotas => marked_infos!(app.data.resource_quotas, "resourcequota"),
        ResourceTab::Pdb => marked_infos!(app.data.pdb, "poddisruptionbudget"),
        ResourceTab::Crds => marked_infos!(app.data.crds, "customresourcedefinition"),
        ResourceTab::DynamicResource => {
            let mut result = Vec::new();
            let kind = if app.dynamic_resource_api_resource.is_empty() {
                app.dynamic_resource_name.to_lowercase()
            } else {
                app.dynamic_resource_api_resource.clone()
            };
            for &idx in &app.data.dynamic_resources.marked {
                if let Some(item) = app.data.dynamic_resources.items.get(idx) {
                    result.push((
                        kind.clone(),
                        item.name().to_string(),
                        item.namespace().to_string(),
                    ));
                }
            }
            result
        }
    }
}

#[derive(Default)]
struct CacheUpdateFlags {
    namespaces: bool,
    crds: bool,
}

impl CacheUpdateFlags {
    fn merge(&mut self, other: CacheUpdateFlags) {
        self.namespaces |= other.namespaces;
        self.crds |= other.crds;
    }
}

/// Handle a single AppEvent (resource update, error, or flash).
fn apply_event(
    app: &mut App,
    event: AppEvent,
) -> CacheUpdateFlags {
    match event {
        AppEvent::ResourceUpdate(update) => apply_resource_update(app, update),
        AppEvent::Flash(flash) => {
            app.flash = Some(flash);
            CacheUpdateFlags::default()
        }
        // ContextSwitchResult is handled in the main event loop before apply_event
        AppEvent::ContextSwitchResult { .. } => CacheUpdateFlags::default(),
        AppEvent::PodMetrics(metrics) => {
            app.pod_metrics = metrics;
            app.apply_pod_metrics();
            CacheUpdateFlags::default()
        }
        AppEvent::NodeMetrics(metrics) => {
            app.node_metrics = metrics;
            app.apply_node_metrics();
            CacheUpdateFlags::default()
        }
        AppEvent::LogStreamEnded => {
            app.logs.streaming = false;
            CacheUpdateFlags::default()
        }
        AppEvent::DaemonDisconnected => {
            app.should_quit = true;
            CacheUpdateFlags::default()
        }
    }
}

fn apply_resource_update(
    app: &mut App,
    update: ResourceUpdate,
) -> CacheUpdateFlags {
    let mut flags = CacheUpdateFlags::default();
    match update {
        ResourceUpdate::Pods(items) => {
            app.data.pods.set_items_filtered(items);
            app.apply_pod_metrics();
        }
        ResourceUpdate::Deployments(items) => {
            app.data.deployments.set_items_filtered(items);
        }
        ResourceUpdate::Services(items) => {
            app.data.services.set_items_filtered(items);
        }
        ResourceUpdate::Nodes(items) => {
            app.data.nodes.set_items_filtered(items);
            app.apply_node_metrics();
        }
        ResourceUpdate::Namespaces(items) => {
            app.data.namespaces.set_items_filtered(items);
            flags.namespaces = true;
        }
        ResourceUpdate::ConfigMaps(items) => app.data.configmaps.set_items_filtered(items),
        ResourceUpdate::Secrets(items) => app.data.secrets.set_items_filtered(items),
        ResourceUpdate::StatefulSets(items) => {
            app.data.statefulsets.set_items_filtered(items);
        }
        ResourceUpdate::DaemonSets(items) => {
            app.data.daemonsets.set_items_filtered(items);
        }
        ResourceUpdate::Jobs(items) => {
            app.data.jobs.set_items_filtered(items);
        }
        ResourceUpdate::CronJobs(items) => {
            app.data.cronjobs.set_items_filtered(items);
        }
        ResourceUpdate::ReplicaSets(items) => app.data.replicasets.set_items_filtered(items),
        ResourceUpdate::Ingresses(items) => app.data.ingresses.set_items_filtered(items),
        ResourceUpdate::NetworkPolicies(items) => app.data.network_policies.set_items_filtered(items),
        ResourceUpdate::ServiceAccounts(items) => app.data.service_accounts.set_items_filtered(items),
        ResourceUpdate::StorageClasses(items) => app.data.storage_classes.set_items_filtered(items),
        ResourceUpdate::Pvs(items) => app.data.pvs.set_items_filtered(items),
        ResourceUpdate::Pvcs(items) => app.data.pvcs.set_items_filtered(items),
        ResourceUpdate::Events(items) => app.data.events.set_items_filtered(items),
        ResourceUpdate::Roles(items) => app.data.roles.set_items_filtered(items),
        ResourceUpdate::ClusterRoles(items) => app.data.cluster_roles.set_items_filtered(items),
        ResourceUpdate::RoleBindings(items) => app.data.role_bindings.set_items_filtered(items),
        ResourceUpdate::ClusterRoleBindings(items) => {
            app.data.cluster_role_bindings.set_items_filtered(items)
        }
        ResourceUpdate::Hpa(items) => app.data.hpa.set_items_filtered(items),
        ResourceUpdate::Endpoints(items) => app.data.endpoints.set_items_filtered(items),
        ResourceUpdate::LimitRanges(items) => app.data.limit_ranges.set_items_filtered(items),
        ResourceUpdate::ResourceQuotas(items) => app.data.resource_quotas.set_items_filtered(items),
        ResourceUpdate::Pdb(items) => app.data.pdb.set_items_filtered(items),
        ResourceUpdate::Crds(items) => {
            // Clone for the table; move the original into discovered_crds
            // to avoid cloning the larger Vec twice.
            let for_table = items.clone();
            app.discovered_crds = items;
            app.data.crds.set_items_filtered(for_table);
            flags.crds = true;
        }
        ResourceUpdate::DynamicResources(items) => app.data.dynamic_resources.set_items_filtered(items),
        ResourceUpdate::Yaml(content) => {
            // Only apply if we're still on a Yaml route — prevents stale
            // daemon-mode results from overwriting newer content.
            if matches!(app.route, crate::app::Route::Yaml { .. }) {
                if let Some((r, n, ns)) = app.pending_yaml_key.take() {
                    app.kubectl_cache.insert(r, n, ns, "yaml", content.clone());
                }
                app.yaml.content = content;
            }
        }
        ResourceUpdate::Describe(content) => {
            // Only apply if we're still on a Describe route — prevents stale
            // daemon-mode results from overwriting newer content.
            if matches!(app.route, crate::app::Route::Describe { .. }) {
                if let Some((r, n, ns)) = app.pending_describe_key.take() {
                    app.kubectl_cache.insert(r, n, ns, "describe", content.clone());
                }
                app.describe.content = content;
            }
        }
        ResourceUpdate::LogLine(line) => app.logs.push(line),
    }
    // Reapply nav stack filters after every data update so that drill-down
    // and grep filters stay active as fresh snapshots arrive.
    if app.nav.is_drilled() || app.filter_input.active {
        app.reapply_nav_filters();
    }
    flags
}

// ---------------------------------------------------------------------------
// Nav drill-down helpers
// ---------------------------------------------------------------------------

/// Drill down to pods filtered by label selector (deployment→pods, service→pods, etc.)
async fn drill_to_pods_by_labels(
    app: &mut App,
    data_source: &mut ClientSession,
    labels: std::collections::BTreeMap<String, String>,
    description: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};
    app.nav.save_selected(app.active_table_selected());
    app.nav.push(NavStep {
        tab: ResourceTab::Pods,
        filter: Some(NavFilter::Labels(labels)),
        dynamic_spec: None,
        saved_selected: 0,
    });
    app.resource_tab = ResourceTab::Pods;
    data_source.switch_tab(ResourceTab::Pods).await;
    app.reapply_nav_filters();
    app.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", description)));
}

/// Drill down to pods filtered by name prefix (fallback when no selector_labels).
async fn drill_to_pods_by_grep(
    app: &mut App,
    data_source: &mut ClientSession,
    name: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};
    let filter = format!("{}-", regex::escape(name));
    app.nav.save_selected(app.active_table_selected());
    app.nav.push(NavStep {
        tab: ResourceTab::Pods,
        filter: Some(NavFilter::Grep(filter)),
        dynamic_spec: None,
        saved_selected: 0,
    });
    app.resource_tab = ResourceTab::Pods;
    data_source.switch_tab(ResourceTab::Pods).await;
    app.reapply_nav_filters();
    app.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
}

/// Begin a context switch. Immediately clears UI state and shows a flash message,
/// then delegates the slow client creation to `DataSource`. In local mode the
/// result arrives via `AppEvent::ContextSwitchResult`; in daemon mode it comes
/// through the event stream.
async fn begin_context_switch(
    app: &mut App,
    data_source: &mut ClientSession,
    ctx_name: &str,
    log_task: &mut Option<JoinHandle<()>>,
    port_forward_task: &mut Option<JoinHandle<()>>,
) {
    // Cancel any active log stream — it belongs to the old context
    if let Some(handle) = log_task.take() {
        handle.abort();
        app.logs.streaming = false;
    }
    // Cancel any active port-forward — it belongs to the old context
    if let Some(handle) = port_forward_task.take() {
        handle.abort();
    }
    // Immediate: clear data and show feedback — keeps TUI responsive.
    // Look up cluster/user from the in-memory contexts list (no disk I/O).
    app.context = ctx_name.to_string();
    app.selected_ns = "all".to_string();
    if let Some(ctx) = app.data.contexts.items.iter().find(|c| c.name == ctx_name) {
        app.cluster = ctx.cluster.clone();
        app.user = ctx.user.clone();
    } else {
        app.cluster = String::new();
        app.user = String::new();
    }
    app.clear_data();
    // Reset nav to the root tab (not the drilled-into tab)
    let root = app.nav.root_tab();
    app.resource_tab = root;
    app.nav.reset(root);
    app.filter_input = Default::default();
    app.kubectl_cache.clear();
    app.discovered_crds.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    app.logs.clear();
    app.yaml = Default::default();
    app.describe = Default::default();
    app.confirm_dialog = None;
    app.pending_batch_delete.clear();
    app.pending_batch_restart.clear();
    app.pending_batch_force_kill.clear();
    app.pending_describe_key = None;
    app.pending_yaml_key = None;
    app.scale_mode = false;
    app.scale_target = (String::new(), String::new(), String::new());
    app.port_forward_mode = false;
    app.port_forward_target = (String::new(), String::new());
    app.last_resource_tab = None;
    app.dynamic_resource_name.clear();
    app.dynamic_resource_api_resource.clear();
    app.pod_metrics.clear();
    app.node_metrics.clear();
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...",
        ctx_name
    )));

    // Delegate background client creation to the session.
    let _ = data_source.switch_context(ctx_name).await;
}

/// Apply the result of a background context switch.
async fn apply_context_switch_result(
    app: &mut App,
    data_source: &mut ClientSession,
    ctx_name: &str,
    result: Result<(), String>,
) {
    // Discard stale result if user already switched to a different context
    if ctx_name != app.context {
        return;
    }
    match result {
        Ok(()) => {
            // cluster/user already set in begin_context_switch — no need to re-read kubeconfig.
            data_source.set_context_info(ctx_name);
            // Update is_current marker on existing contexts list (no disk read).
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
            app.flash = Some(crate::app::FlashMessage::info(format!(
                "Switched to context: {}",
                ctx_name
            )));

            // Pre-populate from disk cache for instant autocomplete
            if let Some(cache) = crate::kube::cache::load_cache(&app.context) {
                if !cache.namespaces.is_empty() {
                    let ns_items = crate::kube::cache::cached_namespaces_to_domain(&cache.namespaces);
                    app.data.namespaces.set_items(ns_items);
                }
                if !cache.crds.is_empty() {
                    app.discovered_crds = crate::kube::cache::cached_crds_to_domain(&cache.crds);
                }
            }
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
            app.flash = Some(crate::app::FlashMessage::error(format!(
                "Context switch failed: {}",
                e
            )));
        }
    }
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
async fn do_switch_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: &str,
    log_task: &mut Option<JoinHandle<()>>,
) {
    // Cancel any active log stream — it belongs to the old namespace
    if let Some(handle) = log_task.take() {
        handle.abort();
        app.logs.streaming = false;
    }
    app.selected_ns = ns.to_string();
    app.clear_data();
    // Reset nav to the root tab (not the drilled-into tab)
    let root = app.nav.root_tab();
    app.resource_tab = root;
    app.nav.reset(root);
    app.filter_input = Default::default();
    app.kubectl_cache.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    app.logs.clear();
    app.confirm_dialog = None;
    app.pending_batch_delete.clear();
    app.pending_batch_restart.clear();
    app.pending_batch_force_kill.clear();
    app.pending_describe_key = None;
    app.pending_yaml_key = None;
    app.scale_mode = false;
    app.scale_target = (String::new(), String::new(), String::new());
    app.port_forward_mode = false;
    app.port_forward_target = (String::new(), String::new());
    app.pod_metrics.clear();
    app.node_metrics.clear();
    app.dynamic_resource_name.clear();
    app.dynamic_resource_api_resource.clear();
    let _ = data_source.switch_namespace(ns).await;
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns
    )));
}

// ---------------------------------------------------------------------------
// Interactive command helpers (shell / edit)
// ---------------------------------------------------------------------------

/// Build `kubectl exec -it` args for shelling into a container.
fn build_shell_args(pod: &str, namespace: &str, container: &str, context: &str) -> Vec<String> {
    let mut args = vec![
        "exec".to_string(),
        "-it".to_string(),
        pod.to_string(),
        "-n".to_string(),
        namespace.to_string(),
        "-c".to_string(),
        container.to_string(),
    ];
    if !context.is_empty() {
        args.push("--context".to_string());
        args.push(context.to_string());
    }
    args.push("--".to_string());
    args.push("/bin/bash".to_string());
    args
}

/// Build `kubectl edit` args for editing a resource.
fn build_edit_args(resource: &str, name: &str, namespace: &str, context: &str) -> Vec<String> {
    let mut args = vec![
        "edit".to_string(),
        resource.to_string(),
        name.to_string(),
    ];
    if !namespace.is_empty() {
        args.push("-n".to_string());
        args.push(namespace.to_string());
    }
    if !context.is_empty() {
        args.push("--context".to_string());
        args.push(context.to_string());
    }
    args
}

/// Suspend the TUI and run an interactive command directly (with bash->sh fallback for shell).
async fn run_interactive_local(
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    app: &mut App,
    command: &str,
    args: &[String],
) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    let is_shell = args.first().map_or(false, |a| a == "exec");

    if is_shell {
        // Show connecting message for shell
        let pod = args.get(2).map(|s| s.as_str()).unwrap_or("?");
        let container = args.iter()
            .position(|a| a == "-c")
            .and_then(|i| args.get(i + 1))
            .map(|s| s.as_str())
            .unwrap_or("?");
        print!("\x1b[2J\x1b[H"); // clear screen + move cursor to top
        println!("Connecting to {}/{}...\n", pod, container);
    }

    // Run the command
    let mut cmd = std::process::Command::new(command);
    cmd.args(args);
    let status = cmd.status();

    // For shell commands: if bash failed, try sh
    if is_shell {
        if status.is_err() || status.as_ref().map_or(false, |s| !s.success()) {
            // Replace /bin/bash with /bin/sh in args
            let sh_args: Vec<String> = args
                .iter()
                .map(|a| {
                    if a == "/bin/bash" {
                        "/bin/sh".to_string()
                    } else {
                        a.clone()
                    }
                })
                .collect();
            let mut cmd2 = std::process::Command::new(command);
            cmd2.args(&sh_args);
            let sh_status = cmd2.status();
            if sh_status.is_err()
                || sh_status.as_ref().map_or(false, |s| !s.success())
            {
                app.flash = Some(crate::app::FlashMessage::error(
                    "Shell failed — no bash or sh available".to_string(),
                ));
            }
        }
    }

    // Resume TUI
    enable_raw_mode()?;
    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
    terminal.clear()?;

    Ok(())
}

/// Parse commands like "deploy kube-system" → (Deployments, "kube-system")
/// Parse commands like "deploy /nginx" → (Deployments, "nginx")
fn parse_resource_filter_command(cmd: &str) -> Option<(ResourceTab, String)> {
    let (resource_part, filter_part) = if let Some(slash_pos) = cmd.find('/') {
        let r = cmd[..slash_pos].trim();
        let f = cmd[slash_pos + 1..].trim();
        (r, f)
    } else {
        return None;
    };
    if filter_part.is_empty() {
        return None;
    }
    let tab = parse_resource_command(resource_part)?;
    Some((tab, filter_part.to_string()))
}

/// Parse commands like "deploy kube-system" → (Deployments, "kube-system")
/// Returns None for cluster-scoped resources (nodes, pvs, storageclasses, clusterroles, etc.)
/// since they don't belong to a namespace.
/// Parse a CRD command with optional namespace: "clickhouseinstallation prod"
fn parse_crd_ns_command(cmd: &str, app: &App) -> Option<(crate::kube::resources::crds::KubeCrd, String)> {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let crd_part = parts[0].trim();
    let ns = parts[1].trim();
    if ns.is_empty() {
        return None;
    }
    let crd = app.find_crd_by_name(crd_part)?;
    // Cluster-scoped CRDs don't take namespace
    if crd.scope == "Cluster" {
        return None;
    }
    Some((crd, ns.to_string()))
}

fn parse_resource_ns_command(cmd: &str) -> Option<(ResourceTab, String)> {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let resource = parts[0].trim();
    let ns = parts[1].trim();
    if ns.is_empty() {
        return None;
    }
    let tab = parse_resource_command(resource)?;
    // Cluster-scoped resources don't accept a namespace argument
    match tab {
        ResourceTab::Nodes
        | ResourceTab::Namespaces
        | ResourceTab::Pvs
        | ResourceTab::StorageClasses
        | ResourceTab::ClusterRoles
        | ResourceTab::ClusterRoleBindings => None,
        _ => Some((tab, ns.to_string())),
    }
}

pub(crate) fn parse_resource_command(cmd: &str) -> Option<ResourceTab> {
    use crate::kube::resource_types::find_by_alias;

    let meta = find_by_alias(cmd)?;

    // Map canonical resource name to ResourceTab variant
    match meta.name {
        "pod" => Some(ResourceTab::Pods),
        "deployment" => Some(ResourceTab::Deployments),
        "service" => Some(ResourceTab::Services),
        "node" => Some(ResourceTab::Nodes),
        "namespace" => Some(ResourceTab::Namespaces),
        "configmap" => Some(ResourceTab::ConfigMaps),
        "secret" => Some(ResourceTab::Secrets),
        "statefulset" => Some(ResourceTab::StatefulSets),
        "daemonset" => Some(ResourceTab::DaemonSets),
        "job" => Some(ResourceTab::Jobs),
        "cronjob" => Some(ResourceTab::CronJobs),
        "replicaset" => Some(ResourceTab::ReplicaSets),
        "ingress" => Some(ResourceTab::Ingresses),
        "event" => Some(ResourceTab::Events),
        "pv" => Some(ResourceTab::Pvs),
        "pvc" => Some(ResourceTab::Pvcs),
        "storageclass" => Some(ResourceTab::StorageClasses),
        "networkpolicy" => Some(ResourceTab::NetworkPolicies),
        "serviceaccount" => Some(ResourceTab::ServiceAccounts),
        "role" => Some(ResourceTab::Roles),
        "clusterrole" => Some(ResourceTab::ClusterRoles),
        "rolebinding" => Some(ResourceTab::RoleBindings),
        "clusterrolebinding" => Some(ResourceTab::ClusterRoleBindings),
        "hpa" => Some(ResourceTab::Hpa),
        "endpoints" => Some(ResourceTab::Endpoints),
        "limitrange" => Some(ResourceTab::LimitRanges),
        "resourcequota" => Some(ResourceTab::ResourceQuotas),
        "poddisruptionbudget" => Some(ResourceTab::Pdb),
        "customresourcedefinition" => Some(ResourceTab::Crds),
        _ => None,
    }
}
