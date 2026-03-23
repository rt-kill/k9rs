pub mod app;
pub mod event;
pub mod kube;
pub mod ui;
pub mod util;

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{Event as CtEvent, EventStream, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use ratatui::prelude::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

use crate::app::{App, ResourceTab};
use crate::event::{AppEvent, ResourceUpdate};

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

#[derive(Parser, Debug)]
#[command(name = "k9rs", version, about = "A fast Kubernetes TUI")]
struct Cli {
    /// Kubernetes context to use
    #[arg(long)]
    context: Option<String>,

    /// Namespace to select on startup
    #[arg(short, long)]
    namespace: Option<String>,

    /// UI tick rate in milliseconds
    #[arg(long, default_value = "250")]
    tick_rate: u64,

    /// Log file path
    #[arg(long)]
    log_file: Option<String>,

    /// Start on a specific resource view
    #[arg(short, long)]
    command: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if let Some(ref log_file) = cli.log_file {
        let file = std::fs::File::create(log_file)?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file)
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::sink)
            .init();
    }

    // Initialize kubernetes client with a graceful error message
    let kube_client = match crate::kube::KubeClient::new(cli.context.as_deref()).await {
        Ok(client) => client,
        Err(e) => {
            eprintln!(
                "Error: Unable to connect to Kubernetes cluster.\n\n\
                 {}\n\n\
                 Make sure:\n  \
                 - Your kubeconfig is valid (~/.kube/config)\n  \
                 - The cluster is reachable\n  \
                 - Your credentials haven't expired\n\n\
                 Tip: Run 'kubectl cluster-info' to verify connectivity.",
                e
            );
            std::process::exit(1);
        }
    };
    let contexts = crate::kube::KubeClient::list_contexts().await.unwrap_or_default();

    // Initialize app state
    let mut app = App::new(
        kube_client.context().to_string(),
        contexts.clone(),
        cli.namespace.unwrap_or_else(|| "all".to_string()),
    );

    // Populate contexts table with actual kubeconfig contexts
    let kube_contexts: Vec<crate::app::KubeContext> = contexts.iter().map(|name| {
        crate::app::KubeContext {
            name: name.clone(),
            cluster: String::new(), // could be enriched later
            is_current: name == kube_client.context(),
        }
    }).collect();
    app.data.contexts.set_items(kube_contexts);

    // Parse initial command/resource
    if let Some(ref cmd) = cli.command {
        if let Some(tab) = parse_resource_command(cmd) {
            app.resource_tab = tab;
        }
    }

    // Create event channel
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(500);

    // Spawn resource watcher with a forwarding channel
    let (resource_tx, mut resource_rx) = mpsc::channel::<ResourceUpdate>(500);
    let forward_tx = event_tx.clone();
    tokio::spawn(async move {
        while let Some(update) = resource_rx.recv().await {
            if forward_tx.send(AppEvent::ResourceUpdate(update)).await.is_err() {
                break;
            }
        }
    });
    let watcher_client = kube_client.client().clone();
    let watcher_ns = app.selected_ns.clone();
    let initial_tab = app.resource_tab;
    let mut resource_watcher =
        crate::kube::watcher::ResourceWatcher::start(watcher_client, resource_tx, watcher_ns, initial_tab)
            .await;

    // Keep kube_client around for context switching; clone the Client for API calls
    let mut kube_client = kube_client;

    // Discover CRDs in the background for command completion
    {
        let crd_client = kube_client.client().clone();
        let crd_tx = event_tx.clone();
        tokio::spawn(async move {
            use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
            let api: ::kube::Api<CustomResourceDefinition> = ::kube::Api::all(crd_client);
            if let Ok(list) = api.list(&Default::default()).await {
                let crds: Vec<crate::kube::resources::crds::KubeCrd> = list.items.into_iter().map(|crd| {
                    let meta = crd.metadata;
                    let spec = crd.spec;
                    let name = meta.name.unwrap_or_default();
                    let group = spec.group;
                    let version = spec.versions.first().map(|v| v.name.clone()).unwrap_or_default();
                    let kind = spec.names.kind;
                    let scope = format!("{:?}", spec.scope).trim_matches('"').to_string();
                    let age = meta.creation_timestamp.map(|ts| ts.0);
                    crate::kube::resources::crds::KubeCrd { name, group, version, kind, scope, age }
                }).collect();
                let _ = crd_tx.send(AppEvent::ResourceUpdate(
                    crate::event::ResourceUpdate::Crds(crds)
                )).await;
            }
        });
    }

    // Track active log streaming task so we can cancel on Back
    let mut log_task: Option<JoinHandle<()>> = None;

    // Generation counters to prevent stale describe/yaml results from overwriting newer ones
    let describe_generation = Arc::new(AtomicU64::new(0));
    let yaml_generation = Arc::new(AtomicU64::new(0));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Async event stream from crossterm (replaces blocking poll/read thread)
    let mut event_stream = EventStream::new();
    let mut tick_interval = tokio::time::interval(Duration::from_millis(cli.tick_rate));

    // Main event loop
    loop {
        // Draw
        terminal.draw(|f| {
            crate::ui::draw(f, &mut app);
        })?;

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(Ok(ct_event)) = event_stream.next() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Fix #2: Capture input in command mode before normal dispatch
                        if app.command_mode {
                            match key.code {
                                KeyCode::Esc => {
                                    app.command_mode = false;
                                    app.command_input.clear();
                                }
                                KeyCode::Enter => {
                                    let cmd = app.command_input.trim().to_lowercase();
                                    app.command_mode = false;
                                    // Push to command history before clearing
                                    if !cmd.is_empty() {
                                        app.command_history.push(cmd.clone());
                                        if app.command_history.len() > 50 {
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
                                            &mut resource_watcher, &mut kube_client,
                                            &mut log_task, &describe_generation, &yaml_generation,
                                        ).await;
                                    } else if cmd == "ctx" || cmd == "context" || cmd == "contexts" {
                                        app.push_route(app.route.clone());
                                        app.route = crate::app::Route::Contexts;
                                    } else if cmd.starts_with("ctx ") || cmd.starts_with("context ") {
                                        let prefix_len = if cmd.starts_with("ctx ") { 4 } else { 8 };
                                        let ctx_name = cmd[prefix_len..].trim().to_string();
                                        do_switch_context(&mut app, &mut kube_client, &mut resource_watcher, &ctx_name).await;
                                    } else if cmd.starts_with("ns ") || cmd.starts_with("namespace ") {
                                        let prefix_len = if cmd.starts_with("ns ") { 3 } else { 10 };
                                        let ns = cmd[prefix_len..].trim().to_string();
                                        do_switch_namespace(&mut app, &mut resource_watcher, &ns).await;
                                    } else if let Some(tab) = parse_resource_command(&cmd) {
                                        // If we were in a drill-down (e.g. Deploy→Pods with
                                        // a filter), clear the stale filter on the pods table
                                        // and reset drill-down state so ClearFilter won't
                                        // unexpectedly switch tabs later.
                                        if let Some(last) = app.last_resource_tab.take() {
                                            if app.resource_tab == ResourceTab::Pods && last != ResourceTab::Pods {
                                                app.data.pods.clear_filter();
                                                app.filter.text.clear();
                                            }
                                        }
                                        app.last_resource_tab = Some(app.resource_tab);
                                        app.resource_tab = tab;
                                        resource_watcher.switch_tab(tab).await;
                                    } else if let Some((tab, ns)) = parse_resource_ns_command(&cmd) {
                                        // :deploy kube-system — switch resource AND namespace
                                        app.last_resource_tab = Some(app.resource_tab);
                                        app.resource_tab = tab;
                                        if ns != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut resource_watcher, &ns).await;
                                        }
                                        resource_watcher.switch_tab(tab).await;
                                        app.flash = Some(crate::app::FlashMessage::info(format!(
                                            "{}({})", tab.label(), ns
                                        )));
                                    } else if let Some((tab, filter)) = parse_resource_filter_command(&cmd) {
                                        // :deploy /nginx — switch resource AND apply filter
                                        app.resource_tab = tab;
                                        resource_watcher.switch_tab(tab).await;
                                        app.filter.text = filter.clone();
                                        app.filter.active = false;
                                        app.apply_filter(&filter);
                                    } else if let Some(crd) = app.find_crd_by_name(&cmd) {
                                        // Dynamic CRD instance browsing
                                        app.dynamic_resource_name = crd.kind.clone();
                                        app.data.dynamic_resources.clear_data();
                                        app.resource_tab = ResourceTab::DynamicResource;
                                        resource_watcher.watch_dynamic(
                                            crd.group.clone(),
                                            crd.version.clone(),
                                            crd.kind.clone(),
                                            crd.scope.clone(),
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
                                        let context = app.context.clone();
                                        let tx = event_tx.clone();
                                        tokio::spawn(async move {
                                            let mut cmd = tokio::process::Command::new("kubectl");
                                            cmd.arg("scale")
                                                .arg(format!("{}/{}", resource, name))
                                                .arg(format!("--replicas={}", replicas));
                                            if !namespace.is_empty() { cmd.arg("-n").arg(&namespace); }
                                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                            match cmd.output().await {
                                                Ok(output) if output.status.success() => {
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                                        format!("Scaled {}/{} to {} replicas", resource, name, replicas)
                                                    ))).await;
                                                }
                                                Ok(output) => {
                                                    let err = String::from_utf8_lossy(&output.stderr);
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                                        format!("Scale failed: {}", err)
                                                    ))).await;
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                                        format!("Failed to run kubectl: {}", e)
                                                    ))).await;
                                                }
                                            }
                                        });
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
                            continue;
                        }

                        // Fix #3: Capture input in filter mode before normal dispatch
                        if app.filter.active {
                            match key.code {
                                KeyCode::Esc => {
                                    app.filter.text.clear();
                                    app.filter.active = false;
                                    app.clear_filter();
                                }
                                KeyCode::Enter => {
                                    app.filter.active = false; // commit filter, keep text
                                }
                                KeyCode::Backspace => {
                                    app.filter.text.pop();
                                    let text = app.filter.text.clone();
                                    app.apply_filter(&text);
                                }
                                KeyCode::Char(c) => {
                                    app.filter.text.push(c);
                                    let text = app.filter.text.clone();
                                    app.apply_filter(&text);
                                }
                                _ => {}
                            }
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
                                            app.yaml.scroll = target.saturating_sub(10);
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
                                            app.describe.scroll = target.saturating_sub(10);
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
                            continue;
                        }

                        if let Some(action) = crate::event::handler::handle_key_event(&app, key) {
                            let result = handle_action(
                                &mut app,
                                action,
                                &event_tx,
                                &mut resource_watcher,
                                &mut kube_client,
                                &mut log_task,
                                &describe_generation,
                                &yaml_generation,
                            ).await;
                            match result {
                                ActionResult::Shell { pod, namespace, container, context } => {
                                    // Suspend TUI
                                    disable_raw_mode()?;
                                    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                                    terminal.show_cursor()?;

                                    // Clear screen and show connecting message
                                    print!("\x1b[2J\x1b[H"); // clear screen + move cursor to top
                                    println!("Connecting to {}/{}...\n", pod, container);

                                    // Try bash first (for tab completion), fall back to sh
                                    let mut cmd = std::process::Command::new("kubectl");
                                    cmd.arg("exec").arg("-it").arg(&pod).arg("-n").arg(&namespace).arg("-c").arg(&container);
                                    if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                    cmd.arg("--").arg("/bin/bash");
                                    let status = cmd.status();

                                    // If bash failed (not found), try sh
                                    if status.is_err() || status.as_ref().map_or(false, |s| !s.success()) {
                                        let mut cmd2 = std::process::Command::new("kubectl");
                                        cmd2.arg("exec").arg("-it").arg(&pod).arg("-n").arg(&namespace).arg("-c").arg(&container);
                                        if !context.is_empty() { cmd2.arg("--context").arg(&context); }
                                        cmd2.arg("--").arg("/bin/sh");
                                        let _ = cmd2.status();
                                    }

                                    // Resume TUI
                                    enable_raw_mode()?;
                                    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
                                    terminal.clear()?;
                                }
                                ActionResult::Edit { resource, name, namespace, context } => {
                                    // Suspend TUI
                                    disable_raw_mode()?;
                                    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                                    terminal.show_cursor()?;

                                    // Run kubectl edit
                                    let mut cmd = std::process::Command::new("kubectl");
                                    cmd.arg("edit").arg(&resource).arg(&name);
                                    if !namespace.is_empty() { cmd.arg("-n").arg(&namespace); }
                                    if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                    let _ = cmd.status(); // blocks until editor exits

                                    // Clear cache after edit (resource may have changed)
                                    app.kubectl_cache.clear();

                                    // Resume TUI
                                    enable_raw_mode()?;
                                    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
                                    terminal.clear()?;
                                }
                                ActionResult::None => {}
                            }
                        }
                    }
                    CtEvent::Mouse(_mouse) => {
                        // TODO: mouse handling
                    }
                    _ => {}
                }
            }
            Some(event) = event_rx.recv() => {
                apply_event(&mut app, event);
                // Drain pending events before redrawing to batch log lines,
                // but cap at 200 to prevent UI freezes during massive bursts.
                let mut drained = 0;
                while drained < 200 {
                    match event_rx.try_recv() {
                        Ok(event) => { apply_event(&mut app, event); drained += 1; }
                        Err(_) => break,
                    }
                }
            }
            _ = tick_interval.tick() => {
                app.tick();
                // Check if the log streaming task has finished
                if let Some(ref handle) = log_task {
                    if handle.is_finished() {
                        log_task.take();
                        app.logs.streaming = false;
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
    resource_watcher.stop().await;
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    Ok(())
}

async fn handle_action(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    resource_watcher: &mut crate::kube::watcher::ResourceWatcher,
    kube_client: &mut crate::kube::KubeClient,
    log_task: &mut Option<JoinHandle<()>>,
    describe_generation: &Arc<AtomicU64>,
    yaml_generation: &Arc<AtomicU64>,
) -> ActionResult {
    let client = kube_client.client().clone();
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
                app.logs.clear();
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
            app.last_resource_tab = Some(app.resource_tab);
            app.next_tab();
            resource_watcher.switch_tab(app.resource_tab).await;
        }
        Action::PrevTab => {
            app.last_resource_tab = Some(app.resource_tab);
            app.prev_tab();
            resource_watcher.switch_tab(app.resource_tab).await;
        }
        Action::GotoTab(tab) => {
            app.last_resource_tab = Some(app.resource_tab);
            app.resource_tab = tab;
            resource_watcher.switch_tab(tab).await;
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
                    app.logs.scroll = app.logs.scroll.saturating_sub(40);
                }
                Route::Yaml { .. } => app.yaml.scroll = app.yaml.scroll.saturating_sub(40),
                Route::Describe { .. } | Route::Aliases => app.describe.scroll = app.describe.scroll.saturating_sub(40),
                Route::Help => {
                    app.help_scroll = app.help_scroll.saturating_sub(10);
                }
                _ => app.page_up(),
            }
        }
        Action::PageDown => {
            match &app.route {
                Route::Logs { .. } | Route::Shell { .. } => {
                    app.logs.follow = false;
                    let total = app.logs.lines.len();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(24).saturating_sub(5);
                    let max = total.saturating_sub(visible);
                    app.logs.scroll = (app.logs.scroll + 40).min(max);
                }
                Route::Yaml { .. } => {
                    let max = app.yaml.content.lines().count().saturating_sub(1);
                    app.yaml.scroll = (app.yaml.scroll + 40).min(max);
                }
                Route::Describe { .. } | Route::Aliases => {
                    let max = app.describe.content.lines().count().saturating_sub(1);
                    app.describe.scroll = (app.describe.scroll + 40).min(max);
                }
                Route::Help => {
                    app.help_scroll += 10;
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
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(24).saturating_sub(5);
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
        Action::Enter => handle_enter(app, resource_watcher, kube_client, event_tx, log_task, describe_generation).await,
        Action::Describe => handle_describe(app, event_tx, &client, describe_generation),
        Action::Yaml => handle_yaml(app, event_tx, &client, yaml_generation),
        Action::Logs => handle_logs(app, event_tx, &client, log_task),
        Action::Shell => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let pod_name = pod.name.clone();
                    let pod_ns = pod.namespace.clone();
                    let container = pod.containers.first().map(|c| c.name.clone()).unwrap_or_default();
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
        Action::Delete => {
            let (resource, name, _namespace) = get_selected_resource_info(app);
            app.confirm_dialog = Some(crate::app::ConfirmDialog {
                message: format!("Delete {}/{}?", resource, name),
                action: Action::Delete,
                yes_selected: false,
            });
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
            // Pre-fill with existing per-table filter if any, so pressing /
            // with a committed filter lets you edit it instead of clearing it.
            let existing = app.active_filter_text().to_string();
            if !existing.is_empty() && app.filter.text.is_empty() {
                app.filter.text = existing;
            }
            app.filter.active = true;
        }
        Action::ClearFilter => {
            app.filter.text.clear();
            app.filter.active = false;
            app.clear_filter();
            // If we were in a drill-down (Enter on workload switched to Pods
            // with a filter), restore the previous tab on clear.
            if let Some(last_tab) = app.last_resource_tab {
                if app.resource_tab == ResourceTab::Pods && last_tab != ResourceTab::Pods {
                    app.resource_tab = last_tab;
                    app.last_resource_tab = None;
                    resource_watcher.switch_tab(last_tab).await;
                }
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
                let (_, rows) = crossterm::terminal::size().unwrap_or((80, 24));
                // Subtract borders (2) + indicator bar (1) + keybinding bar (1)
                let visible = (rows as usize).saturating_sub(4);
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
            app.kubectl_cache.clear();
            let (resource, name, namespace) = get_selected_resource_info(app);
            if !name.is_empty() {
                let context = app.context.clone();
                let tx = event_tx.clone();
                tokio::spawn(async move {
                    let mut cmd = tokio::process::Command::new("kubectl");
                    cmd.arg("rollout").arg("restart").arg(format!("{}/{}", resource, name));
                    if !namespace.is_empty() { cmd.arg("-n").arg(&namespace); }
                    if !context.is_empty() { cmd.arg("--context").arg(&context); }
                    match cmd.output().await {
                        Ok(output) if output.status.success() => {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                format!("Restarted {}/{}", resource, name)
                            ))).await;
                        }
                        Ok(output) => {
                            let err = String::from_utf8_lossy(&output.stderr);
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                format!("Restart failed: {}", err)
                            ))).await;
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                format!("Failed to run kubectl: {}", e)
                            ))).await;
                        }
                    }
                });
            }
        }
        Action::PortForward => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let pod_name = pod.name.clone();
                    let pod_ns = pod.namespace.clone();
                    let context = app.context.clone();
                    // Use a default port (8080:8080) — in k9s this shows a dialog
                    let tx = event_tx.clone();
                    tokio::spawn(async move {
                        let mut cmd = tokio::process::Command::new("kubectl");
                        cmd.arg("port-forward").arg(&pod_name).arg("8080:8080");
                        if !pod_ns.is_empty() { cmd.arg("-n").arg(&pod_ns); }
                        if !context.is_empty() { cmd.arg("--context").arg(&context); }
                        match cmd.output().await {
                            Ok(_) => {
                                let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Port-forward ended for {}", pod_name)))).await;
                            }
                            Err(e) => {
                                let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Port-forward failed: {}", e)))).await;
                            }
                        }
                    });
                    app.flash = Some(crate::app::FlashMessage::info(format!("Port-forwarding pod/{} on 8080:8080", pod.name)));
                }
            } else {
                app.flash = Some(crate::app::FlashMessage::info("Port-forward: select a pod first"));
            }
        }
        Action::ToggleHeader => {
            app.show_header = !app.show_header;
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
            let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(24).saturating_sub(5);
            let max = total.saturating_sub(visible);
            app.logs.scroll = (app.logs.scroll + n).min(max);
        }
        Action::SwitchNamespace(ns) => {
            do_switch_namespace(app, resource_watcher, &ns).await;
        }
        Action::SwitchContext(ctx) => {
            do_switch_context(app, kube_client, resource_watcher, &ctx).await;
        }
        Action::Refresh => {
            resource_watcher.switch_tab_force(app.resource_tab).await;
            app.flash = Some(crate::app::FlashMessage::info("Refreshed"));
        }
        Action::Copy => {
            let (_, name, _) = get_selected_resource_info(app);
            if !name.is_empty() {
                if try_copy_to_clipboard(&name) {
                    app.flash = Some(crate::app::FlashMessage::info(format!("Copied: {}", name)));
                } else {
                    app.flash = Some(crate::app::FlashMessage::warn(
                        "No clipboard tool found (install xclip, xsel, wl-copy, or pbcopy)"
                    ));
                }
            }
        }
        Action::Confirm => {
            // Perform the actual delete if the confirm dialog was for a delete
            let was_delete = app.confirm_dialog.as_ref().map_or(false, |d| {
                matches!(d.action, Action::Delete)
            });
            app.confirm_dialog = None;

            if was_delete {
                // Execute the actual delete via kube API
                app.kubectl_cache.clear();
                let (resource, name, namespace) = get_selected_resource_info(app);
                if !name.is_empty() {
                    let client = client.clone();
                    let event_tx = event_tx.clone();
                    let resource_kind = resource.clone();
                    let res_name = name.clone();
                    let res_ns = namespace.clone();
                    tokio::spawn(async move {
                        let result = execute_delete(&client, &resource_kind, &res_name, &res_ns).await;
                        match result {
                            Ok(()) => {
                                let _ = event_tx
                                    .send(AppEvent::Flash(crate::app::FlashMessage::info(
                                        format!("Deleted {}/{}", resource_kind, res_name),
                                    )))
                                    .await;
                            }
                            Err(e) => {
                                let _ = event_tx
                                    .send(AppEvent::Flash(crate::app::FlashMessage::error(
                                        format!("Delete failed: {}", e),
                                    )))
                                    .await;
                            }
                        }
                    });
                }
            } else {
                app.flash = Some(crate::app::FlashMessage::info("Confirmed"));
            }
        }
        Action::Cancel => {
            app.confirm_dialog = None;
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
            handle_previous_logs(app, event_tx, log_task);
        }
        Action::ForceKill => {
            app.kubectl_cache.clear();
            if app.resource_tab == ResourceTab::Pods {
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
        }
        Action::ShowNode => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let node = pod.node.clone();
                    if !node.is_empty() {
                        app.last_resource_tab = Some(app.resource_tab);
                        app.resource_tab = ResourceTab::Nodes;
                        resource_watcher.switch_tab(ResourceTab::Nodes).await;
                        app.filter.text = node.clone();
                        app.filter.active = false;
                        app.apply_filter(&node);
                    }
                }
            }
        }
        Action::ToggleLastView => {
            if let Some(last) = app.last_resource_tab.take() {
                let current = app.resource_tab;
                app.resource_tab = last;
                app.last_resource_tab = Some(current);
                resource_watcher.switch_tab(app.resource_tab).await;
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
            match std::fs::write(&filename, &content) {
                Ok(_) => {
                    app.flash = Some(crate::app::FlashMessage::info(format!("Saved to {}", filename)));
                }
                Err(e) => {
                    app.flash = Some(crate::app::FlashMessage::error(format!("Save failed: {}", e)));
                }
            }
        }
        Action::ShowAliases => {
            let aliases = vec![
                ("po/pod/pods", "Pods"),
                ("dp/deploy/deployment/deployments", "Deployments"),
                ("svc/service/services", "Services"),
                ("sts/statefulset/statefulsets", "StatefulSets"),
                ("ds/daemonset/daemonsets", "DaemonSets"),
                ("job/jobs", "Jobs"),
                ("cj/cronjob/cronjobs", "CronJobs"),
                ("cm/configmap/configmaps", "ConfigMaps"),
                ("sec/secret/secrets", "Secrets"),
                ("no/node/nodes", "Nodes"),
                ("ns/namespace/namespaces", "Namespaces"),
                ("ing/ingress/ingresses", "Ingresses"),
                ("rs/replicaset/replicasets", "ReplicaSets"),
                ("pv/persistentvolume/pvs", "PersistentVolumes"),
                ("pvc/persistentvolumeclaim/pvcs", "PersistentVolumeClaims"),
                ("sc/storageclass/storageclasses", "StorageClasses"),
                ("sa/serviceaccount/serviceaccounts", "ServiceAccounts"),
                ("np/networkpolicy/networkpolicies", "NetworkPolicies"),
                ("ev/event/events", "Events"),
                ("role/roles", "Roles"),
                ("cr/clusterrole/clusterroles", "ClusterRoles"),
                ("rb/rolebinding/rolebindings", "RoleBindings"),
                ("crb/clusterrolebinding/clusterrolebindings", "ClusterRoleBindings"),
                ("hpa/horizontalpodautoscaler", "HorizontalPodAutoscalers"),
                ("ep/endpoints", "Endpoints"),
            ];
            let mut content = String::from("Resource Aliases\n================\n\n");
            content.push_str(&format!("  {:<45} {}\n", "ALIAS", "RESOURCE"));
            content.push_str(&format!("  {:<45} {}\n", "-----", "--------"));
            for (alias, resource) in &aliases {
                content.push_str(&format!("  {:<45} {}\n", alias, resource));
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
                let handle = spawn_log_stream(
                    event_tx.clone(),
                    pod.clone(),
                    namespace.clone(),
                    container.clone(),
                    app.context.clone(),
                    true,  // follow
                    tail,
                    since.clone(),
                    false, // not previous
                );
                *log_task = Some(handle);
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
                app.apply_filter("error|failed|crashloop|pending|imagepull|oom|evicted|init");
                app.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            } else {
                app.clear_filter();
                app.filter.text.clear();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            }
        }
    }
    ActionResult::None
}

/// Attempt to copy `text` to the system clipboard using available tools.
/// Tries xclip, xsel, wl-copy, and pbcopy in order. Returns true on success.
fn try_copy_to_clipboard(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let tools: &[(&str, &[&str])] = &[
        ("xclip", &["-selection", "clipboard"]),
        ("xsel", &["--clipboard", "--input"]),
        ("wl-copy", &[]),
        ("pbcopy", &[]),
    ];

    for (tool, args) in tools {
        if let Ok(mut child) = Command::new(tool)
            .args(*args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            if let Some(ref mut stdin) = child.stdin {
                let _ = stdin.write_all(text.as_bytes());
            }
            if let Ok(status) = child.wait() {
                if status.success() {
                    return true;
                }
            }
        }
    }
    false
}

/// Execute a delete API call for the given resource.
async fn execute_delete(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<()> {
    use k8s_openapi::api::{
        apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
        batch::v1::{CronJob, Job},
        core::v1::{
            ConfigMap, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
            Service, ServiceAccount,
        },
        networking::v1::{Ingress, NetworkPolicy},
        rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
        storage::v1::StorageClass,
    };
    use ::kube::api::DeleteParams;
    use ::kube::Api;

    let dp = DeleteParams::default();

    macro_rules! delete_namespaced {
        ($k8s_type:ty) => {{
            let api: Api<$k8s_type> = if namespace.is_empty() {
                Api::default_namespaced(client.clone())
            } else {
                Api::namespaced(client.clone(), namespace)
            };
            api.delete(name, &dp).await?;
        }};
    }

    macro_rules! delete_cluster {
        ($k8s_type:ty) => {{
            let api: Api<$k8s_type> = Api::all(client.clone());
            api.delete(name, &dp).await?;
        }};
    }

    match resource {
        "pod" => delete_namespaced!(Pod),
        "deployment" => delete_namespaced!(Deployment),
        "service" => delete_namespaced!(Service),
        "configmap" => delete_namespaced!(ConfigMap),
        "secret" => delete_namespaced!(Secret),
        "statefulset" => delete_namespaced!(StatefulSet),
        "daemonset" => delete_namespaced!(DaemonSet),
        "job" => delete_namespaced!(Job),
        "cronjob" => delete_namespaced!(CronJob),
        "replicaset" => delete_namespaced!(ReplicaSet),
        "ingress" => delete_namespaced!(Ingress),
        "networkpolicy" => delete_namespaced!(NetworkPolicy),
        "serviceaccount" => delete_namespaced!(ServiceAccount),
        "pvc" => delete_namespaced!(PersistentVolumeClaim),
        "role" => delete_namespaced!(Role),
        "rolebinding" => delete_namespaced!(RoleBinding),
        "namespace" => delete_cluster!(Namespace),
        "node" => delete_cluster!(Node),
        "pv" => delete_cluster!(PersistentVolume),
        "storageclass" => delete_cluster!(StorageClass),
        "clusterrole" => delete_cluster!(ClusterRole),
        "clusterrolebinding" => delete_cluster!(ClusterRoleBinding),
        "event" => delete_namespaced!(k8s_openapi::api::core::v1::Event),
        other => {
            return Err(anyhow::anyhow!("Unknown resource type: {}", other));
        }
    }

    Ok(())
}

async fn handle_enter(
    app: &mut App,
    resource_watcher: &mut crate::kube::watcher::ResourceWatcher,
    kube_client: &mut crate::kube::KubeClient,
    event_tx: &mpsc::Sender<AppEvent>,
    log_task: &mut Option<JoinHandle<()>>,
    describe_generation: &Arc<AtomicU64>,
) {
    use crate::app::Route;
    let client = kube_client.client().clone();

    // Fix #11: Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            do_switch_context(app, kube_client, resource_watcher, &ctx_name).await;
        }
        return;
    }

    // Handle ContainerSelect: open logs for the selected container.
    if let Route::ContainerSelect { ref pod, ref namespace } = app.route {
        let pod_name = pod.clone();
        let pod_ns = namespace.clone();
        let idx = app.container_select_index;
        let container_name = app.data.pods.items.iter()
            .find(|p| p.name == pod_name && p.namespace == pod_ns)
            .and_then(|p| p.containers.get(idx).map(|c| c.name.clone()))
            .unwrap_or_default();

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
        let handle = spawn_log_stream(
            event_tx.clone(),
            pod_name,
            pod_ns,
            container_name,
            app.context.clone(),
            true,  // follow
            tail,
            app.logs.since.clone(),
            false, // not previous
        );

        *log_task = Some(handle);
        return;
    }

    // Handle resource tab-specific Enter behavior
    match app.resource_tab {
        ResourceTab::Pods => {
            // Enter on pods goes to describe (consistent with other resources).
            // Use `l` to view logs.
            handle_describe(app, event_tx, &client, describe_generation);
        }
        ResourceTab::Namespaces => {
            let ns_name = app
                .data
                .namespaces
                .selected_item()
                .map(|ns| ns.name.clone());
            if let Some(ns_name) = ns_name {
                do_switch_namespace(app, resource_watcher, &ns_name).await;
            }
        }
        // Workload types: drill down to pods filtered by the workload name.
        // Use "{name}-" to avoid false matches: "nginx" would otherwise match
        // "nginx-ingress-controller" or any column containing "nginx".
        ResourceTab::Deployments | ResourceTab::StatefulSets | ResourceTab::DaemonSets
        | ResourceTab::ReplicaSets | ResourceTab::Jobs | ResourceTab::CronJobs | ResourceTab::Services => {
            let (_, name, _) = get_selected_resource_info(app);
            if !name.is_empty() {
                // Save current tab before switching
                app.last_resource_tab = Some(app.resource_tab);
                app.resource_tab = ResourceTab::Pods;
                // Use switch_tab_force — if we were already on Pods tab, switch_tab
                // returns early without restarting the watcher, leaving an empty table.
                resource_watcher.switch_tab_force(ResourceTab::Pods).await;
                // Escape regex metacharacters so dots/brackets in names don't
                // produce an invalid regex that silently matches nothing.
                let filter = format!("{}-", regex::escape(&name));
                app.data.pods.clear_data(); // Reset pods table so spinner shows while fresh data arrives
                app.filter.text = filter.clone();
                app.filter.active = false;
                app.apply_filter(&filter);
                app.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
            }
        }
        // Nodes: drill down to pods filtered by the node name.
        ResourceTab::Nodes => {
            let (_, name, _) = get_selected_resource_info(app);
            if !name.is_empty() {
                app.last_resource_tab = Some(app.resource_tab);
                app.resource_tab = ResourceTab::Pods;
                resource_watcher.switch_tab_force(ResourceTab::Pods).await;
                let filter = regex::escape(&name);
                app.data.pods.clear_data(); // Reset pods table so spinner shows while fresh data arrives
                app.filter.text = filter.clone();
                app.filter.active = false;
                app.apply_filter(&filter);
                app.flash = Some(crate::app::FlashMessage::info(format!("Pods on node: {}", name)));
            }
        }
        // CRDs: drill down to browse instances of the selected CRD.
        ResourceTab::Crds => {
            if let Some(crd) = app.data.crds.selected_item().cloned() {
                app.last_resource_tab = Some(app.resource_tab);
                app.dynamic_resource_name = crd.kind.clone();
                app.data.dynamic_resources.clear_data();
                app.resource_tab = ResourceTab::DynamicResource;
                resource_watcher.watch_dynamic(
                    crd.group.clone(),
                    crd.version.clone(),
                    crd.kind.clone(),
                    crd.scope.clone(),
                ).await;
                app.flash = Some(crate::app::FlashMessage::info(
                    format!("Browsing CRD: {}", crd.kind)
                ));
            }
        }
        _ => {
            // Config resources and everything else: describe
            handle_describe(app, event_tx, &client, describe_generation);
        }
    }
}

fn handle_describe(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
    _client: &::kube::Client,
    describe_generation: &Arc<AtomicU64>,
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

        // Increment generation counter; only apply result if it still matches
        let gen = describe_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let gen_check = describe_generation.clone();

        // Spawn kubectl describe via tokio::process::Command
        let tx = event_tx.clone();
        let context = app.context.clone();
        let res = resource;
        let n = name;
        let ns = namespace;
        tokio::spawn(async move {
            let mut cmd = tokio::process::Command::new("kubectl");
            cmd.arg("describe").arg(&res).arg(&n);
            if !context.is_empty() {
                cmd.arg("--context").arg(&context);
            }
            if !ns.is_empty() {
                cmd.arg("-n").arg(&ns);
            }
            match cmd.output().await {
                Ok(output) => {
                    let raw = if output.status.success() {
                        String::from_utf8_lossy(&output.stdout).to_string()
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        format!("Error running kubectl describe:\n{}", stderr)
                    };
                    let content = crate::util::strip_ansi(&raw);
                    if gen_check.load(Ordering::SeqCst) == gen {
                        let _ = tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::Describe(content)))
                            .await;
                    }
                }
                Err(e) => {
                    if gen_check.load(Ordering::SeqCst) == gen {
                        let _ = tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::Describe(
                                format!("Failed to run kubectl: {}", e),
                            )))
                            .await;
                    }
                }
            }
        });
    }
}

fn handle_yaml(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
    _client: &::kube::Client,
    yaml_generation: &Arc<AtomicU64>,
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

        // Increment generation counter; only apply result if it still matches
        let gen = yaml_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let gen_check = yaml_generation.clone();

        // Spawn a task to fetch the YAML via kubectl (works for any resource type)
        let tx = event_tx.clone();
        let context = app.context.clone();
        let res = resource;
        let n = name;
        let ns = namespace;
        tokio::spawn(async move {
            let mut cmd = tokio::process::Command::new("kubectl");
            cmd.arg("get").arg(&res).arg(&n).arg("-o").arg("yaml");
            if !context.is_empty() {
                cmd.arg("--context").arg(&context);
            }
            if !ns.is_empty() {
                cmd.arg("-n").arg(&ns);
            }
            match cmd.output().await {
                Ok(output) => {
                    let raw = if output.status.success() {
                        String::from_utf8_lossy(&output.stdout).to_string()
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        format!("Error fetching YAML:\n{}", stderr)
                    };
                    let content = crate::util::strip_ansi(&raw);
                    if gen_check.load(Ordering::SeqCst) == gen {
                        let _ = tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::Yaml(content)))
                            .await;
                    }
                }
                Err(e) => {
                    if gen_check.load(Ordering::SeqCst) == gen {
                        let _ = tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::Yaml(format!(
                                "Failed to run kubectl: {}",
                                e
                            ))))
                            .await;
                    }
                }
            }
        });
    }
}

/// Spawn a kubectl logs streaming task, returning a `JoinHandle` that can be
/// cancelled to stop the stream.  This centralises the command-building logic
/// that was previously duplicated across four call-sites.
fn spawn_log_stream(
    tx: mpsc::Sender<AppEvent>,
    target: String,
    namespace: String,
    container: String,
    context: String,
    follow: bool,
    tail: Option<u64>,
    since: Option<String>,
    previous: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::process::Command;

        let mut cmd = Command::new("kubectl");
        cmd.arg("logs");

        if follow {
            cmd.arg("-f");
        }
        if previous {
            cmd.arg("--previous");
        }

        cmd.arg(&target);

        if !namespace.is_empty() {
            cmd.arg("-n").arg(&namespace);
        }

        if !container.is_empty() && container != "all" {
            cmd.arg("-c").arg(&container);
        } else if container == "all" {
            cmd.arg("--all-containers=true");
        }

        if let Some(ref s) = since {
            cmd.arg(format!("--since={}", s));
        } else if let Some(t) = tail {
            cmd.arg("--tail").arg(t.to_string());
        }

        if !context.is_empty() {
            cmd.arg("--context").arg(&context);
        }

        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let end_label = if previous {
            "--- Previous logs ended ---"
        } else {
            "--- Stream ended ---"
        };

        match cmd.spawn() {
            Ok(mut child) => {
                // Read stderr in a separate task so errors appear in the UI
                if let Some(stderr) = child.stderr.take() {
                    let stderr_tx = tx.clone();
                    tokio::spawn(async move {
                        let reader = BufReader::new(stderr);
                        let mut lines = reader.lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let clean = crate::util::strip_ansi(&line);
                            let _ = stderr_tx
                                .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                                    format!("[stderr] {}", clean),
                                )))
                                .await;
                        }
                    });
                }
                if let Some(stdout) = child.stdout.take() {
                    let reader = BufReader::new(stdout);
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let clean = crate::util::strip_ansi(&line);
                        if tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(clean)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                let _ = tx.send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                    end_label.to_string()
                ))).await;
                let _ = child.kill().await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                        format!("Failed to start kubectl logs: {}", e),
                    )))
                    .await;
            }
        }
    })
}

fn handle_logs(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
    _client: &::kube::Client,
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
                .and_then(|p| p.containers.first().map(|c| c.name.clone()))
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
    let handle = spawn_log_stream(
        event_tx.clone(),
        log_target,
        namespace,
        route_container,
        app.context.clone(),
        true,  // follow
        tail,
        app.logs.since.clone(),
        false, // not previous
    );

    *log_task = Some(handle);
}

fn handle_previous_logs(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
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
                .and_then(|p| p.containers.first().map(|c| c.name.clone()))
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

    let handle = spawn_log_stream(
        event_tx.clone(),
        log_target,
        namespace,
        route_container,
        app.context.clone(),
        false, // no follow for previous logs
        Some(app.logs.tail_lines), // always tail for previous logs
        None,  // no --since for previous logs
        true,  // --previous
    );

    *log_task = Some(handle);
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
        ResourceTab::DynamicResource => selected_info!(app.data.dynamic_resources, "dynamic"),
    }
    (String::new(), String::new(), String::new())
}

/// Handle a single AppEvent (resource update, error, or flash).
fn apply_event(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::ResourceUpdate(update) => {
            apply_resource_update(app, update);
        }
        AppEvent::Error(msg) => {
            app.flash = Some(crate::app::FlashMessage::error(msg));
        }
        AppEvent::Flash(flash) => {
            app.flash = Some(flash);
        }
    }
}

fn apply_resource_update(app: &mut App, update: ResourceUpdate) {
    match update {
        ResourceUpdate::Pods(items) => app.data.pods.set_items_filtered(items),
        ResourceUpdate::Deployments(items) => app.data.deployments.set_items_filtered(items),
        ResourceUpdate::Services(items) => app.data.services.set_items_filtered(items),
        ResourceUpdate::Nodes(items) => app.data.nodes.set_items_filtered(items),
        ResourceUpdate::Namespaces(items) => app.data.namespaces.set_items_filtered(items),
        ResourceUpdate::ConfigMaps(items) => app.data.configmaps.set_items_filtered(items),
        ResourceUpdate::Secrets(items) => app.data.secrets.set_items_filtered(items),
        ResourceUpdate::StatefulSets(items) => app.data.statefulsets.set_items_filtered(items),
        ResourceUpdate::DaemonSets(items) => app.data.daemonsets.set_items_filtered(items),
        ResourceUpdate::Jobs(items) => app.data.jobs.set_items_filtered(items),
        ResourceUpdate::CronJobs(items) => app.data.cronjobs.set_items_filtered(items),
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
            // Also update the discovered CRDs list for dynamic browsing
            app.discovered_crds = items.clone();
            app.data.crds.set_items_filtered(items);
        }
        ResourceUpdate::DynamicResources(items) => app.data.dynamic_resources.set_items_filtered(items),
        ResourceUpdate::Yaml(content) => {
            if let Some((r, n, ns)) = app.pending_yaml_key.take() {
                app.kubectl_cache.insert(r, n, ns, "yaml", content.clone());
            }
            app.yaml.content = content;
        }
        ResourceUpdate::Describe(content) => {
            if let Some((r, n, ns)) = app.pending_describe_key.take() {
                app.kubectl_cache.insert(r, n, ns, "describe", content.clone());
            }
            app.describe.content = content;
        }
        ResourceUpdate::LogLine(line) => app.logs.push(line),
    }
}

/// Perform a context switch: update app state, clear data, replace the watcher client.
/// Returns `true` on success, `false` on failure (with a flash message set).
async fn do_switch_context(
    app: &mut App,
    kube_client: &mut crate::kube::KubeClient,
    resource_watcher: &mut crate::kube::watcher::ResourceWatcher,
    ctx_name: &str,
) -> bool {
    match kube_client.switch_context(ctx_name).await {
        Ok(new_client) => {
            app.context = ctx_name.to_string();
            app.clear_data();
            app.kubectl_cache.clear();
            app.discovered_crds.clear();
            app.route_stack.clear();
            app.route = crate::app::Route::Resources;
            app.filter.active = false;
            app.filter.text.clear();
            resource_watcher
                .replace_client(new_client.clone(), app.selected_ns.clone())
                .await;
            let updated: Vec<crate::app::KubeContext> = app
                .contexts
                .iter()
                .map(|name| crate::app::KubeContext {
                    name: name.clone(),
                    cluster: String::new(),
                    is_current: name == ctx_name,
                })
                .collect();
            app.data.contexts.set_items(updated);
            app.flash = Some(crate::app::FlashMessage::info(format!(
                "Switched to context: {}",
                ctx_name
            )));
            true
        }
        Err(e) => {
            app.flash = Some(crate::app::FlashMessage::error(format!(
                "Context switch failed: {}",
                e
            )));
            false
        }
    }
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
async fn do_switch_namespace(
    app: &mut App,
    resource_watcher: &mut crate::kube::watcher::ResourceWatcher,
    ns: &str,
) {
    app.selected_ns = ns.to_string();
    app.clear_data();
    app.kubectl_cache.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    app.filter.active = false;
    app.filter.text.clear();
    resource_watcher.switch_namespace(ns).await;
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns
    )));
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

fn parse_resource_command(cmd: &str) -> Option<ResourceTab> {
    match cmd.to_lowercase().as_str() {
        "po" | "pod" | "pods" => Some(ResourceTab::Pods),
        "dp" | "deploy" | "deployment" | "deployments" => Some(ResourceTab::Deployments),
        "svc" | "service" | "services" => Some(ResourceTab::Services),
        "no" | "node" | "nodes" => Some(ResourceTab::Nodes),
        "ns" | "namespace" | "namespaces" => Some(ResourceTab::Namespaces),
        "cm" | "configmap" | "configmaps" => Some(ResourceTab::ConfigMaps),
        "sec" | "secret" | "secrets" => Some(ResourceTab::Secrets),
        "sts" | "statefulset" | "statefulsets" => Some(ResourceTab::StatefulSets),
        "ds" | "daemonset" | "daemonsets" => Some(ResourceTab::DaemonSets),
        "job" | "jobs" => Some(ResourceTab::Jobs),
        "cj" | "cronjob" | "cronjobs" => Some(ResourceTab::CronJobs),
        "rs" | "replicaset" | "replicasets" => Some(ResourceTab::ReplicaSets),
        "ing" | "ingress" | "ingresses" => Some(ResourceTab::Ingresses),
        "ev" | "event" | "events" => Some(ResourceTab::Events),
        "pv" | "persistentvolume" | "pvs" => Some(ResourceTab::Pvs),
        "pvc" | "persistentvolumeclaim" | "pvcs" => Some(ResourceTab::Pvcs),
        "sc" | "storageclass" | "storageclasses" => Some(ResourceTab::StorageClasses),
        "np" | "networkpolicy" | "networkpolicies" => Some(ResourceTab::NetworkPolicies),
        "sa" | "serviceaccount" | "serviceaccounts" => Some(ResourceTab::ServiceAccounts),
        "role" | "roles" => Some(ResourceTab::Roles),
        "cr" | "clusterrole" | "clusterroles" => Some(ResourceTab::ClusterRoles),
        "rb" | "rolebinding" | "rolebindings" => Some(ResourceTab::RoleBindings),
        "crb" | "clusterrolebinding" | "clusterrolebindings" => {
            Some(ResourceTab::ClusterRoleBindings)
        }
        "hpa" | "horizontalpodautoscaler" => Some(ResourceTab::Hpa),
        "ep" | "endpoints" => Some(ResourceTab::Endpoints),
        "limits" | "limitrange" | "limitranges" => Some(ResourceTab::LimitRanges),
        "quota" | "resourcequota" | "resourcequotas" => Some(ResourceTab::ResourceQuotas),
        "pdb" | "poddisruptionbudget" | "poddisruptionbudgets" => Some(ResourceTab::Pdb),
        "crd" | "crds" | "customresourcedefinition" | "customresourcedefinitions" => {
            Some(ResourceTab::Crds)
        }
        _ => None,
    }
}
