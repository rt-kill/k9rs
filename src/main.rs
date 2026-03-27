pub mod app;
pub mod cli;
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
    cursor::SetCursorStyle,
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

    /// Run without the cache daemon (local watchers, file cache only)
    #[arg(long)]
    no_daemon: bool,

    /// Read-only mode: disables all destructive actions (delete, edit, scale, restart, shell)
    #[arg(long)]
    readonly: bool,

    #[command(subcommand)]
    subcmd: Option<crate::cli::Command>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Subcommand dispatch: daemon, ctl, get, contexts
    if let Some(subcmd) = cli.subcmd {
        return crate::cli::dispatch(subcmd).await;
    }

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

    // Read kubeconfig ONCE for all early init (contexts list, cluster/user info)
    let kubeconfig = ::kube::config::Kubeconfig::read().ok();
    let contexts: Vec<String> = kubeconfig.as_ref()
        .map(|kc| kc.contexts.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    let current_context = cli.context.clone().or_else(|| {
        kubeconfig.as_ref().and_then(|kc| kc.current_context.clone())
    }).unwrap_or_else(|| "in-cluster".to_string());

    // Initialize app state and show TUI IMMEDIATELY — data loads in background
    let namespace = cli.namespace.unwrap_or_else(|| "all".to_string());
    let mut app = App::new(
        current_context.clone(),
        contexts.clone(),
        namespace,
    );
    if cli.readonly {
        app.read_only = true;
    }

    // Populate contexts table from the single kubeconfig read
    if let Some(ref kc) = kubeconfig {
        let mut ctx_map = std::collections::HashMap::new();
        for named_ctx in &kc.contexts {
            if let Some(ref ctx) = named_ctx.context {
                ctx_map.insert(named_ctx.name.as_str(), (ctx.cluster.as_str(), ctx.user.as_deref().unwrap_or("")));
            }
        }
        let kube_contexts: Vec<crate::app::KubeContext> = contexts.iter().map(|name: &String| {
            let (cluster, _user) = ctx_map.get(name.as_str()).unwrap_or(&("", ""));
            crate::app::KubeContext {
                name: name.clone(),
                cluster: cluster.to_string(),
                is_current: name == &current_context,
            }
        }).collect();
        // Also set cluster/user on app from the same read
        if let Some(&(cluster, user)) = ctx_map.get(current_context.as_str()) {
            app.cluster = cluster.to_string();
            app.user = user.to_string();
        }
        app.data.contexts.set_items(kube_contexts);
    }

    // Connect to the cache daemon (or run in local mode with --no-daemon).
    let startup_cache_key = crate::kube::cache::cache_key(&current_context, &app.cluster);
    let mut data_backend = crate::kube::backend::DataBackend::connect(cli.no_daemon).await;
    if !cli.no_daemon && !data_backend.is_daemon() {
        eprintln!(
            "Cache daemon is not running.\n\n\
             Start it with:  k9rs daemon\n\
             Or run without:  k9rs --no-daemon\n"
        );
        std::process::exit(1);
    }
    let session_id = data_backend.register_session(&current_context, &app.selected_ns).await;
    if let Some((namespaces, crds)) = data_backend.get_discovery(&startup_cache_key).await {
        if !namespaces.is_empty() {
            let ns_items = crate::kube::cache::cached_namespaces_to_domain(&namespaces);
            app.data.namespaces.set_items(ns_items);
        }
        if !crds.is_empty() {
            app.discovered_crds = crate::kube::cache::cached_crds_to_domain(&crds);
        }
    }

    // Parse initial command/resource
    if let Some(ref cmd) = cli.command {
        if let Some(tab) = parse_resource_command(cmd) {
            app.resource_tab = tab;
        }
    }

    // Setup terminal FIRST — show TUI before any network IO
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Create event channel
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(500);

    // Track active log streaming task so we can cancel on Back
    let mut log_task: Option<JoinHandle<()>> = None;

    // Generation counters to prevent stale describe/yaml results from overwriting newer ones
    let describe_generation = Arc::new(AtomicU64::new(0));
    let yaml_generation = Arc::new(AtomicU64::new(0));

    // Now create the kube client — this may be slow (TLS, API validation)
    // but the TUI is already visible and showing a loading state.
    app.flash = Some(crate::app::FlashMessage::info("Connecting to cluster..."));
    // Render one frame so the user sees the TUI immediately
    terminal.draw(|f| crate::ui::draw(f, &mut app))?;

    let kube_client = match crate::kube::KubeClient::new(cli.context.as_deref(), kubeconfig).await {
        Ok(client) => client,
        Err(e) => {
            // Clean up terminal before printing error
            disable_raw_mode()?;
            execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
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

    // Spawn resource watcher
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

    let mut kube_client = kube_client;
    app.flash = None; // Clear "Connecting..." now that we're connected

    // Preload cached data for the initial tab (e.g., Pods) from daemon
    // so data appears instantly while the watcher does its first LIST.
    preload_cached_resources(&app, app.resource_tab, &event_tx);

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
                    let plural = spec.names.plural;
                    let scope = format!("{:?}", spec.scope).trim_matches('"').to_string();
                    let age = meta.creation_timestamp.map(|ts| ts.0);
                    crate::kube::resources::crds::KubeCrd { name, group, version, kind, plural, scope, age }
                }).collect();
                let _ = crd_tx.send(AppEvent::ResourceUpdate(
                    crate::event::ResourceUpdate::Crds(crds)
                )).await;
            }
        });
    }



    // Spawn metrics polling task (fetches pod/node metrics every 30s)
    let mut metrics_task: Option<JoinHandle<()>> = Some(crate::kube::metrics::spawn_metrics_poller(
        kube_client.client().clone(),
        event_tx.clone(),
    ));

    // Async event stream from crossterm (replaces blocking poll/read thread)
    let mut event_stream = EventStream::new();
    let mut tick_interval = tokio::time::interval(Duration::from_millis(cli.tick_rate));

    // Main event loop — only redraw when state changes
    let mut needs_redraw = true; // draw the first frame immediately
    loop {
        if needs_redraw {
            terminal.draw(|f| {
                crate::ui::draw(f, &mut app);
            })?;

            // Set cursor style based on input mode: bar for text input, block otherwise
            let in_input_mode = app.command_mode || app.scale_mode || app.port_forward_mode
                || app.filter.active
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
                                        begin_context_switch(&mut app, &event_tx, &ctx_name, &mut log_task);
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
                                        preload_cached_resources(&app, tab, &event_tx);
                                        resource_watcher.switch_tab(tab).await;
                                    } else if let Some((tab, ns)) = parse_resource_ns_command(&cmd) {
                                        // :deploy kube-system — switch resource AND namespace
                                        app.last_resource_tab = Some(app.resource_tab);
                                        app.resource_tab = tab;
                                        if ns != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut resource_watcher, &ns).await;
                                        }
                                        preload_cached_resources(&app, tab, &event_tx);
                                        resource_watcher.switch_tab(tab).await;
                                        app.flash = Some(crate::app::FlashMessage::info(format!(
                                            "{}({})", tab.label(), ns
                                        )));
                                    } else if let Some((tab, filter)) = parse_resource_filter_command(&cmd) {
                                        // :deploy /nginx — switch resource AND apply filter
                                        app.resource_tab = tab;
                                        preload_cached_resources(&app, tab, &event_tx);
                                        resource_watcher.switch_tab(tab).await;
                                        app.filter.text = filter.clone();
                                        app.filter.active = false;
                                        app.apply_filter(&filter);
                                    } else if let Some((crd, ns)) = parse_crd_ns_command(&cmd, &app) {
                                        // :clickhouseinstallation prod — CRD + namespace
                                        if ns != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut resource_watcher, &ns).await;
                                        }
                                        app.dynamic_resource_name = crd.kind.clone();
                                        app.dynamic_resource_api_resource = crd.name.clone();
                                        app.resource_tab = ResourceTab::DynamicResource;
                                        resource_watcher.watch_dynamic(
                                            crd.group.clone(),
                                            crd.version.clone(),
                                            crd.kind.clone(),
                                            crd.plural.clone(),
                                            crd.scope.clone(),
                                        ).await;
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Browsing CRD: {}({})", crd.kind, ns)
                                        ));
                                    } else if let Some(crd) = app.find_crd_by_name(&cmd) {
                                        // Dynamic CRD instance browsing (no namespace)
                                        app.dynamic_resource_name = crd.kind.clone();
                                        app.dynamic_resource_api_resource = crd.name.clone();
                                        app.resource_tab = ResourceTab::DynamicResource;
                                        resource_watcher.watch_dynamic(
                                            crd.group.clone(),
                                            crd.version.clone(),
                                            crd.kind.clone(),
                                            crd.plural.clone(),
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
                                        let kube_cl = kube_client.client().clone();
                                        let tx = event_tx.clone();
                                        tokio::spawn(async move {
                                            use ::kube::api::{Api, DynamicObject, Patch, PatchParams};
                                            let result = async {
                                                let (ar, scope) = crate::kube::describe::resolve_api_resource(&kube_cl, &resource).await?;
                                                let api: Api<DynamicObject> = if namespace.is_empty() || scope == "Cluster" {
                                                    Api::all_with(kube_cl.clone(), &ar)
                                                } else {
                                                    Api::namespaced_with(kube_cl.clone(), &namespace, &ar)
                                                };
                                                let patch = serde_json::json!({"spec": {"replicas": replicas}});
                                                api.patch(&name, &PatchParams::apply("k9rs"), &Patch::Merge(&patch)).await?;
                                                Ok::<(), anyhow::Error>(())
                                            }.await;
                                            match result {
                                                Ok(()) => {
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                                        format!("Scaled {}/{} to {} replicas", resource, name, replicas)
                                                    ))).await;
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                                        format!("Scale failed: {}", e)
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
                                        tokio::spawn(async move {
                                            let mut cmd = tokio::process::Command::new("kubectl");
                                            cmd.arg("port-forward").arg(&pn).arg(&ports);
                                            if !pod_ns.is_empty() { cmd.arg("-n").arg(&pod_ns); }
                                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                            // Suppress stdout/stderr so kubectl output doesn't
                                            // corrupt the TUI terminal.
                                            cmd.stdout(std::process::Stdio::null());
                                            cmd.stderr(std::process::Stdio::null());
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
                                        });
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
                            needs_redraw = true;
                            continue;
                        }

                        needs_redraw = true;
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
                                        let sh_status = cmd2.status();
                                        if sh_status.is_err() || sh_status.as_ref().map_or(false, |s| !s.success()) {
                                            app.flash = Some(crate::app::FlashMessage::error(
                                                format!("Shell failed for {}/{} — no bash or sh available", pod, container),
                                            ));
                                        }
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
                                ActionResult::None => {
                                    // Check for pending shell from ContainerSelect
                                    if let Some((pod, ns, container)) = app.pending_shell.take() {
                                        disable_raw_mode()?;
                                        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                                        terminal.show_cursor()?;

                                        print!("\x1b[2J\x1b[H");
                                        println!("Connecting to {}/{}...\n", pod, container);

                                        let mut cmd = std::process::Command::new("kubectl");
                                        cmd.arg("exec").arg("-it").arg(&pod).arg("-n").arg(&ns).arg("-c").arg(&container);
                                        if !app.context.is_empty() { cmd.arg("--context").arg(&app.context); }
                                        cmd.arg("--").arg("/bin/bash");
                                        let status = cmd.status();

                                        if status.is_err() || status.as_ref().map_or(false, |s| !s.success()) {
                                            let mut cmd2 = std::process::Command::new("kubectl");
                                            cmd2.arg("exec").arg("-it").arg(&pod).arg("-n").arg(&ns).arg("-c").arg(&container);
                                            if !app.context.is_empty() { cmd2.arg("--context").arg(&app.context); }
                                            cmd2.arg("--").arg("/bin/sh");
                                            let sh_status = cmd2.status();
                                            if sh_status.is_err() || sh_status.as_ref().map_or(false, |s| !s.success()) {
                                                app.flash = Some(crate::app::FlashMessage::error(
                                                    format!("Shell failed for {}/{} — no bash or sh available", pod, container),
                                                ));
                                            }
                                        }

                                        enable_raw_mode()?;
                                        execute!(terminal.backend_mut(), EnterAlternateScreen)?;
                                        terminal.clear()?;
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
                // Context switch results need &mut kube_client and &mut resource_watcher
                if let AppEvent::ContextSwitchResult { context, result } = event {
                    apply_context_switch_result(
                        &mut app, &mut kube_client, &mut resource_watcher,
                        &context, result,
                    ).await;
                    // Restart metrics poller with new client
                    if let Some(h) = metrics_task.take() { h.abort(); }
                    app.pod_metrics.clear();
                    app.node_metrics.clear();
                    metrics_task = Some(crate::kube::metrics::spawn_metrics_poller(
                        kube_client.client().clone(),
                        event_tx.clone(),
                    ));
                } else {
                    cache_flags.merge(apply_event(&mut app, event));
                }
                // Drain pending events before redrawing to batch log lines,
                // but cap at 200 to prevent UI freezes during massive bursts.
                let mut drained = 0;
                while drained < 200 {
                    match event_rx.try_recv() {
                        Ok(event) => {
                            if let AppEvent::ContextSwitchResult { context, result } = event {
                                apply_context_switch_result(
                                    &mut app, &mut kube_client, &mut resource_watcher,
                                    &context, result,
                                ).await;
                                // Restart metrics poller with new client
                                if let Some(h) = metrics_task.take() { h.abort(); }
                                app.pod_metrics.clear();
                                app.node_metrics.clear();
                                metrics_task = Some(crate::kube::metrics::spawn_metrics_poller(
                                    kube_client.client().clone(),
                                    event_tx.clone(),
                                ));
                            } else {
                                cache_flags.merge(apply_event(&mut app, event));
                            }
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }
                // Persist discovery cache when namespaces or CRDs are updated.
                // Send to daemon (shared) and save to disk (fallback).
                if cache_flags.namespaces || cache_flags.crds {
                    let ns_names: Vec<String> = app.data.namespaces.items
                        .iter()
                        .map(|ns| ns.name.clone())
                        .collect();
                    let cached_crds = crate::kube::cache::domain_crds_to_cached(&app.discovered_crds);
                    let ctx = crate::kube::cache::cache_key(&app.context, &app.cluster);
                    let cache = crate::kube::cache::build_cache(
                        &ctx,
                        &ns_names,
                        &cached_crds,
                    );
                    let ns_for_daemon = ns_names.clone();
                    let crds_for_daemon = cached_crds.clone();
                    tokio::spawn(async move {
                        // Send to daemon — it handles disk persistence internally.
                        // Fall back to direct disk save only if daemon is unavailable.
                        let daemon_ok = if let Some(mut dc) = crate::kube::daemon::DaemonClient::connect().await {
                            dc.put(&ctx, &ns_for_daemon, &crds_for_daemon).await
                        } else {
                            false
                        };
                        if !daemon_ok {
                            crate::kube::cache::save_cache(&cache).await;
                        }
                    });
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
    if let Some(ref sid) = session_id {
        data_backend.deregister_session(sid).await;
    }
    if let Some(handle) = log_task.take() {
        handle.abort();
    }
    if let Some(handle) = metrics_task.take() {
        handle.abort();
    }
    resource_watcher.stop().await;
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
            preload_cached_resources(app, app.resource_tab, event_tx);
            resource_watcher.switch_tab(app.resource_tab).await;
        }
        Action::PrevTab => {
            app.last_resource_tab = Some(app.resource_tab);
            app.prev_tab();
            preload_cached_resources(app, app.resource_tab, event_tx);
            resource_watcher.switch_tab(app.resource_tab).await;
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
                    preload_cached_resources(app, last_tab, event_tx);
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
            app.full_fetch_mode = !app.full_fetch_mode;
            resource_watcher.full_fetch.store(app.full_fetch_mode, std::sync::atomic::Ordering::Release);
            app.flash = Some(crate::app::FlashMessage::info(
                if app.full_fetch_mode { "Full-fetch mode ON (wait for complete list)" }
                else { "Full-fetch mode OFF (incremental loading)" }
            ));
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
            begin_context_switch(app, event_tx, &ctx, log_task);
        }
        Action::Refresh => {
            resource_watcher.switch_tab_force(app.resource_tab).await;
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
                if crate::kube::ops::try_copy_to_clipboard(&text) {
                    app.flash = Some(crate::app::FlashMessage::info(label));
                } else {
                    app.flash = Some(crate::app::FlashMessage::warn(
                        "No clipboard tool found (install xclip, xsel, wl-copy, or pbcopy)"
                    ));
                }
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
                    let count = batch.len();
                    app.clear_marks();
                    let kube_cl = client.clone();
                    let tx = event_tx.clone();
                    tokio::spawn(async move {
                        let mut ok_count = 0usize;
                        let mut last_err = String::new();
                        for (resource, name, namespace) in &batch {
                            match crate::kube::ops::restart_via_patch(&kube_cl, resource, name, namespace).await {
                                Ok(()) => { ok_count += 1; }
                                Err(e) => { last_err = e.to_string(); }
                            }
                        }
                        if ok_count == count {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                format!("Restarted {} resources", count)
                            ))).await;
                        } else {
                            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                format!("Restarted {}/{}, last error: {}", ok_count, count, last_err)
                            ))).await;
                        }
                    });
                } else {
                    // Single restart
                    let (resource, name, namespace) = get_selected_resource_info(app);
                    if !name.is_empty() {
                        let kube_cl = client.clone();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            match crate::kube::ops::restart_via_patch(&kube_cl, &resource, &name, &namespace).await {
                                Ok(()) => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                        format!("Restarted {}/{}", resource, name)
                                    ))).await;
                                }
                                Err(e) => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                        format!("Restart failed: {}", e)
                                    ))).await;
                                }
                            }
                        });
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
                // Execute the actual delete via kube API
                app.kubectl_cache.clear();
                let batch = std::mem::take(&mut app.pending_batch_delete);
                if !batch.is_empty() {
                    // Batch delete: iterate through all marked items
                    let count = batch.len();
                    app.clear_marks();
                    let client = client.clone();
                    let event_tx = event_tx.clone();
                    tokio::spawn(async move {
                        let mut ok_count = 0usize;
                        let mut last_err = String::new();
                        for (resource_kind, res_name, res_ns) in &batch {
                            match crate::kube::ops::execute_delete(&client, resource_kind, res_name, res_ns).await {
                                Ok(()) => { ok_count += 1; }
                                Err(e) => { last_err = e.to_string(); }
                            }
                        }
                        if ok_count == count {
                            let _ = event_tx
                                .send(AppEvent::Flash(crate::app::FlashMessage::info(
                                    format!("Deleted {} resources", count),
                                )))
                                .await;
                        } else {
                            let _ = event_tx
                                .send(AppEvent::Flash(crate::app::FlashMessage::error(
                                    format!("Deleted {}/{}, last error: {}", ok_count, count, last_err),
                                )))
                                .await;
                        }
                    });
                } else {
                    // Single delete
                    let (resource, name, namespace) = get_selected_resource_info(app);
                    if !name.is_empty() {
                        let client = client.clone();
                        let event_tx = event_tx.clone();
                        let resource_kind = resource.clone();
                        let res_name = name.clone();
                        let res_ns = namespace.clone();
                        tokio::spawn(async move {
                            let result = crate::kube::ops::execute_delete(&client, &resource_kind, &res_name, &res_ns).await;
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
            handle_previous_logs(app, event_tx, log_task);
        }
        Action::ShowNode => {
            if app.resource_tab == ResourceTab::Pods {
                if let Some(pod) = app.data.pods.selected_item() {
                    let node = pod.node.clone();
                    if !node.is_empty() {
                        app.last_resource_tab = Some(app.resource_tab);
                        app.resource_tab = ResourceTab::Nodes;
                        preload_cached_resources(app, ResourceTab::Nodes, event_tx);
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
                preload_cached_resources(app, app.resource_tab, event_tx);
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
                ("limits/limitrange/limitranges", "LimitRanges"),
                ("quota/resourcequota/resourcequotas", "ResourceQuotas"),
                ("pdb/poddisruptionbudget/pdb", "PodDisruptionBudgets"),
                ("crd/crds/customresourcedefinition", "CustomResourceDefinitions"),
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
                let handle = crate::kube::ops::spawn_log_stream(
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
        Action::FlashInfo(msg) => {
            app.flash = Some(crate::app::FlashMessage::info(msg));
        }
    }
    ActionResult::None
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
            begin_context_switch(app, event_tx, &ctx_name, log_task);
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
        let handle = crate::kube::ops::spawn_log_stream(
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
                preload_cached_resources(app, ResourceTab::Pods, event_tx);
                // Use switch_tab_force — if we were already on Pods tab, switch_tab
                // returns early without restarting the watcher, leaving an empty table.
                resource_watcher.switch_tab_force(ResourceTab::Pods).await;
                // Escape regex metacharacters so dots/brackets in names don't
                // produce an invalid regex that silently matches nothing.
                let filter = format!("{}-", regex::escape(&name));
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
                preload_cached_resources(app, ResourceTab::Pods, event_tx);
                resource_watcher.switch_tab_force(ResourceTab::Pods).await;
                let filter = name.clone();
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
                app.dynamic_resource_api_resource = crd.name.clone();
                app.resource_tab = ResourceTab::DynamicResource;
                resource_watcher.watch_dynamic(
                    crd.group.clone(),
                    crd.version.clone(),
                    crd.kind.clone(),
                    crd.plural.clone(),
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
    client: &::kube::Client,
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

        // Fetch describe natively via kube-rs (reuses existing connection -- much faster
        // than spawning kubectl which re-reads kubeconfig + does TLS from scratch).
        // Falls back to kubectl for resource types that fail native fetch.
        let tx = event_tx.clone();
        let kube_client = client.clone();
        let context = app.context.clone();
        let res = resource;
        let n = name;
        let ns = namespace;
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_describe_native(&kube_client, &res, &n, &ns, &context).await;
            if gen_check.load(Ordering::SeqCst) == gen {
                let _ = tx
                    .send(AppEvent::ResourceUpdate(ResourceUpdate::Describe(content)))
                    .await;
            }
        });
    }
}

fn handle_yaml(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
    client: &::kube::Client,
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

        // Fetch YAML natively via kube-rs (reuses existing connection — much faster
        // than spawning kubectl which re-reads kubeconfig + does TLS from scratch).
        // Falls back to kubectl for resource types that need special handling.
        let tx = event_tx.clone();
        let kube_client = client.clone();
        let res = resource;
        let n = name;
        let ns = namespace;
        let context = app.context.clone();
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_yaml_native(&kube_client, &res, &n, &ns, &context).await;
            if gen_check.load(Ordering::SeqCst) == gen {
                let _ = tx
                    .send(AppEvent::ResourceUpdate(ResourceUpdate::Yaml(content)))
                    .await;
            }
        });
    }
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
    let handle = crate::kube::ops::spawn_log_stream(
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

    let handle = crate::kube::ops::spawn_log_stream(
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

/// Flags indicating which cacheable data was updated by a resource update.
/// Try loading cached resource data from the daemon for instant tab display.
/// Spawns in background; if data arrives it's sent as a CachedResources event.
fn preload_cached_resources(app: &App, tab: ResourceTab, event_tx: &mpsc::Sender<AppEvent>) {
    let cache_ctx = crate::kube::cache::cache_key(&app.context, &app.cluster);
    let ctx = app.context.clone();
    let ns = app.selected_ns.clone();
    let rt = tab.label().to_string();
    let tx = event_tx.clone();
    tokio::spawn(async move {
        if let Some(mut dc) = crate::kube::daemon::DaemonClient::connect().await {
            if let Some(json) = dc.get_resources(&cache_ctx, &ns, &rt).await {
                let _ = tx.send(AppEvent::CachedResources {
                    resource_type: rt,
                    data: json,
                    context: ctx.clone(),
                    namespace: ns.clone(),
                }).await;
            }
        }
    });
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
fn apply_event(app: &mut App, event: AppEvent) -> CacheUpdateFlags {
    match event {
        AppEvent::ResourceUpdate(update) => apply_resource_update(app, update),
        AppEvent::Error(msg) => {
            app.flash = Some(crate::app::FlashMessage::error(msg));
            CacheUpdateFlags::default()
        }
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
        // Cached resource data from daemon — only apply if table is currently empty
        // (don't overwrite live watcher data that may have arrived first).
        // Also reject stale data from a pre-switch request if the user changed
        // context or namespace before the daemon response arrived.
        AppEvent::CachedResources { resource_type, data, context, namespace } => {
            if context == app.context && namespace == app.selected_ns {
                apply_cached_resources(app, &resource_type, &data);
            }
            CacheUpdateFlags::default()
        }
    }
}

/// Push serialized resource data to the daemon cache in the background.
macro_rules! cache_resource_update {
    ($app:expr, $items:expr, $resource_type:expr) => {
        {
            let json_result = serde_json::to_string(&$items);
            match json_result {
                Ok(json) => {
                    let ctx = crate::kube::cache::cache_key(&$app.context, &$app.cluster);
                    let ns = $app.selected_ns.clone();
                    let rt = $resource_type.to_string();
                    tokio::spawn(async move {
                        if let Some(mut dc) = crate::kube::daemon::DaemonClient::connect().await {
                            dc.put_resources(&ctx, &ns, &rt, &json).await;
                        }
                    });
                }
                Err(e) => {
                    tracing::debug!("cache_resource_update: {} serialize failed: {}", $resource_type, e);
                }
            }
        }
    };
}

fn apply_resource_update(app: &mut App, update: ResourceUpdate) -> CacheUpdateFlags {
    let mut flags = CacheUpdateFlags::default();
    match update {
        ResourceUpdate::Pods(items) => {
            cache_resource_update!(app, items, "Pods");
            app.data.pods.set_items_filtered(items);
            app.apply_pod_metrics();
        }
        ResourceUpdate::Deployments(items) => {
            cache_resource_update!(app, items, "Deploy");
            app.data.deployments.set_items_filtered(items);
        }
        ResourceUpdate::Services(items) => {
            cache_resource_update!(app, items, "Svc");
            app.data.services.set_items_filtered(items);
        }
        ResourceUpdate::Nodes(items) => {
            cache_resource_update!(app, items, "Nodes");
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
            cache_resource_update!(app, items, "STS");
            app.data.statefulsets.set_items_filtered(items);
        }
        ResourceUpdate::DaemonSets(items) => {
            cache_resource_update!(app, items, "DS");
            app.data.daemonsets.set_items_filtered(items);
        }
        ResourceUpdate::Jobs(items) => {
            cache_resource_update!(app, items, "Jobs");
            app.data.jobs.set_items_filtered(items);
        }
        ResourceUpdate::CronJobs(items) => {
            cache_resource_update!(app, items, "CronJobs");
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
    flags
}

/// Apply cached resource data from the daemon. Only populates a table if it is
/// currently empty (has_data == false), so live watcher data is never overwritten.
fn apply_cached_resources(app: &mut App, resource_type: &str, data: &str) {
    use crate::kube::resources::{
        pods::KubePod, deployments::KubeDeployment, services::KubeService,
        statefulsets::KubeStatefulSet, daemonsets::KubeDaemonSet, jobs::KubeJob,
        cronjobs::KubeCronJob, nodes::KubeNode,
    };

    macro_rules! apply_if_empty {
        ($table:expr, $ty:ty, $data:expr) => {
            // Apply cached data if the table is currently empty.
            // This covers: first launch, tab switch (watcher restarting),
            // context/namespace switch (data cleared).
            // Once the live watcher sends data, the table is non-empty
            // and the cache won't overwrite it.
            if $table.items.is_empty() {
                if let Ok(items) = serde_json::from_str::<Vec<$ty>>($data) {
                    if !items.is_empty() {
                        $table.set_items_filtered(items);
                    }
                }
            }
        };
    }

    match resource_type {
        "Pods" => apply_if_empty!(app.data.pods, KubePod, data),
        "Deploy" => apply_if_empty!(app.data.deployments, KubeDeployment, data),
        "Svc" => apply_if_empty!(app.data.services, KubeService, data),
        "STS" => apply_if_empty!(app.data.statefulsets, KubeStatefulSet, data),
        "DS" => apply_if_empty!(app.data.daemonsets, KubeDaemonSet, data),
        "Jobs" => apply_if_empty!(app.data.jobs, KubeJob, data),
        "CronJobs" => apply_if_empty!(app.data.cronjobs, KubeCronJob, data),
        "Nodes" => apply_if_empty!(app.data.nodes, KubeNode, data),
        _ => {} // Unknown resource type, ignore
    }
}

/// Perform a context switch: update app state, clear data, replace the watcher client.
/// Returns `true` on success, `false` on failure (with a flash message set).
/// Begin a context switch. Immediately clears UI state and shows a flash message,
/// then spawns the slow client creation in a background task. The result arrives
/// via `AppEvent::ContextSwitchResult` and is applied by `apply_context_switch_result`.
fn begin_context_switch(
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
    ctx_name: &str,
    log_task: &mut Option<JoinHandle<()>>,
) {
    // Cancel any active log stream — it belongs to the old context
    if let Some(handle) = log_task.take() {
        handle.abort();
        app.logs.streaming = false;
    }
    // Immediate: clear data and show feedback — keeps TUI responsive.
    // Set both context AND cluster atomically so cache_resource_update!
    // computes the correct cache key during the transition window.
    app.context = ctx_name.to_string();
    let (cluster, user) = crate::kube::KubeClient::context_info(ctx_name);
    app.cluster = cluster;
    app.user = user;
    app.clear_data();
    app.kubectl_cache.clear();
    app.discovered_crds.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    app.filter.active = false;
    app.filter.text.clear();
    app.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...",
        ctx_name
    )));

    // Spawn slow client creation in background
    let ctx = ctx_name.to_string();
    let tx = event_tx.clone();
    tokio::spawn(async move {
        let result = crate::kube::KubeClient::create_client_for_context(&ctx).await;
        let _ = tx
            .send(AppEvent::ContextSwitchResult {
                context: ctx,
                result: result.map_err(|e| e.to_string()),
            })
            .await;
    });
}

/// Apply the result of a background context switch.
async fn apply_context_switch_result(
    app: &mut App,
    kube_client: &mut crate::kube::KubeClient,
    resource_watcher: &mut crate::kube::watcher::ResourceWatcher,
    ctx_name: &str,
    result: Result<::kube::Client, String>,
) {
    // Discard stale result if user already switched to a different context
    if ctx_name != app.context {
        return;
    }
    match result {
        Ok(new_client) => {
            kube_client.set_client(new_client.clone(), ctx_name);
            let (cluster, user) = crate::kube::KubeClient::context_info(ctx_name);
            app.cluster = cluster;
            app.user = user;
            resource_watcher
                .replace_client(new_client, app.selected_ns.clone())
                .await;
            // Read kubeconfig once for all contexts
            let contexts_info = crate::kube::KubeClient::all_context_info();
            let updated: Vec<crate::app::KubeContext> = app
                .contexts
                .iter()
                .map(|name| {
                    let cluster = contexts_info.get(name.as_str())
                        .map(|(c, _)| c.clone())
                        .unwrap_or_default();
                    crate::app::KubeContext {
                        name: name.clone(),
                        cluster,
                        is_current: name == ctx_name,
                    }
                })
                .collect();
            app.data.contexts.set_items(updated);
            app.flash = Some(crate::app::FlashMessage::info(format!(
                "Switched to context: {}",
                ctx_name
            )));
        }
        Err(e) => {
            // Restore context/cluster to match the still-running kube_client,
            // so the app state is consistent with the actual active client.
            let active_ctx = kube_client.context().to_string();
            let (cluster, user) = crate::kube::KubeClient::context_info(&active_ctx);
            app.context = active_ctx;
            app.cluster = cluster;
            app.user = user;
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


