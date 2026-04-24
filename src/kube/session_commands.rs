use anyhow::Result;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use tokio::sync::mpsc;

use crate::app::App;
use crate::kube::protocol::ResourceId;

/// What kind of interactive command we're about to suspend the TUI for.
/// Determines pre-launch UI (the "Connecting…" box for shells) and
/// post-launch failure handling. Replaces the previous trick of inspecting
/// `args.first() == "exec"` to guess the kind from argv.
pub(crate) enum InteractiveKind {
    /// `$EDITOR /tmp/k9rs-edit-...`. No connecting box, no flash on
    /// non-zero exit (the edit flow surfaces its own errors).
    Editor,
}

/// Suspend the TUI and run an interactive command directly (with bash->sh
/// fallback for shell). The caller hands in a typed `InteractiveKind` so
/// this function doesn't have to inspect `args` to guess what it's about
/// to launch.
pub(crate) async fn run_interactive_local(
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    _app: &mut App,
    command: &str,
    args: &[String],
    _kind: InteractiveKind,
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
) -> Result<Option<std::process::ExitStatus>> {
    use crate::kube::session::SuspendGuard;

    // The guard suspends the TUI and restores it on drop.
    let mut guard = SuspendGuard::new(terminal, input_suspend, input_suspend_ack).await?;

    // Interactive subprocesses need a normal terminal.
    disable_raw_mode()?;
    crossterm::execute!(
        guard.terminal_mut().backend_mut(),
        crossterm::cursor::Show,
        crossterm::cursor::SetCursorStyle::DefaultUserShape,
    )?;

    // Run the subprocess on a blocking thread so the tokio runtime's
    // worker threads stay unblocked.
    let command_owned = command.to_string();
    let args_owned: Vec<String> = args.to_vec();
    let status = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(command_owned);
        cmd.args(args_owned);
        cmd.status()
    })
    .await
    .unwrap_or_else(|join_err| {
        Err(std::io::Error::other(join_err.to_string()))
    });

    // Restore raw mode + hide cursor before guard drops.
    enable_raw_mode()?;
    crossterm::execute!(
        guard.terminal_mut().backend_mut(),
        crossterm::cursor::Hide,
    )?;

    // Guard drops → enter alternate screen, clear, resume input.
    Ok(status.ok())
}

/// A classified `:command`. Produced by [`parse_command_input`] and
/// consumed by [`handle_command_submit`]. The grammar priority is
/// expressed inside the parser; the dispatcher is a single exhaustive
/// match on this enum, so reordering two rules is a one-line change in
/// the parser and the dispatcher is drift-free.
///
/// The variants are ordered by priority in [`parse_command_input`] —
/// listing them in the same order here makes the grammar self-documenting.
pub(crate) enum ParsedCommand {
    /// `:q`, `:quit`, `:exit`, `:q!`
    Quit,
    /// `:help`, `:h`, `:?`
    Help,
    /// `:alias`, `:aliases`, `:a`
    Aliases,
    /// `:overview`, `:home`
    Overview,
    /// `:ctx`, `:context`, `:contexts`
    ContextList,
    /// `:ctx <name>`, `:context <name>`
    ContextSwitch(crate::kube::protocol::ContextName),
    /// `:ns <name>`, `:namespace <name>`
    NamespaceSwitch(crate::kube::protocol::Namespace),
    /// `:pods /Nginx` — drill into a resource tab with a grep filter.
    /// Matched before `ResourceInNamespace` so `pods /Nginx` never parses
    /// as namespace `/Nginx`.
    ResourceFilter { rid: ResourceId, filter: String },
    /// `:deploy kube-system` — browse a resource in a specific namespace.
    ResourceInNamespace { rid: ResourceId, namespace: crate::kube::protocol::Namespace },
    /// `:pods` — plain resource alias.
    Resource(ResourceId),
    /// `:clickhouseinstallation prod` — namespaced CRD with namespace arg.
    CrdInNamespace { crd: crate::app::CrdInfo, namespace: crate::kube::protocol::Namespace },
    /// Fallback: anything that isn't a recognized shape. The dispatcher
    /// tries to resolve it as a known CRD via discovery cache, then falls
    /// back to a typed unresolved placeholder the daemon can resolve.
    CrdCandidate(String),
}

/// Classify a `:command` string into a [`ParsedCommand`]. The caller
/// passes both the raw case-preserving string (for filter text that must
/// keep its case) and the lowercased form (for alias matching). `app`
/// is threaded in for the CRD-discovery lookup.
///
/// Rule ordering is expressed here in one linear function: whichever
/// rule matches first wins. The most common source of bugs in the old
/// else-if chain was reordering pairs of rules without realizing one
/// intercepted the other's input shape (pass 11 caught exactly this
/// between `resource filter` and `resource ns`). Extracting the grammar
/// into a single function makes the ordering visible at a glance and
/// testable in isolation.
pub(crate) fn parse_command_input(raw_cmd: &str, cmd: &str, app: &App) -> ParsedCommand {
    // 1. Static command words.
    if matches!(cmd, "q" | "quit" | "exit" | "q!") { return ParsedCommand::Quit; }
    if matches!(cmd, "help" | "h" | "?") { return ParsedCommand::Help; }
    if matches!(cmd, "alias" | "aliases" | "a") { return ParsedCommand::Aliases; }
    if matches!(cmd, "overview" | "home") { return ParsedCommand::Overview; }
    if matches!(cmd, "ctx" | "context" | "contexts") { return ParsedCommand::ContextList; }

    // 2. `:ctx <name>` / `:context <name>`. The prefix lives in `cmd` (the
    // lowercased form) so alias matching is case-insensitive, but the
    // *value* comes from `raw_cmd` so context names keep their case.
    // Slicing `raw_cmd` by the ASCII prefix length is safe (the prefix
    // is always ASCII; it doesn't shift under lowercasing).
    if let Some(value) = strip_ascii_prefix(raw_cmd, cmd, &["ctx ", "context "]) {
        return ParsedCommand::ContextSwitch(value.into());
    }

    // 3. `:ns <name>` / `:namespace <name>`. Same case-preserving slice.
    if let Some(value) = strip_ascii_prefix(raw_cmd, cmd, &["ns ", "namespace "]) {
        return ParsedCommand::NamespaceSwitch(
            crate::kube::protocol::Namespace::from_user_command(value),
        );
    }

    // 4. Plain resource alias (bare `:pods`, `:deploy`, etc.). Matches
    // before multi-token shapes because a single-token alias can't hit
    // any of them (none have a space).
    if let Some(rid) = ResourceId::from_alias(cmd) {
        return ParsedCommand::Resource(rid);
    }

    // 5. `<resource> /<filter>` — resource drill with grep filter. Runs
    // BEFORE the `<resource> <ns>` rule below so `pods /Nginx` never
    // parses as namespace `/Nginx`. Uses the *raw* command so the filter
    // text keeps its case (smartcase regex).
    if let Some(parsed) = parse_resource_filter(raw_cmd) {
        return ParsedCommand::ResourceFilter { rid: parsed.0, filter: parsed.1 };
    }

    // 6. `<resource> <ns>` — browse a resource in a specific namespace.
    // Rejects values starting with `/` (those would have matched rule 5)
    // and cluster-scoped resources (which have no namespace concept).
    if let Some(parsed) = parse_resource_in_namespace(cmd) {
        return ParsedCommand::ResourceInNamespace { rid: parsed.0, namespace: parsed.1 };
    }

    // 7. `<crd> <ns>` — namespaced CRD browse. Consults the runtime
    // discovery cache on `app`.
    if let Some(parsed) = parse_crd_in_namespace(cmd, app) {
        return ParsedCommand::CrdInNamespace { crd: parsed.0, namespace: parsed.1 };
    }

    // 8. Fallback: treat as a CRD name. The dispatcher resolves via the
    // discovery cache, or falls back to a typed unresolved placeholder.
    ParsedCommand::CrdCandidate(cmd.to_string())
}

/// Internal: check if `cmd` (the lowercased form) starts with any of
/// `prefixes`, and if so, return the corresponding tail of `raw_cmd`
/// (the case-preserving form) with surrounding whitespace trimmed.
///
/// All prefixes must be ASCII. That's the load-bearing invariant: ASCII
/// lowercasing is a byte-for-byte no-op, so slicing `raw_cmd` at the
/// same offset that matched in `cmd` lands at the exact same character.
/// If a prefix contained non-ASCII, lowercasing could change its byte
/// length and the slice would point into the middle of a UTF-8 rune.
fn strip_ascii_prefix<'a>(raw_cmd: &'a str, cmd: &str, prefixes: &[&str]) -> Option<&'a str> {
    for p in prefixes {
        debug_assert!(p.is_ascii(), "command prefix must be ASCII: {:?}", p);
        if cmd.starts_with(p) {
            let tail = raw_cmd.get(p.len()..)?.trim();
            if tail.is_empty() {
                return None;
            }
            return Some(tail);
        }
    }
    None
}

/// Internal: parse `<resource> /<filter>`. Returns `(rid, filter_text)`
/// or None on shape mismatch / empty filter / unknown alias.
fn parse_resource_filter(raw_cmd: &str) -> Option<(ResourceId, String)> {
    let slash_pos = raw_cmd.find('/')?;
    let resource_part = raw_cmd[..slash_pos].trim();
    let filter_part = raw_cmd[slash_pos + 1..].trim();
    if filter_part.is_empty() {
        return None;
    }
    let rid = ResourceId::from_alias(&resource_part.to_lowercase())?;
    Some((rid, filter_part.to_string()))
}

/// Internal: parse `<resource> <ns>`. Returns None for cluster-scoped
/// resources (no namespace concept) or ns values beginning with `/`
/// (those belong to the filter grammar, checked upstream of here).
fn parse_resource_in_namespace(cmd: &str) -> Option<(ResourceId, crate::kube::protocol::Namespace)> {
    let (resource, ns) = cmd.split_once(' ')?;
    let resource = resource.trim();
    let ns = ns.trim();
    if ns.is_empty() || ns.starts_with('/') {
        return None;
    }
    let rid = ResourceId::from_alias(resource)?;
    if rid.is_cluster_scoped() {
        return None;
    }
    Some((rid, crate::kube::protocol::Namespace::from_user_command(ns)))
}

/// Internal: parse `<crd> <ns>` against the discovery cache on `app`.
fn parse_crd_in_namespace(cmd: &str, app: &App) -> Option<(crate::app::CrdInfo, crate::kube::protocol::Namespace)> {
    let (crd_part, ns) = cmd.split_once(' ')?;
    let crd_part = crd_part.trim();
    let ns = ns.trim();
    if ns.is_empty() {
        return None;
    }
    let crd = app.find_crd_by_name(crd_part)?;
    if crd.scope == crate::kube::protocol::ResourceScope::Cluster {
        return None;
    }
    Some((crd, crate::kube::protocol::Namespace::from_user_command(ns)))
}

/// Legacy shim kept for a few callers (CLI dispatch, the startup
/// `--command` flag) that want the single-alias form without going
/// through the full parser. New call sites should use
/// [`parse_command_input`] instead.
pub(crate) fn parse_resource_command(cmd: &str) -> Option<ResourceId> {
    ResourceId::from_alias(cmd)
}

// ---------------------------------------------------------------------------
// Input mode handlers — extracted from the main event loop
// ---------------------------------------------------------------------------

use crossterm::event::{KeyCode, KeyEvent};

use crate::app::InputMode;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, apply_nav_change};
use crate::kube::session_actions::do_switch_namespace;

/// Maximum command history entries.
const COMMAND_HISTORY_LIMIT: usize = 50;

/// Handle a keystroke while in `:command` mode. Returns true if consumed.
pub(crate) fn handle_command_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) -> bool {
    if !matches!(app.ui.input_mode, InputMode::Command { .. }) {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.ui.input_mode = InputMode::Normal;
        }
        KeyCode::Enter => {
            let raw_cmd = if let InputMode::Command { ref input, .. } = app.ui.input_mode {
                input.trim().to_string()
            } else {
                String::new()
            };
            let cmd = raw_cmd.to_lowercase();
            if !cmd.is_empty() {
                app.command_history.push(raw_cmd.clone());
                if app.command_history.len() > COMMAND_HISTORY_LIMIT {
                    app.command_history.remove(0);
                }
            }
            app.ui.input_mode = InputMode::Normal;
            handle_command_submit(app, &raw_cmd, &cmd, data_source, event_tx);
        }
        KeyCode::Tab => {
            app.accept_completion();
        }
        KeyCode::Up => {
            if !app.command_history.is_empty() {
                if let InputMode::Command { ref mut input, ref mut history_index } = app.ui.input_mode {
                    let idx = match *history_index {
                        None => app.command_history.len() - 1,
                        Some(i) => i.saturating_sub(1),
                    };
                    *history_index = Some(idx);
                    *input = app.command_history[idx].clone();
                }
            }
        }
        KeyCode::Down => {
            if let InputMode::Command { ref mut input, ref mut history_index } = app.ui.input_mode {
                if let Some(idx) = *history_index {
                    if idx + 1 < app.command_history.len() {
                        *history_index = Some(idx + 1);
                        *input = app.command_history[idx + 1].clone();
                    } else {
                        *history_index = None;
                        input.clear();
                    }
                }
            }
        }
        KeyCode::Backspace => {
            if let InputMode::Command { ref mut input, .. } = app.ui.input_mode {
                input.pop();
            }
        }
        KeyCode::Char(c) => {
            if let InputMode::Command { ref mut input, .. } = app.ui.input_mode {
                input.push(c);
            }
        }
        _ => {}
    }
    true
}

/// Dispatch a submitted `:command` string. Grammar is classified once via
/// [`parse_command_input`] into a [`ParsedCommand`]; this function is a
/// single exhaustive match over the classification. Adding a new command
/// form means adding a variant to `ParsedCommand`, a rule in the parser,
/// and an arm here — drift between any two becomes a compile error.
fn handle_command_submit(
    app: &mut App,
    raw_cmd: &str,
    cmd: &str,
    data_source: &mut ClientSession,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) {
    use crate::kube::session_actions::handle_action;
    use crate::kube::session_actions::begin_context_switch;

    if cmd.is_empty() {
        return;
    }

    match parse_command_input(raw_cmd, cmd, app) {
        ParsedCommand::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        ParsedCommand::Help => {
            app.navigate_to(crate::app::Route::Help);
        }
        ParsedCommand::Aliases => {
            handle_action(
                app, crate::app::actions::Action::ShowAliases, event_tx,
                data_source,
            );
        }
        ParsedCommand::Overview => {
            app.route = crate::app::Route::Overview;
        }
        ParsedCommand::ContextList => {
            app.navigate_to(crate::app::Route::Contexts);
        }
        ParsedCommand::ContextSwitch(ctx_name) => {
            begin_context_switch(app, data_source, &ctx_name);
        }
        ParsedCommand::NamespaceSwitch(ns) => {
            do_switch_namespace(app, data_source, ns);
        }
        ParsedCommand::Resource(rid) => {
            app.route = crate::app::Route::Resources;
            if rid.is_cluster_scoped() && !app.kube.selected_ns.is_all() {
                // Just update the namespace — don't call do_switch_namespace
                // which would nav.reset + subscribe (creating a subscription
                // that nav.reset below immediately replaces = broken pipe).
                app.kube.selected_ns = crate::kube::protocol::Namespace::All;
            }
            let change = app.nav.reset(rid);
            *app.nav.filter_input_mut() = Default::default();
            apply_nav_change(app, data_source, change);
        }
        ParsedCommand::ResourceFilter { rid, filter } => {
            app.route = crate::app::Route::Resources;
            let change = app.nav.reset(rid.clone());
            apply_nav_change(app, data_source, change);
            let change = app.nav.push(crate::app::nav::NavStep::new(
                rid,
                Some(crate::app::nav::NavFilter::Grep(
                    crate::app::nav::CompiledGrep::new(filter),
                )),
            ));
            apply_nav_change(app, data_source, change);
            *app.nav.filter_input_mut() = Default::default();
            app.reapply_nav_filters();
        }
        ParsedCommand::ResourceInNamespace { rid, namespace } => {
            app.route = crate::app::Route::Resources;
            let change = app.nav.reset(rid.clone());
            *app.nav.filter_input_mut() = Default::default();
            if namespace != app.kube.selected_ns {
                // `do_switch_namespace` calls `nav.reset(root_rid)` and
                // opens a fresh substream for the new namespace itself,
                // so the earlier `change` from our own `reset(rid)` is
                // stale (its stream was already dropped and replaced).
                // Applying it would just churn through clear+resubscribe
                // a second time on the same rid; skip it.
                do_switch_namespace(app, data_source, namespace.clone());
            } else {
                apply_nav_change(app, data_source, change);
            }
            app.ui.flash = Some(crate::app::FlashMessage::info(format!(
                "{}({})", rid.short_label(), namespace.display()
            )));
        }
        ParsedCommand::CrdInNamespace { crd, namespace } => {
            app.route = crate::app::Route::Resources;
            if namespace != app.kube.selected_ns {
                do_switch_namespace(app, data_source, namespace.clone());
            }
            let crd_rid = ResourceId::crd(
                crd.group.clone(), crd.version.clone(),
                crd.kind.clone(), crd.plural.clone(), crd.scope,
            );
            let change = app.nav.reset(crd_rid);
            apply_nav_change(app, data_source, change);
            app.ui.flash = Some(crate::app::FlashMessage::info(
                format!("Browsing CRD: {}({})", crd.kind, namespace.display())
            ));
        }
        ParsedCommand::CrdCandidate(name) => {
            // Unknown shape — try as CRD via the discovery cache, or fall
            // back to a typed unresolved placeholder the daemon will resolve.
            let crd_info = app.find_crd_by_name(&name);
            let (crd_rid, kind_label, scope) = if let Some(crd) = &crd_info {
                let scope = crd.scope;
                let kind = crd.kind.clone();
                (
                    ResourceId::crd(crd.group.clone(), crd.version.clone(), crd.kind.clone(), crd.plural.clone(), scope),
                    kind,
                    scope,
                )
            } else {
                (
                    ResourceId::CrdUnresolved(name.clone()),
                    name,
                    crate::kube::protocol::ResourceScope::Namespaced,
                )
            };
            app.route = crate::app::Route::Resources;
            if scope == crate::kube::protocol::ResourceScope::Cluster && !app.kube.selected_ns.is_all() {
                do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::All);
            }
            let change = app.nav.reset(crd_rid);
            apply_nav_change(app, data_source, change);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Browsing: {}", kind_label)));
        }
    }
}

/// Handle a keystroke while a generic form dialog is open. Returns true if
/// consumed. Replaces the old `handle_scale_key` and the inline port-forward
/// dialog block in `session.rs` — both flowed into one place once `FormDialog`
/// became a single client-side type with typed `FormFieldKind` variants.
///
/// Key bindings:
///   - Esc                       → cancel (close without dispatching)
///   - Tab / Down                → focus next field (or OK button)
///   - BackTab / Up              → focus previous field
///   - Enter                     → submit (collect values, dispatch typed command)
///   - Left / Right              → cycle Select field options
///   - Backspace                 → delete last char from text/number/port field
///   - Char                      → append to text field; digit-only for number/port
pub(crate) fn handle_form_dialog_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
) -> bool {
    use crate::app::FormFieldKind;
    if app.ui.form_dialog.is_none() {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.ui.form_dialog = None;
        }
        KeyCode::Tab | KeyCode::Down => {
            if let Some(ref mut d) = app.ui.form_dialog {
                d.focus_next();
            }
        }
        KeyCode::BackTab | KeyCode::Up => {
            if let Some(ref mut d) = app.ui.form_dialog {
                d.focus_prev();
            }
        }
        KeyCode::Enter => {
            // Take the dialog out so the dispatcher can consume it without
            // borrowing app twice.
            if let Some(dialog) = app.ui.form_dialog.take() {
                dispatch_form_submit(app, data_source, dialog);
            }
        }
        KeyCode::Left | KeyCode::Right => {
            if let Some(ref mut d) = app.ui.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    if let FormFieldKind::Select { ref options } = field.kind {
                        if !options.is_empty() {
                            let n = options.len();
                            let cur: usize = field.value.parse().unwrap_or(0);
                            let new = if key.code == KeyCode::Left {
                                (cur + n - 1) % n
                            } else {
                                (cur + 1) % n
                            };
                            field.value = new.to_string();
                        }
                    }
                }
            }
        }
        KeyCode::Backspace => {
            if let Some(ref mut d) = app.ui.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    if field.is_text_input() {
                        field.value.pop();
                    }
                }
            }
        }
        KeyCode::Char(c) => {
            if let Some(ref mut d) = app.ui.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    let accept = match field.kind {
                        FormFieldKind::Text { max_len } => {
                            max_len.is_none_or(|m| field.value.chars().count() < m)
                        }
                        FormFieldKind::Number { .. } | FormFieldKind::Port => c.is_ascii_digit(),
                        FormFieldKind::Select { .. } => false,
                    };
                    if accept {
                        field.value.push(c);
                    }
                }
            }
        }
        _ => {}
    }
    true
}

/// Dispatch a submitted form dialog to the appropriate typed
/// `SessionCommand`. Reads the collected field values (string-typed,
/// validated by the dialog widget per `FormFieldKind`) and produces a
/// strongly-typed wire payload. Centralized so adding a new operation
/// kind only adds an arm here.
fn dispatch_form_submit(
    app: &mut App,
    data_source: &mut ClientSession,
    dialog: crate::app::FormDialog,
) {
    use crate::app::{FormDialog, FormFieldKind, FormSubmit};

    let FormDialog { submit, target, fields, .. } = dialog;
    match submit {
        FormSubmit::Scale => {
            let replicas_str = fields
                .iter()
                .find(|f| f.name == crate::kube::protocol::form_field_name::REPLICAS)
                .map(|f| f.value.trim().to_string())
                .unwrap_or_default();
            match replicas_str.parse::<u32>() {
                Ok(replicas) => {
                    app.kube.kubectl_cache.clear();
                    ds_try!(app, data_source.scale(&target, replicas));
                    app.ui.flash = Some(crate::app::FlashMessage::info(
                        format!("Scaling to {} replicas", replicas)
                    ));
                }
                Err(_) => {
                    app.ui.flash = Some(crate::app::FlashMessage::warn(
                        format!("Invalid replica count: {}", replicas_str)
                    ));
                }
            }
        }
        FormSubmit::PortForward => {
            // container_port may be Select (value = option index → port string)
            // or Port (value = port string directly). Handle both.
            let container_port = fields
                .iter()
                .find(|f| f.name == crate::kube::protocol::form_field_name::CONTAINER_PORT)
                .and_then(|f| match &f.kind {
                    FormFieldKind::Select { options } => {
                        let idx: usize = f.value.parse().ok()?;
                        options.get(idx).and_then(|opt| opt.value.parse::<u16>().ok())
                    }
                    _ => f.value.trim().parse::<u16>().ok(),
                });
            let local_port = fields
                .iter()
                .find(|f| f.name == crate::kube::protocol::form_field_name::LOCAL_PORT)
                .and_then(|f| f.value.trim().parse::<u16>().ok());
            match (container_port, local_port) {
                (Some(cp), Some(lp)) => {
                    ds_try!(app, data_source.port_forward(&target, lp, cp));
                    app.ui.flash = Some(crate::app::FlashMessage::info(
                        format!("Port-forwarding {}:{} → {}", lp, cp, target.name)
                    ));
                }
                _ => {
                    app.ui.flash = Some(crate::app::FlashMessage::warn(
                        "Invalid port numbers".to_string()
                    ));
                }
            }
        }
    }
}

/// Handle a keystroke while the nav filter bar is active. Returns true if consumed.
pub(crate) fn handle_filter_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
) -> bool {
    if !app.nav.filter_input().active {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.nav.filter_input_mut().text.clear();
            app.nav.filter_input_mut().active = false;
            app.reapply_nav_filters();
        }
        KeyCode::Enter => {
            let text = std::mem::take(&mut app.nav.filter_input_mut().text);
            let col = app.nav.filter_input_mut().column.take();
            app.nav.filter_input_mut().active = false;
            if !text.is_empty() {
                let current_rid = app.nav.resource_id().clone();
                let filter = if let Some(c) = col {
                    crate::app::nav::NavFilter::ColumnGrep {
                        pattern: crate::app::nav::CompiledGrep::new(text),
                        col: c,
                    }
                } else {
                    crate::app::nav::NavFilter::Grep(
                        crate::app::nav::CompiledGrep::new(text),
                    )
                };
                let change = app.nav.push(crate::app::nav::NavStep::new(
                    current_rid,
                    Some(filter),
                ));
                apply_nav_change(app, data_source, change);
            }
            app.reapply_nav_filters();
        }
        KeyCode::Backspace | KeyCode::Char(_) => {
            match key.code {
                KeyCode::Backspace => { app.nav.filter_input_mut().text.pop(); }
                KeyCode::Char(c) => { app.nav.filter_input_mut().text.push(c); }
                _ => {}
            }
            // The nav layer is the only source of truth for filter text;
            // re-derive the visible row set from the updated input. This
            // also refreshes the marked-visible cache.
            app.reapply_nav_filters();
        }
        _ => {}
    }
    true
}

#[cfg(test)]
mod parser_tests {
    use super::*;
    use crate::kube::protocol::Namespace;
    use crate::kube::resource_def::BuiltInKind;

    fn make_app() -> App {
        App::new(crate::kube::protocol::ContextName::default(), String::new())
    }

    fn parse(input: &str) -> ParsedCommand {
        let app = make_app();
        parse_command_input(input, &input.to_lowercase(), &app)
    }

    #[test]
    fn static_words_classify() {
        assert!(matches!(parse("q"), ParsedCommand::Quit));
        assert!(matches!(parse("quit"), ParsedCommand::Quit));
        assert!(matches!(parse("help"), ParsedCommand::Help));
        assert!(matches!(parse("?"), ParsedCommand::Help));
        assert!(matches!(parse("alias"), ParsedCommand::Aliases));
        assert!(matches!(parse("home"), ParsedCommand::Overview));
        assert!(matches!(parse("ctx"), ParsedCommand::ContextList));
    }

    #[test]
    fn ctx_switch_keeps_case() {
        match parse("ctx prod-us-east") {
            ParsedCommand::ContextSwitch(name) => assert_eq!(name.as_str(), "prod-us-east"),
            _ => panic!("expected ContextSwitch"),
        }
    }

    #[test]
    fn ns_switch_respects_all_and_named() {
        match parse("ns kube-system") {
            ParsedCommand::NamespaceSwitch(Namespace::Named(n)) => assert_eq!(n, "kube-system"),
            _ => panic!("expected NamespaceSwitch(Named)"),
        }
        match parse("ns all") {
            ParsedCommand::NamespaceSwitch(Namespace::All) => {}
            _ => panic!("expected NamespaceSwitch(All)"),
        }
    }

    #[test]
    fn bare_resource_alias_resolves() {
        match parse("pods") {
            ParsedCommand::Resource(rid) => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Pod));
            }
            _ => panic!("expected Resource"),
        }
    }

    #[test]
    fn resource_with_filter_runs_before_resource_with_ns() {
        // The pass-11 bug: `pods /Nginx` used to parse as namespace `/Nginx`.
        // The filter rule must come first.
        match parse("pods /Nginx") {
            ParsedCommand::ResourceFilter { rid, filter } => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Pod));
                assert_eq!(filter, "Nginx", "filter must preserve case");
            }
            other => panic!("expected ResourceFilter, got {:?}", match other {
                ParsedCommand::NamespaceSwitch(_) => "NamespaceSwitch",
                ParsedCommand::ResourceInNamespace { .. } => "ResourceInNamespace",
                _ => "other",
            }),
        }
    }

    #[test]
    fn resource_in_namespace_classifies() {
        match parse("deploy kube-system") {
            ParsedCommand::ResourceInNamespace { rid, namespace } => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Deployment));
                assert_eq!(namespace, Namespace::Named("kube-system".into()));
            }
            _ => panic!("expected ResourceInNamespace"),
        }
    }

    #[test]
    fn cluster_scoped_rejects_ns_arg() {
        // Nodes are cluster-scoped — "nodes foo" isn't a ns-switch, so it
        // falls through to the CRD-candidate fallback.
        match parse("nodes foo") {
            ParsedCommand::CrdCandidate(_) => {}
            _ => panic!("expected CrdCandidate fallback for cluster-scoped + ns"),
        }
    }

    #[test]
    fn unknown_input_falls_through_to_crd_candidate() {
        match parse("clickhouseinstallation") {
            ParsedCommand::CrdCandidate(name) => assert_eq!(name, "clickhouseinstallation"),
            _ => panic!("expected CrdCandidate fallback"),
        }
    }

    #[test]
    fn bare_ns_alias_opens_namespace_resource() {
        // `:ns` alone is the resource alias for Namespace (matches rule 4,
        // "plain resource alias"). The `:ns <name>` form only triggers
        // when there's an explicit namespace argument.
        match parse("ns") {
            ParsedCommand::Resource(rid) => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Namespace));
            }
            _ => panic!("expected Resource(Namespace)"),
        }
    }
}
