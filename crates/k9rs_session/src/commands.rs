use k9rs_app::ParsedCommand;
use anyhow::Result;
use tokio::sync::mpsc;

use k9rs_app::App;
use k9rs_proto::protocol::ResourceId;

/// Await `work` while applying everything the daemon sends in the meantime.
///
/// Extracted so the property can actually be tested: the TUI-suspension path
/// around it needs a real terminal, this needs nothing. A closed channel
/// disables its branch and `work` still runs to completion.
async fn draining<F>(
    app: &mut App,
    event_rx: &mut mpsc::Receiver<k9rs_app::AppEvent>,
    work: F,
) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(work);
    loop {
        tokio::select! {
            done = &mut work => return done,
            Some(event) = event_rx.recv() => {
                k9rs_app::apply_event(app, event);
            }
        }
    }
}

/// Suspend the TUI and run an interactive command directly.
///
/// One caller, one kind of command: the `$EDITOR` takeover. It used to take
/// an `InteractiveKind` parameter, bound as `_kind` and never read, whose doc
/// said it "determines pre-launch UI (the 'Connecting…' box for shells) and
/// post-launch failure handling" — it determined nothing, and shells do not
/// come through here at all (they go to `run_shell_bridge`). A one-variant
/// enum constructed at its single call site draws no `dead_code` warning,
/// which is how it survived. The bash→sh fallback the next paragraph used to
/// promise is likewise not here; it is `SHELL_TEMPLATE`'s, on the exec path.
///
/// # Why this drains `event_rx` while the subprocess runs
///
/// The daemon link does not pause because a human is thinking. Events keep
/// arriving on their substreams; if nobody reads them the client's channel
/// fills, its reader tasks stop reading, the yamux stream windows fill, the
/// daemon's writes stall, and its `connection_write_timeout` (60s) tears the
/// whole connection down — so a leisurely edit came back to a dead session.
/// Draining here keeps the transport moving. `apply_event` touches only app
/// state and stores (never the terminal), so it is safe with the TUI
/// suspended; the next render after `restore()` shows the accumulated result.
///
/// This is the same treatment `run_shell_bridge` already gives its own
/// suspension, for the same reason — dropping the events instead would leave
/// stores diverged (a missed delta is never re-sent) and could swallow a
/// `DaemonDisconnected`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_interactive_local(
    app: &mut App,
    event_rx: &mut mpsc::Receiver<k9rs_app::AppEvent>,
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    command: &str,
    args: &[String],
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
    input_rx: &mut mpsc::Receiver<crossterm::event::Event>,
    replay: &mut std::collections::VecDeque<crossterm::event::Event>,
) -> Result<Option<std::process::ExitStatus>> {
    use crate::SuspendGuard;

    // The guard suspends the TUI and restores it on `restore()` (or `Drop`).
    let mut guard = SuspendGuard::new(terminal, input_suspend, input_suspend_ack, input_rx).await?;

    // Raw mode is off already — `SuspendGuard::new` owns it, so no path out
    // of here can leave the TUI in cooked mode.
    crossterm::execute!(
        guard.terminal_mut().backend_mut(),
        crossterm::cursor::Show,
        crossterm::cursor::SetCursorStyle::DefaultUserShape,
    )?;

    // Run the subprocess on a blocking thread so the tokio runtime's
    // worker threads stay unblocked.
    let command_owned = command.to_string();
    let args_owned: Vec<String> = args.to_vec();
    let child = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(command_owned);
        cmd.args(args_owned);
        cmd.status()
    });
    // See the note on this function: the link must keep moving while a human
    // is thinking.
    let status = draining(app, event_rx, child)
        .await
        .unwrap_or_else(|join_err| Err(std::io::Error::other(join_err.to_string())));

    // Raw mode is restored by the guard (on `restore()` AND on `Drop`).
    crossterm::execute!(
        guard.terminal_mut().backend_mut(),
        crossterm::cursor::Hide,
    )?;

    // Reset terminal modes the subprocess may have changed — the same list
    // the shell bridge uses, because it is now literally the same constant
    // (`crate::TERM_MODE_RESET`; see there for the two entries this copy was
    // missing). Without this, stale terminal responses during the alt-screen
    // transition can be parsed as key events (e.g. a spurious 'r' triggering
    // the restart dialog).
    {
        let backend = guard.terminal_mut().backend_mut();
        let _ = std::io::Write::write_all(backend, crate::TERM_MODE_RESET.as_bytes());
        let _ = std::io::Write::flush(backend);
    }

    // Restore the screen, resume input, and drain stale terminal responses
    // (the settling drain catches the spurious-'r' that the mode-reset escapes
    // above don't fully suppress — e.g. the alt-screen re-entry's own reply).
    // Real type-ahead caught in the window comes back for the loop to replay.
    replay.extend(guard.restore().await);
    Ok(status.ok())
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
    if matches!(cmd, "overview" | "home") { return ParsedCommand::Overview; }
    // `:alias` / `:aliases` / `:a` are NOT special-cased, for the same
    // reason `:ctx` is not: aliases ARE an ordinary resource now (a
    // client-owned one), so the resource path below resolves them and they
    // reset the nav stack like every other root-level resource command.
    // `:ctx` / `:context` / `:contexts` are NOT special-cased here — they
    // resolve through the ordinary resource-alias path below, because
    // contexts ARE an ordinary resource (a client-owned one). That is what
    // makes `:ctx` reset the nav stack like every other resource command.

    // 2. `:ctx <name>` / `:context <name>`. The prefix lives in `cmd` (the
    // lowercased form) so alias matching is case-insensitive, but the
    // *value* comes from `raw_cmd` so context names keep their case.
    // Slicing `raw_cmd` by the ASCII prefix length is safe (the prefix
    // is always ASCII; it doesn't shift under lowercasing).
    if let Some(value) = strip_ascii_prefix(raw_cmd, cmd, &["ctx ", "context "]) {
        // `strip_ascii_prefix` already refuses an empty tail, so this is a
        // real name; the fallback keeps the mapping TOTAL rather than
        // leaning on that with an unwrap.
        return match k9rs_proto::protocol::ContextName::new(value) {
            Some(name) => ParsedCommand::ContextSwitch(name),
            // `strip_ascii_prefix` already refuses an empty tail, so this is
            // unreachable; falling back to the list keeps the map TOTAL
            // rather than leaning on that with an unwrap.
            None => ParsedCommand::Resource(
                k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id(),
            ),
        };
    }

    // 3. `:ns <name>` / `:namespace <name>`. Same case-preserving slice.
    if let Some(value) = strip_ascii_prefix(raw_cmd, cmd, &["ns ", "namespace "]) {
        return ParsedCommand::NamespaceSwitch(
            k9rs_proto::protocol::Namespace::from_user_command(value),
        );
    }

    // 4. Plain resource alias (bare `:pods`, `:deploy`, etc.). Matches
    // before multi-token shapes because a single-token alias can't hit
    // any of them (none have a space).
    if let Some(rid) = ResourceId::from_alias(cmd) {
        return ParsedCommand::Resource(rid);
    }

    // 4b. A user-configured exec resource, by its own name (`:node-costs`).
    //
    // These cannot go through `ResourceId::from_alias`: that consults the
    // built-in registry and `LocalResourceKind::ALL`, and a `Custom` kind
    // is in neither — it is created at runtime from daemon config, and
    // `Custom(_).aliases()` is empty by construction. So `:node-costs` fell
    // through to the CRD fallback below and the daemon answered "Unknown
    // resource", while the README promised the name would work. The only
    // other way in was `:exec`, which the daemon answered with
    // `exec_configs.first()`, so a SECOND configured source was unreachable
    // by any keystroke at all. That generic alias is gone as of wire v19;
    // this is the whole way in now, and `seed_aliases` lists these names so
    // they stay discoverable.
    //
    // The client already has the answer: `AppConfig` parses the whole
    // `daemon:` section (it has to — `deny_unknown_fields` on the root would
    // otherwise reject a documented section) and then ignores it. Reading it
    // here needs no wire change and no discovery round trip.
    if let Some(rid) = exec_resource_by_name(cmd, app) {
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

/// Resolve a configured exec-resource name to its typed `ResourceId`.
///
/// Matched case-insensitively against `daemon.execResources[].name`, which is
/// the same string `ExecSource::for_context` uses to build the `Custom` kind
/// daemon-side — so the id the client sends is the id the daemon answers to.
fn exec_resource_by_name(cmd: &str, app: &App) -> Option<ResourceId> {
    let configs = &app.config.daemon.as_ref()?.exec_resources;
    // Through the SAME validation the daemon applies
    // (`ExecSourceConfig::validate`). This read applied none, so an entry
    // the daemon had skipped was still typeable here and came back as
    // `Unknown local resource: …` with no reason — two readers of one
    // config with different arity.
    let matched = configs
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(cmd) && c.validate().is_ok())?;
    Some(
        k9rs_proto::local_types::LocalResourceKind::Custom(matched.name.clone())
            .to_resource_id(),
    )
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
fn parse_resource_in_namespace(cmd: &str) -> Option<(ResourceId, k9rs_proto::protocol::Namespace)> {
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
    Some((rid, k9rs_proto::protocol::Namespace::from_user_command(ns)))
}

/// Internal: parse `<crd> <ns>` against the discovery cache on `app`.
fn parse_crd_in_namespace(cmd: &str, app: &App) -> Option<(k9rs_app::CrdInfo, k9rs_proto::protocol::Namespace)> {
    let (crd_part, ns) = cmd.split_once(' ')?;
    let crd_part = crd_part.trim();
    let ns = ns.trim();
    if ns.is_empty() {
        return None;
    }
    let crd = app.find_crd_by_name(crd_part)?;
    if crd.scope == k9rs_proto::protocol::ResourceScope::Cluster {
        return None;
    }
    Some((crd, k9rs_proto::protocol::Namespace::from_user_command(ns)))
}


// ---------------------------------------------------------------------------
// Input mode handlers — extracted from the main event loop
// ---------------------------------------------------------------------------

use crossterm::event::KeyCode;

use k9rs_app::ClientSession;
use crate::ds_try;
use crate::actions::do_switch_namespace;

// (COMMAND_HISTORY_LIMIT moved to app.config.ui.command_history_size)

/// What a command-bar keystroke did, from [`edit_command_key`] — the half
/// that only touches the buffer.
///
/// Split out because submitting is the ONLY thing that needs a live
/// `ClientSession`, and an unused-in-this-path parameter is what forces a test
/// to build a daemon connection to check that Ctrl-G doesn't type a `g`. The
/// same shape was removed from `handle_filter_key` and `begin_context_switch`
/// for the same reason.
pub(crate) enum CommandKey {
    /// Handled; nothing further to do.
    Consumed,
    /// Not a command-bar key — fall through to the global handler.
    NotMine,
    /// Enter: the bar is already closed and history recorded; run this.
    Submit(String),
}

/// The pure half of command-bar input.
pub(crate) fn edit_command_key(app: &mut App, key: k9rs_app::TextKey) -> CommandKey {
    if app.ui.command_input().is_none() {
        return CommandKey::NotMine;
    }
    match key.code() {
        // The command bar genuinely IS `Modal::Command`, so closing the
        // modal slot is what cancels it.
        KeyCode::Esc => {
            app.ui.close_modal();
        }
        KeyCode::Enter => {
            let raw_cmd = if let Some((input, _)) = app.ui.command_input() {
                input.trim().to_string()
            } else {
                String::new()
            };
            let cmd = raw_cmd.to_lowercase();
            if !cmd.is_empty() {
                app.command_history.push(raw_cmd.clone());
                if app.command_history.len() > app.config.ui.command_history_size {
                    app.command_history.remove(0);
                }
            }
            app.ui.close_modal();
            return CommandKey::Submit(raw_cmd);
        }
        KeyCode::Tab => {
            app.accept_completion();
        }
        KeyCode::Up => {
            if !app.command_history.is_empty() {
                if let Some((input, history_index)) = app.ui.command_input_mut() {
                    let idx = match *history_index {
                        None => app.command_history.len() - 1,
                        Some(i) => i.saturating_sub(1),
                    };
                    *history_index = Some(idx);
                    input.set(app.command_history[idx].clone());
                }
            }
        }
        KeyCode::Down => {
            if let Some((input, history_index)) = app.ui.command_input_mut() {
                if let Some(idx) = *history_index {
                    if idx + 1 < app.command_history.len() {
                        *history_index = Some(idx + 1);
                        input.set(app.command_history[idx + 1].clone());
                    } else {
                        *history_index = None;
                        input.clear();
                    }
                }
            }
        }
        // Editing goes through the ONE text input. Any edit drops the
        // history position: what is in the box is no longer the recalled
        // command, so Up must resume from the end rather than from wherever
        // the recall had got to.
        code @ (KeyCode::Backspace
        | KeyCode::Delete
        | KeyCode::Left
        | KeyCode::Right
        | KeyCode::Home
        | KeyCode::End
        | KeyCode::Char(_)) => {
            if let Some((input, history_index)) = app.ui.command_input_mut() {
                match code {
                    KeyCode::Backspace => {
                        input.backspace();
                        *history_index = None;
                    }
                    KeyCode::Delete => {
                        input.delete();
                        *history_index = None;
                    }
                    KeyCode::Char(c) => {
                        input.insert(c);
                        *history_index = None;
                    }
                    // Moving the caret is not an edit — the recalled command
                    // is still the recalled command.
                    //
                    // These two arms were UNREACHABLE: the or-pattern above
                    // did not admit `Left`/`Right`, so they fell to the
                    // outer `_ => {}`, were reported as consumed, and the
                    // caret never moved. `unreachable_pattern` cannot see
                    // it — `code` is a `KeyCode`, so the arms are
                    // well-typed. The other text sinks all handle the
                    // arrows (the form dialog in its own `Left | Right` arm
                    // above, which is why ITS or-pattern omits them), so
                    // this was the only box where they did nothing — and it
                    // is the one whose renderer has dedicated caret
                    // windowing, until now reachable only via Home/End.
                    KeyCode::Left => input.left(),
                    KeyCode::Right => input.right(),
                    KeyCode::Home => input.home(),
                    KeyCode::End => input.end(),
                    _ => unreachable!("the pattern above admits nothing else"),
                }
            }
        }
        _ => {}
    }
    CommandKey::Consumed
}

/// Handle a keystroke while in `:command` mode. Returns true if consumed.
pub(crate) fn handle_command_key(
    app: &mut App,
    key: k9rs_app::TextKey,
    data_source: &mut ClientSession,
    event_tx: &tokio::sync::mpsc::Sender<k9rs_app::AppEvent>,
) -> bool {
    match edit_command_key(app, key) {
        CommandKey::NotMine => false,
        CommandKey::Consumed => true,
        CommandKey::Submit(raw_cmd) => {
            let cmd = raw_cmd.to_lowercase();
            handle_command_submit(app, &raw_cmd, &cmd, data_source, event_tx);
            true
        }
    }
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
    event_tx: &tokio::sync::mpsc::Sender<k9rs_app::AppEvent>,
) {
    use crate::actions::handle_action;
    use crate::actions::begin_context_switch;

    if cmd.is_empty() {
        return;
    }

    match parse_command_input(raw_cmd, cmd, app) {
        ParsedCommand::Quit => {
            app.exit_reason = Some(k9rs_app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        ParsedCommand::Help => {
            // `:help` and `?` are the same thing, so they go through the same
            // action rather than each constructing the view themselves.
            handle_action(app, k9rs_app::Action::Help, event_tx, data_source);
        }
        ParsedCommand::Overview => {
            app.nav.reset(k9rs_app::Element::Overview(
                k9rs_app::Overview,
            ));
        }
        ParsedCommand::ContextSwitch(ctx_name) => {
            begin_context_switch(app, &ctx_name);
        }
        ParsedCommand::NamespaceSwitch(ns) => {
            do_switch_namespace(app, data_source, ns);
        }
        ParsedCommand::Resource(rid) if !app.can_open_resource_view(&rid) => {
            // Same hole Tab had: with no context this opens a subscription
            // against a session that doesn't exist and `nav.reset`s the
            // picker away, stranding the user on an empty table.
            app.ui.flash = Some(k9rs_app::FlashMessage::warn(
                crate::actions::SELECT_A_CONTEXT_FIRST.to_string(),
            ));
        }
        ParsedCommand::Resource(rid) => {
            if rid.is_cluster_scoped() && !app.kube.selected_ns.is_all() {
                // Display-only selector correction (the element's own
                // query already carries the resolved scope).
                app.kube.selected_ns = k9rs_proto::protocol::Namespace::All;
            }
            let root = crate::actions::root_for(app, data_source, rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
        }
        ParsedCommand::ResourceFilter { rid, filter } => {
            let root = crate::actions::root_for(app, data_source, rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
            let predicate = k9rs_app::RowPredicate::Grep(
                k9rs_util::CompiledGrep::new(filter),
            );
            if let Ok(el) = k9rs_app::Element::derive_filter(app.nav.top(), predicate) {
                app.nav.push(el);
            }
        }
        ParsedCommand::ResourceInNamespace { rid, namespace } => {
            // The element carries the requested namespace as its OWN
            // scope; the selector follows it (explicit selector write).
            app.kube.selected_ns = namespace.clone();
            let root = crate::actions::root_for(app, data_source, rid.clone(), namespace.clone(),
            );
            app.nav.reset(root);
            app.ui.flash = Some(k9rs_app::FlashMessage::info(format!(
                "{}({})", rid.short_label(), namespace.display()
            )));
        }
        ParsedCommand::CrdInNamespace { crd, namespace } => {
            app.kube.selected_ns = namespace.clone();
            let crd_rid = ResourceId::crd(
                crd.group.clone(), crd.version.clone(),
                crd.kind.clone(), crd.plural.clone(), crd.scope,
            );
            let root = crate::actions::root_for(app, data_source, crd_rid, namespace.clone(),
            );
            app.nav.reset(root);
            app.ui.flash = Some(k9rs_app::FlashMessage::info(
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
                    k9rs_proto::protocol::ResourceScope::Namespaced,
                )
            };
            if scope == k9rs_proto::protocol::ResourceScope::Cluster && !app.kube.selected_ns.is_all() {
                app.kube.selected_ns = k9rs_proto::protocol::Namespace::All;
            }
            let root = crate::actions::root_for(app, data_source, crd_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
            app.ui.flash = Some(k9rs_app::FlashMessage::info(format!("Browsing: {}", kind_label)));
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
    key: k9rs_app::TextKey,
    data_source: &mut ClientSession,
) -> bool {
    use k9rs_app::FormFieldKind;
    // The form is an ELEMENT now; its typing still runs here because a
    // keystroke that edits text has to mutate, and `handle_key_event` only
    // names actions. Every other text surface is handled the same way.
    if !matches!(app.nav.top(), k9rs_app::Element::Form(_)) {
        return false;
    }
    match key.code() {
        // NOT consumed: leaving a form goes through the one undo path, so
        // Esc falls through to `handler::handle_form_keys` and comes back as
        // `Action::Back`. It used to call `close_modal()` here, which clears
        // the modal SLOT — and a form is a nav element, so Esc did nothing
        // at all and OK was the only exit.
        KeyCode::Esc => return false,
        KeyCode::Tab | KeyCode::Down => {
            if let Some(d) = app.nav.top_mut().form_dialog_mut() {
                d.focus_next();
            }
        }
        KeyCode::BackTab | KeyCode::Up => {
            if let Some(d) = app.nav.top_mut().form_dialog_mut() {
                d.focus_prev();
            }
        }
        KeyCode::Enter => {
            let Some(d) = app.nav.top().form_dialog() else { return false };
            if d.cancel_focused() {
                // Same as Esc: the action layer pops. Handled there so there
                // is one place that knows how to leave a form.
                return false;
            }
            if d.ok_focused() {
                if let Some(k9rs_app::Element::Form(dialog)) = app.nav.pop() {
                    let dialog = *dialog;
                    dispatch_form_submit(app, data_source, dialog);
                }
            } else if let Some(d) = app.nav.top_mut().form_dialog_mut() {
                d.focus_next();
            }
        }
        // Left/Right mean different things to different FIELD kinds, and the
        // field decides: a Select cycles its options, a text field moves the
        // caret. Before the caret existed only the first half had a meaning,
        // so arrows on a text field did nothing at all.
        KeyCode::Left | KeyCode::Right => {
            if let Some(field) = app.nav.top_mut().form_dialog_mut().and_then(|d| d.current_field_mut()) {
                match field.kind {
                    FormFieldKind::Select { ref options, ref mut selected } => {
                        if !options.is_empty() {
                            let n = options.len();
                            *selected = if key.code() == KeyCode::Left {
                                (*selected + n - 1) % n
                            } else {
                                (*selected + 1) % n
                            };
                        }
                    }
                    _ => {
                        if key.code() == KeyCode::Left {
                            field.value.left()
                        } else {
                            field.value.right()
                        }
                    }
                }
            }
        }
        // Editing goes through the ONE text input, so a form field takes the
        // caret like every other box. What a field ACCEPTS is still the
        // field's own business — a Port takes digits wherever the caret is.
        code @ (KeyCode::Backspace
        | KeyCode::Delete
        | KeyCode::Home
        | KeyCode::End
        | KeyCode::Char(_)) => {
            if let Some(field) = app.nav.top_mut().form_dialog_mut().and_then(|d| d.current_field_mut()) {
                if !field.is_text_input() {
                    return true;
                }
                match code {
                    KeyCode::Backspace => field.value.backspace(),
                    KeyCode::Delete => field.value.delete(),
                    KeyCode::Home => field.value.home(),
                    KeyCode::End => field.value.end(),
                    KeyCode::Char(c) => {
                        let accept = match field.kind {
                            FormFieldKind::Text { max_len } => {
                                max_len.is_none_or(|m| field.value.value().chars().count() < m)
                            }
                            FormFieldKind::Number { .. } | FormFieldKind::Port => {
                                c.is_ascii_digit()
                            }
                            FormFieldKind::Select { .. } => false,
                        };
                        if accept {
                            field.value.insert(c);
                        }
                    }
                    _ => unreachable!("the pattern above admits nothing else"),
                }
            }
        }
        // Don't consume keys we don't handle — let them fall through
        // to the global handler (e.g., Ctrl-C to quit).
        _ => return false,
    }
    true
}

/// Dispatch a submitted form dialog to the appropriate typed
/// `SessionCommand`. Reads the collected field values (string-typed,
/// validated by the dialog widget per `FormFieldKind`) and produces a
/// strongly-typed wire payload. Centralized so adding a new operation
/// kind only adds an arm here.
///
/// INVARIANT this ungated send leans on: a form dialog cannot coexist
/// with select mode or read-only — its only openers (`Action::Scale` /
/// `Action::PortForward`) are blocked by the select gate and the
/// readonly check BEFORE the dialog exists, and while it is open every
/// Char key is consumed by the form, so no marks can appear under it.
/// A new form-dialog opener must preserve that, or this becomes a
/// select/readonly bypass.
fn dispatch_form_submit(
    app: &mut App,
    data_source: &mut ClientSession,
    dialog: k9rs_app::FormDialog,
) {
    match dialog.submit.build_command(&dialog.target, &dialog.fields) {
        Ok(cmd) => {
            app.kube.kubectl_cache.clear();
            // Gated for the reason `ds_try` gives: the form has already
            // been popped, so an unconditional "Scale: nginx" over the
            // connection error left nothing on screen to contradict it.
            if ds_try!(app, data_source.send_command(cmd)) {
                let label = dialog.submit.label();
                app.ui.flash = Some(k9rs_app::FlashMessage::info(
                    format!("{}: {}", label, dialog.target.name)
                ));
            }
        }
        Err(msg) => {
            app.ui.flash = Some(k9rs_app::FlashMessage::warn(msg));
        }
    }
}

/// Handle a keystroke while the nav filter bar is active. Returns true if consumed.
/// Takes no `ClientSession`: filtering is pure client-side state. The unused
/// parameter it used to carry is the same shape that kept the context-switch
/// flow untestable — an argument nothing reads still forces every test to
/// build a live daemon connection.
pub(crate) fn handle_filter_key(app: &mut App, key: k9rs_app::TextKey) -> bool {
    if !app.nav.top().filter_input().active() {
        return false;
    }
    match key.code() {
        // Draft mutations need no explicit re-filter: the draft is a
        // derive input, so the next frame's view re-derives naturally.
        KeyCode::Esc => {
            if let Some(fi) = app.nav.top_mut().filter_input_mut() {
                fi.cancel();
            }
        }
        KeyCode::Enter => app.nav.commit_filter(),
        // Editing goes through the ONE text input, so the caret behaves
        // here exactly as it does in the log grep and the command prompt.
        code @ (KeyCode::Backspace
        | KeyCode::Delete
        | KeyCode::Left
        | KeyCode::Right
        | KeyCode::Home
        | KeyCode::End
        | KeyCode::Char(_)) => {
            let Some(fi) = app.nav.top_mut().filter_input_mut() else { return false };
            let t = fi.input_mut();
            match code {
                KeyCode::Backspace => t.backspace(),
                KeyCode::Delete => t.delete(),
                KeyCode::Left => t.left(),
                KeyCode::Right => t.right(),
                KeyCode::Home => t.home(),
                KeyCode::End => t.end(),
                KeyCode::Char(c) => t.insert(c),
                _ => unreachable!("the pattern above admits nothing else"),
            }
        }
        // Don't consume keys we don't handle — let them fall through
        // to the global handler (e.g., Ctrl-C to quit).
        _ => return false,
    }
    true
}


#[cfg(test)]
#[path = "tests/commands.rs"]
mod drain_tests;

#[cfg(test)]
#[path = "tests/commands_exec.rs"]
mod exec_name_tests;
