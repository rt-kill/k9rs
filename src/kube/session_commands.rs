use anyhow::Result;
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

/// Await `work` while applying everything the daemon sends in the meantime.
///
/// Extracted so the property can actually be tested: the TUI-suspension path
/// around it needs a real terminal, this needs nothing. A closed channel
/// disables its branch and `work` still runs to completion.
async fn draining<F>(
    app: &mut App,
    event_rx: &mut mpsc::Receiver<crate::event::AppEvent>,
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
                crate::kube::session_events::apply_event(app, event);
            }
        }
    }
}

/// Suspend the TUI and run an interactive command directly (with bash->sh
/// fallback for shell). The caller hands in a typed `InteractiveKind` so
/// this function doesn't have to inspect `args` to guess what it's about
/// to launch.
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
    event_rx: &mut mpsc::Receiver<crate::event::AppEvent>,
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    command: &str,
    args: &[String],
    _kind: InteractiveKind,
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
    input_rx: &mut mpsc::Receiver<crossterm::event::Event>,
    replay: &mut std::collections::VecDeque<crossterm::event::Event>,
) -> Result<Option<std::process::ExitStatus>> {
    use crate::kube::session::SuspendGuard;

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

    // Reset terminal modes the subprocess may have changed. Matches the
    // shell bridge cleanup (session.rs) — without this, stale terminal
    // responses during the alt-screen transition can be parsed as key
    // events (e.g. a spurious 'r' triggering the restart dialog).
    {
        let backend = guard.terminal_mut().backend_mut();
        let _ = std::io::Write::write_all(backend, concat!(
            "\x1b[?1000l",  // disable mouse tracking
            "\x1b[?1006l",  // disable SGR mouse extension
            "\x1b[?2004l",  // disable bracketed paste
            "\x1b(B",       // select US-ASCII charset for G0
        ).as_bytes());
        let _ = std::io::Write::flush(backend);
    }

    // Restore the screen, resume input, and drain stale terminal responses
    // (the settling drain catches the spurious-'r' that the mode-reset escapes
    // above don't fully suppress — e.g. the alt-screen re-entry's own reply).
    // Real type-ahead caught in the window comes back for the loop to replay.
    replay.extend(guard.restore().await);
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
#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) enum ParsedCommand {
    /// `:q`, `:quit`, `:exit`, `:q!`
    Quit,
    /// `:help`, `:h`, `:?`
    Help,
    /// `:overview`, `:home`
    Overview,
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

/// One static `:` word, for the aliases view to list, the completer to
/// offer, and this module's test to probe.
///
/// The same shape as [`crate::event::keymap::Binding`], for the same reason:
/// the grammar below is a linear match, so the only way the documentation and
/// the parser can be held together is to declare the documentation once and
/// probe it. There were THREE hand-written copies of this list — `Ctrl-a`'s
/// text document, and two slices in the completer — and the words that take
/// an argument were spelled differently in each.
///
/// Resource aliases are NOT here: they come from the registry and from
/// `LocalResourceKind`, which are already their single declaration.
pub(crate) struct CommandDoc {
    /// The words that select it, exactly as typed — what the completer
    /// offers. A word that takes an argument appears here bare; the
    /// placeholder is display only.
    pub words: &'static [&'static str],
    /// What the argument is called, for display. `None` takes none.
    pub arg: Option<&'static str>,
    pub label: &'static str,
    /// One input the parser must classify, and the check that it did.
    /// Read only by `every_documented_command_word_is_one_the_parser_accepts`
    /// — it exists to hold the declaration and the grammar together, which is
    /// a test's job, so it is dead in a release build by design.
    #[cfg_attr(not(test), allow(dead_code))]
    pub probe: &'static str,
    #[cfg_attr(not(test), allow(dead_code))]
    pub accepts: fn(&ParsedCommand) -> bool,
}

impl CommandDoc {
    /// How a reference view prints it: `q / quit / exit` or `ns <name>`.
    pub fn display(&self) -> String {
        match self.arg {
            None => self.words.join(" / "),
            Some(arg) => self
                .words
                .iter()
                .map(|w| format!("{w} <{arg}>"))
                .collect::<Vec<_>>()
                .join(" / "),
        }
    }
}

/// Every `:` word that is a command rather than a resource, in the order the
/// grammar tries them.
pub(crate) const COMMAND_WORDS: &[CommandDoc] = &[
    CommandDoc {
        words: &["q", "quit", "exit", "q!"],
        arg: None,
        label: "Quit",
        probe: "q",
        accepts: |p| matches!(p, ParsedCommand::Quit),
    },
    CommandDoc {
        words: &["help", "h", "?"],
        arg: None,
        label: "Show help for this view",
        probe: "help",
        accepts: |p| matches!(p, ParsedCommand::Help),
    },
    CommandDoc {
        words: &["overview", "home"],
        arg: None,
        label: "Cluster overview",
        probe: "overview",
        accepts: |p| matches!(p, ParsedCommand::Overview),
    },
    CommandDoc {
        words: &["ctx", "context"],
        arg: Some("name"),
        label: "Switch context",
        probe: "ctx dev",
        accepts: |p| matches!(p, ParsedCommand::ContextSwitch(_)),
    },
    CommandDoc {
        words: &["ns", "namespace"],
        arg: Some("name"),
        label: "Switch namespace",
        probe: "ns kube-system",
        accepts: |p| matches!(p, ParsedCommand::NamespaceSwitch(_)),
    },
];

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
        return match crate::kube::protocol::ContextName::new(value) {
            Some(name) => ParsedCommand::ContextSwitch(name),
            // `strip_ascii_prefix` already refuses an empty tail, so this is
            // unreachable; falling back to the list keeps the map TOTAL
            // rather than leaning on that with an unwrap.
            None => ParsedCommand::Resource(
                crate::kube::local::LocalResourceKind::Context.to_resource_id(),
            ),
        };
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

    // 4b. A user-configured exec resource, by its own name (`:node-costs`).
    //
    // These cannot go through `ResourceId::from_alias`: that consults the
    // built-in registry and `LocalResourceKind::all()`, and a `Custom` kind
    // is in neither — it is created at runtime from daemon config, and
    // `Custom(_).aliases()` is empty by construction. So `:node-costs` fell
    // through to the CRD fallback below and the daemon answered "Unknown
    // resource", while the README promised the name would work. `:exec`
    // resolved to `exec_configs.first()`, so a SECOND configured source was
    // unreachable by any keystroke at all.
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
    let matched = configs.iter().find(|c| c.name.eq_ignore_ascii_case(cmd))?;
    Some(
        crate::kube::local::LocalResourceKind::Custom(matched.name.clone())
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


// ---------------------------------------------------------------------------
// Input mode handlers — extracted from the main event loop
// ---------------------------------------------------------------------------

use crossterm::event::KeyCode;

use crate::kube::client_session::ClientSession;
use crate::kube::session::ds_try;
use crate::kube::session_actions::do_switch_namespace;

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
pub(crate) fn edit_command_key(app: &mut App, key: crate::app::TextKey) -> CommandKey {
    if app.ui.command_input().is_none() {
        return CommandKey::NotMine;
    }
    match key.code() {
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
    key: crate::app::TextKey,
    data_source: &mut ClientSession,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
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
            // `:help` and `?` are the same thing, so they go through the same
            // action rather than each constructing the view themselves.
            handle_action(app, crate::app::actions::Action::Help, event_tx, data_source);
        }
        ParsedCommand::Overview => {
            app.nav.reset(crate::app::element::Element::Overview(
                crate::app::element::Overview,
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
            app.ui.flash = Some(crate::app::FlashMessage::warn(
                "Select a context first".to_string(),
            ));
        }
        ParsedCommand::Resource(rid) => {
            if rid.is_cluster_scoped() && !app.kube.selected_ns.is_all() {
                // Display-only selector correction (the element's own
                // query already carries the resolved scope).
                app.kube.selected_ns = crate::kube::protocol::Namespace::All;
            }
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
        }
        ParsedCommand::ResourceFilter { rid, filter } => {
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
            let predicate = crate::app::store::RowPredicate::Grep(
                crate::app::nav::CompiledGrep::new(filter),
            );
            if let Ok(el) = crate::app::element::Element::derive_filter(app.nav.top(), predicate) {
                app.nav.push(el);
            }
        }
        ParsedCommand::ResourceInNamespace { rid, namespace } => {
            // The element carries the requested namespace as its OWN
            // scope; the selector follows it (explicit selector write).
            app.kube.selected_ns = namespace.clone();
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, rid.clone(), namespace.clone(),
            );
            app.nav.reset(root);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!(
                "{}({})", rid.short_label(), namespace.display()
            )));
        }
        ParsedCommand::CrdInNamespace { crd, namespace } => {
            app.kube.selected_ns = namespace.clone();
            let crd_rid = ResourceId::crd(
                crd.group.clone(), crd.version.clone(),
                crd.kind.clone(), crd.plural.clone(), crd.scope,
            );
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, crd_rid, namespace.clone(),
            );
            app.nav.reset(root);
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
            if scope == crate::kube::protocol::ResourceScope::Cluster && !app.kube.selected_ns.is_all() {
                app.kube.selected_ns = crate::kube::protocol::Namespace::All;
            }
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, crd_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
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
    key: crate::app::TextKey,
    data_source: &mut ClientSession,
) -> bool {
    use crate::app::FormFieldKind;
    // The form is an ELEMENT now; its typing still runs here because a
    // keystroke that edits text has to mutate, and `handle_key_event` only
    // names actions. Every other text surface is handled the same way.
    if !matches!(app.nav.top(), crate::app::element::Element::Form(_)) {
        return false;
    }
    match key.code() {
        KeyCode::Esc => {
            app.ui.close_modal();
        }
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
            let ok = app.nav.top().form_dialog().as_ref().is_some_and(|d| d.ok_focused());
            if ok {
                if let Some(crate::app::element::Element::Form(dialog)) = app.nav.pop() {
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
    dialog: crate::app::FormDialog,
) {
    match dialog.submit.build_command(&dialog.target, &dialog.fields) {
        Ok(cmd) => {
            app.kube.kubectl_cache.clear();
            ds_try!(app, data_source.send_command(cmd));
            let label = dialog.submit.from_operation_label();
            app.ui.flash = Some(crate::app::FlashMessage::info(
                format!("{}: {}", label, dialog.target.name)
            ));
        }
        Err(msg) => {
            app.ui.flash = Some(crate::app::FlashMessage::warn(msg));
        }
    }
}

/// Handle a keystroke while the nav filter bar is active. Returns true if consumed.
/// Takes no `ClientSession`: filtering is pure client-side state. The unused
/// parameter it used to carry is the same shape that kept the context-switch
/// flow untestable — an argument nothing reads still forces every test to
/// build a live daemon connection.
pub(crate) fn handle_filter_key(app: &mut App, key: crate::app::TextKey) -> bool {
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
        KeyCode::Enter => {
            if let Some(committed) = app.nav.top_mut().filter_input_mut().and_then(|fi| fi.commit()) {
                let predicate = crate::app::element::predicate_for_commit(
                    committed.text,
                    committed.column.map(|c| (c.index, c.header)),
                );
                // Commit = one RowFilter element over the top's output.
                // Tops without a row output (projections) can't commit —
                // the draft simply clears, matching the old behavior.
                if let Ok(el) = crate::app::element::Element::derive_filter(app.nav.top(), predicate) {
                    app.nav.push(el);
                }
            }
        }
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
mod parser_tests {
    use super::*;
    use crate::kube::protocol::Namespace;
    use crate::kube::resource_def::BuiltInKind;

    fn make_app() -> App {
        App::new_for_test()
    }

    fn parse(input: &str) -> ParsedCommand {
        let app = make_app();
        parse_command_input(input, &input.to_lowercase(), &app)
    }

    /// Every documented `:` word really parses to what it claims.
    ///
    /// `COMMAND_WORDS` exists so the aliases view does not hand-write its own
    /// copy of this grammar — which is exactly what `Ctrl-a` used to do, and
    /// why its key list had drifted from the keymap. A declaration the parser
    /// does not read needs a probe, so this is the same arrangement the
    /// keymap has: documentation beside a check that it is true.
    #[test]
    fn every_documented_command_word_is_one_the_parser_accepts() {
        for doc in COMMAND_WORDS {
            let parsed = parse(doc.probe);
            assert!(
                (doc.accepts)(&parsed),
                "`:{}` is documented as `{}` but parsed as {:?}",
                doc.probe,
                doc.label,
                parsed,
            );
            // The declared words must include the probe's first token, or
            // the row documents one thing and the test checks another.
            let head = doc.probe.split(' ').next().unwrap_or(doc.probe);
            assert!(
                doc.words.contains(&head),
                "`{}` lists {:?}, which does not include the probe `{}`",
                doc.label,
                doc.words,
                head,
            );
            // And a word that takes an argument must be probed WITH one,
            // or the probe exercises a different grammar rule.
            assert_eq!(
                doc.arg.is_some(),
                doc.probe.contains(' '),
                "`{}`: the probe and the declared arity disagree",
                doc.label,
            );
        }
    }

    #[test]
    fn static_words_classify() {
        assert!(matches!(parse("q"), ParsedCommand::Quit));
        assert!(matches!(parse("quit"), ParsedCommand::Quit));
        assert!(matches!(parse("help"), ParsedCommand::Help));
        assert!(matches!(parse("?"), ParsedCommand::Help));
        // `:alias` resolves as the ordinary client-owned RESOURCE it is now.
        assert!(matches!(
            parse("alias"),
            ParsedCommand::Resource(ResourceId::Local(
                crate::kube::local::LocalResourceKind::Alias
            ))
        ));
        assert!(matches!(parse("home"), ParsedCommand::Overview));
    }

    /// `:ctx` is not a special word any more — it resolves through the same
    /// alias path as `:pods` and `:pf`, which is what makes it RESET the nav
    /// stack (contexts are a root, not a drill) and pick up grep, sort and
    /// the rest of the table machinery for free.
    #[test]
    fn ctx_resolves_as_an_ordinary_resource_alias() {
        for word in ["ctx", "context", "contexts"] {
            match parse(word) {
                ParsedCommand::Resource(rid) => assert_eq!(
                    rid,
                    crate::kube::local::LocalResourceKind::Context.to_resource_id(),
                    "`:{word}` must name the contexts resource",
                ),
                _ => panic!("`:{word}` should resolve to a resource"),
            }
        }
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

#[cfg(test)]
#[path = "../tests/kube/session_commands.rs"]
mod drain_tests;

#[cfg(test)]
#[path = "../tests/kube/session_commands_exec.rs"]
mod exec_name_tests;
