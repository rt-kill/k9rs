use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::actions::{Action, LogRange};
use crate::{App, TakeoverOp};
use crate::element::Element;
use super::keymap::ViewClass;

/// The chord of a key event, if it is a character key (Ctrl tracked,
/// Shift riding in the character's case, Alt chords excluded — an
/// Alt-modified key is a different chord, not its bare character).
fn combo_of(key: &KeyEvent) -> Option<crate::KeyCombo> {
    crate::KeyCombo::of_event(key)
}

/// Whether these modifiers make the event a DIFFERENT CHORD from its bare
/// character. Ctrl or Alt — never Shift, which rides in the character's own
/// case and is how `Shift-F` and `C` are expressed.
///
/// The per-class handlers below match on `key.code` alone, so an unclaimed
/// chord falls straight through to the literal arm for its bare character:
/// `Alt-q` reached `KeyCode::Char('q')` and QUIT THE APP. [`combo_of`]
/// already refuses both modifiers for the data-driven lookups — but only the
/// lookups go through it, and the structural arms do not.
///
/// This replaces four hand-written copies of the rule, one per view class,
/// each of which named CONTROL and none of which named ALT. The same
/// modifier set, asked as a different question ("is this text?"), is
/// `TextKey::new`'s first guard in `text_input.rs`.
fn carries_chord_modifier(key: &KeyEvent) -> bool {
    key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
}

/// Map a key press to an action via the resource's declared operations.
/// Data-driven: each operation's EFFECTIVE chord is the user's `keys.*`
/// override or the descriptor default (`KeysConfig::op_key`), matched
/// EXACTLY (Ctrl state included) — an override to `ctrl-l` neither
/// leaves plain `l` bound nor lets a stray Ctrl chord alias its bare
/// character. First match wins — resources control priority via
/// `operations()` order.
fn lookup_view_op_key(app: &App, key: &KeyEvent) -> Option<Action> {
    let combo = combo_of(key)?;
    // Use the top element's capabilities (not the registry) — works for
    // both resource and derived views. Derived views declare their own
    // operations on DerivedViewKind; resource views delegate to ResourceId.
    let caps = app.nav.top().capabilities();
    for op in &caps.operations {
        if app.config.keys.op_key(op) == Some(combo) {
            return Some(Action::from(op));
        }
    }
    None
}

/// Look up a key binding from user-defined overlays for the current resource.
/// Maps key → capability name. No implementation details leak into the action.
fn lookup_overlay_key(app: &App, key: char) -> Option<Action> {
    let rid = app.nav.top().rid()?;
    let overlay = k9rs_proto::overlay::overlay_for(rid.plural())?;
    let cap_name = overlay.bindings.get(&key)?;
    Some(Action::OverlayCapability(cap_name.clone()))
}

/// Maps a `KeyEvent` to an `Action` based on the current application state.
/// Returns `None` if the key has no binding in the current context.
///
/// Note: filter mode and command mode input are handled directly in the main
/// event loop (main.rs) before this function is called. This function only
/// handles normal and confirmation-dialog key mappings.
pub fn handle_key_event(app: &App, key: KeyEvent) -> Option<Action> {
    // -----------------------------------------------------------------------
    // Overlays first. Checked BEFORE global keys so `:q`, Ctrl-C, etc.
    // can't leak through during the edit flow or a shell connect.
    // -----------------------------------------------------------------------
    match &app.ui.takeover() {
        Some(TakeoverOp::Edit { .. }) | Some(TakeoverOp::Shell(_)) => {
            // Modal operations block ALL keys except Esc. (Shell during
            // Connecting: Esc cancels; during bridge mode the TUI is
            // suspended and this path is never reached.)
            return match key.code {
                KeyCode::Esc => Some(Action::Back),
                _ => None,
            };
        }
        None => {}
    }

    // -----------------------------------------------------------------------
    // Detail views and log view override `/` to start search instead of filter.
    // -----------------------------------------------------------------------
    if matches!(
        app.nav.top(),
        Element::ContentView(_) | Element::LogSession(_) | Element::LogFilter(_)
    ) && key.code == KeyCode::Char('/')
    {
        return Some(Action::SearchStart);
    }

    // -----------------------------------------------------------------------
    // Global keys (available in every view) — EXCEPT under a modal element,
    // which owns every key, globals included. See
    // `Element::captures_all_input`; the rule is about the element, not
    // about `:`, which is why it is one check here and not a condition in
    // each global binding.
    //
    // Modality only SKIPS the globals. Dispatch always goes through the one
    // per-class match below. This used to re-dispatch here, with a
    // `ViewClass::Dialog => … , _ => None` of its own: unreachable today
    // only because `captures_all_input` and `ViewClass::of` happen to agree
    // about which class is modal — two independent matches, in two files. A
    // modal element of any other class would have fallen into that `_` and
    // become a black hole, Esc included, while the exhaustive match twenty
    // lines down would have dispatched it correctly.
    // -----------------------------------------------------------------------
    if !app.nav.top().captures_all_input() {
        if let Some(action) = handle_global_keys(app, key) {
            return Some(action);
        }
    }

    // -----------------------------------------------------------------------
    // Per-kind keys: EXHAUSTIVE over the top element — adding a kind
    // forces a decision here.
    // -----------------------------------------------------------------------
    // The class mapping lives in `keymap::ViewClass::of`, so the keymap that
    // RUNS and the keymap that is DOCUMENTED are the same partition of the
    // element kinds — help cannot describe a view it isn't dispatching for.
    match super::keymap::ViewClass::of(app.nav.top()) {
        ViewClass::Overview => handle_overview_keys(key),
        ViewClass::Table => handle_resource_view_keys(app, key),
        ViewClass::Detail => handle_detail_view_keys(key),
        ViewClass::Log => handle_log_view_keys(app, key),
        // Three keymaps, three arms — `handle_dialog_keys`'s inner match
        // over `Element` is gone with them. It had to re-derive which dialog
        // this was, after `ViewClass::of` had already decided, and its
        // `_ => handle_confirm_dialog` arm meant a fourth dialog kind would
        // have been dismissed by any key.
        ViewClass::Confirm => handle_confirm_dialog(app, key),
        ViewClass::Form => handle_form_keys(app, key),
        ViewClass::Picker => handle_container_select_keys(key),
    }
}

// ---------------------------------------------------------------------------
// Confirm dialog
// ---------------------------------------------------------------------------

fn handle_confirm_dialog(app: &App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => Some(Action::Confirm),
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Some(Action::Cancel),
        KeyCode::Enter => {
            // Confirm or cancel based on which button is selected
            if matches!(app.nav.top(), Element::Confirm(d) if d.action_focused()) {
                Some(Action::Confirm)
            } else {
                Some(Action::Cancel)
            }
        }
        KeyCode::Left | KeyCode::Right | KeyCode::Tab => Some(Action::ToggleDialogButton),
        // Any other key cancels. Cancel is always the safe direction, so a
        // stray keystroke can never trap the user behind a modal — in
        // particular a spurious 'r' (a stale terminal response misparsed as a
        // key after an alt-screen transition) opens the restart dialog, and
        // the user's next keypress dismisses it instead of being swallowed.
        _ => Some(Action::Cancel),
    }
}

// ---------------------------------------------------------------------------
// Global keys
// ---------------------------------------------------------------------------

fn handle_global_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Ctrl-C: quit (unless noExitOnCtrlC is set).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        if !app.no_exit_on_ctrl_c {
            return Some(Action::Quit);
        } else {
            return None;
        }
    }

    // Ctrl-R: force refresh (global).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
        return Some(Action::Refresh);
    }

    // Ctrl-E: toggle header (global).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('e') {
        return Some(Action::ToggleHeader);
    }

    // Ctrl-S: save logs (in log views) or save table (everywhere else).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
        return if matches!(app.nav.top(), Element::LogSession(_) | Element::LogFilter(_)) {
            Some(Action::SaveLogs)
        } else {
            Some(Action::SaveTable)
        };
    }

    // Ctrl-A: show aliases view.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('a') {
        return Some(Action::ShowAliases);
    }

    // Ctrl-W: toggle wide column mode.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('w') {
        return Some(Action::ToggleWide);
    }

    // Ctrl-Space: span-mark (select range).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char(' ') {
        return Some(Action::SpanMark);
    }

    // Ctrl-\: clear all marks.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('\\') {
        return Some(Action::ClearMarks);
    }

    // Ctrl-Z: toggle fault filter.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('z') {
        return Some(Action::ToggleFaultFilter);
    }

    match key.code {
        // `q` is NOT global quit — it is context-sensitive (handled per-view).
        KeyCode::Char(':') => Some(Action::CommandMode),
        KeyCode::Char('/') => Some(Action::Filter(String::new())),
        KeyCode::Char('?') => Some(Action::Help),
        // Esc is NOT global — it is context-sensitive (clear filter in resource
        // view, go back in sub-views). Handled per-view below.
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Resource list view
// ---------------------------------------------------------------------------

fn handle_resource_view_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // STRUCTURAL chords — Ctrl-D, Ctrl-K, Shift-F — read off
    // `OperationKind::structural_chord`, the one place they are declared.
    //
    // Iterating what the resource DECLARES is what makes "only if it is
    // supported" true by construction. It used to be an `if` per chord with
    // the support check written out each time, and `Shift-F` (further down
    // the match, before this) had no check at all: help only advertised it
    // where port-forward was declared, but the key fired anywhere and failed
    // later with "No target found". Unconditional Ctrl-D had the same shape
    // once — it offered "Delete Context/prod?" on the contexts view, whose
    // `operations()` is deliberately empty because a kubeconfig context is
    // switched to, not deleted.
    if let Some(combo) = combo_of(&key) {
        for op in &app.current_capabilities().operations {
            if super::keymap::structural_chord(op) == Some(combo) {
                return Some(Action::from(op));
            }
        }
    }

    // Configured column-cursor bindings (exact chord). Checked before the
    // operation lookup so a user binding `colRight: l` shadows an `l`
    // operation default. Arrows below are structural and always work.
    if let Some(action) = configured_column_action(app, &key) {
        return Some(action);
    }
    // Rebound switch-to-all-namespaces (structural default `0`). Checked
    // before the `Char('0')` structural arm below, so a user who bound
    // `colFirst: "0"` can still reach the action on another key (e.g.
    // `namespaceAll: ")"`).
    if let Some(combo) = combo_of(&key) {
        if app.config.keys.namespace_all == Some(combo) {
            return Some(Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All));
        }
    }

    // Data-driven operation bindings: user override or descriptor
    // default, matched exactly (Ctrl state included). Before the
    // structural match so a `ctrl-x` override can't be shadowed by a
    // modifier-blind `Char(x)` arm.
    if let Some(action) = lookup_view_op_key(app, &key) {
        return Some(action);
    }

    // An UNMATCHED chord stops here: the structural arms below match on
    // `key.code` alone and would silently alias it to its bare character
    // (Ctrl-Q → quit, Ctrl-G → Home, Ctrl-Y → the overlay 'y' binding, and
    // Alt-q → quit). A chord either means something as a chord or nothing.
    if carries_chord_modifier(&key) {
        return None;
    }

    match key.code {
        // `q` in resource view: if drilled/filtered, pop one level. Otherwise quit.
        KeyCode::Char('q') => {
            if app.nav.is_drilled() {
                Some(Action::ClearFilter)
            } else {
                Some(Action::Quit)
            }
        }

        // Esc in a resource view. Emitted UNCONDITIONALLY: whether it does
        // anything is the action layer's call, not this one's. It used to
        // ask `is_drilled()` first and return `None` at the root — a copy of
        // the "nothing to pop at the root" rule that `ClearFilter` already
        // enforces, and one that swallowed the key before
        // `select_gate::gate_action` could shadow it into `ClearMarks`.
        // Overview is only a startup page — `:overview` or `:home` to return.
        KeyCode::Esc => Some(Action::ClearFilter),

        // Navigation. Column-cursor movement is arrow-keys-only by
        // default (`l` is logs, k9s-style); `keys.colLeft`/`colRight`
        // above add chords like h/l back for vim-column users.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::Left => Some(Action::ColLeft),
        KeyCode::Right => Some(Action::ColRight),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Drill down.
        KeyCode::Enter => Some(Action::Enter),

        // Detail views. Describe/Yaml bind through the operation lookup
        // above (descriptor defaults d/y, user-rebindable); Edit is a
        // client-side flow, not an OperationKind, so it stays structural.
        KeyCode::Char('e') => Some(Action::Edit),
        // `F` (port-forward) is a STRUCTURAL chord, handled above against
        // the resource's declared operations — not here, where it fired
        // whether or not the resource had a port to forward.
        // f: show active port-forwards for this resource.
        KeyCode::Char('f') => Some(Action::ShowPortForwards),

        // Jump to owner (Shift+J). Navigates up the ownerReferences chain.
        KeyCode::Char('J') => Some(Action::JumpToOwner),

        // UsedBy (U). Shows which resources reference the selected row.
        KeyCode::Char('U') => Some(Action::UsedBy),

        // Events (E). The events ABOUT the selected row.
        KeyCode::Char('E') => Some(Action::ShowEvents),

        // Column-restricted grep: filter by hovered column only.
        KeyCode::Char('~') => Some(Action::ColumnFilter),

        // Toggle between last two views.
        KeyCode::Char('-') => Some(Action::ToggleLastView),

        // Sort by current column (toggle direction).
        KeyCode::Char('O') => Some(Action::ToggleSortDirection),

        // Sort by NAME / AGE — BY NAME, resolved against the live headers.
        // These used to be positional guesses (0-or-1 from a boolean, and
        // "the last column"), which were wrong on any table whose shape
        // didn't match the assumption. See `SortTarget::Named`.
        KeyCode::Char('N') => {
            Some(Action::Sort(crate::SortTarget::Named(crate::actions::COL_NAME)))
        }
        KeyCode::Char('A') => {
            Some(Action::Sort(crate::SortTarget::Named(crate::actions::COL_AGE)))
        }

        // Sort by the currently selected (cursor) column — works in every table
        // view. Repeats toggle direction (sort_by_column handles that). Uses the
        // DATA index (sort indexes the full cells array), translating from the
        // cursor's visible index so hidden columns don't shift the target.
        KeyCode::Char('S') => Some(Action::Sort(crate::SortTarget::CursorColumn)),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        // Mark/select rows.
        KeyCode::Char(' ') => Some(Action::ToggleMark),

        // 0: switch to all namespaces
        KeyCode::Char('0') => Some(Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All)),

        // Tab cycling.
        KeyCode::Tab => Some(Action::NextTab),
        KeyCode::BackTab => Some(Action::PrevTab),

        // Operation bindings were tried above the structural match;
        // remaining chars fall through to user-overlay bindings.
        KeyCode::Char(c) => lookup_overlay_key(app, c),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// YAML / Describe detail views
// ---------------------------------------------------------------------------

fn handle_detail_view_keys(key: KeyEvent) -> Option<Action> {
    // Ctrl-d / Ctrl-u for half-page scroll (vim-style). This view is the one
    // that CLAIMS Ctrl, so its chord guard comes after the claim rather than
    // before it; what is left to refuse is Alt.
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('d') => Some(Action::PageDown),
            KeyCode::Char('u') => Some(Action::PageUp),
            _ => None,
        };
    }
    // Any other chord is nothing — never its bare character.
    if carries_chord_modifier(&key) {
        return None;
    }

    match key.code {
        // `q` or Esc in detail views goes back (also clears search).
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        // Navigation.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Search navigation.
        KeyCode::Char('n') => Some(Action::SearchNext),
        KeyCode::Char('N') => Some(Action::SearchPrev),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Log view
// ---------------------------------------------------------------------------

/// The configured column-cursor action this key is, if any — for BOTH the
/// table and the log view.
///
/// One lookup, because the four column actions are a property of the ACTION
/// and not of one view, which is what the log view's own comment claimed
/// while checking three of them: `keys: { colLast: "e" }` jumped columns in
/// a table and did nothing in a log. Two copies of a four-way list is how
/// one of them comes to be three-way.
fn configured_column_action(app: &App, key: &KeyEvent) -> Option<Action> {
    let combo = combo_of(key)?;
    let keys = &app.config.keys;
    // Named rather than looped, because each one names its own action.
    if keys.col_left == Some(combo) {
        return Some(Action::ColLeft);
    }
    if keys.col_right == Some(combo) {
        return Some(Action::ColRight);
    }
    if keys.col_first == Some(combo) {
        return Some(Action::ColFirst);
    }
    if keys.col_last == Some(combo) {
        return Some(Action::ColLast);
    }
    None
}

/// The log range a bare digit selects: `0` the tail, `1`–`6` the windows of
/// `LogSince::ALL` in its declared order, `7` everything there is.
///
/// Indexing `ALL` is what keeps this ladder and the keymap's labelled rows
/// from drifting — they read one list, and that they agree is pinned by
/// `every_documented_view_key_dispatches_to_the_action_it_claims`. `7` is
/// its own arm because "from the beginning" is not a duration, which is why
/// the old `Option<String>` payload could not hold it and the oldest logs
/// were unreachable past 24h.
fn log_range_for_digit(key: &KeyEvent) -> Option<Action> {
    let KeyCode::Char(c) = key.code else { return None };
    let range = match c {
        '0' => LogRange::Tail,
        '7' => LogRange::Head,
        '1'..='6' => {
            // `'1'` is index 0. The slice's length is asserted at the build
            // in `keymap.rs`, so this cannot be out of range.
            let idx = (c as u8 - b'1') as usize;
            LogRange::Since(k9rs_proto::protocol::LogSince::ALL[idx])
        }
        _ => return None,
    };
    Some(Action::LogRange(range))
}

fn handle_log_view_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Shift-C: clear logs.
    if key.modifiers.contains(KeyModifiers::SHIFT) && key.code == KeyCode::Char('C') {
        return Some(Action::ClearLogs);
    }

    // Unmatched chords are nothing — never their bare character
    // (Ctrl-Q and Alt-q would otherwise both alias to 'q' → Back).
    if carries_chord_modifier(&key) {
        return None;
    }

    // The range ladder FIRST, ahead of the configured column chords — the
    // one place this view does not let a rebind shadow a literal.
    //
    // `colFirst: "0"` is a documented, validating shape, and on a table it
    // costs nothing: switch-to-all-namespaces keeps its own `namespaceAll`
    // rebind. Here it cost the whole `0`–`7` ladder, which the hint bar goes
    // on advertising as `<0-7> range` and which has no other key. Column
    // panning does: `^` and `$` are structural and always work. So the
    // digits win, and a user who rebinds `colFirst` keeps both.
    if let Some(action) = log_range_for_digit(&key) {
        return Some(action);
    }

    // Configured column bindings, same chords the table honours — this is
    // what makes `colLeft`/`colRight` a property of the ACTION rather than
    // of one view. Checked before the remaining literals so a rebind
    // shadows them, exactly as it does on a table.
    if let Some(action) = configured_column_action(app, &key) {
        return Some(action);
    }

    match key.code {
        // `q` in log view goes back.
        KeyCode::Char('q') => Some(Action::Back),
        // Esc: pop — a committed log filter IS an element, so the first
        // press pops one filter, the next leaves the log view; a live
        // draft is cancelled by the input path before this fires.
        KeyCode::Esc => Some(Action::Back),

        // Scrolling.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::ScrollDown(1)),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::ScrollUp(1)),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Log-specific toggles.
        KeyCode::Char('s') => Some(Action::ToggleLogFollow),
        KeyCode::Char('w') => Some(Action::ToggleLogWrap),
        // One key, whole cycle — see `Role::SHED_ORDER`.
        KeyCode::Char('t') => Some(Action::ToggleLogTimestamps),

        // Horizontal pan. `ColLeft`/`ColRight` are the SAME actions the
        // tables use for their column cursor — one intent, answered by each
        // element in its own units. `p` hides the prefix; this reads past it,
        // and past the right edge of a long line without turning wrap on.
        // `ColFirst` is the way back: undoing a 400-column pan at a quarter
        // of a screen per press is twenty keystrokes.
        KeyCode::Left | KeyCode::Char('h') => Some(Action::ColLeft),
        KeyCode::Right | KeyCode::Char('l') => Some(Action::ColRight),
        // vim's own shape: `h`/`l` a column at a time, `^`/`$` the ends,
        // `H`/`L` a stride in between.
        KeyCode::Char('^') => Some(Action::ColFirst),
        KeyCode::Char('$') => Some(Action::ColLast),
        KeyCode::Char('H') => Some(Action::PageLeft),
        KeyCode::Char('L') => Some(Action::PageRight),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Overview (the landing page)
// ---------------------------------------------------------------------------

fn handle_overview_keys(key: KeyEvent) -> Option<Action> {
    // Unmatched chords are nothing — never their bare character.
    if carries_chord_modifier(&key) {
        return None;
    }
    match key.code {
        KeyCode::Char('q') => Some(Action::Quit),
        // Tab goes to the first resource view
        KeyCode::Tab => Some(Action::NextTab),
        KeyCode::BackTab => Some(Action::PrevTab),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Dialog-class elements: confirm, picker, form
// ---------------------------------------------------------------------------


/// The ways OUT of a form, and only those.
///
/// A form is a nav element, so leaving it is the ordinary pop —
/// `Action::Back`, the one undo path. Typing and focus movement are
/// mutations and stay in `session_commands::handle_form_dialog_key`; naming
/// them in both places is what let Esc be "handled" by a call that cleared
/// the modal slot a form does not live in.
fn handle_form_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Same guard, same reason: a form's Esc and Enter are bare keys, and a
    // chord that happens to carry one of their codes is not them.
    if carries_chord_modifier(&key) {
        return None;
    }
    let cancel_focused = app.nav.top().form_dialog().is_some_and(|d| d.cancel_focused());
    match key.code {
        KeyCode::Esc => Some(Action::Back),
        KeyCode::Enter if cancel_focused => Some(Action::Back),
        _ => None,
    }
}

fn handle_container_select_keys(key: KeyEvent) -> Option<Action> {
    // Unmatched chords are nothing — never their bare character. The four
    // per-class handlers got this guard when `Alt-q` was found quitting;
    // this one was reached through `handle_dialog_keys` and was not among
    // them, so `Ctrl-j` scrolled the picker and `Ctrl-q` left it.
    if carries_chord_modifier(&key) {
        return None;
    }
    match key.code {
        // `q` or Esc in container select goes back.
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') | KeyCode::BackTab => Some(Action::PrevItem),
        KeyCode::Enter => Some(Action::Enter),
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/handler.rs"]
mod tests;
