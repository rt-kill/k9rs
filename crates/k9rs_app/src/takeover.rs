//! Everything that takes the screen, or part of it, away from the view.
//!
//! The `:` prompt and the screen takeovers (`$EDITOR`, a shell) — [`Modal`]
//! is ONE slot, so two of THOSE at once is unrepresentable. It replaced four
//! mutually-exclusive `Option` fields that 53 write sites had to clear in
//! concert, and any path that forgot left a modal buried under another,
//! invisible until the top one closed.
//!
//! The list used to name six things, including the confirm dialog, the form
//! and the container picker. Those are nav-stack elements now (see the
//! comments inside [`Modal`]), so the slot's exclusion covers two of the six
//! states it was claiming: `Modal::Command` alongside an `Element::Confirm`
//! IS representable. Nothing reaches it today, but what prevents it is the
//! INPUT ROUTER, not this slot, and from both directions:
//!
//! - `:` under a dialog never opens the prompt. A modal element skips the
//!   globals, so `:` reaches the confirm box's own keymap and lands on its
//!   dismiss-on-any-key arm (pinned by
//!   `a_colon_under_a_dialog_cancels_it_rather_than_opening_the_prompt`).
//! - A submitted command cannot push a dialog UNDER the prompt: the bar
//!   closes before the command runs, and no command word pushes a confirm
//!   box anyway.
//!
//! If either changed, the prompt would swallow the dialog's keys —
//! `edit_command_key` consumes everything whenever `command_input` is
//! `Some`, whatever is on the nav stack — and the box would be unanswerable.
//!
//! [`TempFile`] and its `Drop` are here because the edit flow owns a file on
//! disk for exactly as long as the takeover exists.

use k9rs_proto::protocol::ObjectRef;
/// Everything that can be modal over the current view: the `:` prompt, or a
/// screen takeover (`$EDITOR` / a shell).
///
/// Three variants. The confirm dialog, the form, the container picker and
/// help all left for the nav stack — see the comments on the variants below
/// — and this doc went on listing them, which made the exclusion it claims
/// sound broader than it is.
///
/// These were four independent fields (`input_mode`, `confirm_dialog`,
/// `form_dialog`, `overlay`) that were mutually exclusive *by intent*: every
/// path that opened one had to clear the other three, at 53 write sites.
/// `begin_context_switch` cleared all four in a row. Any path that forgot
/// left a modal buried under another, invisible until the top one closed.
///
/// One field makes that impossible FOR THESE THREE: opening a modal is an
/// assignment, and an assignment displaces whatever was there. A modal
/// co-existing with a dialog ELEMENT is a different question, answered by
/// the input router rather than here.
pub enum Modal {
    /// Nothing modal — keys go to the view.
    None,
    /// The `:` command prompt.
    Command {
        input: crate::text_input::TextInput,
        history_index: Option<usize>,
    },
    // `Confirm` used to live here. It is a nav ELEMENT now
    // (`Element::Confirm`), which is what makes Esc the ordinary pop and
    // puts its keys through the same per-kind dispatch as every other view.
    // `Form` used to live here. It is a nav ELEMENT now (`Element::Form`)
    // like the other dialogs; only its TYPING still goes through the input
    // sink, which is where every text surface in the app is handled.
    /// An operation that TAKES OVER the terminal — `$EDITOR`, a live shell.
    ///
    /// Not a view, which is why it is not a nav element: you do not navigate
    /// to it and you cannot drill from it. It suspends the TUI, holds RAII
    /// (an `ExecStream`, a `TempFile`) and completes. Every dialog that WAS
    /// here — help, the container picker, the confirm box — is an element
    /// now, so this variant finally means one thing instead of "whatever
    /// isn't an element".
    Takeover(TakeoverOp),
}

impl Modal {
    /// Owned-variant extractors, for flows that must MOVE what they close
    /// (the edit flow takes its `TempFile`, the shell its `ExecStream`).
    pub fn into_takeover(self) -> Option<TakeoverOp> {
        match self {
            Modal::Takeover(o) => Some(o),
            _ => None,
        }
    }


}

// ---------------------------------------------------------------------------
// TakeoverOp — the single modal slot
// ---------------------------------------------------------------------------

/// A dialogue or operation layered over the current view. NOT navigation:
/// views live on the nav stack ([`crate::element::Element`]); an
/// overlay has no history, no data handle, no self-definition — Esc
/// closes it (dropping its RAII: `client::ExecStream`,
/// [`TempFile`]) and the view underneath was never displaced.
pub enum TakeoverOp {
    // The `?` help sheet used to live here. It is a nav ELEMENT now
    // (`LocalResourceKind::Keybinding`), which is what gives it grep,
    // scroll, sort and copy for free and makes Esc the ordinary pop —
    // see `Element::extent` for how it still floats.
    // `ContainerSelect` used to live here. It is a nav ELEMENT now
    // (`Element::Picker`) for the same reasons help and confirm are: Esc is
    // the ordinary pop, and its keys arrive through the one per-kind
    // dispatch instead of a branch that has to be remembered.
    /// The unified edit flow (a transient state machine that suspends the
    /// TUI for `$EDITOR`):
    ///
    ///   1. `Action::Edit` opens this overlay in `EditState::AwaitingYaml`
    ///      and sends `SessionCommand::Yaml(target)`.
    ///   2. The server returns YAML; `apply_event` writes the temp file
    ///      and transitions to `EditState::EditorReady`.
    ///   3. The session loop sees `EditorReady`, suspends raw mode, runs
    ///      `$EDITOR`, sends `Apply`, transitions to `Applying`.
    ///   4. The target-gated `OpResult` clears the overlay (or re-opens
    ///      the editor with the server error prepended).
    Edit {
        target: k9rs_proto::protocol::ObjectRef,
        state: EditState,
    },
    /// Live shell session. During Connecting the TUI shows a full-frame
    /// connect screen (all keys blocked except Esc); once Connected the
    /// session loop suspends the TUI and raw-bridges stdin↔daemon bytes.
    /// Drop aborts the bridge.
    Shell(Box<ShellState>),
}

impl TakeoverOp {
    /// The frame area this operation claims — the same
    /// [`ViewExtent`](crate::element::ViewExtent) a nav element
    /// declares, because it is the same question.
    ///
    /// There used to be an `OpExtent` beside it with its own `Dialog` and
    /// `FullFrame`, which meant one question had two answers and the
    /// takeover's `Dialog` could not say how big it was — so its renderer
    /// centred itself, a second geometry authority beside the float path's.
    /// Now `Dialog` carries a size here too and the renderer's `dialog_box`
    /// places both.
    ///
    /// EXHAUSTIVE: a new operation must choose, and the central erase in
    /// the draw entry point reads the answer rather than each renderer
    /// re-remembering it.
    pub fn extent(&self, frame: ratatui::layout::Rect) -> crate::element::ViewExtent {
        use crate::element::ViewExtent;
        match self {
            // A status line in a box, sized from the line — the same rule
            // the confirm/form/picker elements follow, in the same units.
            // `+ 6`: two cells of padding each side plus the border.
            TakeoverOp::Edit { .. } => ViewExtent::Dialog {
                width: (self.label().chars().count() as u16 + 6).min(frame.width),
                height: 3u16.min(frame.height),
            },
            // The connect screen is a full-screen titled frame: the view
            // beneath must not render inside it. It used to — the shell
            // block painted its border over the resource table and left
            // every interior cell as the table had drawn it.
            TakeoverOp::Shell(_) => ViewExtent::Full,
        }
    }

    /// The one line this operation has to say for itself, when it has one.
    ///
    /// On the op rather than in the renderer because [`Self::extent`] has to
    /// measure it: a box sized from a label the renderer invents separately
    /// is two statements about one string.
    pub fn label(&self) -> String {
        match self {
            TakeoverOp::Edit { target, state } => {
                let kind = target.resource.display_label();
                let name = &target.name;
                match state {
                    EditState::AwaitingYaml => format!("Fetching YAML for {}/{}…", kind, name),
                    EditState::EditorReady { .. } => {
                        format!("Launching editor for {}/{}…", kind, name)
                    }
                    EditState::Applying { .. } => format!("Applying {}/{}…", kind, name),
                }
            }
            // The shell draws its own titled frame; it has no one-liner.
            TakeoverOp::Shell(s) => s.title.clone(),
        }
    }
}

impl std::fmt::Debug for TakeoverOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {

            TakeoverOp::Edit { target, .. } => {
                f.debug_struct("Edit").field("target", target).finish_non_exhaustive()
            }
            TakeoverOp::Shell(state) => f.debug_tuple("Shell").field(state).finish(),
        }
    }
}

/// Shell connection lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShellConnectState {
    /// Waiting for the first byte from the daemon's PTY.
    #[default]
    Connecting,
    /// At least one ExecData frame has arrived — shell is live.
    /// The main loop will transition to raw bridge mode on the next
    /// iteration, suspending the TUI and piping bytes directly.
    Connected,
}

/// State for a live shell session. During the Connecting phase the TUI
/// renders a loading bar and the user has full navigation control. Once
/// the daemon confirms the connection (first ExecData), the session loop
/// suspends the TUI and enters a raw byte bridge — stdin→daemon,
/// daemon→stdout — with no parsing. The vt100 crate is not involved.
pub struct ShellState {
    pub title: String,
    pub stream: Option<crate::client::ExecStream>,
    pub connect_state: ShellConnectState,
    /// Bytes received before the bridge loop starts. Written to stdout
    /// on attach so the initial prompt isn't lost.
    pub pending_output: Vec<u8>,
}

impl std::fmt::Debug for ShellState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShellState")
            .field("title", &self.title)
            .field("connect_state", &self.connect_state)
            .finish_non_exhaustive()
    }
}

/// What the user is going to do with the container picked from the
/// container picker ([`crate::element::Element::Picker`]). Each variant
/// names one intent so call
/// sites don't have to thread a separate boolean (e.g. `previous`)
/// alongside a `for_shell` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerAction {
    /// Open a streaming (follow-mode) log view for the chosen container.
    Logs,
    /// Open the previous container incarnation's logs (static, `previous=true`).
    PreviousLogs,
    /// Open a `kubectl exec -it` shell into the chosen container.
    Shell,
}

/// One row of the container picker: what it shows, and what picking it
/// means. Keeping the two together is what lets "all containers" be an
/// ordinary row instead of a magic index the renderer and the handler both
/// have to remember to offset around.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerChoice {
    /// The rendered label (`"all containers"`, `"web"`, `"init:setup"`).
    pub label: String,
    /// The log selector this row means. `All` appears only in a LOG
    /// picker — there is no shell into every container at once, and
    /// [`ContainerChoice::list`] is the single place that upholds it.
    pub container: k9rs_proto::protocol::LogContainer,
}

impl ContainerChoice {
    /// The picker's rows for `action`.
    ///
    /// A multi-container pod's logs used to be unreachable as a WHOLE:
    /// `LogContainer::All` existed on the wire but no UI path produced it
    /// for a pod, so "show me this pod's logs" always meant one container.
    /// The log pickers therefore lead with it.
    pub fn list(
        action: ContainerAction,
        containers: &[k9rs_proto::row::ContainerInfo],
    ) -> Vec<Self> {
        use k9rs_proto::protocol::LogContainer;
        let mut out = Vec::with_capacity(containers.len() + 1);
        if !matches!(action, ContainerAction::Shell) {
            out.push(Self { label: "all containers".to_string(), container: LogContainer::All });
        }
        out.extend(containers.iter().map(|ci| Self {
            label: ci.display_name(),
            container: LogContainer::Named(ci.name.clone()),
        }));
        out
    }
}

/// Where we are in the unified edit flow. See `Route::EditingResource`.
/// RAII wrapper for an edit temp file. Drop deletes the file. Ownership
/// moves between EditState variants without triggering cleanup — only
/// when the TempFile is truly dropped (route popped, context switch, quit)
/// does the file get removed. Prevents the premature-deletion bug that
/// Clone+Drop on EditState would cause.
#[derive(Debug)]
pub struct TempFile(pub std::path::PathBuf);

impl TempFile {
    pub fn path(&self) -> &std::path::Path { &self.0 }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Debug)]
pub enum EditState {
    /// Sent the `Yaml(target)` command, waiting for the server's response.
    AwaitingYaml,
    /// YAML on disk, ready for the session loop to suspend + exec the
    /// editor. `original` is the unmodified YAML for diff comparison.
    EditorReady { temp_file: TempFile, original: String },
    /// Sent the `Apply { target, yaml }` command, waiting for this
    /// target's `OpResult` to know whether the apply succeeded. Carries
    /// the temp file and original YAML so the editor can re-open on
    /// server error (same UX as `kubectl edit`).
    Applying { temp_file: TempFile, original: String },
}

/// Single-target operation that can sit behind a confirmation dialog.
/// Closed enum: only the three operations the TUI ever raises a confirm
/// for. Replaces the prior `PendingAction::Single { action: Action, ... }`
/// which carried the full 60-variant `Action` enum and forced every
/// match site to add a `_ => unreachable!` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleOp {
    Delete,
    Restart,
    ForceKill,
}

/// What a confirm dialog will do when confirmed.
#[derive(Debug, Clone)]
pub enum PendingAction {
    /// Single-target action (delete, restart, force-kill one resource).
    Single {
        op: SingleOp,
        target: ObjectRef,
    },
    /// Batch delete marked resources.
    BatchDelete(Vec<ObjectRef>),
    /// Batch restart marked resources.
    BatchRestart(Vec<ObjectRef>),
    /// Batch force-kill marked resources.
    BatchForceKill(Vec<ObjectRef>),
}

/// Which of a confirm box's two buttons has focus.
///
/// It was a `bool` named `action_focused`, with its meaning in prose —
/// "True = action button focused, false = cancel focused (safe default)" —
/// read in the key handler, flipped in the action layer and branched on in
/// the widget, each of which had to remember the polarity. Its sibling five
/// declarations away, [`crate::FormFocus`], exists precisely because a bare
/// index "is why there was exactly one button": two dialogs in one file with
/// two models of focus.
///
/// Named, the safe default is `Cancel` by NAME rather than by `false`, and
/// `focus_next` is a cycle rather than a flip — so a third button would be
/// expressible, which `!action_focused` could never be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConfirmFocus {
    /// The Cancel button. The default, and the safe direction.
    #[default]
    Cancel,
    /// The action button — Delete, Restart, Force Kill.
    Action,
}

#[derive(Debug, Clone)]
pub struct ConfirmDialog {
    pub message: String,
    /// Label for the action button (e.g. "Delete", "Restart", "Force Kill").
    pub action_label: String,
    pub pending: PendingAction,
    /// Which button has focus. Opens on [`ConfirmFocus::Cancel`].
    pub focused: ConfirmFocus,
}

impl ConfirmDialog {
    /// The next button in the cycle. ONE function owns the order, as
    /// [`crate::FormDialog::focus_next`] does for the form's — so the two
    /// buttons cannot be reachable in one direction and not the other, and
    /// a third would join the cycle here and nowhere else.
    pub fn focus_next(&mut self) {
        self.focused = match self.focused {
            ConfirmFocus::Cancel => ConfirmFocus::Action,
            ConfirmFocus::Action => ConfirmFocus::Cancel,
        };
    }

    /// Whether the ACTION button is the one Enter would activate.
    pub fn action_focused(&self) -> bool {
        matches!(self.focused, ConfirmFocus::Action)
    }
}

/// "Which container?" — a pick-one list floating over the view that asked.
///
/// Captures its choices at construction: self-contained, so no table lookup
/// happens under a dialog that may outlive the row it came from.
#[derive(Debug, Clone)]
pub struct ContainerPicker {
    pub target: k9rs_proto::protocol::ObjectRef,
    /// The rows to pick from, already resolved for `action` — see
    /// [`ContainerChoice::list`]. `selected` indexes THIS, so there is one
    /// index space and no "row 0 means something else" offset.
    pub choices: Vec<ContainerChoice>,
    pub selected: usize,
    pub action: ContainerAction,
}

#[cfg(test)]
#[path = "tests/takeover.rs"]
mod tests;
