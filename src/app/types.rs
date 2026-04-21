use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

use crate::kube::protocol::{LogContainer, ObjectRef};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Named constants for configurable limits.
const MAX_LOG_LINES: usize = 50_000;
const KUBECTL_CACHE_CAPACITY: usize = 100;
const FLASH_EXPIRY_SECS: u64 = 5;

/// Duration a row-change flash highlight stays visible.
pub const CHANGE_HIGHLIGHT_DURATION: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// DeltaTracker — atomic ownership of row-change detection state
// ---------------------------------------------------------------------------

/// Owns the two maps that must stay in sync for row-change flash
/// highlights: per-row content hashes from the previous snapshot, and
/// timestamps of recently-changed rows. The only mutation paths go through
/// this struct's methods, so it's structurally impossible to update one
/// map without the other or to forget to clear both on a context switch.
///
/// Replaces the prior pair of loose `prev_rows: HashMap<ObjectKey, u64>` +
/// `changed_rows: HashMap<ObjectKey, Instant>` fields on `App`.
pub struct DeltaTracker {
    prev_hashes: HashMap<crate::kube::protocol::ObjectKey, u64>,
    changed: HashMap<crate::kube::protocol::ObjectKey, Instant>,
}

impl Default for DeltaTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaTracker {
    pub fn new() -> Self {
        Self {
            prev_hashes: HashMap::new(),
            changed: HashMap::new(),
        }
    }

    /// Compare incoming rows against the previous snapshot's hashes.
    /// Rows whose content hash changed get a fresh timestamp in `changed`.
    /// The old hash map is replaced wholesale — rows that disappeared from
    /// the new snapshot are implicitly forgotten.
    pub fn update(&mut self, rows: &[crate::kube::resources::row::ResourceRow]) {
        use crate::kube::protocol::ObjectKey;
        let now = Instant::now();
        let mut new_prev = HashMap::with_capacity(rows.len());
        for row in rows {
            let key = ObjectKey::new(
                row.namespace.clone().unwrap_or_default(),
                row.name.clone(),
            );
            let mut hasher = DefaultHasher::new();
            for cell in &row.cells {
                cell.hash(&mut hasher);
            }
            let new_hash = hasher.finish();
            if let Some(prev_hash) = self.prev_hashes.get(&key) {
                if *prev_hash != new_hash {
                    self.changed.insert(key.clone(), now);
                }
            }
            new_prev.insert(key, new_hash);
        }
        self.prev_hashes = new_prev;
    }

    /// Remove change highlights older than `max_age`. Returns `true` if
    /// any entries were removed (signals the UI should redraw).
    pub fn expire(&mut self, max_age: Duration) -> bool {
        let now = Instant::now();
        let before = self.changed.len();
        self.changed.retain(|_, ts| now.duration_since(*ts) < max_age);
        self.changed.len() != before
    }

    /// Clear all tracking state. Used on context and namespace switches
    /// so stale hashes from the old scope can't produce false change
    /// highlights in the new scope.
    pub fn clear(&mut self) {
        self.prev_hashes.clear();
        self.changed.clear();
    }

    /// Borrow the changed-rows map for the table widget's flash-highlight
    /// renderer. The map is keyed by `ObjectKey`; the widget looks up each
    /// visible row to decide whether to apply the highlight style.
    pub fn changed_rows(&self) -> &HashMap<crate::kube::protocol::ObjectKey, Instant> {
        &self.changed
    }
}

/// User config settings (loaded from ~/.config/k9rs/config.yaml).
#[derive(Debug, Clone, Copy)]
pub struct AppConfig {
    pub no_exit_on_ctrl_c: bool,
    pub read_only: bool,
}

/// The three-state lifecycle of a context switch. Replaces the prior pair
/// of `context_switch_pending: bool` + `pending_context_switch: Option<String>`
/// with a single source of truth:
///
/// - [`Stable`] — no switch in flight, new switches allowed.
/// - [`Requested`] — user asked to switch; the session main loop will
///   pick it up at the top of its next iteration and drop the current
///   `ClientSession`.
/// - [`InFlight`] — main loop has taken the request and is bringing up
///   the new connection. Blocks further switches until `ConnectionEstablished`
///   transitions back to `Stable`.
///
/// Transitions are linear: Stable → Requested → InFlight → Stable.
/// Attempting a new switch from Requested or InFlight is rejected at
/// `begin_context_switch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextSwitchState {
    Stable,
    Requested(crate::kube::protocol::ContextName),
    InFlight,
}

impl ContextSwitchState {
    /// True if a new switch can be initiated. The only "accepting" state.
    pub fn is_stable(&self) -> bool {
        matches!(self, Self::Stable)
    }

    /// If the state is [`Requested`], take the target name and
    /// transition to [`InFlight`]. Returns `None` in any other state
    /// and leaves the state alone. Used by the main loop to atomically
    /// consume a pending request without racing a second call.
    pub fn take_requested(&mut self) -> Option<crate::kube::protocol::ContextName> {
        // Discriminant check first so we don't swap out state we can't
        // recover from. If it's not Requested, bail untouched.
        let Self::Requested(_) = self else { return None; };
        // Safe: we just checked the discriminant on the line above, and
        // `&mut self` means nothing else can mutate in between.
        let Self::Requested(name) = std::mem::replace(self, Self::InFlight) else {
            unreachable!("discriminant checked immediately above")
        };
        Some(name)
    }

    /// Reset to [`Stable`]. Called on `ConnectionEstablished` once the
    /// new session is up.
    pub fn mark_stable(&mut self) {
        *self = Self::Stable;
    }
}

/// Filtered and total item counts for a resource table.
#[derive(Debug, Clone, Copy)]
pub struct ItemCounts {
    pub filtered: usize,
    pub total: usize,
}

/// Result of processing a search input keystroke.
pub enum SearchInputResult {
    /// Key was consumed, input updated (Char, Backspace).
    Updated,
    /// User pressed Esc — cancel search.
    Cancelled,
    /// User pressed Enter — commit search with this term (empty = clear).
    Committed(String),
}

/// Process a keystroke against a search input buffer.
/// Returns what happened so the caller can apply view-specific logic.
/// Callers gate on their own "search active" state before dispatching.
pub fn handle_search_key(
    input: &mut String,
    key: crossterm::event::KeyCode,
) -> SearchInputResult {
    match key {
        crossterm::event::KeyCode::Esc => {
            input.clear();
            SearchInputResult::Cancelled
        }
        crossterm::event::KeyCode::Enter => {
            let text = std::mem::take(input);
            SearchInputResult::Committed(text)
        }
        crossterm::event::KeyCode::Backspace => {
            input.pop();
            SearchInputResult::Updated
        }
        crossterm::event::KeyCode::Char(c) => {
            input.push(c);
            SearchInputResult::Updated
        }
        _ => SearchInputResult::Updated,
    }
}

/// Parse a formatted age string like "5m", "2h3m", "3d5h" to total seconds.
/// Used for correct age-column sorting (lexicographic comparison gets it wrong).
pub(crate) fn parse_age_seconds(s: &str) -> u64 {
    let mut total: u64 = 0;
    let mut num: u64 = 0;
    for c in s.chars() {
        match c {
            '0'..='9' => num = num * 10 + (c as u64 - '0' as u64),
            'd' => { total += num * 86400; num = 0; }
            'h' => { total += num * 3600; num = 0; }
            'm' => { total += num * 60; num = 0; }
            's' => { total += num; num = 0; }
            _ => {}
        }
    }
    total + num // handle trailing number without suffix
}

// ---------------------------------------------------------------------------
// CrdInfo — lightweight CRD metadata extracted from unified ResourceRow
// ---------------------------------------------------------------------------

/// Lightweight CRD metadata extracted from the typed `crd_info` field of a
/// `ResourceRow`. Type alias over [`crate::kube::protocol::CrdRef`] — the
/// shape is identical and `find_crd_by_name` returns the row's stored
/// CrdRef directly instead of re-cloning fields.
pub type CrdInfo = crate::kube::protocol::CrdRef;

// ---------------------------------------------------------------------------
// ContainerRef
// ---------------------------------------------------------------------------

/// Reference to a specific container within a pod. The `container` field
/// is the typed [`LogContainer`] selector — never a magic string. The
/// log-view widget reads it directly to decide whether to show a
/// per-container prefix; the relaunch-with-since path passes it back to
/// `stream_log_substream` without any string round-trip.
///
/// `namespace: String` is deliberate — it's a *location* (which namespace
/// the pod actually lives in), not a *selection* (the all-vs-named
/// semantic encoded by `Namespace`). Same distinction as
/// [`crate::kube::protocol::ObjectKey`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRef {
    pub pod: String,
    pub namespace: String,
    pub container: LogContainer,
}

impl ContainerRef {
    pub fn new(pod: impl Into<String>, namespace: impl Into<String>, container: LogContainer) -> Self {
        Self { pod: pod.into(), namespace: namespace.into(), container }
    }

    /// Short user-facing label for the route header / breadcrumbs. Wraps
    /// the typed enum so the UI never sees a magic sentinel.
    pub fn container_label(&self) -> &str {
        match &self.container {
            LogContainer::All => "all",
            LogContainer::Named(n) => n.as_str(),
            LogContainer::Default => "(default)",
        }
    }
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Route {
    Overview,
    Resources,
    Yaml {
        target: ObjectRef,
        awaiting_response: bool,
        state: ContentViewState,
    },
    Describe {
        target: ObjectRef,
        awaiting_response: bool,
        state: ContentViewState,
    },
    Logs {
        target: ContainerRef,
        state: Box<LogState>,
        /// The log stream permit. When this route is dropped (navigation
        /// away, pop, tab switch), the stream drops → bridge aborts →
        /// daemon's log handler exits. Impossible to leak: the stream's
        /// lifetime IS the route's lifetime.
        stream: Option<crate::kube::client_session::LogStream>,
    },
    /// Live shell session rendered inside the TUI. The daemon spawns
    /// kubectl in a PTY and proxies bytes over yamux. The vt100 parser
    /// maintains the screen buffer; the TUI renders it every frame.
    /// `writer` sends keystrokes to the daemon. A background bridge task
    /// reads from the yamux substream and delivers `AppEvent::ExecData`.
    /// Drop aborts the bridge (same RAII as LogStream).
    Shell(Box<ShellState>),
    /// In-progress edit on `target`. The unified edit flow:
    ///
    ///   1. `Action::Edit` enters this route in `EditState::AwaitingYaml`
    ///      and sends `SessionCommand::Yaml(target)`.
    ///   2. The server returns the resource's YAML via `YamlResult`.
    ///      `apply_resource_update` writes it to a temp file and
    ///      transitions to `EditState::EditorReady { temp_path }`.
    ///   3. The session loop sees `EditorReady` on its next iteration,
    ///      suspends raw mode, runs `$EDITOR <temp_path>`, reads the
    ///      result back, sends `SessionCommand::Apply { target, yaml }`,
    ///      and transitions to `EditState::Applying`.
    ///   4. The server's `Apply` handler dispatches by `is_local()` and
    ///      sends back a `CommandResult`. `apply_event` flashes the result
    ///      and pops the route.
    ///
    /// One state machine, one wire flow, no per-resource branching.
    EditingResource {
        target: ObjectRef,
        state: EditState,
    },
    Help,
    Contexts,
    ContainerSelect {
        /// The pod whose containers are being chosen from. Stored as a
        /// typed `ObjectRef` so the dispatch handler in `handle_enter_key`
        /// can look the pod up by structural identity (no flat `(name,
        /// ns)` tuple round-trip, and no `Namespace::from_row` re-parse
        /// of an empty-string sentinel).
        target: crate::kube::protocol::ObjectRef,
        selected: usize,
        action: ContainerAction,
    },
    Aliases { state: ContentViewState },
}

/// State for a live shell session rendered inside the TUI.
pub struct ShellState {
    pub title: String,
    pub parser: vt100::Parser,
    pub writer: Option<tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>>,
    pub _bridge: Option<tokio::task::AbortHandle>,
    /// False until the first ExecData arrives from the daemon. The
    /// renderer shows a loading indicator while connecting.
    pub connected: bool,
}

impl std::fmt::Debug for ShellState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShellState").field("title", &self.title).finish_non_exhaustive()
    }
}

impl Drop for ShellState {
    fn drop(&mut self) {
        if let Some(handle) = self._bridge.take() {
            handle.abort();
        }
    }
}

/// What the user is going to do with the container picked from
/// [`Route::ContainerSelect`]. Each variant names one intent so call
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
    /// Sent the `Apply { target, yaml }` command, waiting for the
    /// `CommandResult` to know whether the apply succeeded. Carries
    /// the temp file and original YAML so the editor can re-open on
    /// server error (same UX as `kubectl edit`).
    Applying { temp_file: TempFile, original: String },
}

// ---------------------------------------------------------------------------
// InputMode — text input overlay state
// ---------------------------------------------------------------------------

/// Active text input overlay. Each variant carries only the state it needs.
#[derive(Debug, Clone)]
pub enum InputMode {
    /// No text input active.
    Normal,
    /// `:` command prompt.
    Command {
        input: String,
        history_index: Option<usize>,
    },
}

impl InputMode {
    pub fn is_active(&self) -> bool {
        !matches!(self, InputMode::Normal)
    }

    /// The text buffer for the active input, if any.
    pub fn input(&self) -> Option<&str> {
        match self {
            InputMode::Normal => None,
            InputMode::Command { input, .. } => Some(input),
        }
    }

    /// The prompt string for the active input.
    pub fn prompt(&self) -> &'static str {
        match self {
            InputMode::Normal => "",
            InputMode::Command { .. } => ":",
        }
    }
}

// ---------------------------------------------------------------------------
// Flash / Filter / Confirm / Log / Yaml / Describe state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct FlashMessage {
    pub message: String,
    pub level: FlashLevel,
    pub created: Instant,
}

impl FlashMessage {
    pub fn info(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Info, created: Instant::now() }
    }
    pub fn warn(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Warn, created: Instant::now() }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Error, created: Instant::now() }
    }
    pub fn is_expired(&self) -> bool {
        self.created.elapsed().as_secs() >= FLASH_EXPIRY_SECS
    }
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

#[derive(Debug, Clone)]
pub struct ConfirmDialog {
    pub message: String,
    pub pending: PendingAction,
    pub yes_selected: bool,
}

// ---------------------------------------------------------------------------
// Form dialog
// ---------------------------------------------------------------------------
//
// One generic dialog that handles every operation needing user input. The
// shape is built client-side per-operation in `session_handlers::build_*_form`
// from row context — for PortForward we read the pod's `containerPorts`
// off the selected `ResourceRow`; for Scale we read the current replica
// count.
//
// Submit dispatch is centralized: a single function pattern-matches on
// `FormDialog::kind` to build the typed `SessionCommand` from the collected
// field values. The widget never sees a wire command.

/// One field's live state inside an open `FormDialog`. `kind` decides what
/// input control the widget draws and what keystrokes the input handler
/// accepts; `value` is the user's current input — text-typed for every
/// kind so partial input (e.g. mid-typed numbers) parses cleanly.
#[derive(Debug, Clone)]
pub struct FormFieldState {
    /// Stable identifier — used by the per-OperationKind dispatcher to look
    /// the value up at submit time. Names come from the
    /// [`crate::kube::protocol::form_field_name`] constants module so the
    /// builder and dispatcher share a single source of truth.
    pub name: String,
    /// User-facing label rendered to the left of the input.
    pub label: String,
    /// Discriminator that decides which input control the widget renders
    /// and what keystrokes the input handler accepts.
    pub kind: FormFieldKind,
    /// Current input. For Text/Number/Port this is whatever the user has
    /// typed; for Select this is the *selected option index* serialised as
    /// a digit string ("0", "1", …) so the same field can be cycled by the
    /// shared input handler.
    pub value: String,
}

/// Field type discriminator. Owned client-side — there is no wire-level
/// equivalent (the daemon only sends a list of [`OperationKind`] and the
/// client builds the form shape from row context).
#[derive(Debug, Clone)]
pub enum FormFieldKind {
    /// Free-form text. `max_len` is advisory.
    Text { max_len: Option<usize> },
    /// Integer with explicit bounds. Input is digits-only.
    Number { min: i64, max: i64 },
    /// Network port (1..=65535). Input is digits-only.
    Port,
    /// One of a fixed set of choices, cycled with Left/Right.
    Select { options: Vec<SelectOption> },
}

/// A single entry in a [`FormFieldKind::Select`]. `value` is what the
/// form submits; `label` is what the user sees. Named instead of the
/// prior `(String, String)` so call sites can't swap the positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

impl SelectOption {
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self { value: value.into(), label: label.into() }
    }
}

/// Which form-driven operation a [`FormDialog`] represents. The dispatch
/// site at submit time exhaustively matches this enum to build a typed
/// `SessionCommand`. Only the operations that actually need user input
/// are listed — adding a new form-driven op adds a new variant and a new
/// match arm (compile-error if either is forgotten).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormKind {
    Scale,
    PortForward,
}

/// A modal form dialog gathering input for a single operation.
#[derive(Debug, Clone)]
pub struct FormDialog {
    /// Which operation's typed `SessionCommand` we'll build on submit.
    /// Closed enum — replaces the wider `OperationKind` which made the
    /// dispatcher need a `_ => unreachable!()` arm.
    pub kind: FormKind,
    /// Title shown in the dialog border (e.g. "Scale: deploy/nginx").
    pub title: String,
    /// Optional context line under the title (e.g. "namespace: default").
    pub subtitle: String,
    /// The object the operation will run on. Carried through to dispatch.
    pub target: ObjectRef,
    /// Schema fields, in display order.
    pub fields: Vec<FormFieldState>,
    /// Currently focused position. `0..fields.len()` are field indices;
    /// `fields.len()` is the OK button. Esc cancels regardless of focus.
    pub focused: usize,
}

impl FormDialog {
    /// Number of focusable positions (one per field, plus the OK button).
    pub fn focus_count(&self) -> usize {
        self.fields.len() + 1
    }

    /// Move focus to the next position, wrapping around.
    pub fn focus_next(&mut self) {
        self.focused = (self.focused + 1) % self.focus_count();
    }

    /// Move focus to the previous position, wrapping around.
    pub fn focus_prev(&mut self) {
        let n = self.focus_count();
        self.focused = (self.focused + n - 1) % n;
    }

    /// True if the OK button is currently focused.
    pub fn ok_focused(&self) -> bool {
        self.focused == self.fields.len()
    }

    /// Mutably borrow the currently focused field, if any.
    pub fn current_field_mut(&mut self) -> Option<&mut FormFieldState> {
        self.fields.get_mut(self.focused)
    }

}

impl FormFieldState {
    /// True if this field accepts text/digit input (i.e. the cursor sits
    /// inside an edit box, not on a Select picker or button).
    pub fn is_text_input(&self) -> bool {
        matches!(self.kind, FormFieldKind::Text { .. } | FormFieldKind::Number { .. } | FormFieldKind::Port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogState {
    pub lines: VecDeque<String>,
    pub max_lines: usize,
    pub scroll: usize,
    pub follow: bool,
    pub wrap: bool,
    pub show_timestamps: bool,
    pub streaming: bool,
    pub since: Option<String>,
    pub tail_lines: u64,
    /// Stack of committed grep filters. Each narrows the visible lines further.
    pub filters: Vec<String>,
    /// Draft filter being typed (live preview). None = not in filter input mode.
    pub draft_filter: Option<String>,
    /// Indices into `lines` that pass all filters (committed + draft).
    pub filtered_indices: Vec<usize>,
    /// Generation id stamped by the client_session when this log stream
    /// was opened. The bridge tags every emitted `LogLine` with the same
    /// id; the apply path drops lines whose id doesn't match. Without
    /// this, lines already in flight from a *previous* log stream (whose
    /// bridge has been aborted but whose queued events haven't drained)
    /// would bleed into a freshly-opened log view.
    pub generation: u64,
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            max_lines: MAX_LOG_LINES,
            scroll: 0,
            follow: false,
            wrap: false,
            show_timestamps: true,
            streaming: false,
            since: None,
            tail_lines: 500,
            filters: Vec::new(),
            draft_filter: None,
            filtered_indices: Vec::new(),
            generation: 0,
        }
    }
}

impl LogState {
    pub fn new() -> Self {
        Self { follow: true, show_timestamps: true, streaming: false, ..Default::default() }
    }
    pub fn push(&mut self, line: String) {
        let evicted = self.lines.len() >= self.max_lines;
        if evicted {
            self.lines.pop_front();
            if !self.follow {
                self.scroll = self.scroll.saturating_sub(1);
            }
            // Adjust all filtered_indices: decrement by 1, drop index 0.
            self.filtered_indices.retain_mut(|idx| {
                if *idx == 0 { return false; }
                *idx -= 1;
                true
            });
        }
        self.lines.push_back(line);
        // Incrementally check the new line against active filters.
        let new_idx = self.lines.len() - 1;
        let patterns: Vec<crate::util::SearchPattern> = self.filters.iter()
            .chain(self.draft_filter.iter())
            .filter(|s| !s.is_empty())
            .map(|s| crate::util::SearchPattern::new(s))
            .collect();
        if patterns.is_empty() || patterns.iter().all(|p| p.is_match(&self.lines[new_idx])) {
            self.filtered_indices.push(new_idx);
        }
    }
    pub fn clear(&mut self) {
        self.lines.clear();
        self.scroll = 0;
        self.filtered_indices.clear();
        self.filters.clear();
        self.draft_filter = None;
    }

    /// Rebuild filtered_indices from all committed filters + draft.
    pub fn rebuild_filter(&mut self) {
        let all_patterns: Vec<crate::util::SearchPattern> = self.filters.iter()
            .chain(self.draft_filter.iter())
            .filter(|s| !s.is_empty())
            .map(|s| crate::util::SearchPattern::new(s))
            .collect();

        if all_patterns.is_empty() {
            self.filtered_indices = (0..self.lines.len()).collect();
        } else {
            self.filtered_indices = (0..self.lines.len())
                .filter(|&i| {
                    let line = &self.lines[i];
                    all_patterns.iter().all(|p| p.is_match(line))
                })
                .collect();
        }
    }

    /// Whether we're in filter input mode.
    pub fn is_filtering(&self) -> bool {
        self.draft_filter.is_some()
    }

    /// Start a new draft filter (user pressed `/`).
    pub fn start_filter(&mut self) {
        self.draft_filter = Some(String::new());
    }

    /// Update the draft filter text (user typed a character).
    pub fn update_draft(&mut self, text: String) {
        self.draft_filter = Some(text);
        self.rebuild_filter();
    }

    /// Commit the draft filter (user pressed Enter).
    pub fn commit_filter(&mut self) {
        if let Some(draft) = self.draft_filter.take() {
            if !draft.is_empty() {
                self.filters.push(draft);
            }
            self.rebuild_filter();
        }
    }

    /// Cancel the draft filter (user pressed Esc).
    pub fn cancel_filter(&mut self) {
        self.draft_filter = None;
        self.rebuild_filter();
    }

    /// Pop the last committed filter (user pressed Esc with no draft).
    pub fn pop_filter(&mut self) -> bool {
        if self.filters.pop().is_some() {
            self.rebuild_filter();
            true
        } else {
            false
        }
    }

    /// The active filter patterns for highlighting (committed + draft).
    pub fn active_patterns(&self) -> Vec<String> {
        let mut all: Vec<String> = self.filters.clone();
        if let Some(ref d) = self.draft_filter {
            if !d.is_empty() { all.push(d.clone()); }
        }
        all
    }

    /// Total visible line count (after filtering).
    pub fn visible_count(&self) -> usize {
        self.filtered_indices.len()
    }
}

/// Shared state for YAML and Describe content views (previously duplicated as
/// `YamlState` and `DescribeState`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentViewState {
    pub content: String,
    pub scroll: usize,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
    pub search_input_active: bool,
    pub search_input: String,
    /// Cached line count — updated when content changes.
    line_count: usize,
}

impl ContentViewState {
    /// Set content and update cached line count.
    pub fn set_content(&mut self, content: String) {
        self.line_count = content.lines().count();
        self.content = content;
    }

    /// Get the cached line count (O(1) instead of O(n)).
    pub fn line_count(&self) -> usize {
        self.line_count
    }
}

impl ContentViewState {
    /// Recompute search matches from current content (smartcase regex).
    pub fn update_search(&mut self) {
        self.search_matches.clear();
        self.current_match = 0;
        if let Some(ref term) = self.search {
            if term.is_empty() { return; }
            let pat = crate::util::SearchPattern::new(term);
            for (i, line) in self.content.lines().enumerate() {
                if pat.is_match(line) {
                    self.search_matches.push(i);
                }
            }
        }
    }

    /// Jump to the next search match, centering it in the viewport.
    pub fn next_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
    }

    /// Jump to the previous search match, centering it in the viewport.
    pub fn prev_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = if self.current_match == 0 {
            self.search_matches.len() - 1
        } else {
            self.current_match - 1
        };
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
    }

    /// Clear search state.
    pub fn clear_search(&mut self) {
        self.search = None;
        self.search_matches.clear();
        self.current_match = 0;
        self.search_input_active = false;
        self.search_input.clear();
    }
}

/// Type alias for backward compatibility.
pub type YamlState = ContentViewState;
/// Type alias for backward compatibility.
pub type DescribeState = ContentViewState;

// ---------------------------------------------------------------------------
// KubectlCache — TTL cache for describe/yaml kubectl output
// ---------------------------------------------------------------------------

/// What kind of content is cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentKind {
    Yaml,
    Describe,
}

/// Key for the kubectl output cache. Typed end-to-end: keyed on the full
/// `ObjectRef` so two CRDs sharing a Kind name across groups never collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub target: ObjectRef,
    pub kind: ContentKind,
}

impl CacheKey {
    pub fn new(target: ObjectRef, kind: ContentKind) -> Self {
        Self { target, kind }
    }
}

/// A cached content entry with its timestamp.
pub(crate) struct CacheEntry {
    pub(crate) content: String,
    pub(crate) cached_at: Instant,
}

pub struct KubectlCache {
    entries: HashMap<CacheKey, CacheEntry>,
    insertion_order: Vec<CacheKey>,
    ttl: Duration,
    max_capacity: usize,
}

impl KubectlCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: Vec::new(),
            ttl,
            max_capacity: KUBECTL_CACHE_CAPACITY,
        }
    }

    pub fn get(&self, target: &ObjectRef, kind: ContentKind) -> Option<&str> {
        let key = CacheKey::new(target.clone(), kind);
        self.entries
            .get(&key)
            .and_then(|entry| {
                if entry.cached_at.elapsed() < self.ttl {
                    Some(entry.content.as_str())
                } else {
                    None
                }
            })
    }

    pub fn insert(&mut self, target: ObjectRef, kind: ContentKind, content: String) {
        let key = CacheKey::new(target, kind);
        // If the key already exists, just update the value (no change to insertion order).
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key.clone()) {
            e.insert(CacheEntry { content, cached_at: std::time::Instant::now() });
            return;
        }
        // Evict the oldest entry if at capacity.
        if self.entries.len() >= self.max_capacity {
            if let Some(oldest_key) = self.insertion_order.first().cloned() {
                self.entries.remove(&oldest_key);
                self.insertion_order.remove(0);
            }
        }
        self.insertion_order.push(key.clone());
        self.entries.insert(key, CacheEntry { content, cached_at: Instant::now() });
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }
}

#[cfg(test)]
mod context_switch_tests {
    use super::*;

    #[test]
    fn stable_accepts_new_switches() {
        let s = ContextSwitchState::Stable;
        assert!(s.is_stable());
    }

    #[test]
    fn requested_rejects_new_switches() {
        let s = ContextSwitchState::Requested("prod".into());
        assert!(!s.is_stable());
    }

    #[test]
    fn in_flight_rejects_new_switches() {
        let s = ContextSwitchState::InFlight;
        assert!(!s.is_stable());
    }

    #[test]
    fn take_requested_transitions_requested_to_in_flight() {
        let mut s = ContextSwitchState::Requested("prod".into());
        assert_eq!(s.take_requested().as_ref().map(|c| c.as_str()), Some("prod"));
        assert_eq!(s, ContextSwitchState::InFlight);
    }

    #[test]
    fn take_requested_is_noop_from_stable() {
        let mut s = ContextSwitchState::Stable;
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::Stable);
    }

    #[test]
    fn take_requested_is_noop_from_in_flight() {
        let mut s = ContextSwitchState::InFlight;
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::InFlight);
    }

    #[test]
    fn mark_stable_from_any_state() {
        for mut s in [
            ContextSwitchState::Stable,
            ContextSwitchState::Requested("prod".into()),
            ContextSwitchState::InFlight,
        ] {
            s.mark_stable();
            assert_eq!(s, ContextSwitchState::Stable);
        }
    }

    #[test]
    fn full_lifecycle() {
        // Stable → Requested(name) → InFlight → Stable
        let mut s = ContextSwitchState::Stable;
        assert!(s.is_stable());
        s = ContextSwitchState::Requested("prod".into());
        assert!(!s.is_stable());
        let taken = s.take_requested();
        assert_eq!(taken.as_ref().map(|c| c.as_str()), Some("prod"));
        assert_eq!(s, ContextSwitchState::InFlight);
        s.mark_stable();
        assert!(s.is_stable());
    }
}
