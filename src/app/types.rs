use crate::app::kubectl_cache::KubectlCache;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::kube::protocol::ObjectRef;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// ---------------------------------------------------------------------------
// UiState — pure display/interaction state
// ---------------------------------------------------------------------------

/// Pure display and interaction state, factored out of `App` to reduce the
/// god-object surface. Fields here are only read/written by the UI layer
/// and the action handlers that respond to user input — they have no
/// cluster data semantics.
pub struct UiState {
    pub flash: Option<FlashMessage>,
    /// THE modal slot. One field, so "two modals at once" is unrepresentable
    /// rather than something 53 write sites each had to remember to prevent
    /// by clearing the other three. Private: reads go through the accessors
    /// below, writes through [`UiState::open`] / [`UiState::close_modal`],
    /// which is what makes the exclusivity structural instead of a
    /// convention. See [`Modal`].
    modal: Modal,
    pub show_header: bool,
    pub theme: crate::theme::Theme,
    pub tick_count: usize,
    pub column_level: crate::kube::resource_def::ColumnLevel,
    /// Self-contained animation clock for loading spinners — owns its phase,
    /// liveness, and cadence so it can't be perturbed by the data path.
    pub anim: crate::app::anim::Anim,
}

// ---------------------------------------------------------------------------
// KubeState — cluster/data state
// ---------------------------------------------------------------------------

/// Cluster and data state, factored out of `App`. Fields here are populated
/// from daemon events and read by the UI for display — they represent the
/// current cluster identity, namespace selection, metrics, and caches.
pub struct KubeState {
    /// The context we are actually on, or `None` before any has been
    /// confirmed — startup before the first `Ready`, or a launch with no
    /// resolvable context at all (the contexts picker is the whole UI then).
    /// An `Option` rather than an empty name so "not known yet" is a state
    /// the compiler makes every reader handle, instead of a blank string
    /// that renders as a context called "".
    pub context: Option<crate::kube::protocol::ContextName>,
    /// The context this session is being BROUGHT UP against, with the
    /// identity the kubeconfig says it has — read from disk, not confirmed by
    /// any daemon. `Some` from the moment a session starts connecting until
    /// it succeeds or is replaced.
    ///
    /// Separate from `context` because they answer different questions, and
    /// conflating them cost two bugs: the kubeconfig's candidate used to be
    /// written straight into `context` under an `is_none()` guard, so after a
    /// no-context start (where `context` stays `None`) a FAILED switch fell
    /// back to the context that had just failed and quit the app — and
    /// `identity` kept asserting the OLD cluster through every switch,
    /// because the candidate's identity arrived in the same event and was
    /// dropped with it.
    pub connecting: Option<(
        crate::kube::protocol::ContextName,
        crate::kube::protocol::ClusterIdentity,
    )>,
    pub identity: crate::kube::protocol::ClusterIdentity,
    pub selected_ns: crate::kube::protocol::Namespace,
    pub context_switch: ContextSwitchState,
    /// Latest metrics-server usage. Elements bind to it at construction
    /// ([`crate::app::store::MetricsBinding`]); values overlay at derive
    /// time — rows are never mutated.
    pub metrics: std::sync::Arc<crate::app::store::MetricsHub>,
    pub kubectl_cache: KubectlCache,
}



/// Everything that can be modal over the current view — the `:` prompt, a
/// confirm dialog, a form, or an overlay (help / container picker / edit /
/// shell).
///
/// These were four independent fields (`input_mode`, `confirm_dialog`,
/// `form_dialog`, `overlay`) that were mutually exclusive *by intent*: every
/// path that opened one had to clear the other three, at 53 write sites.
/// `begin_context_switch` cleared all four in a row. Any path that forgot
/// left a modal buried under another, invisible until the top one closed.
///
/// One field makes that impossible: opening a modal is an assignment, and an
/// assignment displaces whatever was there.
pub enum Modal {
    /// Nothing modal — keys go to the view.
    None,
    /// The `:` command prompt.
    Command {
        input: crate::app::text_input::TextInput,
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

impl UiState {
    /// Fresh UI state with nothing modal. A constructor rather than a
    /// struct literal so `modal` can stay private — which is what stops a
    /// fifth modal slot from being bolted on beside it later.
    pub fn new(theme: crate::theme::Theme) -> Self {
        Self {
            flash: None,
            modal: Modal::None,
            show_header: true,
            theme,
            tick_count: 0,
            column_level: crate::kube::resource_def::ColumnLevel::Default,
            anim: crate::app::anim::Anim::default(),
        }
    }

    /// The `:` prompt's buffer, if it is the open modal.
    pub fn command_input(&self) -> Option<(&str, Option<usize>)> {
        match &self.modal {
            Modal::Command { input, history_index } => Some((input.value(), *history_index)),
            _ => None,
        }
    }

    /// The `:` prompt's buffer itself, for the renderer — which needs the
    /// CARET, not just the text, to window a long command around it.
    pub fn command_text(&self) -> Option<&crate::app::text_input::TextInput> {
        match &self.modal {
            Modal::Command { input, .. } => Some(input),
            _ => None,
        }
    }

    /// Mutable access to the `:` prompt's buffer, for the editing keys.
    pub fn command_input_mut(
        &mut self,
    ) -> Option<(&mut crate::app::text_input::TextInput, &mut Option<usize>)> {
        match &mut self.modal {
            Modal::Command { input, history_index } => Some((input, history_index)),
            _ => None,
        }
    }

    /// Take whatever is open, leaving nothing. For flows that need to OWN
    /// the modal they are closing (the edit flow moves its `TempFile` out).
    pub fn take_modal(&mut self) -> Modal {
        std::mem::replace(&mut self.modal, Modal::None)
    }

    /// Close whatever is open.
    pub fn close_modal(&mut self) {
        self.modal = Modal::None;
    }

    /// Open a modal, replacing anything already there. The single write
    /// site — which is what stops two of them being open at once.
    pub fn open(&mut self, modal: Modal) {
        self.modal = modal;
    }

    /// What is open, for the render dispatch.
    pub fn modal(&self) -> &Modal {
        &self.modal
    }

    pub fn takeover(&self) -> Option<&TakeoverOp> {
        match &self.modal {
            Modal::Takeover(o) => Some(o),
            _ => None,
        }
    }

    pub fn takeover_mut(&mut self) -> Option<&mut TakeoverOp> {
        match &mut self.modal {
            Modal::Takeover(o) => Some(o),
            _ => None,
        }
    }
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

impl UiState {

    /// The `:` prompt's rendered prefix, or `""` when it isn't open.
    pub fn command_prompt(&self) -> &'static str {
        match &self.modal {
            Modal::Command { .. } => ":",
            _ => "",
        }
    }

    /// Whether ANY modal is up — the old
    /// `input_mode.is_active() || dialog.is_some() || …` chain.
    ///
    /// A question about DISPLAY, not about key routing. It was briefly wired
    /// as the fall-through guard in `try_handle_input_mode` and that was
    /// wrong in the most direct way possible: it is true for modals that
    /// already HAVE handlers, so it swallowed their keys before they could
    /// run and left every overlay — help, the container picker, a delete
    /// confirmation, a hanging shell connect — with no working Esc. Use
    /// [`Self::modal_without_key_handler`] for routing.
    pub fn is_modal(&self) -> bool {
        !matches!(self.modal, Modal::None)
    }

    /// Whether a modal is up whose keys NOTHING downstream will handle.
    ///
    /// This is the backstop the routing pass needs: a modal nobody handles
    /// must swallow input (visibly stuck) rather than leak it to the view
    /// underneath (invisibly wrong — keys keep driving the table, and Esc
    /// pops the nav stack out from under whatever is on screen).
    ///
    /// EXHAUSTIVE over `Modal` on purpose, and every arm is `false` today.
    /// That is the point: the value is not the answer, it is that adding a
    /// modal kind will not compile until someone states who handles its
    /// keys. A blanket `is_modal()` here answered a question nobody asked
    /// and broke every modal that was already correct.
    pub fn modal_without_key_handler(&self) -> bool {
        match &self.modal {
            Modal::None => false,
            // `Command` genuinely consumes every key it is handed.
            Modal::Command { .. } => false,
            // There is no `Form` arm because a form is not a modal: it is a
            // nav element, and `handler::handle_dialog_keys` owns its keys.
            // (The comment that used to sit here described a `Modal::Form`
            // variant that no longer exists, and broke off mid-sentence.)
            // That same fact is why Esc in a form could not be cancelled by
            // `close_modal` — see `FormDialog::focused`.
            //
            // Owned by `handler::handle_key_event`, whose `TakeoverOp` match is
            // wildcard-free — a new overlay variant is a compile error
            // there, which is the guard that actually holds.
            Modal::Takeover(_) => false,
        }
    }
}

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
#[derive(Debug)]
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

    /// Baseline apply: compare incoming rows against the previous hashes.
    /// Rows whose content hash changed get a fresh timestamp in `changed`.
    /// The old hash map is replaced wholesale — rows that disappeared are
    /// implicitly forgotten. `prev_hashes` surviving across baselines is
    /// what gives cross-recovery highlight continuity (rows that changed
    /// while a watcher was down still flash after the recovery baseline)
    /// without mass-highlighting unchanged rows.
    pub fn rebaseline(&mut self, rows: &[crate::kube::resources::row::ResourceRow]) {
        use crate::kube::protocol::ObjectKey;
        let now = Instant::now();
        let mut new_prev = HashMap::with_capacity(rows.len());
        for row in rows {
            let key = ObjectKey::new(
                row.namespace.clone().unwrap_or_default(),
                row.name.clone(),
            );
            let new_hash = Self::hash_cells(row);
            if let Some(prev_hash) = self.prev_hashes.get(&key) {
                if *prev_hash != new_hash {
                    self.changed.insert(key.clone(), now);
                }
            }
            new_prev.insert(key, new_hash);
        }
        self.prev_hashes = new_prev;
        // Prune change highlights for rows that no longer exist so stale
        // entries don't accumulate between expire() ticks.
        self.changed.retain(|k, _| self.prev_hashes.contains_key(k));
    }

    /// Delta apply: O(batch), replacing the per-snapshot full-store hash
    /// pass. The hash comparison stays — kube watches deliver metadata-only
    /// churn (heartbeats, resourceVersion bumps) as upserts with identical
    /// cells, and those must NOT flash. New keys don't flash (matches the
    /// baseline path's insert semantics); removes clear their state in
    /// O(1) (replacing the old O(n) retain).
    pub fn apply_changes(&mut self, changes: &[crate::kube::protocol::RowChange]) {
        use crate::kube::protocol::RowChange;
        let now = Instant::now();
        for change in changes {
            match change {
                RowChange::Upsert(row) => {
                    let key = change.key();
                    let new_hash = Self::hash_cells(row);
                    match self.prev_hashes.insert(key.clone(), new_hash) {
                        Some(prev) if prev != new_hash => {
                            self.changed.insert(key, now);
                        }
                        Some(_) => {} // metadata-only churn: no flash
                        None => {}    // insert: no flash (matches baseline)
                    }
                }
                RowChange::Remove(key) => {
                    self.prev_hashes.remove(key);
                    self.changed.remove(key);
                }
            }
        }
    }

    fn hash_cells(row: &crate::kube::resources::row::ResourceRow) -> u64 {
        let mut hasher = DefaultHasher::new();
        for cell in &row.cells {
            cell.hash(&mut hasher);
        }
        hasher.finish()
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
/// All fields use `#[serde(default)]` so missing keys use defaults.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct AppConfig {
    pub no_exit_on_ctrl_c: bool,
    pub read_only: bool,
    pub ui: UiConfig,
    pub keys: KeysConfig,
    /// The daemon's section of the SAME file. The TUI never reads these
    /// values (the daemon loads its own section), but the field must
    /// exist: AppConfig deserializes the whole `k9rs:` root with
    /// `deny_unknown_fields`, so without it a documented `daemon:`
    /// section would be rejected as a typo. Typed (not a raw Value) so
    /// the TUI's loud startup validation covers daemon typos too.
    pub daemon: Option<crate::kube::daemon_config::DaemonConfig>,
}

/// A single key chord: a character plus an optional Ctrl modifier.
/// Parsed from config strings — `"l"`, `"L"` (shift rides in the
/// character's case), `"ctrl-l"` / `"C-l"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(try_from = "String")]
pub struct KeyCombo {
    pub ctrl: bool,
    pub ch: char,
}

impl KeyCombo {
    pub const fn plain(ch: char) -> Self {
        Self { ctrl: false, ch }
    }

    /// Display label for hints/help ("l", "Shift-l", "Ctrl-l").
    ///
    /// `Ctrl-` and not `C-`, because the static keymap rows spell it that way
    /// (`b("Ctrl-r", ..)`) and so does the README — so help showed `Ctrl-r`
    /// on one row and `C-h` on the next for the same kind of thing. Display
    /// only: nothing parses this back (the config reader has its own
    /// `TryFrom`, which already accepts `ctrl-x`).
    pub fn label(&self) -> String {
        if self.ctrl {
            format!("Ctrl-{}", self.ch)
        } else if self.ch.is_uppercase() {
            format!("Shift-{}", self.ch.to_lowercase())
        } else {
            self.ch.to_string()
        }
    }
}

impl TryFrom<String> for KeyCombo {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        // Raw single character first — BEFORE trimming, so `" "` binds
        // the space key instead of collapsing to an empty spec.
        let mut chars = s.chars();
        if let (Some(c), None) = (chars.next(), chars.next()) {
            return Ok(Self::plain(c)); // '-' and ' ' included
        }
        let t = s.trim();
        let mut chars = t.chars();
        if let (Some(c), None) = (chars.next(), chars.next()) {
            return Ok(Self::plain(c));
        }
        if let Some((prefix, rest)) = t.rsplit_once('-') {
            let ctrl = matches!(prefix.to_ascii_lowercase().as_str(), "ctrl" | "c");
            let mut chars = rest.chars();
            if let (true, Some(c), None) = (ctrl, chars.next(), chars.next()) {
                // Lowercased: the terminal delivers Ctrl chords as
                // lowercase chars (legacy keyboard protocol), so an
                // uppercase spec like "Ctrl-L" would parse into a chord
                // no key event can ever produce.
                return Ok(Self { ctrl: true, ch: c.to_ascii_lowercase() });
            }
        }
        Err(format!(
            "invalid key spec '{t}': expected a single character or ctrl-<character>"
        ))
    }
}

/// User key rebindings for the resource view (`keys:` config section).
///
/// - An OPERATION entry REPLACES that operation's default key (bind
///   `logs: ctrl-l` and plain `l` no longer streams logs). Operations
///   whose default is a structural chord outside the descriptor
///   (delete = Ctrl-D, force-kill = Ctrl-K, port-forward = Shift-F)
///   keep it; their entry here binds an ADDITIONAL key.
/// - `colLeft` / `colRight` ADD to the arrow keys (arrows are
///   structural) and are matched before operation bindings, so
///   `colRight: l` shadows an `l` operation default. `colFirst` /
///   `colLast` jump the column cursor to the first / last column
///   (vim `0` / `$`) and shadow defaults the same way — bind
///   `colFirst: "0"` and `0` no longer switches to all namespaces.
/// - `namespaceAll` rebinds the switch-to-all-namespaces action
///   (structural default `0`); set it (e.g. `namespaceAll: ")"` for
///   Shift-0) to restore the action after `colFirst: "0"` shadowed
///   the `0` key.
///
/// TakeoverOp-defined (`Custom`) operations bind through the overlay
/// config, never here.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct KeysConfig {
    pub col_left: Option<KeyCombo>,
    pub col_right: Option<KeyCombo>,
    pub col_first: Option<KeyCombo>,
    pub col_last: Option<KeyCombo>,
    pub namespace_all: Option<KeyCombo>,
    pub describe: Option<KeyCombo>,
    pub yaml: Option<KeyCombo>,
    pub logs: Option<KeyCombo>,
    pub previous_logs: Option<KeyCombo>,
    pub shell: Option<KeyCombo>,
    pub restart: Option<KeyCombo>,
    pub scale: Option<KeyCombo>,
    pub delete: Option<KeyCombo>,
    pub force_kill: Option<KeyCombo>,
    pub port_forward: Option<KeyCombo>,
    pub show_node: Option<KeyCombo>,
    pub node_shell: Option<KeyCombo>,
    pub decode_secret: Option<KeyCombo>,
    pub trigger_cron_job: Option<KeyCombo>,
    pub toggle_suspend_cron_job: Option<KeyCombo>,
}

impl KeysConfig {
    /// The configured override for an operation, if any. Exhaustive so a
    /// new operation must decide whether it is user-bindable.
    pub fn op_override(&self, op: &crate::kube::protocol::OperationKind) -> Option<KeyCombo> {
        use crate::kube::protocol::OperationKind as Op;
        match op {
            Op::Describe => self.describe,
            Op::Yaml => self.yaml,
            Op::StreamLogs => self.logs,
            Op::PreviousLogs => self.previous_logs,
            Op::Shell => self.shell,
            Op::Restart => self.restart,
            Op::Scale => self.scale,
            Op::Delete => self.delete,
            Op::ForceKill => self.force_kill,
            Op::PortForward => self.port_forward,
            Op::ShowNode => self.show_node,
            Op::NodeShell => self.node_shell,
            Op::DecodeSecret => self.decode_secret,
            Op::TriggerCronJob => self.trigger_cron_job,
            Op::ToggleSuspendCronJob => self.toggle_suspend_cron_job,
            Op::Custom(_) => None,
            // Not user-bindable: the edit overlay drives Apply; it exists
            // on the wire for OpResult correlation only.
            Op::Apply => None,
        }
    }

    /// The EFFECTIVE chord for an operation: the user override, or the
    /// descriptor's default key as a plain chord. `None` = only bound
    /// structurally (Ctrl-D / Ctrl-K / Shift-F) or not at all.
    pub fn op_key(&self, op: &crate::kube::protocol::OperationKind) -> Option<KeyCombo> {
        self.op_override(op)
            .or_else(|| op.descriptor().default_key.map(KeyCombo::plain))
    }

    /// Every configured binding as `(config name, chord)`.
    ///
    /// The operation half is DERIVED from `op_override` + `config_key`, both
    /// of which are compiler-exhaustive. It used to be a third hand-written
    /// array parallel to the struct fields and to `op_override`, so a binding
    /// added to those two and missed here slipped past both `validate`
    /// checks — in the function whose doc says the whole point is "a startup
    /// error, not a mystery".
    fn entries(&self) -> Vec<(&'static str, KeyCombo)> {
        let mut out: Vec<(&'static str, KeyCombo)> = [
            ("colLeft", self.col_left),
            ("colRight", self.col_right),
            ("colFirst", self.col_first),
            ("colLast", self.col_last),
            ("namespaceAll", self.namespace_all),
        ]
        .into_iter()
        .filter_map(|(name, combo)| combo.map(|c| (name, c)))
        .collect();
        for op in crate::kube::protocol::OperationKind::BUILT_IN {
            if let (Some(name), Some(combo)) = (op.config_key(), self.op_override(op)) {
                out.push((name, combo));
            }
        }
        out
    }

    /// Reject bindings that could never fire: chords consumed by earlier
    /// dispatch layers (globals, structural Ctrl-D/Ctrl-K, the `:`/`/`/`?`
    /// prompts) would leave the binding dead AND — since an override
    /// REPLACES the default — the operation unreachable, silently.
    /// Duplicates are rejected too (only the first-checked entry would
    /// win, invisibly). Runs in the LOUD config-load path so a rejected
    /// binding is a startup error, not a mystery.
    pub fn validate(&self) -> Result<(), String> {
        // Chords an earlier dispatch layer always consumes before
        // bindings are consulted. Ctrl-K is capability-dependent at
        // dispatch (force-kill views only) — reserved anyway, because a
        // binding that works on configmaps but dies on pods is worse
        // than a rejected one.
        const RESERVED: &[(KeyCombo, &str)] = &[
            (KeyCombo { ctrl: true, ch: 'c' }, "quit"),
            (KeyCombo { ctrl: true, ch: 'r' }, "refresh"),
            (KeyCombo { ctrl: true, ch: 'e' }, "toggle header"),
            (KeyCombo { ctrl: true, ch: 's' }, "save table/logs"),
            (KeyCombo { ctrl: true, ch: 'a' }, "aliases"),
            (KeyCombo { ctrl: true, ch: 'w' }, "wide columns"),
            (KeyCombo { ctrl: true, ch: 'z' }, "fault filter"),
            (KeyCombo { ctrl: true, ch: ' ' }, "span-mark"),
            (KeyCombo { ctrl: true, ch: '\\' }, "clear marks"),
            (KeyCombo { ctrl: true, ch: 'd' }, "delete"),
            (KeyCombo { ctrl: true, ch: 'k' }, "force-kill"),
            (KeyCombo { ctrl: false, ch: ':' }, "command mode"),
            (KeyCombo { ctrl: false, ch: '/' }, "filter"),
            (KeyCombo { ctrl: false, ch: '?' }, "help"),
        ];
        let mut seen: std::collections::HashMap<KeyCombo, &'static str> =
            std::collections::HashMap::new();
        for (name, combo) in self.entries() {
            if let Some((_, what)) = RESERVED.iter().find(|(r, _)| *r == combo) {
                return Err(format!(
                    "keys.{name}: '{}' is reserved ({what}) — the binding could never fire",
                    combo.label(),
                ));
            }
            if let Some(first) = seen.insert(combo, name) {
                return Err(format!(
                    "keys.{name}: '{}' is already bound by keys.{first}",
                    combo.label(),
                ));
            }
        }
        Ok(())
    }
}

/// TUI rendering and interaction preferences.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct UiConfig {
    /// Skin name (loaded by theme.rs separately).
    #[serde(default)]
    pub skin: Option<String>,
    /// Base palette: `auto` (detect the terminal background), `dark` or
    /// `light`. A skin, if any, layers on top of whichever is chosen.
    #[serde(default)]
    pub theme: crate::theme::ThemeMode,
    pub max_column_width: u16,
    pub search_context_lines: usize,
    pub command_history_size: usize,
    pub change_highlight_secs: u64,
    pub cache_capacity: usize,
    pub flash: FlashConfig,
    pub logs: LogConfig,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            skin: None,
            theme: crate::theme::ThemeMode::default(),
            max_column_width: 64,
            search_context_lines: 10,
            command_history_size: 50,
            change_highlight_secs: 5,
            cache_capacity: 100,
            flash: FlashConfig::default(),
            logs: LogConfig::default(),
        }
    }
}

/// Flash message auto-dismiss durations.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct FlashConfig {
    pub info_secs: u64,
    pub warn_secs: u64,
    pub error_secs: u64,
}

impl Default for FlashConfig {
    fn default() -> Self {
        Self { info_secs: 3, warn_secs: 5, error_secs: 10 }
    }
}

/// Log view defaults.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct LogConfig {
    pub max_lines: usize,
    pub tail_lines: u64,
    pub default_follow: bool,
    pub default_wrap: bool,
    /// Which per-line fields the view starts on. ONE key (`t`) cycles this at
    /// runtime, so the config names a point on that cycle rather than a flag
    /// per field — two flags were four combinations for three reachable
    /// states, and a config could ask for the unreachable one.
    pub default_fields: LogFields,
}

/// A point on the field cycle — see `kube::protocol::Role::SHED_ORDER`, which
/// defines the order fields are shed in.
///
/// One variant per level, and `log_fields_covers_every_shed_level` pins that,
/// so adding a role to `SHED_ORDER` fails the build here until someone names
/// the state it creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogFields {
    /// Timestamp, source tag and message.
    All,
    /// Source tag and message. The kubelet's stamp is 31 columns duplicating
    /// one the service almost always prints itself.
    Source,
    /// Message only. The source tag exists solely on multi-source streams,
    /// where it is the one thing saying which replica spoke — so this is a
    /// deliberate choice, not a default.
    Message,
}

impl LogFields {
    /// How many roles this level hides.
    pub fn shed(self) -> usize {
        match self {
            LogFields::All => 0,
            LogFields::Source => 1,
            LogFields::Message => 2,
        }
    }
}

impl LogConfig {
    /// The shed level the configured defaults describe.
    ///
    /// The config keeps a flag per role because that is what a user writes;
    /// the VIEW keeps a level because that is what one key cycles. A config
    /// asking for the one combination the cycle cannot express — a timestamp
    /// without a source tag — resolves to showing both, since the cycle's
    /// next press takes the timestamp away anyway.
    pub fn initial_shed(&self) -> usize {
        self.default_fields.shed()
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_lines: 50_000,
            tail_lines: 100,
            default_follow: true,
            default_wrap: false,
            // Source tag and message: one shed level in.
            //
            // The daemon still asks the kubelet for timestamps and the typed
            // field is still on the wire — `t` cycles a DISPLAY choice over
            // data we always have, and the fan-out needs the stamps to merge
            // replicas in time order. But that stamp is 31 columns of a ~97
            // column line spent duplicating the 27 the service prints next
            // to it, whereas the source tag only exists when more than one
            // pod is talking and is then the only thing saying which.
            default_fields: LogFields::Source,
        }
    }
}

/// The three-state lifecycle of a context switch. Replaces the prior pair
/// of `context_switch_pending: bool` + `pending_context_switch: Option<String>`
/// with a single source of truth:
///
/// - [`Self::Stable`] — no switch in flight, new switches allowed.
/// - [`Self::Requested`] — user asked to switch; the session main loop will
///   pick it up at the top of its next iteration and drop the current
///   `ClientSession`.
/// - [`Self::InFlight`] — main loop has taken the request and is bringing up
///   the new connection; it carries the target so the state fully
///   describes the switch in progress. Blocks further switches until the
///   connection *resolves*, either way.
///
/// Transitions form a single cycle: Stable → Requested → InFlight → Stable.
/// The closing edge fires on EITHER outcome of the connection attempt —
/// `ConnectionEstablished` (success) or `ConnectionFailed` (the target was
/// unreachable) — both via [`settle`](Self::settle). There is deliberately
/// no lingering `Failed` state: a resolved switch is `Stable` or it is still
/// in flight, nothing in between, so "stuck InFlight" is unrepresentable.
/// Attempting a new switch from Requested or InFlight is rejected at
/// `begin_context_switch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextSwitchState {
    Stable,
    Requested(crate::kube::protocol::ContextName),
    InFlight(crate::kube::protocol::ContextName),
}

impl ContextSwitchState {
    /// True if a new switch can be initiated. The only "accepting" state.
    pub fn is_stable(&self) -> bool {
        matches!(self, Self::Stable)
    }

    /// The context this switch is aiming at. `Some` for both [`Self::Requested`]
    /// and [`Self::InFlight`], `None` for [`Self::Stable`] — the RECONNECT path uses it
    /// ("keep aiming at the switch target across a mid-switch daemon blip"),
    /// which genuinely wants both states.
    ///
    /// CAUTION for connection-OUTCOME edges (`ConnectionEstablished` /
    /// `ConnectionFailed`): those must match [`Self::InFlight`] explicitly, never
    /// this method. `Requested` means the switch's session hasn't been built
    /// yet, so an outcome arriving in that state belongs to a PRIOR attempt
    /// — reading it as the switch's outcome tears down the wrong world (or
    /// settles away a switch the user just queued).
    pub fn target(&self) -> Option<&crate::kube::protocol::ContextName> {
        match self {
            Self::Stable => None,
            Self::Requested(name) | Self::InFlight(name) => Some(name),
        }
    }

    /// If the state is [`Self::Requested`], take the target name and
    /// transition to [`Self::InFlight`], which keeps the target for the duration
    /// of the connection attempt. Returns `None` in any other state and
    /// leaves the state alone. Used by the main loop to atomically consume
    /// a pending request without racing a second call.
    pub fn take_requested(&mut self) -> Option<crate::kube::protocol::ContextName> {
        // Discriminant check first so we don't swap out state we can't
        // recover from. If it's not Requested, bail untouched.
        let Self::Requested(name) = self else { return None; };
        let name = name.clone();
        // Safe: we just checked the discriminant on the line above, and
        // `&mut self` means nothing else can mutate in between.
        *self = Self::InFlight(name.clone());
        Some(name)
    }

    /// Resolve an in-flight switch back to [`Self::Stable`], so the next switch is
    /// accepted. This is the closing edge of the cycle and fires on EITHER
    /// outcome: `ConnectionEstablished` (the new session is up) or
    /// `ConnectionFailed` (the target was unreachable). Idempotent — a no-op
    /// from `Stable`, so a steady-state reconnect success can call it freely.
    pub fn settle(&mut self) {
        *self = Self::Stable;
    }
}

/// Whether the daemon connection is LIVE right now — handshake complete and
/// not since dropped. The single authority the render layer consults to show
/// a "Connecting…" screen instead of stale, no-longer-live rows during a
/// reconnect or context switch.
///
/// One axis of [`Connection`]; see its docs for how the axes divide. It is
/// a STORED flag, deliberately NOT derived from the reconnect plan — the
/// plan is consumed at the *start* of the rebuild (i.e. mid-gap), so a
/// derived predicate would flicker the screen back to stale rows before
/// the new connection is actually up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// No session is wanted: no context has been chosen. Nothing is being
    /// attempted and nothing will be until the user picks one. Distinct from
    /// [`Connecting`](Self::Connecting), which promises the screen is about
    /// to fill in on its own — here it never will, and saying "connecting…"
    /// forever would be the same lie in a new place.
    NoContext,
    Connecting,
    Live,
}

/// When the main loop should next attempt a session rebuild. Replaces the
/// `reconnect_requested: bool` + `reconnect_at: Option<Instant>` pair,
/// whose meaning lived in the cross-product ("`at` is only read under
/// `requested`") — the exact two-fields-one-truth shape
/// [`ContextSwitchState`] was created to kill for switches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconnectPlan {
    /// No rebuild wanted.
    Idle,
    /// Rebuild on the next loop turn.
    Now,
    /// Rebuild once the backoff deadline passes (a previous attempt
    /// failed; don't hammer a down daemon).
    At(std::time::Instant),
}

/// The client↔daemon connection lifecycle, owned as ONE value with total
/// edges — previously five loose `App` fields (`link`, `has_connected`,
/// `reconnect_requested`, `reconnect_at`, `reconnect_backoff`) that every
/// event arm had to co-update in the right combination; the 2026-07
/// context-lock bug and the 2026-08 audit's top findings were both missed
/// edges of that implicit machine.
///
/// Three independent axes, each with one owner:
/// - **link**: is the connection live THIS FRAME (render gate)?
/// - **has_connected**: did any session EVER connect (initial-failure-is-
///   fatal rule)? Monotonic.
/// - **plan** + **backoff**: when should the loop rebuild next?
///
/// [`ContextSwitchState`] stays deliberately separate: it answers WHICH
/// context is being brought up; this type doesn't care why the link is
/// down, only that it is and when to try again.
#[derive(Debug)]
pub struct Connection {
    link: LinkState,
    has_connected: bool,
    plan: ReconnectPlan,
    backoff: Duration,
}

impl Connection {
    const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);

    /// Startup state: the initial handshake hasn't landed yet.
    pub fn new() -> Self {
        Self {
            link: LinkState::Connecting,
            has_connected: false,
            plan: ReconnectPlan::Idle,
            backoff: Self::INITIAL_BACKOFF,
        }
    }

    /// The link's own state — read by [`crate::app::Liveness::of_link`],
    /// which is the one place the render layer asks.
    pub fn link(&self) -> LinkState {
        self.link
    }

    /// There is no context to connect to. Terminal until a switch is
    /// requested: no plan, so the main loop never rebuilds — a retry with
    /// nothing to retry against would just be a loop.
    pub fn no_context(&mut self) {
        self.link = LinkState::NoContext;
        self.plan = ReconnectPlan::Idle;
    }

    /// Whether any session EVER connected — distinguishes a fatal initial
    /// failure from a retryable reconnect failure.
    pub fn has_ever_connected(&self) -> bool {
        self.has_connected
    }

    /// Is a rebuild due at `now`? Pure read — the choke point consumes the
    /// plan via [`Self::begin_rebuild`] once it commits.
    pub fn rebuild_due(&self, now: std::time::Instant) -> bool {
        match self.plan {
            ReconnectPlan::Idle => false,
            ReconnectPlan::Now => true,
            ReconnectPlan::At(t) => now >= t,
        }
    }

    /// The choke point committed to a session rebuild (reconnect OR context
    /// switch): consume the plan and show the connecting screen for the
    /// whole gap. Cleared to Live only by [`Self::established`].
    pub fn begin_rebuild(&mut self) {
        self.plan = ReconnectPlan::Idle;
        self.link = LinkState::Connecting;
    }

    /// A context switch was REQUESTED (session not rebuilt yet — that
    /// happens at the top of the next loop turn): show the connecting
    /// screen immediately rather than one stale frame later.
    pub fn switch_requested(&mut self) {
        self.link = LinkState::Connecting;
    }

    /// `ConnectionEstablished`: the link is live, future failures are
    /// reconnects, the backoff resets. A still-pending plan (arrived
    /// between a failure and its deadline) collapses to Now — its backoff
    /// deadline was aimed at a connection that no longer exists.
    pub fn established(&mut self) {
        self.link = LinkState::Live;
        self.has_connected = true;
        self.backoff = Self::INITIAL_BACKOFF;
        if self.plan != ReconnectPlan::Idle {
            self.plan = ReconnectPlan::Now;
        }
    }

    /// `DaemonDisconnected`: the live session died — rebuild on the next
    /// turn and show the connecting screen NOW (this frame).
    pub fn disconnected(&mut self) {
        self.link = LinkState::Connecting;
        if self.plan == ReconnectPlan::Idle {
            self.plan = ReconnectPlan::Now;
        }
    }

    /// A context SWITCH failed and we're falling back to the previous
    /// context: reconnect immediately (the daemon is up — the TARGET was
    /// unreachable — so no backoff).
    pub fn switch_failed_fallback(&mut self) {
        self.plan = ReconnectPlan::Now;
    }

    /// A steady-state RECONNECT attempt failed: schedule the next one a
    /// backoff out, and double the backoff (clamped).
    pub fn reconnect_failed_backoff(&mut self, now: std::time::Instant) {
        self.plan = ReconnectPlan::At(now + self.backoff);
        self.backoff = (self.backoff * 2).min(Self::MAX_BACKOFF);
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
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
/// A keystroke that is TEXT.
///
/// Every text sink — the `:` command bar, form fields, the resource filter,
/// the log grep draft, the yaml/describe search — matched on `key.code` alone,
/// so a Ctrl chord typed its bare character instead of falling through to the
/// global handler. Three of the five were fixed one at a time across two
/// separate bug reports; `handle_search_key` could not be fixed at its call
/// site at all, because it took a `KeyCode` and the modifier was already gone.
///
/// So the guard became a TYPE. A sink takes a `TextKey`, which can only be
/// built by [`TextKey::new`], which refuses Ctrl chords — "forgot the guard"
/// is no longer something you can write.
///
/// Concretely this is not hypothetical: `BEL` terminates an OSC reply and
/// crossterm delivers `0x07` as Ctrl-G, so a terminal colour response leaking
/// into a resume window typed a stray `g` into whichever text box was open.
#[derive(Debug, Clone, Copy)]
pub struct TextKey(crossterm::event::KeyEvent);

impl TextKey {
    /// `None` for a Ctrl chord — that is a command, not text, and belongs to
    /// the global handler. Alt chords are likewise not text (an Alt-modified
    /// key is a different chord, not its bare character — the same rule
    /// `combo_of` follows).
    pub fn new(key: crossterm::event::KeyEvent) -> Option<Self> {
        use crossterm::event::KeyModifiers;
        if key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
            return None;
        }
        Some(Self(key))
    }

    pub fn code(&self) -> crossterm::event::KeyCode {
        self.0.code
    }

    /// The character this key types, if it types one.
    pub fn char(&self) -> Option<char> {
        match self.0.code {
            crossterm::event::KeyCode::Char(c) => Some(c),
            _ => None,
        }
    }
}

pub fn handle_search_key(
    input: &mut crate::app::text_input::TextInput,
    key: TextKey,
) -> SearchInputResult {
    use crossterm::event::KeyCode;
    match key.code() {
        KeyCode::Esc => {
            input.clear();
            SearchInputResult::Cancelled
        }
        KeyCode::Enter => SearchInputResult::Committed(input.take()),
        // Editing and caret motion are the input's, not this function's —
        // the fifth and last place that used to keep its own `String` and
        // reimplement backspace.
        KeyCode::Backspace => {
            input.backspace();
            SearchInputResult::Updated
        }
        KeyCode::Delete => {
            input.delete();
            SearchInputResult::Updated
        }
        KeyCode::Left => {
            input.left();
            SearchInputResult::Updated
        }
        KeyCode::Right => {
            input.right();
            SearchInputResult::Updated
        }
        KeyCode::Home => {
            input.home();
            SearchInputResult::Updated
        }
        KeyCode::End => {
            input.end();
            SearchInputResult::Updated
        }
        KeyCode::Char(c) => {
            input.insert(c);
            SearchInputResult::Updated
        }
        _ => SearchInputResult::Updated,
    }
}


// ---------------------------------------------------------------------------
// CrdInfo — lightweight CRD metadata extracted from unified ResourceRow
// ---------------------------------------------------------------------------

/// Lightweight CRD metadata extracted from the typed `crd_info` field of a
/// `ResourceRow`. Type alias over [`crate::kube::protocol::CrdRef`] — the
/// shape is identical and `find_crd_by_name` returns the row's stored
/// CrdRef directly instead of re-cloning fields.
pub type CrdInfo = crate::kube::protocol::CrdRef;

// (`ContainerRef` lived here: pod + namespace + `LogContainer`, carried
// beside a `LogSession`'s `LogInit` as its display identity. It was a
// second copy of what the spec already said, and a fan-out target —
// "3 pods", "deploy/api" — is precisely where the copy would have had to
// lie. The element now derives its header from the spec, and the
// container label lives on `LogContainer::label` next to the variants.)

// ---------------------------------------------------------------------------
// TakeoverOp — the single modal slot
// ---------------------------------------------------------------------------

/// A dialogue or operation layered over the current view. NOT navigation:
/// views live on the nav stack ([`crate::app::element::Element`]); an
/// overlay has no history, no data handle, no self-definition — Esc
/// closes it (dropping its RAII: [`crate::app::client::ExecStream`],
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
        target: crate::kube::protocol::ObjectRef,
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
    /// [`ViewExtent`](crate::app::element::ViewExtent) a nav element
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
    pub fn extent(&self, frame: ratatui::layout::Rect) -> crate::app::element::ViewExtent {
        use crate::app::element::ViewExtent;
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
    pub stream: Option<crate::app::client::ExecStream>,
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
/// container picker ([`crate::app::element::Element::Picker`]). Each variant
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
    pub container: crate::kube::protocol::LogContainer,
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
        containers: &[crate::kube::resources::row::ContainerInfo],
    ) -> Vec<Self> {
        use crate::kube::protocol::LogContainer;
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

// ---------------------------------------------------------------------------

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
    pub fn is_expired(&self, flash_config: &FlashConfig) -> bool {
        let lifetime_secs = match self.level {
            FlashLevel::Info => flash_config.info_secs,
            FlashLevel::Warn => flash_config.warn_secs,
            FlashLevel::Error => flash_config.error_secs,
        };
        self.created.elapsed().as_secs() >= lifetime_secs
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
    /// Label for the action button (e.g. "Delete", "Restart", "Force Kill").
    pub action_label: String,
    pub pending: PendingAction,
    /// True = action button focused, false = cancel focused (safe default).
    pub action_focused: bool,
}



#[cfg(test)]
#[path = "../tests/app/types.rs"]
mod mirror_tests;

/// "Which container?" — a pick-one list floating over the view that asked.
///
/// Captures its choices at construction: self-contained, so no table lookup
/// happens under a dialog that may outlive the row it came from.
#[derive(Debug, Clone)]
pub struct ContainerPicker {
    pub target: crate::kube::protocol::ObjectRef,
    /// The rows to pick from, already resolved for `action` — see
    /// [`ContainerChoice::list`]. `selected` indexes THIS, so there is one
    /// index space and no "row 0 means something else" offset.
    pub choices: Vec<ContainerChoice>,
    pub selected: usize,
    pub action: ContainerAction,
}
