use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::kube::protocol::{LogContainer, ObjectKey, ObjectRef};

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
    pub confirm_dialog: Option<ConfirmDialog>,
    pub form_dialog: Option<FormDialog>,
    pub input_mode: InputMode,
    /// The single modal slot: dialogues and operations ABOUT the current
    /// view (help, container picker, edit flow, live shell). Mutually
    /// exclusive by construction; Esc closes; closing is not navigation.
    pub overlay: Option<Overlay>,
    pub show_header: bool,
    pub theme: crate::ui::theme::Theme,
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
    pub context: crate::kube::protocol::ContextName,
    pub identity: crate::kube::protocol::ClusterIdentity,
    pub selected_ns: crate::kube::protocol::Namespace,
    pub context_switch: ContextSwitchState,
    /// Latest metrics-server usage. Elements bind to it at construction
    /// ([`crate::app::store::MetricsBinding`]); values overlay at derive
    /// time — rows are never mutated.
    pub metrics: std::sync::Arc<crate::app::store::MetricsHub>,
    pub kubectl_cache: KubectlCache,
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

#[cfg(test)]
mod delta_tracker_tests {
    use super::*;
    use crate::kube::protocol::{ObjectKey, RowChange};
    use crate::kube::resources::row::{CellValue, ResourceRow};

    fn row(name: &str, val: &str) -> ResourceRow {
        ResourceRow {
            name: name.into(),
            namespace: Some("ns".into()),
            cells: vec![CellValue::Text(val.into())],
            ..Default::default()
        }
    }
    fn key(name: &str) -> ObjectKey {
        ObjectKey::new("ns".to_string(), name.to_string())
    }

    /// E4: a metadata-only upsert (identical cells) must NOT flash — kube
    /// heartbeat churn would otherwise light the table permanently. A real
    /// change flashes; an insert doesn't (matches the baseline path);
    /// removes clear their state.
    #[test]
    fn apply_changes_flashes_only_genuine_changes() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1")]);
        assert!(d.changed_rows().is_empty(), "baseline seeds hashes, no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "1"))]); // heartbeat
        assert!(d.changed_rows().is_empty(), "identical cells: no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "2"))]); // real change
        assert!(d.changed_rows().contains_key(&key("a")));

        d.apply_changes(&[RowChange::Upsert(row("new", "1"))]); // insert
        assert!(!d.changed_rows().contains_key(&key("new")), "inserts don't flash");

        d.apply_changes(&[RowChange::Remove(key("a"))]);
        assert!(!d.changed_rows().contains_key(&key("a")), "remove clears state");
    }

    /// Cross-recovery continuity: hashes survive a re-baseline, so a row
    /// that changed while a watcher was down flashes after the recovery
    /// baseline — and unchanged rows do NOT mass-flash.
    #[test]
    fn rebaseline_keeps_continuity_without_mass_flash() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1"), row("b", "1")]);
        d.rebaseline(&[row("a", "1"), row("b", "2")]); // recovery baseline
        assert!(!d.changed_rows().contains_key(&key("a")), "unchanged: quiet");
        assert!(d.changed_rows().contains_key(&key("b")), "changed-during-gap: flash");
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

    /// Display label for hints/help ("l", "C-l").
    pub fn label(&self) -> String {
        if self.ctrl {
            format!("C-{}", self.ch)
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
///
/// Overlay-defined (`Custom`) operations bind through the overlay
/// config, never here.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct KeysConfig {
    pub col_left: Option<KeyCombo>,
    pub col_right: Option<KeyCombo>,
    pub col_first: Option<KeyCombo>,
    pub col_last: Option<KeyCombo>,
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
    fn entries(&self) -> Vec<(&'static str, KeyCombo)> {
        [
            ("colLeft", self.col_left),
            ("colRight", self.col_right),
            ("colFirst", self.col_first),
            ("colLast", self.col_last),
            ("describe", self.describe),
            ("yaml", self.yaml),
            ("logs", self.logs),
            ("previousLogs", self.previous_logs),
            ("shell", self.shell),
            ("restart", self.restart),
            ("scale", self.scale),
            ("delete", self.delete),
            ("forceKill", self.force_kill),
            ("portForward", self.port_forward),
            ("showNode", self.show_node),
            ("nodeShell", self.node_shell),
            ("decodeSecret", self.decode_secret),
            ("triggerCronJob", self.trigger_cron_job),
            ("toggleSuspendCronJob", self.toggle_suspend_cron_job),
        ]
        .into_iter()
        .filter_map(|(name, combo)| combo.map(|c| (name, c)))
        .collect()
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
    /// `light`. A skin, if any, layers on top of it.
    pub theme: crate::ui::theme::ThemeMode,
    pub max_column_width: u16,
    pub page_scroll_lines: usize,
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
            theme: crate::ui::theme::ThemeMode::default(),
            max_column_width: 64,
            page_scroll_lines: 40,
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
    pub default_timestamps: bool,
    pub default_wrap: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_lines: 50_000,
            tail_lines: 100,
            default_follow: true,
            default_timestamps: true,
            default_wrap: false,
        }
    }
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
// Overlay — the single modal slot
// ---------------------------------------------------------------------------

/// A dialogue or operation layered over the current view. NOT navigation:
/// views live on the nav stack ([`crate::app::element::Element`]); an
/// overlay has no history, no data handle, no self-definition — Esc
/// closes it (dropping its RAII: [`crate::kube::client_session::ExecStream`],
/// [`TempFile`]) and the view underneath was never displaced.
pub enum Overlay {
    /// The `?` help sheet.
    Help { scroll: usize },
    /// "Which container?" picker for multi-container pods. Captures its
    /// container list at construction — self-contained (no table lookups
    /// under a dialog that may outlive the row).
    ContainerSelect {
        target: crate::kube::protocol::ObjectRef,
        containers: Vec<crate::kube::resources::row::ContainerInfo>,
        selected: usize,
        action: ContainerAction,
    },
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
    /// Live shell session. During Connecting the TUI shows a loading bar
    /// with full navigation; once Connected the session loop suspends the
    /// TUI and raw-bridges stdin↔daemon bytes. Drop aborts the bridge.
    Shell(Box<ShellState>),
}

impl std::fmt::Debug for Overlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Overlay::Help { scroll } => f.debug_struct("Help").field("scroll", scroll).finish(),
            Overlay::ContainerSelect { target, .. } => {
                f.debug_struct("ContainerSelect").field("target", target).finish_non_exhaustive()
            }
            Overlay::Edit { target, .. } => {
                f.debug_struct("Edit").field("target", target).finish_non_exhaustive()
            }
            Overlay::Shell(state) => f.debug_tuple("Shell").field(state).finish(),
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
    pub stream: Option<crate::kube::client_session::ExecStream>,
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
    /// Sent the `Apply { target, yaml }` command, waiting for this
    /// target's `OpResult` to know whether the apply succeeded. Carries
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

// ---------------------------------------------------------------------------
// Batch tracker — correlates per-target OpResults back to a batch launch
// ---------------------------------------------------------------------------

/// An in-flight batch operation. Marks are NOT cleared at dispatch: each
/// Ok result unmarks its row (so "what is still marked" always means
/// "not yet succeeded"), failures keep their marks — retrying exactly
/// the failed set is one keypress away. Results aggregate into ONE
/// summary flash instead of N racing per-item flashes (single flash
/// slot, last write wins — failures used to vanish behind a final Ok).
#[derive(Debug)]
pub struct BatchTracker {
    /// Past-tense verb for the summary ("Deleted", "Restarted", …).
    verb: &'static str,
    /// Resource noun for the summary ("pod", "deployment", …).
    noun: String,
    /// The batch's resource kind. Every item of one batch shares it
    /// (targets are built from one element's rid), and `consume` requires
    /// it — without the check, a result for a DIFFERENT kind with the
    /// same ns+name (delete service `web` while a deployment-`web` batch
    /// is outstanding) would be silently misattributed. Same-kind
    /// duplicates (a second op on the same object racing the batch)
    /// remain ambiguous — full disambiguation needs a wire correlation
    /// id on OpResult; documented, not attempted here.
    rid: crate::kube::protocol::ResourceId,
    /// Keys still awaiting a result.
    outstanding: std::collections::HashSet<ObjectKey>,
    ok: usize,
    /// (name, error) per failed item.
    failures: Vec<(String, String)>,
    /// Confirmed targets that were already gone at confirm time.
    skipped: usize,
    /// The store the batch launched from — weak: result bookkeeping must
    /// not keep a popped element's store alive.
    store: std::sync::Weak<crate::app::store::RowStore>,
}

impl BatchTracker {
    pub fn new(
        verb: &'static str,
        noun: String,
        rid: crate::kube::protocol::ResourceId,
        targets: &[ObjectRef],
        skipped: usize,
        store: std::sync::Weak<crate::app::store::RowStore>,
    ) -> Self {
        Self {
            verb,
            noun,
            rid,
            outstanding: targets.iter().map(Self::key_of).collect(),
            ok: 0,
            failures: Vec::new(),
            skipped,
            store,
        }
    }

    /// The mark-identity of a target — inverse of how batch `ObjectRef`s
    /// are built from marked keys (`Namespace::from_row` maps "" ↔ `All`).
    pub fn key_of(target: &ObjectRef) -> ObjectKey {
        ObjectKey::new(
            target.namespace.as_option().unwrap_or("").to_string(),
            target.name.clone(),
        )
    }

    /// Record a send failure at dispatch time (the command never left the
    /// client) — same accounting as a server-side Err.
    pub fn fail_send(&mut self, target: &ObjectRef, error: String) {
        self.outstanding.remove(&Self::key_of(target));
        self.failures.push((target.name.clone(), error));
    }

    /// Consume a result if it belongs to this batch; `false` = not ours
    /// (the caller should handle it as an ordinary single-op result).
    /// Correlation = resource kind AND identity: the daemon echoes the
    /// request's full `ObjectRef`, so both are authoritative.
    pub fn consume(&mut self, target: &ObjectRef, result: &Result<String, String>) -> bool {
        if target.resource != self.rid {
            return false;
        }
        let key = Self::key_of(target);
        if !self.outstanding.remove(&key) {
            return false;
        }
        match result {
            Ok(_) => {
                self.ok += 1;
                // Success unmarks the row. For Delete/ForceKill the row's
                // removal prunes the mark anyway; Restart leaves the row
                // in place, so this is the path that clears it.
                if let Some(store) = self.store.upgrade() {
                    store.unmark_keys(std::iter::once(&key));
                }
            }
            Err(e) => self.failures.push((target.name.clone(), e.clone())),
        }
        true
    }

    pub fn is_done(&self) -> bool {
        self.outstanding.is_empty()
    }

    /// The aggregate flash. Info when everything succeeded; error with
    /// the first failure spelled out otherwise.
    pub fn summary(&self) -> FlashMessage {
        let mut msg = format!("{} {} {}{}", self.verb, self.ok, self.noun,
            if self.ok == 1 { "" } else { "s" });
        if self.skipped > 0 {
            msg.push_str(&format!(", {} skipped (gone)", self.skipped));
        }
        if self.failures.is_empty() {
            FlashMessage::info(msg)
        } else {
            let (name, err) = &self.failures[0];
            msg.push_str(&format!(
                ", {} FAILED — {}: {}",
                self.failures.len(), name, err,
            ));
            FlashMessage::error(msg)
        }
    }

    /// Summary for a batch cut short (daemon disconnected with results
    /// still outstanding).
    pub fn interrupted_summary(&self) -> FlashMessage {
        FlashMessage::warn(format!(
            "Batch interrupted: {} ok, {} failed, {} unanswered",
            self.ok, self.failures.len(), self.outstanding.len(),
        ))
    }
}

#[cfg(test)]
mod batch_tracker_tests {
    use super::*;
    use crate::kube::protocol::{Namespace, ResourceId};
    use crate::kube::resource_def::BuiltInKind;

    fn target(name: &str, ns: &str) -> ObjectRef {
        ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Pod),
            name.to_string(),
            Namespace::from_row(ns),
        )
    }

    /// key_of is the exact inverse of how batch ObjectRefs are built from
    /// marked keys (Namespace::from_row): "" ↔ All round-trips.
    #[test]
    fn key_of_round_trips_the_marked_key() {
        let key = crate::kube::protocol::ObjectKey::new("ns1".to_string(), "a".to_string());
        let t = ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Pod),
            key.name.clone(),
            Namespace::from_row(&key.namespace),
        );
        assert_eq!(BatchTracker::key_of(&t), key);

        let cluster_key = crate::kube::protocol::ObjectKey::new(String::new(), "n1".to_string());
        let t = ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Node),
            cluster_key.name.clone(),
            Namespace::from_row(&cluster_key.namespace),
        );
        assert_eq!(BatchTracker::key_of(&t), cluster_key);
    }

    fn pod_rid() -> ResourceId {
        ResourceId::BuiltIn(BuiltInKind::Pod)
    }

    #[test]
    fn consume_correlates_and_aggregates() {
        let targets = [target("a", "ns"), target("b", "ns"), target("c", "ns")];
        let mut tr = BatchTracker::new("Deleted", "pod".to_string(), pod_rid(), &targets, 1, std::sync::Weak::new());
        assert!(!tr.is_done());

        // A result for a foreign target is NOT ours.
        assert!(!tr.consume(&target("other", "ns"), &Ok("Deleted".into())));

        // A result for a DIFFERENT KIND with the same ns+name is NOT
        // ours either — the rid gate is what keeps a concurrent
        // single-op on a same-named object of another kind from being
        // misattributed to the batch.
        let foreign_kind = ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Deployment),
            "a".to_string(),
            Namespace::from_row("ns"),
        );
        assert!(!tr.consume(&foreign_kind, &Ok("Deleted".into())));
        assert!(!tr.is_done());

        assert!(tr.consume(&targets[0], &Ok("Deleted pod/a".into())));
        assert!(tr.consume(&targets[1], &Err("Forbidden".into())));
        assert!(!tr.is_done());
        // A duplicate result for an already-consumed target is not ours.
        assert!(!tr.consume(&targets[0], &Ok("again".into())));

        assert!(tr.consume(&targets[2], &Ok("Deleted pod/c".into())));
        assert!(tr.is_done());

        let summary = tr.summary();
        assert!(summary.message.contains("Deleted 2 pods"), "{}", summary.message);
        assert!(summary.message.contains("1 skipped"), "{}", summary.message);
        assert!(summary.message.contains("1 FAILED"), "{}", summary.message);
        assert!(summary.message.contains("Forbidden"), "{}", summary.message);
    }

    #[test]
    fn ok_results_unmark_their_rows_through_the_weak_store() {
        use crate::app::store::{RowStore, StorePayload};
        use crate::kube::protocol::TableBaseline;
        use crate::kube::resources::row::{CellValue, ResourceRow};

        let store = RowStore::new("pods");
        let row = ResourceRow {
            cells: vec![CellValue::Text("a".into())],
            name: "a".into(),
            namespace: Some("ns".into()),
            ..Default::default()
        };
        store.apply(1, StorePayload::Baseline(TableBaseline {
            resource: ResourceId::BuiltIn(BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![row],
        }));
        let key = crate::kube::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
        assert_eq!(store.toggle_mark(&key), Some(true));

        let t = target("a", "ns");
        let mut tr = BatchTracker::new(
            "Restarted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
            std::sync::Arc::downgrade(&store),
        );
        assert!(tr.consume(&t, &Ok("Restarted".into())));
        assert!(!store.has_marks(), "success unmarked the row (restart keeps rows in place)");
        assert!(tr.is_done());
    }

    #[test]
    fn failed_results_keep_their_marks() {
        use crate::app::store::{RowStore, StorePayload};
        use crate::kube::protocol::TableBaseline;
        use crate::kube::resources::row::{CellValue, ResourceRow};

        let store = RowStore::new("pods");
        let row = ResourceRow {
            cells: vec![CellValue::Text("a".into())],
            name: "a".into(),
            namespace: Some("ns".into()),
            ..Default::default()
        };
        store.apply(1, StorePayload::Baseline(TableBaseline {
            resource: ResourceId::BuiltIn(BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows: vec![row],
        }));
        let key = crate::kube::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
        store.toggle_mark(&key);

        let t = target("a", "ns");
        let mut tr = BatchTracker::new(
            "Deleted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
            std::sync::Arc::downgrade(&store),
        );
        assert!(tr.consume(&t, &Err("RBAC".into())));
        assert!(store.has_marks(), "failure keeps the mark for retry");
        let summary = tr.summary();
        assert!(summary.message.contains("Deleted 0 pods"), "{}", summary.message);
        assert!(summary.message.contains("RBAC"), "{}", summary.message);
    }

    #[test]
    fn fail_send_counts_as_a_result() {
        let t = target("a", "ns");
        let mut tr = BatchTracker::new(
            "Deleted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0, std::sync::Weak::new(),
        );
        tr.fail_send(&t, "send failed: broken pipe".into());
        assert!(tr.is_done());
        assert!(tr.summary().message.contains("broken pipe"));
    }
}

#[cfg(test)]
mod key_combo_tests {
    use super::*;

    fn parse(s: &str) -> Result<KeyCombo, String> {
        KeyCombo::try_from(s.to_string())
    }

    #[test]
    fn parses_plain_ctrl_and_case() {
        assert_eq!(parse("l"), Ok(KeyCombo::plain('l')));
        assert_eq!(parse("L"), Ok(KeyCombo::plain('L')));
        assert_eq!(parse("-"), Ok(KeyCombo::plain('-')), "a lone dash is a character");
        assert_eq!(parse(" "), Ok(KeyCombo::plain(' ')), "space is a character");
        assert_eq!(parse("ctrl-l"), Ok(KeyCombo { ctrl: true, ch: 'l' }));
        // Ctrl chords arrive lowercase from the terminal — an uppercase
        // spec is normalized so it can actually match.
        assert_eq!(parse("Ctrl-L"), Ok(KeyCombo { ctrl: true, ch: 'l' }));
        assert_eq!(parse("C-x"), Ok(KeyCombo { ctrl: true, ch: 'x' }));
        assert_eq!(parse(" ctrl-l "), Ok(KeyCombo { ctrl: true, ch: 'l' }), "trimmed");
    }

    #[test]
    fn rejects_malformed_specs() {
        for bad in ["", "ctrl-", "meta-l", "ll", "ctrl-ll", "alt-x"] {
            assert!(parse(bad).is_err(), "'{bad}' must be rejected");
        }
    }

    #[test]
    fn validate_rejects_reserved_and_duplicate_chords() {
        // Reserved: a binding an earlier dispatch layer always consumes.
        let keys = KeysConfig {
            logs: Some(KeyCombo { ctrl: true, ch: 'r' }),
            ..Default::default()
        };
        let err = keys.validate().unwrap_err();
        assert!(err.contains("keys.logs") && err.contains("reserved"), "{err}");

        // Duplicate chords across entries.
        let keys = KeysConfig {
            col_right: Some(KeyCombo::plain('x')),
            logs: Some(KeyCombo::plain('x')),
            ..Default::default()
        };
        let err = keys.validate().unwrap_err();
        assert!(err.contains("already bound"), "{err}");

        // The user's real shape is fine.
        let keys = KeysConfig {
            col_left: Some(KeyCombo::plain('h')),
            col_right: Some(KeyCombo::plain('l')),
            logs: Some(KeyCombo { ctrl: true, ch: 'l' }),
            ..Default::default()
        };
        assert!(keys.validate().is_ok());
    }

    #[test]
    fn op_key_override_replaces_default() {
        use crate::kube::protocol::OperationKind as Op;
        let mut keys = KeysConfig::default();
        // Default: descriptor key as a plain chord.
        assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo::plain('l')));
        // Override REPLACES it (the default char is no longer the binding).
        keys.logs = Some(KeyCombo { ctrl: true, ch: 'l' });
        assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo { ctrl: true, ch: 'l' }));
        // Structural-default ops have no chord unless the user adds one.
        assert_eq!(keys.op_key(&Op::Delete), None);
        keys.delete = Some(KeyCombo::plain('x'));
        assert_eq!(keys.op_key(&Op::Delete), Some(KeyCombo::plain('x')));
        // Overlay-defined ops are never bindable here.
        assert_eq!(keys.op_key(&Op::Custom("z".into())), None);
    }

    #[test]
    fn labels_render_ctrl_and_shift() {
        assert_eq!(KeyCombo::plain('l').label(), "l");
        assert_eq!(KeyCombo::plain('L').label(), "Shift-l");
        assert_eq!(KeyCombo { ctrl: true, ch: 'l' }.label(), "C-l");
    }

    /// Pins the documented config shape (what `load_section` hands to
    /// AppConfig after stripping the `k9rs:` root) — INCLUDING a sibling
    /// `daemon:` section, which the README documents in the same file
    /// and must not be rejected as an unknown field.
    #[test]
    fn app_config_parses_keys_section() {
        let yaml = "keys:\n  colLeft: h\n  colRight: l\n  colFirst: \"0\"\n  colLast: \"$\"\n  logs: ctrl-l\n\
                    daemon:\n  watcherPageSize: 500\n";
        let cfg: AppConfig = serde_yaml::from_str(yaml).expect("keys + daemon sections parse");
        assert_eq!(cfg.keys.col_left, Some(KeyCombo::plain('h')));
        assert_eq!(cfg.keys.col_right, Some(KeyCombo::plain('l')));
        assert_eq!(cfg.keys.col_first, Some(KeyCombo::plain('0')));
        assert_eq!(cfg.keys.col_last, Some(KeyCombo::plain('$')));
        // `0` / `$` are not reserved chords, so the binding validates.
        assert!(cfg.keys.validate().is_ok(), "vim 0/$ column jumps must validate");
        assert_eq!(cfg.keys.logs, Some(KeyCombo { ctrl: true, ch: 'l' }));
        // A typo'd binding name is a load-time error (deny_unknown_fields),
        // not a silently ignored key.
        assert!(serde_yaml::from_str::<AppConfig>("keys:\n  colleft: h\n").is_err());
        // A malformed chord is too.
        assert!(serde_yaml::from_str::<AppConfig>("keys:\n  logs: meta-l\n").is_err());
        // And so is a typo inside the daemon section — the TUI's loud
        // startup validation covers the whole file.
        assert!(serde_yaml::from_str::<AppConfig>("daemon:\n  watcherPagesize: 5\n").is_err());
    }
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
    /// Current text input — the characters the user has typed. Used by the
    /// text-like kinds (Text/Number/Port). For `Select` the chosen option is
    /// the typed `selected` index carried on the kind itself, and this field
    /// stays empty.
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
    /// One of a fixed set of choices, cycled with Left/Right. `selected`
    /// indexes `options` — typed, so the chosen option can't desync from a
    /// parallel digit-string the way the old `value`-encoded index could.
    Select { options: Vec<SelectOption>, selected: usize },
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

/// What happens when a form dialog is submitted. Each variant knows how
/// to extract field values and produce a wire command. The generic dialog
/// framework just collects input — it never knows what operation it's for.
///
/// Adding a new form-driven operation: add a variant here, implement its
/// `submit` arm in `dispatch_form_submit`, and write a builder function.
#[derive(Debug, Clone)]
pub enum FormSubmit {
    /// Scale a workload: reads the REPLICAS field → SessionCommand::Scale.
    Scale,
    /// Create a port-forward: reads CONTAINER_PORT + LOCAL_PORT fields
    /// → SessionCommand::PortForward.
    PortForward,
}

impl FormSubmit {
    /// Map an `OperationKind` to the corresponding `FormSubmit` variant.
    pub fn from_operation(op: crate::kube::protocol::OperationKind) -> Option<Self> {
        match op {
            crate::kube::protocol::OperationKind::Scale => Some(FormSubmit::Scale),
            crate::kube::protocol::OperationKind::PortForward => Some(FormSubmit::PortForward),
            _ => None,
        }
    }

    /// Human-readable label for flash messages.
    pub fn from_operation_label(&self) -> &'static str {
        match self {
            FormSubmit::Scale => "Scaling",
            FormSubmit::PortForward => "Port-forwarding",
        }
    }

    /// Build the wire command from form field values. Each variant knows
    /// which fields to extract and how to parse them. Returns the command
    /// to send to the daemon, or an error message for the user.
    pub fn build_command(
        &self,
        target: &crate::kube::protocol::ObjectRef,
        fields: &[FormFieldState],
    ) -> Result<crate::kube::protocol::SessionCommand, String> {
        use crate::kube::protocol::{SessionCommand, form_field_name};
        match self {
            FormSubmit::Scale => {
                let val = find_field_value(fields, form_field_name::REPLICAS)?;
                let replicas = val.parse::<u32>()
                    .map_err(|_| format!("Invalid replica count: {}", val))?;
                Ok(SessionCommand::Scale { target: target.clone(), replicas })
            }
            FormSubmit::PortForward => {
                let cp = parse_port_field(fields, form_field_name::CONTAINER_PORT)?;
                let lp = parse_port_field(fields, form_field_name::LOCAL_PORT)?;
                Ok(SessionCommand::PortForward {
                    target: target.clone(),
                    local_port: lp,
                    container_port: cp,
                })
            }
        }
    }
}

/// Extract a field's trimmed value by name.
fn find_field_value(fields: &[FormFieldState], name: &str) -> Result<String, String> {
    fields.iter()
        .find(|f| f.name == name)
        .map(|f| f.value.trim().to_string())
        .ok_or_else(|| format!("Missing field: {}", name))
}

/// Parse a port field — handles both Select (value is option index) and
/// direct Port input (value is the port number string).
fn parse_port_field(fields: &[FormFieldState], name: &str) -> Result<u16, String> {
    let field = fields.iter()
        .find(|f| f.name == name)
        .ok_or_else(|| format!("Missing field: {}", name))?;
    match &field.kind {
        FormFieldKind::Select { options, selected } => {
            options.get(*selected)
                .and_then(|opt| opt.value.parse::<u16>().ok())
                .ok_or_else(|| "Invalid port selection".to_string())
        }
        _ => field.value.trim().parse::<u16>()
            .map_err(|_| format!("Invalid port: {}", field.value.trim())),
    }
}

/// A modal form dialog gathering input for a single operation.
#[derive(Debug, Clone)]
pub struct FormDialog {
    /// What to do on submit.
    pub submit: FormSubmit,
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

/// Shared state for YAML and Describe content views (previously duplicated as
/// `YamlState` and `DescribeState`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentViewState {
    pub content: String,
    /// Typed describe lines when this view is a structured *describe* (empty
    /// for YAML, aliases, and kubectl-fallback text). When non-empty the
    /// renderer styles by the producer's role tags instead of re-inferring
    /// structure from `content`.
    pub describe_lines: Vec<crate::kube::protocol::DescribeLine>,
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
    /// Set plain-text content (YAML, aliases, error text) and update the cached
    /// line count. Clears any typed describe lines so the renderer falls back
    /// to text inference for these genuinely-opaque views.
    pub fn set_content(&mut self, content: String) {
        self.line_count = content.lines().count();
        self.content = content;
        self.describe_lines.clear();
    }

    /// Set typed describe lines (a structured describe). Derives the flat
    /// `content` (line texts joined) so search / scroll / clipboard keep
    /// operating on a `String` unchanged.
    pub fn set_describe_lines(&mut self, lines: Vec<crate::kube::protocol::DescribeLine>) {
        self.content = crate::kube::protocol::describe_lines_text(&lines);
        self.line_count = lines.len();
        self.describe_lines = lines;
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

    pub fn next_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        let target = self.search_matches[self.current_match];
        let max = crate::util::content_max_scroll(self.line_count(), visible);
        self.scroll = target.saturating_sub(visible / 2).min(max);
    }

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
        let max = crate::util::content_max_scroll(self.line_count(), visible);
        self.scroll = target.saturating_sub(visible / 2).min(max);
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
    /// Typed describe lines for `ContentKind::Describe` entries; empty for
    /// YAML. Lets a cached describe re-open render by role tags exactly like a
    /// fresh one, with no inference-on-cache-hit inconsistency.
    pub(crate) describe_lines: Vec<crate::kube::protocol::DescribeLine>,
    pub(crate) cached_at: Instant,
}

pub struct KubectlCache {
    entries: HashMap<CacheKey, CacheEntry>,
    insertion_order: Vec<CacheKey>,
    ttl: Duration,
    max_capacity: usize,
}

impl KubectlCache {
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: Vec::new(),
            ttl,
            max_capacity: capacity,
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
        self.insert_entry(
            CacheKey::new(target, kind),
            CacheEntry { content, describe_lines: Vec::new(), cached_at: Instant::now() },
        );
    }

    /// Cache a structured describe by its typed lines. The flat `content` is
    /// derived so the text-keyed `get` keeps working for this entry too.
    pub fn insert_describe(
        &mut self,
        target: ObjectRef,
        lines: Vec<crate::kube::protocol::DescribeLine>,
    ) {
        let content = crate::kube::protocol::describe_lines_text(&lines);
        self.insert_entry(
            CacheKey::new(target, ContentKind::Describe),
            CacheEntry { content, describe_lines: lines, cached_at: Instant::now() },
        );
    }

    /// Shared insert: update in place if the key exists (preserving insertion
    /// order), else append and, once at capacity, evict the oldest entry —
    /// FIFO by insertion order, not LRU (an in-place update does not refresh
    /// recency, so a frequently-read entry is still evicted on schedule).
    fn insert_entry(&mut self, key: CacheKey, entry: CacheEntry) {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key.clone()) {
            e.insert(entry);
            return;
        }
        if self.entries.len() >= self.max_capacity {
            if let Some(oldest_key) = self.insertion_order.first().cloned() {
                self.entries.remove(&oldest_key);
                self.insertion_order.remove(0);
            }
        }
        self.insertion_order.push(key.clone());
        self.entries.insert(key, entry);
    }

    /// Fresh typed describe lines for `target`, if cached and unexpired. Used by
    /// the describe cache-hit path so a re-open renders identically to a fresh
    /// describe instead of falling back to text inference.
    pub fn get_describe_lines(
        &self,
        target: &ObjectRef,
    ) -> Option<Vec<crate::kube::protocol::DescribeLine>> {
        let key = CacheKey::new(target.clone(), ContentKind::Describe);
        self.entries.get(&key).and_then(|entry| {
            // Empty lines count as a miss, not a hit: a describe view fed empty
            // lines would derive empty `content` and wedge on "Loading…". A real
            // describe always has lines, so this only rejects a degenerate
            // entry and re-fetches.
            let usable = entry.cached_at.elapsed() < self.ttl && !entry.describe_lines.is_empty();
            usable.then(|| entry.describe_lines.clone())
        })
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

#[cfg(test)]
mod form_submit_tests {
    use super::*;
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    use crate::kube::resource_def::BuiltInKind;

    fn pod_target() -> ObjectRef {
        ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Pod),
            "test-pod",
            Namespace::from_user_command("default"),
        )
    }

    #[test]
    fn scale_build_command_valid() {
        let target = pod_target();
        let fields = vec![FormFieldState {
            name: "replicas".into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 100 },
            value: "3".into(),
        }];
        let cmd = FormSubmit::Scale.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::Scale { replicas, .. } => {
                assert_eq!(replicas, 3);
            }
            _ => panic!("expected Scale command"),
        }
    }

    #[test]
    fn scale_build_command_invalid() {
        let target = pod_target();
        let fields = vec![FormFieldState {
            name: "replicas".into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 100 },
            value: "abc".into(),
        }];
        assert!(FormSubmit::Scale.build_command(&target, &fields).is_err());
    }

    #[test]
    fn port_forward_build_command_valid() {
        let target = pod_target();
        let fields = vec![
            FormFieldState {
                name: "container_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "8080".into(),
            },
            FormFieldState {
                name: "local_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "9090".into(),
            },
        ];
        let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::PortForward { local_port, container_port, .. } => {
                assert_eq!(container_port, 8080);
                assert_eq!(local_port, 9090);
            }
            _ => panic!("expected PortForward command"),
        }
    }

    #[test]
    fn port_forward_build_command_select_uses_typed_index() {
        let target = pod_target();
        // The pf_ports path builds a Select container_port. The chosen option is
        // the typed `selected` index — no digit-string round-trip — so selected=1
        // resolves to the second option ("8443").
        let fields = vec![
            FormFieldState {
                name: "container_port".into(),
                label: "".into(),
                kind: FormFieldKind::Select {
                    options: vec![
                        SelectOption::new("8080", "8080"),
                        SelectOption::new("8443", "8443"),
                    ],
                    selected: 1,
                },
                value: String::new(),
            },
            FormFieldState {
                name: "local_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "9090".into(),
            },
        ];
        let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::PortForward { container_port, local_port, .. } => {
                assert_eq!(container_port, 8443);
                assert_eq!(local_port, 9090);
            }
            _ => panic!("expected PortForward command"),
        }
    }
}


