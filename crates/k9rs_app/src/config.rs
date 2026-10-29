//! The TUI's section of `~/.config/k9rs/config.yaml`.
//!
//! Where the file lives and what a missing section means is
//! [`k9rs_config`]'s; what a key MEANS is here. The daemon's section is
//! interpreted by `k9rs_kube::daemon_config`, which cannot live down there
//! because its `execResources` entries are a `k9rs_kube` type.
//!
//! These ~370 lines were part of `types.rs`, a 1468-line file holding six
//! unrelated concerns. A file named `types` is a file named "stuff"; config
//! was the clearest seam in it and the one the user named as its own concern.
//!
//! Strictness is the whole character of this module: every struct is
//! `deny_unknown_fields` and [`load`] returns `Err` rather than falling back
//! to defaults, because a silent fallback means one typo in `keys:` reverts
//! `readOnly` too, with the explanation going to a tracing sink nobody reads.

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
    pub daemon: Option<k9rs_kube::daemon_config::DaemonConfig>,
}

/// A single key chord: a character plus an optional Ctrl modifier.
/// Parsed from config strings — `"l"`, `"L"` (shift rides in the
/// character's case), `"ctrl-l"` / `"C-l"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(try_from = "String")]
pub struct KeyCombo {
    /// Private, with [`Self::ctrl`] the only way to set it, because the
    /// invariant below is not expressible in the type: a Ctrl chord's
    /// character must be lowercase. The fields were `pub` and the only
    /// constructor was `plain`, so `KeyCombo::ctrl('L')` was
    /// constructible — a chord no key event can produce, against which every
    /// comparison in the app is exact equality.
    ctrl: bool,
    ch: char,
}

impl KeyCombo {
    /// An unmodified chord. Shift rides in the character's own case, which
    /// is how `Shift-F` and `C` are expressed.
    pub const fn plain(ch: char) -> Self {
        Self { ctrl: false, ch }
    }

    /// A Ctrl chord. LOWERCASES its character, because the terminal delivers
    /// Ctrl chords as lowercase (legacy keyboard protocol) — the rule the
    /// config parser already applied and nothing else did.
    pub const fn ctrl(ch: char) -> Self {
        Self { ctrl: true, ch: ch.to_ascii_lowercase() }
    }

    /// The chord a key event is, or `None` when it is not one.
    ///
    /// Alt is never a binding: an unclaimed Alt chord must not alias to its
    /// bare character (that is how `Alt-q` came to quit). A non-character
    /// key is not a configurable chord at all.
    ///
    /// Here rather than in the key handler so that this and
    /// [`Self::as_key_event`] — the two halves of the correspondence between
    /// a chord and an event — sit together and uphold the lowercase rule
    /// once. The handler's version built the struct directly, so a terminal
    /// delivering `Ctrl-Shift-L` as `Char('L')` + CONTROL produced a combo
    /// no config spec could equal.
    pub fn of_event(key: &crossterm::event::KeyEvent) -> Option<Self> {
        use crossterm::event::{KeyCode, KeyModifiers};
        let KeyCode::Char(c) = key.code else { return None };
        if key.modifiers.contains(KeyModifiers::ALT) {
            return None;
        }
        Some(if key.modifiers.contains(KeyModifiers::CONTROL) {
            Self::ctrl(c)
        } else {
            Self::plain(c)
        })
    }

    /// The key event this chord corresponds to — the inverse of
    /// [`Self::of_event`].
    pub fn as_key_event(&self) -> crossterm::event::KeyEvent {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mods = if self.ctrl { KeyModifiers::CONTROL } else { KeyModifiers::NONE };
        KeyEvent::new(KeyCode::Char(self.ch), mods)
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
                return Ok(Self::ctrl(c));
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
/// - In a LOG view the `0`–`7` range ladder is answered BEFORE these,
///   so `colFirst: "0"` shadows the table's `0` and not the log's.
///   The ladder is the only way to reach a range and the hint bar
///   advertises it as `<0-7> range`, where column panning also has
///   the structural `^` / `$`. All four column chords work in a log
///   view (`configured_column_action` is one lookup for both views);
///   only the digits are reserved there.
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
    pub fn op_override(&self, op: &k9rs_proto::protocol::OperationKind) -> Option<KeyCombo> {
        use k9rs_proto::protocol::OperationKind as Op;
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
    pub fn op_key(&self, op: &k9rs_proto::protocol::OperationKind) -> Option<KeyCombo> {
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
        for op in k9rs_proto::protocol::OperationKind::BUILT_IN {
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
        // The app's OWN chords: consumed by a dispatch layer above the
        // bindings, and not derivable from anything, so they are a list.
        const GLOBAL: &[(KeyCombo, &str)] = &[
            (KeyCombo::ctrl('c'), "quit"),
            (KeyCombo::ctrl('r'), "refresh"),
            (KeyCombo::ctrl('e'), "toggle header"),
            (KeyCombo::ctrl('s'), "save table/logs"),
            (KeyCombo::ctrl('a'), "aliases"),
            (KeyCombo::ctrl('w'), "wide columns"),
            (KeyCombo::ctrl('z'), "fault filter"),
            (KeyCombo::ctrl(' '), "span-mark"),
            (KeyCombo::ctrl('\\'), "clear marks"),
            (KeyCombo::plain(':'), "command mode"),
            (KeyCombo::plain('/'), "filter"),
            (KeyCombo::plain('?'), "help"),
            // A log-view key checked ahead of the configured chords, same as
            // the structural ones below — `C` clears the buffer.
            (KeyCombo::plain('C'), "clear logs"),
        ];
        // The STRUCTURAL chords, derived. These were two literals here
        // — `ctrl-d` and `ctrl-k` — against a `structural_chord` that
        // declares THREE, and the missing one was `Shift-F`. The structural
        // loop runs first in `handle_resource_view_keys`, ahead of every
        // config lookup, so `keys: { describe: "F" }` validated clean and
        // then, on each resource declaring `PortForward` (pods, deployments,
        // services), `F` port-forwarded while `d` no longer described —
        // describe unreachable there and working on a configmap. Exactly the
        // asymmetry the comment above reserves Ctrl-K to avoid.
        let structural = k9rs_proto::protocol::OperationKind::BUILT_IN
            .iter()
            .filter_map(|op| {
                crate::keymap::structural_chord(op)
                    .map(|chord| (chord, op.descriptor().label))
            });
        let reserved: Vec<(KeyCombo, &str)> =
            GLOBAL.iter().copied().chain(structural).collect();

        let mut seen: std::collections::HashMap<KeyCombo, &'static str> =
            std::collections::HashMap::new();
        for (name, combo) in self.entries() {
            if let Some((_, what)) = reserved.iter().find(|(r, _)| *r == combo) {
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
    pub theme: k9rs_theme::ThemeMode,
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
            theme: k9rs_theme::ThemeMode::default(),
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

    /// Whether a freshly opened log stream should follow its tail.
    ///
    /// THE reader of `default_follow`, which had none: the key parsed, passed
    /// the strict loader, and was discarded, while its two siblings in this
    /// struct — `default_wrap` and `default_fields` — were read in the same
    /// constructor. Every `LogInit` built `follow: true` or `follow:
    /// !previous` by hand, so `defaultFollow: false` was a documented
    /// setting (README: "auto-scroll logs") that did nothing. That is the
    /// exact failure this module's header says it exists to avoid — loud
    /// about typos, silent about a correctly-spelled key nobody reads.
    ///
    /// `previous` overrides the preference rather than being ANDed with it by
    /// each caller: a terminated container's log is finite, so there is
    /// nothing to follow and following would pin the view to a bottom that
    /// never moves.
    pub fn follows(&self, previous: bool) -> bool {
        self.default_follow && !previous
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

/// Load and VALIDATE the TUI's settings.
///
/// Missing file or section → defaults; a present-but-invalid file (YAML
/// error, unknown field, malformed chord, reserved-key collision) → `Err`
/// with the full story. `main` calls this BEFORE the alternate screen so the
/// error lands on a visible stderr and the process exits — never a silent
/// fallback to defaults.
///
/// Was `App::load_config`, which put the file-reading half of config on the
/// state type. Nothing here needs an `App`.
pub fn load() -> Result<AppConfig, String> {
    // The `k9rs` root deserializes straight into `AppConfig` — hence the
    // empty section name.
    let config = k9rs_config::load_section::<AppConfig>("")?.unwrap_or_default();
    config.keys.validate()?;
    config.validate()?;
    Ok(config)
}

impl AppConfig {
    /// Refuse a numeric value whose behaviour contradicts its own name.
    ///
    /// `load` validated `keys` and nothing else, so the three settings below
    /// were accepted at zero and then did something other than what they
    /// say. That is the silent fallback this module's header says it exists
    /// to refuse — it is loud about a misspelled key and was quiet about a
    /// correctly-spelled one with a meaningless value.
    ///
    /// Only these three. Several siblings have a perfectly good zero and are
    /// deliberately not checked: `flash.*Secs: 0` is "do not linger",
    /// `searchContextLines: 0` is "no context", `commandHistorySize: 0` is
    /// "no history" (and the trim loop handles it exactly), and
    /// `logs.tailLines: 0` is kubectl's `--tail=0`, i.e. follow without
    /// backfill. Refusing those would be strictness for its own sake.
    fn validate(&self) -> Result<(), String> {
        if self.ui.max_column_width == 0 {
            return Err(
                "ui.maxColumnWidth: 0 truncates every column to nothing — it is a cap, \
                 not a switch"
                    .to_string(),
            );
        }
        if self.ui.logs.max_lines == 0 {
            return Err(
                "ui.logs.maxLines: 0 keeps no log lines at all — it is a cap, not a switch"
                    .to_string(),
            );
        }
        if self.ui.cache_capacity == 0 {
            return Err(
                "ui.cacheCapacity: 0 does not disable the cache — the eviction is one entry \
                 per insert, so it behaves as 1. Say 1."
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
