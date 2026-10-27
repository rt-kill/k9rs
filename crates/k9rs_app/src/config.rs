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
    Ok(config)
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
