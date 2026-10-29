//! What the TUI's section of the config file means, and how loudly it
//! refuses nonsense.
//!
//! These were in `k9rs_kube`'s `daemon_config` mirror, next to the daemon's
//! half of the same file. `AppConfig` is the TUI's section, so the assertions
//! belong here — and a lower crate's tests reaching up is a dev-dependency
//! cycle that costs `cargo test -p k9rs_kube` the entire TUI.

use crate::config::{AppConfig, KeyCombo, KeysConfig, LogConfig};

#[test]
fn app_config_defaults() {
    let cfg = AppConfig::default();
    assert!(!cfg.no_exit_on_ctrl_c);
    assert!(!cfg.read_only);
    assert_eq!(cfg.ui.flash.info_secs, 3);
    assert_eq!(cfg.ui.logs.max_lines, 50_000);
}

#[test]
fn app_config_partial_yaml() {
    let yaml = r#"
readOnly: true
ui:
  maxColumnWidth: 20
"#;
    let cfg: AppConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(cfg.read_only);
    assert_eq!(cfg.ui.max_column_width, 20);
    // Nested defaults preserved
    assert_eq!(cfg.ui.flash.info_secs, 3);
    assert_eq!(cfg.ui.logs.tail_lines, 100);
}

#[test]
fn app_config_unknown_field_rejected() {
    let yaml = r#"
readOnly: true
typoField: 42
"#;
    let result = serde_yaml::from_str::<AppConfig>(yaml);
    assert!(result.is_err(), "unknown fields should be rejected");
}

#[test]
fn app_config_nested_unknown_rejected() {
    let yaml = r#"
ui:
  maxColumnWitdh: 32
"#;
    let result = serde_yaml::from_str::<AppConfig>(yaml);
    assert!(result.is_err(), "typo in nested field should be rejected");
}

#[test]
fn empty_yaml_uses_defaults() {
    let cfg: AppConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(cfg.ui.max_column_width, 64);
}

/// `entries()` is what `validate()` walks for reserved-chord and duplicate
/// checks, and it used to be a THIRD hand-written list parallel to the struct
/// fields and to `op_override` — so a binding added to those two and missed
/// here escaped both checks, in the function whose doc says the point is
/// "a startup error, not a mystery". It is derived now; this pins that.
#[test]
fn every_bindable_operation_reaches_the_validator() {
use k9rs_proto::protocol::OperationKind as Op;

// Give every bindable operation an override, then assert each one shows
// up by name in the list `validate` sees.
let keys = KeysConfig {
    describe: Some(KeyCombo::plain('1')),
    yaml: Some(KeyCombo::plain('2')),
    logs: Some(KeyCombo::plain('3')),
    previous_logs: Some(KeyCombo::plain('4')),
    shell: Some(KeyCombo::plain('5')),
    restart: Some(KeyCombo::plain('6')),
    scale: Some(KeyCombo::plain('7')),
    delete: Some(KeyCombo::plain('8')),
    force_kill: Some(KeyCombo::plain('9')),
    port_forward: Some(KeyCombo::plain('A')),
    show_node: Some(KeyCombo::plain('B')),
    node_shell: Some(KeyCombo::plain('C')),
    decode_secret: Some(KeyCombo::plain('D')),
    trigger_cron_job: Some(KeyCombo::plain('E')),
    toggle_suspend_cron_job: Some(KeyCombo::plain('F')),
    ..Default::default()
};

let seen: Vec<&str> = keys.entries().into_iter().map(|(n, _)| n).collect();
for op in Op::BUILT_IN {
    let Some(name) = op.config_key() else { continue };
    assert!(
        seen.contains(&name),
        "{op:?} is bindable as `keys.{name}` but never reaches validate(): {seen:?}",
    );
}
}

/// The view starts one shed level in: source tag and message, no timestamp.
///
/// Not two independent flags — `t` cycles ONE level, so the config names a
/// point on that cycle. Two booleans were four combinations for three
/// reachable states, and the fourth (a timestamp with no attribution) was
/// something a config could ask for and no keypress could produce.
///
/// The reasons for the level are not symmetric, which is why this is pinned:
/// the kubelet stamp is 31 of ~97 columns duplicating one the service prints
/// itself, while a source tag only EXISTS on a multi-source stream and is
/// then the one thing saying which replica spoke.
#[test]
fn the_log_view_starts_on_source_and_message() {
let cfg = crate::LogConfig::default();
assert_eq!(cfg.default_fields, crate::LogFields::Source);
assert_eq!(cfg.initial_shed(), 1);
use k9rs_proto::protocol::Role;
assert!(!Role::Timestamp.shown(cfg.initial_shed()), "the stamp is shed first");
assert!(Role::Source.shown(cfg.initial_shed()), "attribution survives it");
}

/// Every level of the cycle has a name a config can write. Adding a role to
/// `SHED_ORDER` creates a state, and this fails until someone names it —
/// which is the only thing stopping the config from being a stale parallel
/// list of the roles.
#[test]
fn log_fields_covers_every_shed_level() {
use crate::LogFields;
use k9rs_proto::protocol::Role;
let named = [LogFields::All, LogFields::Source, LogFields::Message];
assert_eq!(
    named.len(),
    Role::levels(),
    "a role was added to SHED_ORDER without naming the level it creates",
);
// …and the names map onto distinct, ordered levels.
let mut levels: Vec<usize> = named.iter().map(|f| f.shed()).collect();
let before = levels.clone();
levels.sort_unstable();
levels.dedup();
assert_eq!(levels.len(), named.len(), "two names for one level");
assert_eq!(before, levels, "the names are not in shed order");
}

/// The README's sample config must PARSE. Every config struct carries
/// `deny_unknown_fields` and `config::load` returns `Err` rather than falling
/// back to defaults — deliberately — so one stale key in the documented
/// sample is not a doc nit, it is an app that refuses to start for anyone who
/// copies it. The README carried `ui.pageScrollLines` for two months after
/// the Viewport rework deleted the concept.
///
/// The prior version of this test fed only the `keys:` and `daemon:` blocks,
/// so it pinned "the documented config shape" without ever parsing the `ui:`
/// block that had the dead key in it.
#[test]
fn the_readme_sample_config_actually_parses() {
// Anchored to the crate, not to this file: a relative `../../../` broke
// the moment the mirror moved one directory deeper, and the failure was
// a missing file rather than a wrong answer only by luck.
let readme = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md"));
let block = readme
    .split("```yaml")
    .nth(1)
    .and_then(|b| b.split("```").next())
    .expect("README has a yaml config block");
// The file is rooted at `k9rs:`; `load_section::<AppConfig>("")` reads
// that section, so the test parses the same shape the app does.
#[derive(serde::Deserialize)]
struct Root {
    k9rs: AppConfig,
}
let cfg = serde_yaml::from_str::<Root>(block)
    .unwrap_or_else(|e| panic!("the documented config is rejected by the app: {e}\n{block}"))
    .k9rs;
// …and the values it advertises are the ones the app actually defaults to,
// where it claims to be showing defaults.
assert_eq!(cfg.ui.logs.tail_lines, LogConfig::default().tail_lines);
assert_eq!(cfg.ui.logs.default_fields, LogConfig::default().default_fields);
}

// ---------------------------------------------------------------------------
// key_combo_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------

fn parse(s: &str) -> Result<KeyCombo, String> {
    KeyCombo::try_from(s.to_string())
}

#[test]
fn parses_plain_ctrl_and_case() {
    assert_eq!(parse("l"), Ok(KeyCombo::plain('l')));
    assert_eq!(parse("L"), Ok(KeyCombo::plain('L')));
    assert_eq!(parse("-"), Ok(KeyCombo::plain('-')), "a lone dash is a character");
    assert_eq!(parse(" "), Ok(KeyCombo::plain(' ')), "space is a character");
    assert_eq!(parse("ctrl-l"), Ok(KeyCombo::ctrl('l')));
    // Ctrl chords arrive lowercase from the terminal — an uppercase
    // spec is normalized so it can actually match.
    assert_eq!(parse("Ctrl-L"), Ok(KeyCombo::ctrl('l')));
    assert_eq!(parse("C-x"), Ok(KeyCombo::ctrl('x')));
    assert_eq!(parse(" ctrl-l "), Ok(KeyCombo::ctrl('l')), "trimmed");
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
        logs: Some(KeyCombo::ctrl('r')),
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
        logs: Some(KeyCombo::ctrl('l')),
        ..Default::default()
    };
    assert!(keys.validate().is_ok());
}

#[test]
fn op_key_override_replaces_default() {
    use k9rs_proto::protocol::OperationKind as Op;
    let mut keys = KeysConfig::default();
    // Default: descriptor key as a plain chord.
    assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo::plain('l')));
    // Override REPLACES it (the default char is no longer the binding).
    keys.logs = Some(KeyCombo::ctrl('l'));
    assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo::ctrl('l')));
    // Structural-default ops have no chord unless the user adds one.
    assert_eq!(keys.op_key(&Op::Delete), None);
    keys.delete = Some(KeyCombo::plain('x'));
    assert_eq!(keys.op_key(&Op::Delete), Some(KeyCombo::plain('x')));
    // TakeoverOp-defined ops are never bindable here.
    assert_eq!(keys.op_key(&Op::Custom("z".into())), None);
}

#[test]
fn labels_render_ctrl_and_shift() {
    assert_eq!(KeyCombo::plain('l').label(), "l");
    assert_eq!(KeyCombo::plain('L').label(), "L", "the capital, not `Shift-l`");
    assert_eq!(KeyCombo::ctrl('l').label(), "Ctrl-l");
}

/// A label is also a config SPEC: what `?` shows you is what you type under
/// `keys:` to move it.
///
/// It was not. `label()` rendered an uppercase char as `Shift-f` while
/// `TryFrom<String>` accepts only a single character or `ctrl-<char>`, so a
/// user who read `Shift-f  Port forward` off the help sheet and wrote
/// `portForward: Shift-f` got `invalid key spec 'Shift-f'` at startup —
/// help advertising an input its own parser refuses. The same sheet wrote
/// nine other shifted letters as bare capitals, so `F` and `Shift-f` also
/// appeared as two rows for one chord.
///
/// Over every chord the config can express, both directions.
#[test]
fn every_label_parses_back_to_its_own_chord() {
    let mut chords: Vec<KeyCombo> = Vec::new();
    for c in ('a'..='z').chain('A'..='Z').chain('0'..='9') {
        chords.push(KeyCombo::plain(c));
        chords.push(KeyCombo::ctrl(c));
    }
    for c in ['-', ' ', '/', ':', '~', '?', '\\', '|', '='] {
        chords.push(KeyCombo::plain(c));
    }

    for combo in chords {
        let label = combo.label();
        let back = KeyCombo::try_from(label.clone());
        assert_eq!(
            back,
            Ok(combo),
            "`{label}` is shown in help but does not parse back as a `keys:` spec",
        );
    }
}

/// Pins the documented config shape (what `load_section` hands to
/// AppConfig after stripping the `k9rs:` root) — INCLUDING a sibling
/// `daemon:` section, which the README documents in the same file
/// and must not be rejected as an unknown field.
#[test]
fn app_config_parses_keys_section() {
    let yaml = "keys:\n  colLeft: h\n  colRight: l\n  colFirst: \"0\"\n  colLast: \"$\"\n  namespaceAll: \")\"\n  logs: ctrl-l\n\
                daemon:\n  watcherPageSize: 500\n";
    let cfg: AppConfig = serde_yaml::from_str(yaml).expect("keys + daemon sections parse");
    assert_eq!(cfg.keys.col_left, Some(KeyCombo::plain('h')));
    assert_eq!(cfg.keys.col_right, Some(KeyCombo::plain('l')));
    assert_eq!(cfg.keys.col_first, Some(KeyCombo::plain('0')));
    assert_eq!(cfg.keys.col_last, Some(KeyCombo::plain('$')));
    // Shift-0 (`)`) restores switch-to-all-namespaces once `0` is colFirst.
    assert_eq!(cfg.keys.namespace_all, Some(KeyCombo::plain(')')));
    // `0` / `$` / `)` are not reserved chords, so the binding validates.
    assert!(cfg.keys.validate().is_ok(), "vim 0/$ column jumps + namespaceAll must validate");
    assert_eq!(cfg.keys.logs, Some(KeyCombo::ctrl('l')));
    // A typo'd binding name is a load-time error (deny_unknown_fields),
    // not a silently ignored key.
    assert!(serde_yaml::from_str::<AppConfig>("keys:\n  colleft: h\n").is_err());
    // A malformed chord is too.
    assert!(serde_yaml::from_str::<AppConfig>("keys:\n  logs: meta-l\n").is_err());
    // And so is a typo inside the daemon section — the TUI's loud
    // startup validation covers the whole file.
    assert!(serde_yaml::from_str::<AppConfig>("daemon:\n  watcherPagesize: 5\n").is_err());
}

// ---------------------------------------------------------------------------
// The README is a hand-written copy of an exhaustive source
// ---------------------------------------------------------------------------

/// The README's "Rebindable operations:" list matches the code.
///
/// Fifteen names, written out in prose beside an exhaustive `config_key`
/// match — the "one decision, many copies" shape, with the copy in a file no
/// compiler reads. Adding an operation forces a `config_key` arm (the match
/// is exhaustive) but nothing forces the README, so this list drifts the
/// moment anyone adds one. The in-app surfaces already project from one
/// declaration; the docs were the last copy.
#[test]
fn the_readme_lists_exactly_the_rebindable_operations() {
use k9rs_proto::protocol::OperationKind;

// Anchored to the crate, not to this file: a relative `../../../` broke
// the moment the mirror moved one directory deeper, and the failure was
// a missing file rather than a wrong answer only by luck.
let readme = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md"));
let marker = "# Rebindable operations:";
let at = readme.find(marker).expect("the README documents the rebindable set");
// The sentence runs to its full stop, wrapped over comment lines.
let tail = &readme[at + marker.len()..];
let sentence = &tail[..tail.find('.').expect("the list ends in a full stop")];
let documented: std::collections::BTreeSet<String> = sentence
    .split(',')
    .map(|w| w.trim().trim_start_matches('#').trim().to_string())
    .filter(|w| !w.is_empty())
    .collect();

let actual: std::collections::BTreeSet<String> = OperationKind::BUILT_IN
    .iter()
    .filter_map(|op| op.config_key())
    .map(str::to_string)
    .collect();

assert_eq!(
    documented, actual,
    "the README's rebindable list and `OperationKind::config_key` disagree \
     — documented-but-absent: {:?}; real-but-undocumented: {:?}",
    documented.difference(&actual).collect::<Vec<_>>(),
    actual.difference(&documented).collect::<Vec<_>>(),
);
}

/// The client gate and the daemon gate answer "read-only?" identically.
///
/// They were computed in two places from two different inputs: the entry
/// point handed `ConnectionParams.readonly` a bare `--readonly` flag while
/// `App::new` took `config.read_only`, so `readOnly: true` in config.yaml
/// armed the client and left the DAEMON open — every mutation the TUI did
/// not happen to refuse first went straight through. And because the
/// reconnect path sends `app.read_only`, which does include the config, the
/// daemon-side gate switched itself on after a dropped connection: one
/// session, enforced differently before and after a reconnect.
#[test]
fn both_read_only_gates_resolve_the_same_answer() {
    for (flag, in_config) in [(false, false), (false, true), (true, false), (true, true)] {
        let cfg = crate::AppConfig { read_only: in_config, ..Default::default() };
        assert_eq!(
            crate::ConnectionParams::read_only(flag, &cfg),
            flag || in_config,
            "flag={flag} config={in_config}: either source arms the session",
        );

        // What `App` stores is the SAME function of the same config. `main`
        // applies the flag on top of this, so both halves are checked.
        let namespace = k9rs_proto::protocol::Namespace::All;
        let metrics = crate::store::MetricsHub::new();
        let root = crate::element::Element::ResourceList(
            crate::element::ResourceList::open_for_test(
                crate::element::QuerySpec {
                    rid: k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                    namespace: namespace.clone(),
                    filter: None,
                },
                &metrics,
                "pods".to_string(),
            ),
        );
        let app = crate::App::new_with_root(
            None,
            namespace,
            metrics,
            root,
            cfg.clone(),
            k9rs_theme::Theme::for_appearance(k9rs_theme::term_bg::Appearance::Dark),
        );
        assert_eq!(
            app.read_only,
            crate::ConnectionParams::read_only(false, &cfg),
            "config={in_config}: the client gate takes the config half \
             through the same function the wire does",
        );
    }
}

/// EVERY structural chord is reserved, derived from the one declaration
/// rather than re-spelled.
///
/// `Shift-F` was the missing one. The structural loop runs first in
/// `handle_resource_view_keys`, ahead of every config lookup, so
/// `keys: { describe: "F" }` validated clean and then on each resource
/// declaring `PortForward` — pods, deployments, services — `F`
/// port-forwarded while `d` no longer described: describe unreachable
/// there, and still working on a configmap. That split is what the
/// reserved list exists to prevent.
#[test]
fn every_structural_chord_is_reserved() {
    use k9rs_proto::protocol::OperationKind;
    let structural: Vec<(OperationKind, crate::config::KeyCombo)> = OperationKind::BUILT_IN
        .iter()
        .filter_map(|op| crate::keymap::structural_chord(op).map(|c| (op.clone(), c)))
        .collect();
    assert_eq!(structural.len(), 3, "three structural chords: {structural:?}");

    for (op, chord) in structural {
        // Bind `describe` to it — the operation whose default `d` the user
        // loses when an override is accepted.
        let keys = KeysConfig {
            describe: Some(chord),
            ..Default::default()
        };
        let err = keys.validate().unwrap_err();
        assert!(
            err.contains("keys.describe") && err.contains("reserved"),
            "{op:?}'s chord {} must be refused: {err}",
            chord.label(),
        );
        assert!(
            err.contains(op.descriptor().label),
            "…and the refusal must name what holds it: {err}",
        );
    }
}

/// The log view's `C` is reserved for the same reason: it is answered before
/// the configured chords are consulted.
#[test]
fn the_log_views_clear_key_is_reserved() {
    let keys = KeysConfig {
        describe: Some(crate::config::KeyCombo::plain('C')),
        ..Default::default()
    };
    let err = keys.validate().unwrap_err();
    assert!(err.contains("reserved") && err.contains("clear logs"), "{err}");
}

/// `defaultFollow` is read, and `previous` overrides it.
///
/// It had NO reader: the key parsed, passed the strict loader and was
/// discarded, while its two siblings in the same struct were read in the
/// same constructor. `defaultFollow: false` was a documented setting
/// ("auto-scroll logs" in the README) that did nothing.
#[test]
fn default_follow_decides_whether_a_fresh_log_follows() {
    let mut cfg = crate::config::LogConfig::default();
    assert!(cfg.default_follow, "the shipped default");
    assert!(cfg.follows(false), "a live stream follows by default");

    cfg.default_follow = false;
    assert!(!cfg.follows(false), "…and the preference is honoured");
}

/// A terminated container's log is finite, so following it would pin the
/// view to a bottom that never moves. `previous` wins over the preference
/// in BOTH directions — which is why the rule is here and not ANDed by each
/// of the five callers.
#[test]
fn a_previous_log_never_follows() {
    let mut cfg = crate::config::LogConfig::default();
    assert!(!cfg.follows(true), "even with the preference on");
    cfg.default_follow = false;
    assert!(!cfg.follows(true));
}

/// A Ctrl chord's character is lowercase whichever door it comes through.
///
/// The invariant was enforced only in the config parser: the fields were
/// `pub`, so `KeyCombo { ctrl: true, ch: 'L' }` was constructible, and every
/// comparison in the app is exact equality against what the terminal
/// delivers — which for a Ctrl chord is lowercase (legacy keyboard
/// protocol). The key handler built one directly, so a terminal reporting
/// `Ctrl-Shift-L` as `Char('L')` + CONTROL produced a chord no config spec
/// could ever equal.
#[test]
fn a_ctrl_chord_is_lowercase_through_every_door() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    let lower = crate::config::KeyCombo::ctrl('l');
    assert_eq!(crate::config::KeyCombo::ctrl('L'), lower, "the constructor lowercases");
    assert_eq!(parse("Ctrl-L"), Ok(lower), "…and so does the parser");
    assert_eq!(
        crate::config::KeyCombo::of_event(&KeyEvent::new(
            KeyCode::Char('L'),
            KeyModifiers::CONTROL,
        )),
        Some(lower),
        "…and so does the event reader, which used to build the struct raw",
    );
    assert_eq!(lower.label(), "Ctrl-l");
}

/// Shift is NOT lowercased: it rides in the character's own case, which is
/// how `Shift-F` and `C` are expressed.
#[test]
fn shift_rides_in_the_characters_case() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    let upper = crate::config::KeyCombo::plain('F');
    assert_eq!(parse("F"), Ok(upper));
    assert_eq!(
        crate::config::KeyCombo::of_event(&KeyEvent::new(KeyCode::Char('F'), KeyModifiers::SHIFT)),
        Some(upper),
    );
    assert_ne!(upper, crate::config::KeyCombo::plain('f'), "two different bindings");
    assert_eq!(upper.label(), "F", "the capital IS the spec — see `label`");
}

/// A chord and its key event are inverses, so the two halves of the
/// correspondence cannot drift.
#[test]
fn a_chord_round_trips_through_its_key_event() {
    for combo in [
        crate::config::KeyCombo::plain('l'),
        crate::config::KeyCombo::plain('F'),
        crate::config::KeyCombo::ctrl('d'),
        crate::config::KeyCombo::ctrl('\\'),
        crate::config::KeyCombo::plain(' '),
    ] {
        assert_eq!(
            crate::config::KeyCombo::of_event(&combo.as_key_event()),
            Some(combo),
            "{} must survive the round trip",
            combo.label(),
        );
    }
}

/// An Alt chord is not a binding at all — an unclaimed one must not alias to
/// its bare character, which is how `Alt-q` came to quit the app.
#[test]
fn an_alt_chord_is_not_a_binding() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    assert_eq!(
        crate::config::KeyCombo::of_event(&KeyEvent::new(KeyCode::Char('q'), KeyModifiers::ALT)),
        None,
    );
    assert_eq!(
        crate::config::KeyCombo::of_event(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        None,
        "nor is a non-character key",
    );
}

// ---------------------------------------------------------------------------
// Numeric settings: a zero that lies about itself
// ---------------------------------------------------------------------------

/// The three values whose zero does something other than what the name
/// says are refused at load. `load()` validated `keys` and nothing else —
/// loud about a misspelled key, silent about a correctly-spelled one with a
/// meaningless value, which is the exact fallback this module exists to
/// refuse.
#[test]
fn a_meaningless_zero_is_a_startup_error() {
    let base = AppConfig::default();

    let mut cfg = base.clone();
    cfg.ui.max_column_width = 0;
    let err = cfg.validate().expect_err("every column would be invisible");
    assert!(err.contains("maxColumnWidth"), "{err}");

    let mut cfg = base.clone();
    cfg.ui.logs.max_lines = 0;
    let err = cfg.validate().expect_err("no log line would be kept");
    assert!(err.contains("maxLines"), "{err}");

    let mut cfg = base.clone();
    cfg.ui.cache_capacity = 0;
    let err = cfg.validate().expect_err("0 is not off, it is 1");
    assert!(err.contains("cacheCapacity"), "{err}");
}

/// The zeros that MEAN something are left alone. Strictness that refuses a
/// legitimate value is as wrong as silence about an illegitimate one:
/// `tailLines: 0` is kubectl's `--tail=0` (follow with no backfill), and
/// the rest are ordinary "none".
#[test]
fn a_meaningful_zero_is_accepted() {
    let mut cfg = AppConfig::default();
    cfg.ui.logs.tail_lines = 0;
    cfg.ui.search_context_lines = 0;
    cfg.ui.command_history_size = 0;
    cfg.ui.change_highlight_secs = 0;
    cfg.ui.flash.info_secs = 0;
    cfg.ui.flash.warn_secs = 0;
    cfg.ui.flash.error_secs = 0;
    assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
}

/// The shipped defaults pass their own validation — the check cannot be one
/// nobody could satisfy.
#[test]
fn the_default_config_validates() {
    let cfg = AppConfig::default();
    assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
    assert!(cfg.keys.validate().is_ok());
}
