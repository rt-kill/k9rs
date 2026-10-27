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
    use k9rs_proto::protocol::OperationKind as Op;
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
    // TakeoverOp-defined ops are never bindable here.
    assert_eq!(keys.op_key(&Op::Custom("z".into())), None);
}

#[test]
fn labels_render_ctrl_and_shift() {
    assert_eq!(KeyCombo::plain('l').label(), "l");
    assert_eq!(KeyCombo::plain('L').label(), "Shift-l");
    assert_eq!(KeyCombo { ctrl: true, ch: 'l' }.label(), "Ctrl-l");
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
