//! The crate root's own decisions.
//!
//! `lib.rs` is the largest module in the crate and had no mirror: 22 of its
//! 23 siblings have one, and it pulled in only `tests/theme_integration.rs`.
//! What that left untested was a set of PURE functions over `&CoreData` and
//! over slices — command completion, the pinned-tab cycle, the context
//! projection, the resource-command list — every one of which is a
//! user-visible answer and none of which needs an App, a session or a
//! daemon.

use super::*;

// ---------------------------------------------------------------------------
// The pinned-tab cycle
// ---------------------------------------------------------------------------

/// Tab and Shift-Tab walk the pinned list in opposite directions and wrap,
/// from one function.
///
/// They were two near-identical bodies, each with its own `Pod` fallback and
/// its own wrap arithmetic, and both took `&mut self` for a question that
/// mutates nothing.
#[test]
fn the_tab_cycle_walks_both_ways_and_wraps() {
    let app = App::new_for_test();
    let pinned = app.pinned_resources.clone();
    assert!(pinned.len() > 2, "the fixture needs a few pins: {}", pinned.len());

    // `new_for_test` roots at pods, which is the first pin.
    assert_eq!(app.nav.resource_id(), Some(&pinned[0]));
    assert_eq!(app.next_tab(), pinned[1], "forward");
    assert_eq!(
        app.prev_tab(),
        *pinned.last().expect("non-empty"),
        "backward from the first wraps to the last",
    );

    // A step of the list's length is a full lap.
    assert_eq!(app.tab(pinned.len() as isize), pinned[0], "a full lap returns");
    assert_eq!(app.tab(-(pinned.len() as isize)), pinned[0]);
}

/// With nothing pinned, both directions answer pods rather than panicking on
/// an empty slice — the fallback that used to be written twice.
#[test]
fn an_empty_pin_list_falls_back_to_pods() {
    let mut app = App::new_for_test();
    app.pinned_resources.clear();
    let pods = k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id();
    assert_eq!(app.next_tab(), pods);
    assert_eq!(app.prev_tab(), pods);
    assert_eq!(app.tab(7), pods);
}

// ---------------------------------------------------------------------------
// What the `:` prompt completes
// ---------------------------------------------------------------------------

/// Every resource alias the registry declares is completable, and the list
/// has no duplicates — a hand-appended `["alias", "aliases", "a"]` was
/// invisible here only because of the `dedup`.
#[test]
fn resource_commands_are_the_registry_aliases_deduped() {
    let cmds = super::resource_commands();
    assert!(!cmds.is_empty());
    let mut sorted = cmds.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(cmds, sorted, "sorted and deduped, which the completer relies on");

    // A built-in and a local kind, from the two sources the list unions.
    assert!(cmds.contains(&"pods"), "{cmds:?}");
    assert!(cmds.contains(&"aliases"), "the alias view is a LocalResourceKind now");
}

/// A bare prefix completes to the resource commands that start with it.
#[test]
fn a_bare_prefix_completes_to_matching_resources() {
    let core = CoreData::default();
    let candidates = super::complete_command("dep", &core);
    assert!(
        candidates.iter().any(|c| c.starts_with("deploy")),
        "deploy* must be offered for `dep`: {candidates:?}",
    );
    assert!(
        candidates.iter().all(|c| c.starts_with("dep")),
        "and nothing else is: {candidates:?}",
    );
}

/// An input nothing matches completes to nothing — not to everything.
#[test]
fn an_unmatched_prefix_completes_to_nothing() {
    let core = CoreData::default();
    assert!(super::complete_command("zzzznope", &core).is_empty());
}

/// Completion is case-insensitive on the way in; the candidates are what the
/// user must type.
#[test]
fn completion_is_case_insensitive() {
    let core = CoreData::default();
    let upper = super::complete_command("POD", &core);
    let lower = super::complete_command("pod", &core);
    assert_eq!(upper, lower, "`POD` and `pod` offer the same thing");
    assert!(!upper.is_empty());
}

// ---------------------------------------------------------------------------
// The contexts projection
// ---------------------------------------------------------------------------

fn ctx(name: &str, cluster: &str, user: &str, current: bool) -> crate::KubeContext {
    crate::KubeContext {
        name: k9rs_proto::protocol::ContextName::new(name).expect("non-empty"),
        identity: k9rs_proto::protocol::ClusterIdentity::new(
            cluster.to_string(),
            user.to_string(),
        ),
        is_current: current,
    }
}

/// A context row carries its switch as a `DrillTarget`, so Enter needs no
/// special case in the key handler.
#[test]
fn a_context_row_drills_to_its_own_switch() {
    let rows = context_rows(&[ctx("prod", "c1", "u1", true)]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "prod");
    assert_eq!(rows[0].namespace, None, "a context is not in a namespace");
    let Some(k9rs_proto::row::DrillTarget::SwitchContext(ref name)) = rows[0].drill_target else {
        panic!("expected a SwitchContext drill: {:?}", rows[0].drill_target)
    };
    assert_eq!(name.as_str(), "prod");
}

/// A context whose kubeconfig has no `user:` renders the SAME word the
/// header does. These were raw strings once, so the cell was blank while the
/// header said `n/a` for the very same field.
#[test]
fn an_unknown_context_field_renders_the_headers_word() {
    let rows = context_rows(&[ctx("prod", "", "", false)]);
    let cells = &rows[0].cells;
    assert!(
        matches!(cells[1], k9rs_proto::row::CellValue::Placeholder),
        "cluster: {:?}",
        cells[1],
    );
    assert!(
        matches!(cells[2], k9rs_proto::row::CellValue::Placeholder),
        "user: {:?}",
        cells[2],
    );
}

/// The current context is marked, and only it.
#[test]
fn only_the_current_context_is_ticked() {
    let rows = context_rows(&[
        ctx("a", "c", "u", false),
        ctx("b", "c", "u", true),
        ctx("c", "c", "u", false),
    ]);
    let ticks: Vec<&str> = rows
        .iter()
        .map(|r| match &r.cells[3] {
            k9rs_proto::row::CellValue::Text(t) => t.as_str(),
            other => panic!("expected text: {other:?}"),
        })
        .collect();
    assert_eq!(ticks, vec!["", "\u{2713}", ""]);
}

// ---------------------------------------------------------------------------
// The alias view and the configured exec resources
//
// `:exec` used to be a `LocalResourceKind` of its own, and the daemon
// answered it with `exec_configs.first()` — so with two sources configured
// the second was unreachable by any keystroke, and the first was shown under
// the label `EXEC`. The variant is gone (wire v19); each source answers to
// its own name, which is what the README always promised.
//
// Deleting it only helps if the names are discoverable, and they were not:
// `seed_aliases` skipped them, saying the list "comes from the DAEMON's
// config and the client never sees it" — while `exec_resource_by_name`
// resolves `:node-costs` by reading exactly that list.
// ---------------------------------------------------------------------------

fn exec_cfg(name: &str, headers: &[&str], keys: &[&str]) -> k9rs_kube::local::exec_source::ExecSourceConfig {
    k9rs_kube::local::exec_source::ExecSourceConfig {
        name: name.to_string(),
        command: "kubectl".to_string(),
        args: vec![],
        poll_interval_secs: std::num::NonZeroU64::new(30).expect("30 is nonzero"),
        headers: headers.iter().map(|h| (*h).to_string()).collect(),
        json_field_keys: keys.iter().map(|k| (*k).to_string()).collect(),
    }
}

fn alias_rows(config: &crate::config::AppConfig) -> Vec<Vec<String>> {
    let store = crate::store::RowStore::client("aliases");
    crate::seed_aliases(&store, config);
    store.with_read(|i| {
        i.rows
            .iter()
            .map(|r| {
                r.cells
                    .iter()
                    .map(|c| match c {
                        k9rs_proto::row::CellValue::Text(t) => t.clone(),
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .collect()
    })
}

/// Every exec source `:` accepts is listed, under its own name — and the
/// generic word that used to stand in for all of them is not.
#[test]
fn the_alias_view_lists_each_configured_exec_source() {
    let config = crate::config::AppConfig {
        daemon: Some(k9rs_kube::daemon_config::DaemonConfig {
            exec_resources: vec![
                exec_cfg("node-costs", &["NAME"], &["name"]),
                exec_cfg("gpu-usage", &["NAME"], &["name"]),
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let rows = alias_rows(&config);
    let aliases: Vec<&String> = rows.iter().map(|r| &r[1]).collect();

    for name in ["node-costs", "gpu-usage"] {
        assert!(
            aliases.iter().any(|a| *a == name),
            "`:{name}` works, so it must be listed; got {aliases:?}",
        );
    }
    assert!(
        !aliases.iter().any(|a| a.split(" / ").any(|w| w == "exec")),
        "`:exec` resolves to nothing now — advertising it would promise a \
         view the daemon answers 'Unknown local resource' for",
    );
}

/// An entry the daemon would skip is not advertised either: the view lists
/// what the prompt accepts, and `exec_resource_by_name` applies the same
/// `validate`.
#[test]
fn the_alias_view_skips_an_exec_source_the_daemon_would_reject() {
    let config = crate::config::AppConfig {
        daemon: Some(k9rs_kube::daemon_config::DaemonConfig {
            exec_resources: vec![
                // Headers and keys of different lengths — the converter zips
                // them positionally, so this one never serves rows.
                exec_cfg("mismatched", &["NAME", "COST"], &["name"]),
                // A name that is already a built-in alias: `:pods` could never
                // reach it.
                exec_cfg("pods", &["NAME"], &["name"]),
                exec_cfg("good", &["NAME"], &["name"]),
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let rows = alias_rows(&config);
    let exec_rows: Vec<&String> = rows
        .iter()
        .filter(|r| r[2] == "ExecResource")
        .map(|r| &r[1])
        .collect();
    assert_eq!(
        exec_rows,
        vec!["good"],
        "only the usable one is typeable, so only it is listed",
    );
}

/// With no `daemon:` section the view is exactly what it was: the registry
/// and the command words.
#[test]
fn an_exec_free_config_adds_no_exec_rows() {
    let rows = alias_rows(&crate::config::AppConfig::default());
    assert!(!rows.is_empty(), "the view has content without any config");
    assert!(
        !rows.iter().any(|r| r[2] == "ExecResource"),
        "nothing configured, nothing advertised",
    );
}
