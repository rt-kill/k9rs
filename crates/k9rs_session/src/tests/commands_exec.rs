//! `:<name>` for a user-configured exec resource.

use super::*;
use k9rs_kube::local::exec_source::ExecSourceConfig;

fn app_with_exec(names: &[&str]) -> App {
    let mut app = App::new_for_test();
    app.config.daemon = Some(k9rs_kube::daemon_config::DaemonConfig {
        exec_resources: names
            .iter()
            .map(|n| ExecSourceConfig {
                name: (*n).to_string(),
                command: "kubectl".to_string(),
                args: vec![],
                poll_interval_secs: std::num::NonZeroU64::new(30).unwrap(),
                headers: vec!["NAME".to_string()],
                json_field_keys: vec!["name".to_string()],
            })
            .collect(),
        ..Default::default()
    });
    app
}

fn parse_in(app: &App, input: &str) -> ParsedCommand {
    parse_command_input(input, &input.to_lowercase(), app)
}

/// EVERY configured exec resource is reachable by its own name.
///
/// The README documents `name: pod-metrics  # unique name (navigate via
/// :pod-metrics)`. It didn't work: `ResourceId::from_alias` consults the
/// built-in registry and `LocalResourceKind::all()`, and a `Custom` kind is in
/// neither — it is built at runtime from daemon config, and `Custom(_)`
/// has no aliases by construction. So the name fell through to the CRD
/// fallback and the daemon replied "Unknown resource".
///
/// The only way in was `:exec`, which resolves to `exec_configs.first()` — so
/// a SECOND configured source was unreachable by any keystroke at all. That
/// second one is the point of this test.
#[test]
fn every_configured_exec_resource_is_reachable_by_name() {
    use k9rs_proto::local_types::LocalResourceKind;
    let app = app_with_exec(&["node-costs", "gpu-usage"]);

    for name in ["node-costs", "gpu-usage"] {
        let expected = LocalResourceKind::Custom(name.to_string()).to_resource_id();
        assert_eq!(
            parse_in(&app, name),
            ParsedCommand::Resource(expected),
            "`:{name}` must resolve to its own exec source, not a CRD guess",
        );
    }
}

/// Matching is case-insensitive, like every other resource name.
#[test]
fn an_exec_name_matches_regardless_of_case() {
    use k9rs_proto::local_types::LocalResourceKind;
    let app = app_with_exec(&["Node-Costs"]);
    assert_eq!(
        parse_in(&app, "node-costs"),
        ParsedCommand::Resource(
            LocalResourceKind::Custom("Node-Costs".to_string()).to_resource_id()
        ),
        "…and resolves to the CONFIGURED spelling, which is what the daemon keys on",
    );
}

/// A name nobody configured is still a CRD candidate — this must not swallow
/// the fallback that makes `:widgets` work for a real CRD.
#[test]
fn an_unconfigured_name_still_falls_through_to_the_crd_path() {
    let app = app_with_exec(&["node-costs"]);
    assert_eq!(
        parse_in(&app, "widgets"),
        ParsedCommand::CrdCandidate("widgets".to_string()),
    );
    // And with no daemon section at all, nothing changes.
    let bare = App::new_for_test();
    assert_eq!(
        parse_in(&bare, "node-costs"),
        ParsedCommand::CrdCandidate("node-costs".to_string()),
    );
}

/// A built-in alias still wins — an exec resource cannot shadow `:pods`.
#[test]
fn a_builtin_alias_is_not_shadowed_by_an_exec_name() {
    let app = app_with_exec(&["pods"]);
    assert_eq!(
        parse_in(&app, "pods"),
        ParsedCommand::Resource(k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id()),
        "rule 4 (built-in aliases) runs first, so the registry keeps priority",
    );
}
