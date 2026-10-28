//! The CLI's parsing and its one formatter.
//!
//! This crate had NO tests. Both things below are the kind that fail
//! silently: an output format that was a `String` compared against `"json"`,
//! so a typo printed human output and exited 0; and a duration formatter
//! that looks enough like `k9rs_util::units::format_age_secs` to invite
//! someone to "unify" them.

use super::*;
use clap::Parser;

/// A minimal parser over [`Command`], because the real `Cli` lives in the
/// BIN crate and this one cannot see it. `Command` is the `Subcommand`, so
/// this exercises exactly the argument definitions under test.
#[derive(Parser, Debug)]
struct TestCli {
    #[command(subcommand)]
    command: Command,
}

/// `--output` admits exactly the formats that exist, and refuses the rest.
///
/// It was `output: String` with `default_value = "human"` and a
/// `output == "json"` comparison at the use site. `k9rs ctl --output jsom
/// status` therefore parsed, compared false, printed human output and
/// returned 0 — a typo in a script that looked like success. A typed
/// `ValueEnum` makes clap answer instead.
#[test]
fn the_output_format_refuses_what_it_does_not_understand() {
    let ok = TestCli::try_parse_from(["k9rs", "ctl", "--output", "json", "status"])
        .expect("json is a format");
    match ok.command {
        Command::Ctl { output, .. } => assert_eq!(output, OutputFormat::Json),
        other => panic!("expected ctl, got {other:?}"),
    }

    let human = TestCli::try_parse_from(["k9rs", "ctl", "status"]).expect("defaults");
    match human.command {
        Command::Ctl { output, .. } => {
            assert_eq!(output, OutputFormat::Human, "human is the default")
        }
        other => panic!("expected ctl, got {other:?}"),
    }

    // The typo. This is the whole point.
    let err = TestCli::try_parse_from(["k9rs", "ctl", "--output", "jsom", "status"])
        .expect_err("`jsom` is not a format");
    let msg = err.to_string();
    assert!(msg.contains("jsom"), "the refusal names what was asked for: {msg}");
    assert!(
        msg.contains("human") && msg.contains("json"),
        "...and what it could have been: {msg}",
    );
}

/// `ctl`'s duration format is SPACED and keeps its middle unit.
///
/// It looks enough like `k9rs_util::units::format_age_secs` to invite
/// someone to merge them, and they are not the same: that one has to fit a
/// table column, so it writes `1d1h` and drops the middle unit; this reads
/// as a line of prose in a terminal, so it writes `1d 1h 1m`. Pinned in this
/// shape so "these are duplicates" is a question with an answer rather than
/// a plausible-looking refactor. (`k9rs_util` is not a dependency of this
/// crate, which is itself part of the answer.)
#[test]
fn the_ctl_duration_format_keeps_its_spaces_and_its_middle_unit() {
    let cases = [
        (0u64, "0s"),
        (45, "45s"),
        (90, "1m 30s"),
        (3661, "1h 1m 1s"),
        (90_061, "1d 1h 1m"),
    ];
    for (secs, want) in cases {
        assert_eq!(crate::ctl::format_duration(secs), want, "{secs}s");
    }
}
