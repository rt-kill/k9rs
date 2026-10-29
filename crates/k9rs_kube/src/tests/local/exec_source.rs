//! The exec source's subprocess handling: how much it will buffer, and what
//! it says when the command fails.
//!
//! `Command::output()` buffered the whole of stdout with no bound — a
//! `kubectl get -o json` on a big cluster, or a runaway script, held entire
//! and then copied twice more inside a long-lived daemon shared by every
//! session on the context. The only limit was the poll timeout, which does
//! not bound memory. And stderr went to `/dev/null`, so a non-zero exit was
//! reported as a bare status with nothing saying why.

use super::{capped_output, MAX_EXEC_STDOUT};

fn spawn(sh: &str) -> std::io::Result<tokio::process::Child> {
    tokio::process::Command::new("sh")
        .arg("-c")
        .arg(sh)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
}

/// Ordinary output comes back whole, with its exit status.
#[tokio::test]
async fn output_under_the_cap_arrives_entire() {
    let out = capped_output(spawn("printf '[{\"a\":1}]'")).await.expect("ran");
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout), "[{\"a\":1}]");
}

/// Past the cap it is a FAILED poll, not a truncated parse — half a JSON
/// document is a parse error, not a shorter table, so saying so is the
/// honest outcome and the last-known rows are kept.
#[tokio::test]
async fn output_over_the_cap_fails_the_poll() {
    // `yes` emits until it is stopped: this is the runaway shape, and the
    // test also checks the child does not keep the test alive.
    let err = capped_output(spawn("yes 0123456789012345678901234567890123456789"))
        .await
        .expect_err("a runaway command is refused, not buffered");
    let msg = err.to_string();
    assert!(
        msg.contains("more than") && msg.contains(&MAX_EXEC_STDOUT.to_string()),
        "the refusal says what the limit was: {msg}",
    );
}

/// stderr is captured, so a non-zero exit can say why.
#[tokio::test]
async fn a_failing_command_brings_its_stderr_back() {
    let out = capped_output(spawn("echo 'no such context' >&2; exit 3"))
        .await
        .expect("ran, and failed on its own terms");
    assert!(!out.status.success(), "it exited non-zero");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no such context"),
        "and its reason came back rather than going to /dev/null",
    );
}

// ---------------------------------------------------------------------------
// One validation, for both readers of the config
// ---------------------------------------------------------------------------

use crate::local::exec_source::ExecSourceConfig;

fn config(name: &str) -> ExecSourceConfig {
    ExecSourceConfig {
        name: name.to_string(),
        command: "kubectl".to_string(),
        args: vec!["get".to_string(), "nodes".to_string()],
        poll_interval_secs: std::num::NonZeroU64::new(30).expect("non-zero"),
        headers: vec!["NAME".to_string()],
        json_field_keys: vec![".metadata.name".to_string()],
    }
}

/// A name that is already a built-in alias is REFUSED, loudly.
///
/// `:`-command resolution tries `ResourceId::from_alias` first and
/// exec-by-name fourth, so an entry named `pods` could never be reached:
/// its poll never started and nothing said why. The collision LOSES rather
/// than shadowing, which is exactly what made it invisible.
#[test]
fn a_name_that_is_a_builtin_alias_is_refused() {
    for taken in ["pods", "po", "deploy", "svc"] {
        let err = config(taken)
            .validate()
            .expect_err("a built-in alias cannot name an exec resource");
        assert!(err.contains(taken), "the refusal names it: {err}");
        assert!(
            err.contains("never reach"),
            "…and says what would have happened: {err}",
        );
    }
}

/// A name of its own is fine, including one that merely LOOKS like a
/// resource — the guard asks the alias table, not a heuristic.
#[test]
fn an_unclaimed_name_is_accepted() {
    for free in ["node-costs", "podsx", "my-pods", "costs"] {
        assert!(config(free).validate().is_ok(), "{free} is not taken");
    }
}

/// The four shape guards, each naming the entry it rejected.
#[test]
fn a_malformed_entry_says_which_and_why() {
    let mut c = config("costs");
    c.name = String::new();
    assert!(c.validate().unwrap_err().contains("empty name"));

    let mut c = config("costs");
    c.command = String::new();
    assert!(c.validate().unwrap_err().contains("empty command"));

    let mut c = config("costs");
    c.headers = Vec::new();
    assert!(c.validate().unwrap_err().contains("no headers"));

    let mut c = config("costs");
    c.json_field_keys.push(".extra".to_string());
    let err = c.validate().unwrap_err();
    assert!(err.contains("1 headers but 2 jsonFieldKeys"), "{err}");
}

/// `usable` drops the invalid and the duplicates, first-wins — the only
/// order a reader could predict.
#[test]
fn usable_keeps_the_first_of_each_name_and_drops_the_rest() {
    let mut second = config("costs");
    second.command = "curl".to_string();
    let kept = ExecSourceConfig::usable(vec![
        config("costs"),
        second,
        config("pods"),   // a built-in alias
        config("other"),
    ]);
    assert_eq!(
        kept.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["costs", "other"],
    );
    assert_eq!(kept[0].command, "kubectl", "first wins");
}
