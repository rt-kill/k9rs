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
