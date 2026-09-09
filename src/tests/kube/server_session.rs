use super::{parse_k8s_minor, split_all_containers_prefix};

#[test]
fn parse_standard_versions() {
    assert_eq!(parse_k8s_minor("v1.32.1"), Some(32));
    assert_eq!(parse_k8s_minor("v1.30.0"), Some(30));
    assert_eq!(parse_k8s_minor("v1.28.11"), Some(28));
}

#[test]
fn parse_eks_version() {
    assert_eq!(parse_k8s_minor("v1.30.14-eks-40737a8"), Some(30));
}

#[test]
fn parse_no_prefix() {
    assert_eq!(parse_k8s_minor("1.32.0"), Some(32));
}

#[test]
fn parse_edge_cases() {
    assert_eq!(parse_k8s_minor("v1"), None);
    assert_eq!(parse_k8s_minor(""), None);
    assert_eq!(parse_k8s_minor("garbage"), None);
    assert_eq!(parse_k8s_minor("v1.abc.3"), None);
}

#[test]
fn all_containers_prefix_pod_slash_container() {
    assert_eq!(split_all_containers_prefix("[mypod/web] hello"), Some(("web", "hello")));
}

#[test]
fn all_containers_prefix_bare_container() {
    // Single-pod streams may emit just `[container]`; rsplit still works.
    assert_eq!(split_all_containers_prefix("[web] hello"), Some(("web", "hello")));
}

#[test]
fn all_containers_prefix_multi_segment_source() {
    // Any `/`-separated source resolves to its final (container) segment.
    assert_eq!(split_all_containers_prefix("[ns/mypod/web] hi"), Some(("web", "hi")));
}

#[test]
fn all_containers_prefix_preserves_bracketed_body() {
    // kubectl prepends its prefix once; the container's own `[INFO]` text
    // follows untouched (we split on the FIRST `] `, which is the prefix).
    assert_eq!(
        split_all_containers_prefix("[mypod/web] [INFO] up] done"),
        Some(("web", "[INFO] up] done")),
    );
}

#[test]
fn all_containers_prefix_unprefixed_is_none() {
    // A line without kubectl's prefix rides untagged rather than guessing.
    assert_eq!(split_all_containers_prefix("plain log line"), None);
    assert_eq!(split_all_containers_prefix("[no-close-bracket hi"), None);
}

// ---------------------------------------------------------------------------
// Auth failures must be readable, not an environment dump
// ---------------------------------------------------------------------------

/// kube-rs renders a failed credential plugin as
/// `auth exec command '{cmd}' failed with status {status}: {out:?}`, where
/// `cmd` carries the whole environment — PATH, HOME, AWS_PROFILE, every nix
/// store path. The user saw several thousand characters wrapping their
/// terminal, of which one line mattered.
#[test]
fn an_exec_auth_failure_reports_the_plugin_not_the_environment() {
    use std::os::unix::process::ExitStatusExt;
    let err = kube::Error::Auth(kube::client::AuthError::AuthExecRun {
        cmd: "AWS_PROFILE=\"CrawlingEngAdmin\" HOME=\"/home/u\" \
              PATH=\"/nix/store/aaaa/bin:/nix/store/bbbb/bin\" \
              \"aws\" \"--region\" \"us-west-2\" \"eks\" \"get-token\""
            .to_string(),
        status: std::process::ExitStatus::from_raw(65280),
        out: std::process::Output {
            status: std::process::ExitStatus::from_raw(65280),
            stdout: Vec::new(),
            stderr: b"\nError loading SSO Token: Token for production does not exist\n".to_vec(),
        },
    });

    let msg = super::client_build::summarize_client_error(err).to_string();

    assert!(msg.contains("Error loading SSO Token"), "keeps the useful line: {msg}");
    assert!(msg.contains("aws"), "names the plugin that failed: {msg}");
    assert!(msg.contains("aws sso login"), "says what to do: {msg}");
    assert!(!msg.contains("/nix/store"), "no environment dump: {msg}");
    assert!(!msg.contains("PATH"), "no environment dump: {msg}");
    assert!(!msg.contains("Output {"), "no raw Debug struct: {msg}");
    assert!(msg.len() < 200, "one readable line, got {} chars: {msg}", msg.len());
}

/// Anything we don't recognise passes through untouched — a wrong guess is
/// worse than none.
#[test]
fn a_non_auth_error_is_not_rewritten() {
    let err = kube::Error::LinesCodecMaxLineLengthExceeded;
    let original = err.to_string();
    assert_eq!(super::client_build::summarize_client_error(err).to_string(), original);
}

/// A subscription/log bridge must notice its client going away.
///
/// From the 2026-09-09 audit: these bridges are write-only after the init
/// frame, so a client-side close was only ever observed via a FAILING WRITE
/// — which on a quiet resource never comes. The bridge parked forever
/// holding its `Subscription`, pinning the watcher, its apiserver watch and
/// its full row store for the life of the session and bypassing the 60s
/// grace. This pins the primitive that fixes it: the peer-close watch must
/// resolve on EOF, and must NOT resolve while the peer is merely idle.
#[tokio::test]
async fn a_closed_peer_half_is_observable() {
    use tokio::io::AsyncWriteExt;

    // Peer still attached but silent: the bridge must keep running.
    let (mut peer, daemon) = tokio::io::duplex(64);
    let mut daemon = daemon;
    let idle = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        super::peer_closed(&mut daemon),
    )
    .await;
    assert!(idle.is_err(), "an idle client must not look like a departed one");

    // Peer drops: resolves promptly.
    peer.shutdown().await.unwrap();
    drop(peer);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        super::peer_closed(&mut daemon),
    )
    .await
    .expect("a closed peer must be observed, or the watcher leaks for the session");
}
