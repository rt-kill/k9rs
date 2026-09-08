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
