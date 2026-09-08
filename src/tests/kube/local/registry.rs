use super::*;
use crate::kube::protocol::ContextName;

fn cfg(name: &str, headers: &[&str], keys: &[&str]) -> ExecSourceConfig {
    ExecSourceConfig {
        name: name.to_string(),
        command: "echo".to_string(),
        args: vec![],
        poll_interval_secs: 30,
        headers: headers.iter().map(|s| s.to_string()).collect(),
        json_field_keys: keys.iter().map(|s| s.to_string()).collect(),
    }
}

fn cid(n: &str) -> ContextId {
    ContextId::new(ContextName::new(n.to_string()).unwrap(), format!("https://{n}"), 1)
}

/// `LocalRegistry::new` keeps only well-formed exec configs: it drops ones
/// with a header/key length mismatch (would misalign columns), an empty
/// command or headers, a duplicate name, or an empty name.
#[test]
fn new_filters_malformed_exec_configs() {
    let mut nocmd = cfg("nocmd", &["A"], &["a"]);
    nocmd.command = String::new();
    let configs = vec![
        cfg("ok", &["A", "B"], &["a", "b"]),            // valid — kept
        cfg("mismatch", &["A", "B", "C"], &["a", "b"]), // 3 vs 2 → dropped
        cfg("ok", &["A"], &["a"]),                      // duplicate name → dropped
        cfg("", &["A"], &["a"]),                        // empty name → dropped
        nocmd,                                          // empty command → dropped
        cfg("noheaders", &[], &[]),                     // empty headers → dropped
    ];
    let reg = LocalRegistry::new(configs, None);
    assert_eq!(reg.exec_configs.len(), 1);
    assert_eq!(reg.exec_configs[0].name, "ok");
    assert_eq!(reg.exec_configs[0].headers, vec!["A", "B"]);
}

/// Two attaches to the same context share ONE slice (and its
/// port-forward source); a different context gets its own.
#[tokio::test]
async fn attach_shares_the_slice_per_context() {
    let reg = LocalRegistry::new(vec![], Some(Duration::from_secs(60)));
    let a = reg.attach(&cid("one"));
    let b = reg.attach(&cid("one"));
    let other = reg.attach(&cid("two"));
    assert!(Arc::ptr_eq(&a.0, &b.0), "same context = same slice");
    assert!(!Arc::ptr_eq(&a.0, &other.0), "different context = different slice");
}

/// After the last keepalive drops with no grace, a re-attach builds a
/// FRESH slice (the old one is gone, its entry swept).
#[tokio::test]
async fn reattach_after_teardown_builds_fresh_slice() {
    let reg = LocalRegistry::new(vec![], None); // no grace: dies on drop
    let a = reg.attach(&cid("one"));
    let weak = Arc::downgrade(&a.0);
    drop(a);
    assert!(weak.upgrade().is_none(), "slice tore down with its keepalive");
    let b = reg.attach(&cid("one"));
    assert!(weak.upgrade().is_none() && Arc::strong_count(&b.0) >= 1);
}
