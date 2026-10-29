//! The two rules this module exists to enforce, driven over the pure
//! projection rather than over the developer's own `~/.kube/config`.
//!
//! Neither was pinned anywhere. The empty `current-context` is the fix for a
//! reported bug — `kubectl config unset current-context` does not remove the
//! key, it writes `current-context: ""`, and kube-rs hands back `Some("")`,
//! which sailed past the absence guard and travelled on as a context named
//! `""`. The only assertion in the workspace was on the PRIMITIVE
//! (`ContextName::new("").is_none()`), not on the `and_then` here that is
//! the line that was missing.

use super::*;

fn named(name: &str, cluster: Option<&str>, user: Option<&str>) -> kube::config::NamedContext {
    kube::config::NamedContext {
        name: name.to_string(),
        context: cluster.map(|c| kube::config::Context {
            cluster: c.to_string(),
            user: user.map(str::to_string),
            ..Default::default()
        }),
    }
}

fn raw(current: Option<&str>, contexts: Vec<kube::config::NamedContext>) -> kube::config::Kubeconfig {
    kube::config::Kubeconfig {
        current_context: current.map(str::to_string),
        contexts,
        ..Default::default()
    }
}

fn project(
    current: Option<&str>,
    contexts: Vec<kube::config::NamedContext>,
) -> Kubeconfig {
    Kubeconfig::project(raw(current, contexts), None).expect("the projection is infallible here")
}

/// `current-context: ""` is ABSENCE, collapsed at the one place a name
/// enters the process.
///
/// This one cannot currently FAIL, and that is worth saying: `ContextName`
/// is total — its only constructor returns `Option` — so `and_then` is the
/// only way the line compiles. It is here as the guard for the day that
/// stops being true (an `unchecked` constructor, a `From<String>`), because
/// the bug it covers was reported from a real kubeconfig and the only
/// assertion in the workspace was on the primitive rather than on this use
/// of it. The two tests below DO discriminate: pointing `is_current` at the
/// raw string, or listing a nameless entry under a stand-in name, each
/// fails exactly one of them.
#[test]
fn an_empty_current_context_is_no_context() {
    let cfg = project(Some(""), vec![named("prod", Some("c"), Some("u"))]);
    assert!(
        cfg.current.is_none(),
        "an empty string travelled on as a context named \"\", and the daemon \
         then failed on a name that cannot exist",
    );
    assert!(
        !cfg.contexts[0].is_current,
        "and nothing is marked current, so the picker opens with no tick",
    );

    // A real name still comes through.
    let named_cfg = project(Some("prod"), vec![named("prod", Some("c"), Some("u"))]);
    assert_eq!(named_cfg.current.as_ref().map(|c| c.as_str()), Some("prod"));
    assert!(named_cfg.contexts[0].is_current);
}

/// A nameless entry is DROPPED, not listed as a blank row the cursor can
/// land on and select.
#[test]
fn a_nameless_context_entry_is_dropped() {
    let cfg = project(
        Some("prod"),
        vec![
            named("", Some("c"), Some("u")),
            named("prod", Some("c"), Some("u")),
        ],
    );
    let names: Vec<&str> = cfg.contexts.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["prod"], "the blank row never reaches the picker");
}

/// `--context` wins over the file's own `current-context`.
#[test]
fn a_requested_context_overrides_the_files_own() {
    let requested = ContextName::new("staging").expect("a real name");
    let cfg = Kubeconfig::project(
        raw(Some("prod"), vec![named("prod", Some("c"), None), named("staging", Some("c2"), None)]),
        Some(&requested),
    )
    .expect("infallible");
    assert_eq!(cfg.current.as_ref().map(|c| c.as_str()), Some("staging"));
    let current: Vec<&str> = cfg
        .contexts
        .iter()
        .filter(|c| c.is_current)
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(current, ["staging"], "exactly one row is ticked, and it is that one");
}

/// A context the file does not name, and one whose entry names no user,
/// both yield an all-unknown identity — which readers destructure rather
/// than printing as a blank.
#[test]
fn an_unknown_context_yields_an_all_unknown_identity() {
    let cfg = project(
        Some("prod"),
        vec![named("prod", Some("eks-prod"), None), named("dev", Some("kind"), Some("me"))],
    );

    let absent = ContextName::new("nowhere").expect("a real name");
    assert_eq!(
        cfg.identity_of(&absent),
        k9rs_proto::protocol::ClusterIdentity::default(),
        "a context this file does not name is all-unknown, not half-filled",
    );

    // Present, but its entry names no user: the CLUSTER still comes through.
    let prod = ContextName::new("prod").expect("a real name");
    let id = cfg.identity_of(&prod);
    assert_eq!(id.cluster(), Some("eks-prod"));
    assert_eq!(
        id.user(),
        k9rs_proto::protocol::ClusterIdentity::default().user(),
        "an absent user reads as unknown, like an absent context's",
    );

    let dev = ContextName::new("dev").expect("a real name");
    assert_eq!(cfg.identity_of(&dev).user(), Some("me"));
}
