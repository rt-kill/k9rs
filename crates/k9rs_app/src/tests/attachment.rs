//! The name and the identity describe the same context, in every state.

use crate::event::{apply_event, AppEvent};
use crate::{App, Attachment, ContextSwitchState};
use k9rs_proto::protocol::{ClusterIdentity, ContextName};

fn ctx(name: &str) -> ContextName {
    ContextName::new(name).expect("non-empty")
}

/// Settle on `prod`, confirmed by a handshake.
fn on_prod() -> App {
    let mut app = App::new_for_test();
    apply_event(
        &mut app,
        AppEvent::ConnectionEstablished {
            context: ctx("prod"),
            identity: ClusterIdentity::new("prod-eks".into(), "prod-admin".into()),
            namespaces: vec![],
        },
    );
    app
}

/// THE regression. A switch request names the target immediately; the new
/// session's `KubeconfigLoaded` arrives later, after a disk read and the
/// start of a handshake. In between, the header used to read
/// `staging (connecting…)` above `prod-eks / prod-admin` — the name of the
/// context we were going to over the identity of the one we were leaving.
///
/// It was two functions in the renderer: `context_label` matched on
/// `(switch target, connecting, context)` and `display_identity` on
/// `(connecting, identity)`, so only one of them knew a switch target
/// existed. One value from one arm is what makes the pairing unwritable.
#[test]
fn a_switch_target_is_never_shown_with_the_leaving_context_identity() {
    let mut app = on_prod();
    // Exactly what `begin_context_switch` does to this state.
    app.conn.switch_requested();
    app.kube.context_switch = ContextSwitchState::Requested(ctx("staging"));

    let at = app.attachment();
    assert_eq!(at.context(), Some(&ctx("staging")), "we name the target");
    assert_eq!(
        at.identity(),
        ClusterIdentity::default(),
        "…and admit we do not know its cluster yet, rather than showing prod's: {at:?}",
    );
}

/// The same hole one switch further along: A → B → C, where `connecting`
/// still holds B's entry when C becomes the target. A `filter` on the
/// candidate's NAME is what rejects it; without one, the identity is simply
/// "whatever candidate was last read".
#[test]
fn a_stale_candidate_is_not_borrowed_for_a_newer_target() {
    let mut app = on_prod();
    apply_event(
        &mut app,
        AppEvent::KubeconfigLoaded {
            contexts: vec![],
            current_context: Some(ctx("staging")),
            current_identity: ClusterIdentity::new("staging-eks".into(), "staging-admin".into()),
        },
    );
    // Now switch again, before any read for `canary` has reported.
    app.conn.switch_requested();
    app.kube.context_switch = ContextSwitchState::Requested(ctx("canary"));

    let at = app.attachment();
    assert_eq!(at.context(), Some(&ctx("canary")));
    assert_eq!(
        at.identity().cluster(),
        None,
        "staging's cluster must not ride under canary's name: {at:?}",
    );
}

/// The candidate IS the target: now its identity is the right one to show,
/// and the pairing is honest rather than empty.
#[test]
fn a_matching_candidate_supplies_the_identity() {
    let mut app = on_prod();
    app.conn.switch_requested();
    app.kube.context_switch = ContextSwitchState::Requested(ctx("staging"));
    apply_event(
        &mut app,
        AppEvent::KubeconfigLoaded {
            contexts: vec![],
            current_context: Some(ctx("staging")),
            current_identity: ClusterIdentity::new("staging-eks".into(), "staging-admin".into()),
        },
    );

    let at = app.attachment();
    assert_eq!(at.context(), Some(&ctx("staging")));
    assert_eq!(at.identity().cluster(), Some("staging-eks"));
    assert_eq!(at.identity().user(), Some("staging-admin"));
    assert!(
        matches!(at, Attachment::Connecting(_)),
        "a candidate is not a handshake: {at:?}",
    );
}

/// A plain reconnect — no switch — pairs the last-confirmed context with the
/// last-confirmed identity. Same context, so they agree.
#[test]
fn a_reconnect_keeps_describing_the_context_it_is_returning_to() {
    let mut app = on_prod();
    apply_event(&mut app, AppEvent::DaemonDisconnected);

    let at = app.attachment();
    assert_eq!(at.context(), Some(&ctx("prod")));
    assert_eq!(at.identity().cluster(), Some("prod-eks"));
    assert!(matches!(at, Attachment::Connecting(_)), "not confirmed: {at:?}");
}

/// Confirmed means confirmed: the daemon's identity, which is the only one
/// carrying a server version.
#[test]
fn a_live_link_reports_what_the_daemon_confirmed() {
    let at = on_prod().attachment();
    assert!(matches!(at, Attachment::Live(_)), "the daemon confirmed: {at:?}");
    assert_eq!(at.context(), Some(&ctx("prod")));
    assert_eq!(at.identity().cluster(), Some("prod-eks"));
}

/// No current-context is its own answer, not a degenerate "connecting".
#[test]
fn no_context_names_nothing_at_all() {
    let mut app = App::new_for_test();
    apply_event(&mut app, AppEvent::NoContextConfigured);
    let at = app.attachment();
    assert_eq!(at, Attachment::NoContext);
    assert_eq!(at.context(), None);
    assert_eq!(at.identity(), ClusterIdentity::default());
}

/// A candidate read from disk does not move the attachment while the link is
/// LIVE. `KubeconfigLoaded` does not lower the link — only
/// `switch_requested`/`disconnected` do — so on a live link the confirmed
/// context wins, which is right: a file on disk changing does not detach you
/// from the cluster you are talking to.
#[test]
fn a_candidate_does_not_displace_a_confirmed_context() {
    let mut app = on_prod();
    apply_event(
        &mut app,
        AppEvent::KubeconfigLoaded {
            contexts: vec![],
            current_context: Some(ctx("staging")),
            current_identity: ClusterIdentity::new("staging-eks".into(), "staging-admin".into()),
        },
    );
    let at = app.attachment();
    assert!(matches!(at, Attachment::Live(_)), "still confirmed: {at:?}");
    assert_eq!(at.context(), Some(&ctx("prod")), "still attached to prod: {at:?}");
    assert_eq!(at.identity().cluster(), Some("prod-eks"));
}

/// `reconnect_target` answers a DIFFERENT question — what to connect TO, not
/// what to say — and the divergence is the switch target, which outranks the
/// last-confirmed context so a mid-switch daemon blip cannot strand the user
/// on the context they asked to leave.
#[test]
fn the_reconnect_target_prefers_an_in_flight_switch() {
    let mut app = on_prod();
    assert_eq!(app.kube.reconnect_target(), Some(ctx("prod")));

    app.conn.switch_requested();
    app.kube.context_switch = ContextSwitchState::Requested(ctx("canary"));
    assert_eq!(app.kube.reconnect_target(), Some(ctx("canary")));
    // And the chrome agrees about the NAME — it is only the identity the two
    // questions were ever allowed to differ about, and now neither invents one.
    assert_eq!(app.attachment().context(), Some(&ctx("canary")));
}
