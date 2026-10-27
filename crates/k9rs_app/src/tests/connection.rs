//! The link lifecycle: every edge, including the ones whose absence
//! shipped a context-switch lock and stale rows behind a connecting
//! overlay.

use super::*;

mod connection {
    use super::{Connection, LinkState};
    use std::time::{Duration, Instant};

    #[test]
    fn starts_connecting_and_never_connected() {
        let conn = Connection::new();
        assert!(conn.link() == LinkState::Connecting, "initial handshake hasn't landed");
        assert!(!conn.has_ever_connected(), "initial failure must read as fatal");
        assert!(!conn.rebuild_due(Instant::now()), "nothing to rebuild yet");
    }

    #[test]
    fn establish_goes_live_and_arms_the_fatality_rule() {
        let mut conn = Connection::new();
        conn.established();
        assert!(conn.link() != LinkState::Connecting);
        assert!(conn.has_ever_connected());
    }

    #[test]
    fn disconnect_shows_connecting_now_and_schedules_a_rebuild() {
        let mut conn = Connection::new();
        conn.established();
        conn.disconnected();
        assert!(conn.link() == LinkState::Connecting, "stale rows must gate THIS frame");
        assert!(conn.rebuild_due(Instant::now()));
    }

    #[test]
    fn begin_rebuild_consumes_the_plan_but_stays_connecting() {
        // THE mid-gap invariant: the plan is consumed at the START of the
        // rebuild, but the screen must not flicker back to stale rows
        // before the new connection is up — liveness is stored, not
        // derived from the plan.
        let mut conn = Connection::new();
        conn.established();
        conn.disconnected();
        conn.begin_rebuild();
        assert!(!conn.rebuild_due(Instant::now()), "plan consumed");
        assert!(conn.link() == LinkState::Connecting, "still gating until established()");
    }

    #[test]
    fn backoff_doubles_clamps_and_resets_on_establish() {
        let mut conn = Connection::new();
        conn.established();
        let t0 = Instant::now();

        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0), "backed off — not due immediately");
        assert!(conn.rebuild_due(t0 + Duration::from_millis(500)));

        // Each failure doubles the NEXT delay: 500ms, 1s, 2s, 4s, 5s cap.
        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0 + Duration::from_millis(999)));
        assert!(conn.rebuild_due(t0 + Duration::from_millis(1000)));
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_secs(2)));
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_secs(4)));
        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0 + Duration::from_secs(4)), "clamped at 5s");
        assert!(conn.rebuild_due(t0 + Duration::from_secs(5)));

        // Success resets the ladder.
        conn.established();
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_millis(500)));
    }

    #[test]
    fn switch_failure_falls_back_without_backoff() {
        // The daemon is UP (the switch TARGET was unreachable) — the
        // fallback reconnect to the previous context runs immediately.
        let mut conn = Connection::new();
        conn.established();
        conn.switch_failed_fallback();
        assert!(conn.rebuild_due(Instant::now()));
    }

    #[test]
    fn switch_request_gates_the_frame_without_a_plan() {
        let mut conn = Connection::new();
        conn.established();
        conn.switch_requested();
        assert!(conn.link() == LinkState::Connecting, "connecting screen this frame");
        assert!(
            !conn.rebuild_due(Instant::now()),
            "the SWITCH drives the rebuild, not the reconnect plan"
        );
    }
}

// ---------------------------------------------------------------------------
// NoContext — "nothing to connect to" is its own state, not a slow connect
// ---------------------------------------------------------------------------

mod no_context {
    use super::{Connection, LinkState};
    use std::time::Instant;

    #[test]
    fn no_context_is_terminal_until_the_user_picks_one() {
        // Saying "connecting…" forever would be the same lie the connecting
        // screen was invented to stop telling — nothing is being attempted.
        let mut conn = Connection::new();
        conn.no_context();
        assert_eq!(conn.link(), LinkState::NoContext);
        assert!(
            !conn.rebuild_due(Instant::now()),
            "a retry with no target to retry against is just a loop",
        );
    }

    #[test]
    fn picking_a_context_is_the_way_out() {
        let mut conn = Connection::new();
        conn.no_context();
        conn.switch_requested();
        assert_eq!(conn.link(), LinkState::Connecting, "now something IS being attempted");
    }

    #[test]
    fn a_dead_daemon_still_backs_off_from_no_context() {
        // The escape hatch must not disarm the normal machinery: once a
        // switch is under way, an ordinary failure has to back off as usual.
        let mut conn = Connection::new();
        conn.no_context();
        conn.switch_requested();
        conn.reconnect_failed_backoff(Instant::now());
        assert!(!conn.rebuild_due(Instant::now()), "backoff deadline not reached yet");
    }
}

// ---------------------------------------------------------------------------
// context_switch_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------

    use k9rs_proto::protocol::ContextName;

    #[test]
    fn stable_accepts_new_switches() {
        let s = ContextSwitchState::Stable;
        assert!(s.is_stable());
    }

    #[test]
    fn requested_rejects_new_switches() {
        let s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
    }

    #[test]
    fn in_flight_rejects_new_switches() {
        let s = ContextSwitchState::InFlight(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
    }

    #[test]
    fn target_names_the_context_in_flight() {
        assert_eq!(ContextSwitchState::Stable.target(), None);
        assert_eq!(
            ContextSwitchState::Requested(ContextName::new("prod").unwrap()).target().map(|c| c.as_str()),
            Some("prod")
        );
        assert_eq!(
            ContextSwitchState::InFlight(ContextName::new("prod").unwrap()).target().map(|c| c.as_str()),
            Some("prod")
        );
    }

    #[test]
    fn take_requested_transitions_requested_to_in_flight_keeping_target() {
        let mut s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert_eq!(s.take_requested().as_ref().map(|c| c.as_str()), Some("prod"));
        // InFlight carries the target forward, so the failure path can name it.
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
    }

    #[test]
    fn take_requested_is_noop_from_stable() {
        let mut s = ContextSwitchState::Stable;
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::Stable);
    }

    #[test]
    fn take_requested_is_noop_from_in_flight() {
        let mut s = ContextSwitchState::InFlight(ContextName::new("prod").unwrap());
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
    }

    #[test]
    fn settle_from_any_state() {
        for mut s in [
            ContextSwitchState::Stable,
            ContextSwitchState::Requested(ContextName::new("prod").unwrap()),
            ContextSwitchState::InFlight(ContextName::new("prod").unwrap()),
        ] {
            s.settle();
            assert_eq!(s, ContextSwitchState::Stable);
        }
    }

    #[test]
    fn settle_resolves_a_failed_switch() {
        // The failure edge: an in-flight switch whose connection never comes
        // up settles back to Stable, so the next switch is accepted (no lock).
        let mut s = ContextSwitchState::Requested(ContextName::new("does-not-exist").unwrap());
        s.take_requested();
        assert!(!s.is_stable(), "in flight while connecting");
        s.settle(); // ConnectionFailed for the target
        assert!(s.is_stable(), "a failed switch must not stay stuck InFlight");
    }

    #[test]
    fn full_lifecycle() {
        // Stable → Requested(name) → InFlight(name) → Stable
        let mut s = ContextSwitchState::Stable;
        assert!(s.is_stable());
        s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
        let taken = s.take_requested();
        assert_eq!(taken.as_ref().map(|c| c.as_str()), Some("prod"));
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
        s.settle();
        assert!(s.is_stable());
    }
