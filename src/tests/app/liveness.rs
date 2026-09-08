use crate::app::table::TableDataState;
use crate::app::types::Connection;
use crate::app::Liveness;

fn live_conn() -> Connection {
    let mut c = Connection::new();
    c.established();
    c
}

#[test]
fn the_link_outranks_anything_the_store_believes() {
    // A store's state is itself delivered over the link, so a store that
    // says "Ready" across a disconnect is reporting the last thing it heard,
    // not the truth. Every store state must collapse to Connecting.
    let mut conn = live_conn();
    conn.disconnected();
    for state in [
        TableDataState::Ready,
        TableDataState::Initializing,
        TableDataState::Stale("watch died".into()),
        TableDataState::Failed("forbidden".into()),
    ] {
        assert_eq!(
            Liveness::of(&conn, state.clone()),
            Liveness::Connecting,
            "{state:?} must not outvote a dead link",
        );
    }
}

#[test]
fn each_store_state_maps_to_exactly_one_liveness() {
    let conn = live_conn();
    assert_eq!(Liveness::of(&conn, TableDataState::Ready), Liveness::Live);
    assert_eq!(Liveness::of(&conn, TableDataState::Initializing), Liveness::Loading);
    assert_eq!(
        Liveness::of(&conn, TableDataState::Stale("gone".into())),
        Liveness::Stale("gone".into()),
    );
    assert_eq!(
        Liveness::of(&conn, TableDataState::Failed("boom".into())),
        Liveness::Failed("boom".into()),
    );
}

#[test]
fn data_is_never_shown_without_a_reason_to_trust_it() {
    // The pairing that keeps staleness from ever being silent: anything
    // that paints rows either vouches for them (Live) or says why it can't.
    for l in [
        Liveness::Live,
        Liveness::Connecting,
        Liveness::NoContext,
        Liveness::Loading,
        Liveness::Stale("x".into()),
        Liveness::Failed("y".into()),
    ] {
        if l.shows_data() && l != Liveness::Live {
            assert!(l.warning().is_some(), "{l:?} paints rows with no warning");
        }
        if !l.shows_data() {
            assert!(l.warning().is_none(), "{l:?} warns about rows it doesn't paint");
        }
    }
}

#[test]
fn stale_and_failed_keep_their_rows_connecting_does_not() {
    // The rule is "can the user still act on this", not "is it perfect":
    // under Stale/Failed the link is up and every operation still reaches
    // the apiserver, so the cursor, marks and scroll survive the outage.
    assert!(Liveness::Stale("x".into()).shows_data());
    assert!(Liveness::Failed("y".into()).shows_data());
    assert!(!Liveness::Connecting.shows_data());
    assert!(!Liveness::Loading.shows_data());
    assert!(!Liveness::NoContext.shows_data());
}

#[test]
fn only_a_live_view_can_call_itself_empty() {
    let anim = crate::app::anim::Anim::default();
    let empty = || "No pods found.".to_string();
    assert_eq!(Liveness::Live.status_text(&anim, empty), "No pods found.");
    assert!(Liveness::Failed("boom".into()).status_text(&anim, empty).contains("boom"));
    assert!(Liveness::Stale("gone".into()).status_text(&anim, empty).contains("gone"));
    assert!(Liveness::Connecting.status_text(&anim, empty).contains("Connecting"));
    assert!(Liveness::Loading.status_text(&anim, empty).contains("Loading"));
}
