//! The registry's contract, especially the checks that only matter under a
//! race. Four hand-written copies of this existed; these pin the one.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A value whose liveness is separate from its refcount — exactly the case
/// `alive` exists for (a watcher whose task has finished still upgrades).
struct Thing {
    id: usize,
    usable: bool,
}

fn reg() -> WeakRegistry<&'static str, Thing> {
    WeakRegistry::new()
}

#[test]
fn an_absent_key_is_created_once_and_then_reused() {
    let r = reg();
    let made = AtomicUsize::new(0);
    let mk = |_: &&str| {
        Arc::new(Thing { id: made.fetch_add(1, Ordering::SeqCst), usable: true })
    };

    let (a, first) = r.get_or_create("k", |_| true, mk);
    assert_eq!(first, Outcome::Created);
    let (b, second) = r.get_or_create("k", |_| true, mk);
    assert_eq!(second, Outcome::Reused, "`create` must not run again");
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(made.load(Ordering::SeqCst), 1);
}

/// The whole point of holding `Weak`: once the last holder drops, the next
/// caller gets a fresh resource rather than a resurrected one.
#[test]
fn the_resource_dies_with_its_last_holder() {
    let r = reg();
    let (a, _) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 1, usable: true }));
    assert_eq!(r.live().len(), 1);
    drop(a);
    assert_eq!(r.live().len(), 0, "no holders left, so nothing is live");

    let (b, outcome) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 2, usable: true }));
    assert_eq!(outcome, Outcome::Created);
    assert_eq!(b.id, 2);
}

/// `alive` is the axis the four copies disagreed about: two checked a task's
/// `is_finished`, two checked nothing. A value that upgrades but is unusable
/// must be REPLACED, and the old `Arc` must stay valid for whoever holds it.
#[test]
fn a_held_but_unusable_resource_is_replaced_not_handed_out() {
    let r = reg();
    let (stale, _) = r.get_or_create("k", |t| t.usable, |_| Arc::new(Thing { id: 1, usable: false }));

    let (fresh, outcome) = r.get_or_create(
        "k",
        |t| t.usable,
        |_| Arc::new(Thing { id: 2, usable: true }),
    );
    assert_eq!(outcome, Outcome::ReplacedDead);
    assert_eq!(fresh.id, 2);
    assert_eq!(stale.id, 1, "the displaced resource is still valid for its holder");
    assert!(!Arc::ptr_eq(&stale, &fresh));
}

/// Without the `alive` predicate the same state hands back the corpse. This
/// is what the two registries that omitted the check were relying on being
/// true of their value types — now they say so by passing `|_| true`.
#[test]
fn without_a_liveness_predicate_an_upgradable_resource_is_reused() {
    let r = reg();
    let (first, _) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 1, usable: false }));
    let (second, outcome) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 2, usable: true }));
    assert_eq!(outcome, Outcome::Reused);
    assert!(Arc::ptr_eq(&first, &second));
}

/// `replace` is force-create: it displaces unconditionally, and the holder of
/// the old resource keeps it.
#[test]
fn replace_displaces_a_live_resource_without_invalidating_it() {
    let r = reg();
    let (old, _) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 1, usable: true }));
    let new = r.replace("k", |_| Arc::new(Thing { id: 2, usable: true }));
    assert_eq!(old.id, 1, "still usable by whoever held it");
    assert_eq!(new.id, 2);

    let (now, outcome) = r.get_or_create("k", |_| true, |_| unreachable!("should reuse"));
    assert_eq!(outcome, Outcome::Reused);
    assert!(Arc::ptr_eq(&now, &new), "the map points at the replacement");
}

/// Reporting must not mutate what it reports on — stated in prose at two
/// status call sites before this was one function.
#[test]
fn live_skips_dead_slots_without_reaping_them() {
    let r = reg();
    let (a, _) = r.get_or_create("gone", |_| true, |_| Arc::new(Thing { id: 1, usable: true }));
    let (_b, _) = r.get_or_create("kept", |_| true, |_| Arc::new(Thing { id: 2, usable: true }));
    drop(a);

    assert_eq!(r.live().len(), 1, "only the held one is live");
    assert_eq!(r.slot_count(), 2, "…and the dead slot is still THERE");

    r.reap();
    assert_eq!(r.slot_count(), 1, "reaping is the explicit act");
}

/// Holder counts come back with the live set, which is what `ctl status`
/// prints.
///
/// The count is read BEFORE the upgrade, so it does not include the `Arc`
/// being handed out. It does include any still-held result of an earlier
/// `live()` call, though — the returned `Arc`s are real holders, which is why
/// this test drops each snapshot before taking the next. (A status caller
/// prints and discards, so it never sees the inflation; a caller that kept
/// the vector around and polled again would.)
#[test]
fn live_reports_the_holder_count() {
    let r = reg();
    let (a, _) = r.get_or_create("k", |_| true, |_| Arc::new(Thing { id: 1, usable: true }));
    let b = Arc::clone(&a);

    let holders = r.live()[0].2;
    assert_eq!(holders, 2, "`a` and `b`, not the snapshot's own Arc");

    drop(b);
    assert_eq!(r.live()[0].2, 1, "just `a`");
}

/// `replace` holds the slot for the WHOLE of build-and-insert, so a
/// concurrent get-or-create cannot slip between the two halves.
///
/// `create` used to run BEFORE `entry()` was called, which left a window:
/// a get-or-create for the same key took the lock first, built its own
/// resource, inserted it, and handed it to its caller — and then `replace`
/// overwrote the slot, so that caller was holding a resource nothing could
/// reach. Two live resources for one key, one of them invisible. The doc
/// said, in two places, that this could not happen.
///
/// Sequenced rather than hammered: the replacing thread parks INSIDE
/// `create`, which is precisely when the lock must already be held, and the
/// other thread then tries to get-or-create the same key. If the lock is
/// held it waits and finds the replacement; if it is not, it builds its own
/// and is stranded.
#[test]
fn replace_cannot_be_interleaved_with_a_get_or_create() {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    let r = Arc::new(reg());

    let (inside_tx, inside_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();

    let replacer = {
        let r = Arc::clone(&r);
        std::thread::spawn(move || {
            r.replace("k", |_| {
                // Inside `create`: the entry lock is held here, or the
                // claim this test exists for is false.
                inside_tx.send(()).expect("test thread alive");
                go_rx.recv().expect("test thread alive");
                Arc::new(Thing { id: 1, usable: true })
            })
        })
    };

    inside_rx.recv().expect("the replacer reached its `create`");

    let joined_early = Arc::new(AtomicBool::new(false));
    let getter = {
        let r = Arc::clone(&r);
        let joined_early = Arc::clone(&joined_early);
        std::thread::spawn(move || {
            let (arc, outcome) =
                r.get_or_create("k", |t| t.usable, |_| Arc::new(Thing { id: 2, usable: true }));
            joined_early.store(true, Ordering::SeqCst);
            (arc, outcome)
        })
    };

    // Long enough that a get-or-create which was NOT blocked would have
    // finished. This is the only timing-shaped assertion here, and the
    // outcome check below stands on its own.
    std::thread::sleep(std::time::Duration::from_millis(150));
    let ran_while_locked = joined_early.load(Ordering::SeqCst);

    go_tx.send(()).expect("the replacer is parked");
    let replacement = replacer.join().expect("replacer");
    let (got, outcome) = getter.join().expect("getter");

    assert!(
        !ran_while_locked,
        "a get-or-create completed while `replace` was inside `create` — the \
         slot was not held across build-and-insert",
    );
    assert_eq!(
        outcome,
        Outcome::Reused,
        "the getter must find the replacement, not build a second resource",
    );
    assert!(
        Arc::ptr_eq(&got, &replacement),
        "and it must be the SAME resource the map points at (ids {} vs {})",
        got.id,
        replacement.id,
    );
}
