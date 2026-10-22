use super::*;
use std::sync::Arc;

#[test]
fn new_some_and_none() {
    let a: AtomicOption<u32> = AtomicOption::none();
    assert!(a.is_none(Ordering::Acquire));
    assert!(!a.is_some(Ordering::Acquire));

    let b = AtomicOption::some(42u32);
    assert!(b.is_some(Ordering::Acquire));
}

#[test]
fn swap_returns_old_value() {
    let a = AtomicOption::some(1u32);
    let old = a.swap(Some(2), Ordering::AcqRel);
    assert_eq!(old, Some(1));
    assert_eq!(a.take(Ordering::AcqRel), Some(2));
    assert!(a.is_none(Ordering::Acquire));
}

#[test]
fn take_then_store_roundtrips() {
    let a = AtomicOption::some(7u32);
    assert_eq!(a.take(Ordering::AcqRel), Some(7));
    assert_eq!(a.take(Ordering::AcqRel), None);
    a.store(Some(9), Ordering::AcqRel);
    assert_eq!(a.take(Ordering::AcqRel), Some(9));
}

#[test]
fn works_for_align_one_values() {
    // `Slot2` must keep the low-bit tag free even when `T` is `align == 1`.
    let a = AtomicOption::some(0xABu8);
    assert!(a.is_some(Ordering::Acquire));
    assert_eq!(a.load_cloned(), Some(0xAB));
    assert_eq!(a.take(Ordering::AcqRel), Some(0xAB));
    assert!(a.is_none(Ordering::Acquire));
}

#[test]
fn load_cloned_on_arc_roundtrips() {
    let a: AtomicOption<Arc<Vec<u32>>> = AtomicOption::some(Arc::new(vec![1, 2, 3]));

    let snap = a.load_cloned().expect("slot has value");
    assert_eq!(&*snap, &[1, 2, 3]);

    // Slot was restored — a second load sees the same data.
    let snap2 = a.load_cloned().expect("slot still has value");
    assert_eq!(&*snap2, &[1, 2, 3]);
}

#[test]
fn load_cloned_returns_none_on_empty() {
    let a: AtomicOption<Arc<Vec<u32>>> = AtomicOption::none();
    assert!(a.load_cloned().is_none());
}

#[test]
fn load_cloned_sees_latest_write() {
    let a: AtomicOption<Arc<Vec<u32>>> = AtomicOption::some(Arc::new(vec![1]));
    a.store(Some(Arc::new(vec![9, 9, 9])), Ordering::AcqRel);
    let snap = a.load_cloned().unwrap();
    assert_eq!(&*snap, &[9, 9, 9]);
}

#[test]
fn drop_releases_value() {
    // Use Arc refcount as a drop witness — when the slot drops, the inner
    // Arc strong-count must fall to 1 (held only by our handle).
    let shared = Arc::new(());
    {
        let a = AtomicOption::some(shared.clone());
        assert_eq!(Arc::strong_count(&shared), 2);
        drop(a);
    }
    assert_eq!(Arc::strong_count(&shared), 1);
}

#[test]
fn load_cloned_never_leaks_under_take_restore() {
    // The claim+restore must hand the box back to the slot on the common
    // path (no leak) and free it only when a writer intervenes. Witness via
    // Arc strong count after a quiet load_cloned: back to exactly 1.
    let witness = Arc::new(());
    let cell = AtomicOption::some(witness.clone());
    for _ in 0..1000 {
        let got = cell.load_cloned().expect("occupied");
        drop(got); // drop the clone; the original stays in the slot
    }
    // Only the slot's copy + our `witness` handle remain.
    assert_eq!(Arc::strong_count(&witness), 2);
    drop(cell);
    assert_eq!(Arc::strong_count(&witness), 1);
}

/// Stress the exact scenario the generation tag fixes: many readers cloning
/// while a writer republishes a strictly-increasing value. After everyone
/// joins, the slot MUST hold the final write — the old null-token take /
/// restore could leave a *resurrected* older value here. Also a sanitizer
/// magnet for any use-after-free / double-free in the claim path.
#[test]
fn concurrent_readers_never_resurrect_a_stale_write() {
    use std::thread;

    // Miri interprets every step, so scale the workload way down under it
    // — a handful of rounds is enough for its data-race / UB detection to
    // bite, while the native run does a proper soak.
    let (rounds, reads, writes) = if cfg!(miri) { (3u64, 30u64, 20u64) } else { (100, 400, 300) };

    for round in 0..rounds {
        let base = round * 1000;
        let cell: Arc<AtomicOption<Arc<u64>>> = Arc::new(AtomicOption::some(Arc::new(base)));
        let last = base + writes;

        let mut handles = Vec::new();
        for _ in 0..3 {
            let c = Arc::clone(&cell);
            handles.push(thread::spawn(move || {
                for _ in 0..reads {
                    // Reads must always be coherent: either empty (lost a
                    // race) or a published value in [base, last].
                    if let Some(v) = c.load_cloned() {
                        assert!(*v >= base && *v <= last);
                    }
                }
            }));
        }
        {
            let c = Arc::clone(&cell);
            handles.push(thread::spawn(move || {
                for v in (base + 1)..=last {
                    c.store(Some(Arc::new(v)), Ordering::AcqRel);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // No writer or reader is in flight now; the slot must reflect the
        // final write, never a resurrected earlier value.
        let final_val = cell.load_cloned().expect("slot occupied");
        assert_eq!(*final_val, last, "round {round}: slot lost the latest write");
    }
}
