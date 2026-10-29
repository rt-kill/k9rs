//! `retry_jitter`'s spread, which it did not have.
//!
//! The old hash folded `attempt` in and then reduced `mod 50`, and every
//! backoff passed as `attempt` is a multiple of 50 — so the attempt term
//! vanished and each seed got ONE constant forever. Measured with the real
//! call shape, `pods` and `services` both answered 1.03 on every attempt.

use super::*;

/// The real call shape: a resource plural as the seed, a doubling backoff in
/// milliseconds as the attempt. Successive attempts for one resource must
/// not all land on the same factor.
#[test]
fn successive_attempts_do_not_share_one_factor() {
    for plural in ["pods", "services", "nodes", "events", "deployments"] {
        let mut factors = Vec::new();
        let mut backoff = 2_000u64;
        for _ in 0..6 {
            factors.push(retry_jitter(plural.as_bytes(), backoff));
            backoff *= 2;
        }
        let distinct: std::collections::BTreeSet<String> =
            factors.iter().map(|f| format!("{f:.2}")).collect();
        assert!(
            distinct.len() >= 3,
            "{plural}: six attempts produced {} distinct factors ({factors:?}) — \
             a multiple-of-50 attempt used to cancel out of the hash entirely",
            distinct.len(),
        );
    }
}

/// One process's resources spread against each other — this is the herd that
/// actually exists: a client holds a subscription per resource type and they
/// all retry together.
#[test]
fn sibling_resources_spread_within_one_process() {
    let plurals = [
        "pods", "services", "nodes", "events", "deployments", "configmaps",
        "secrets", "replicasets", "statefulsets", "daemonsets", "jobs",
        "cronjobs", "ingresses", "endpoints", "namespaces", "nodes",
    ];
    let factors: std::collections::BTreeSet<String> = plurals
        .iter()
        .map(|p| format!("{:.2}", retry_jitter(p.as_bytes(), 2_000)))
        .collect();
    // 50 buckets, 15 distinct seeds: collisions are expected, a single
    // bucket is not.
    assert!(
        factors.len() >= 8,
        "fifteen resources landed in {} buckets: {factors:?}",
        factors.len(),
    );
}

/// Within one process the answer is stable, so a retry schedule is
/// reproducible and debuggable.
#[test]
fn the_same_inputs_answer_the_same_within_a_process() {
    for attempt in [2_000u64, 4_000, 60_000] {
        let a = retry_jitter(b"pods", attempt);
        let b = retry_jitter(b"pods", attempt);
        assert_eq!(a, b, "deterministic per (seed, attempt)");
    }
}

/// And it stays in the band the callers multiply a backoff by.
#[test]
fn the_factor_stays_in_its_band() {
    for seed in [b"pods".as_slice(), b"", b"a-very-long-resource-plural-name"] {
        for attempt in [0u64, 1, 50, 2_000, u64::MAX] {
            let f = retry_jitter(seed, attempt);
            assert!(
                (0.75..1.25).contains(&f),
                "seed {seed:?} attempt {attempt}: {f} is outside [0.75, 1.25)",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Backoff: the policy as one value
// ---------------------------------------------------------------------------

use std::time::Duration;

/// It starts at the floor, doubles, and stops at the ceiling.
#[tokio::test(start_paused = true)]
async fn a_backoff_doubles_from_its_floor_and_stops_at_the_ceiling() {
    let mut b = Backoff::new(Duration::from_secs(2), Duration::from_secs(30));
    let mut waited = Vec::new();
    for _ in 0..8 {
        let before = tokio::time::Instant::now();
        b.sleep_and_double(b"pods").await;
        waited.push(before.elapsed());
    }
    // Jitter moves each one, so compare the shape rather than the values.
    assert!(
        waited.windows(2).all(|w| w[1] >= w[0]),
        "never gets faster while failing: {waited:?}",
    );
    let last = *waited.last().expect("non-empty");
    assert!(
        last <= Duration::from_secs(45),
        "the ceiling is 30s plus jitter, not unbounded: {last:?}",
    );
    assert!(
        waited[0] <= Duration::from_secs(4),
        "the first wait is the floor plus jitter: {:?}",
        waited[0],
    );
}

/// `reset` really returns to the floor — this is the half the client called
/// six times by hand.
#[tokio::test(start_paused = true)]
async fn a_reset_returns_to_the_floor() {
    let mut b = Backoff::new(Duration::from_secs(2), Duration::from_secs(30));
    for _ in 0..5 {
        b.sleep_and_double(b"pods").await;
    }
    let grown = b.peek(b"pods");
    assert!(grown >= Duration::from_secs(20), "it did grow: {grown:?}");

    b.reset();
    let back = b.peek(b"pods");
    assert!(
        back <= Duration::from_secs(4),
        "and comes all the way back, not halfway: {back:?}",
    );
}

/// `peek` is what the NEXT sleep will wait, so a log line naming the
/// interval cannot name a different one from the sleep that follows it.
#[tokio::test(start_paused = true)]
async fn peek_is_what_the_next_sleep_waits() {
    let mut b = Backoff::new(Duration::from_secs(2), Duration::from_secs(30));
    b.sleep_and_double(b"pods").await;
    let announced = b.peek(b"pods");
    let before = tokio::time::Instant::now();
    b.sleep_and_double(b"pods").await;
    assert_eq!(before.elapsed(), announced, "the message and the sleep agree");
}

/// Resources back off on different schedules — the jitter reaches through
/// `Backoff`, which must not have flattened it.
///
/// Over a SET of seeds, not a pair: there are 50 jitter buckets, so any two
/// given seeds collide about 2% of the time and a two-seed `assert_ne` is a
/// 1-in-50 flake. (It flaked on its first run, which is how this reads the
/// way it does.) The property that matters, and the one the constant-jitter
/// defect broke, is that the whole herd does not share one interval.
#[tokio::test(start_paused = true)]
async fn resources_do_not_retry_in_lockstep() {
    let b = Backoff::new(Duration::from_secs(2), Duration::from_secs(30));
    let plurals: [&[u8]; 8] = [
        b"pods", b"services", b"nodes", b"events",
        b"deployments", b"configmaps", b"secrets", b"jobs",
    ];
    let intervals: std::collections::BTreeSet<Duration> =
        plurals.iter().map(|p| b.peek(p)).collect();
    assert!(
        intervals.len() >= 4,
        "eight resources landed in {} intervals: {intervals:?}",
        intervals.len(),
    );
}
