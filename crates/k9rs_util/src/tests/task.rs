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
