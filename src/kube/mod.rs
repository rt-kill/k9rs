pub mod cache;
pub mod client_session;

/// How long a watcher (or local resource source) stays alive after the last
/// subscriber drops. Shared between the K8s `LiveQuery` grace-period task and
/// the `LocalSubscription` grace-period task (both via [`spawn_grace`]) so the
/// two systems use the same window and can't silently drift.
///
/// 60s: long enough that quick nav-away/back (Esc then re-enter, tab cycling)
/// reuses the warm watcher; short enough that a linear sweep through many
/// resource types doesn't pin a *union* of watchers — each holding its full
/// store and an open apiserver watch — for minutes. Most reuse value decays
/// under a minute.
pub(crate) const GRACE_PERIOD_SECS: u64 = 60;

/// The shared "last subscriber dropped" grace dance, used by both the K8s
/// `live_query::Subscription` and the local `LocalSubscription` `Drop` impls.
/// Their grace logic is structurally identical — keep the kept-alive `arc`
/// around for [`GRACE_PERIOD_SECS`], then release a single-owner claim flag and
/// drop it — but the *mechanism* of that flag differs (a concrete `AtomicBool`
/// on the `LiveQuery` vs. the dyn-dispatched `try_begin_grace`/`end_grace` hook
/// on the `LocalResourceSource` trait). Parameterizing the variance as closures
/// keeps the one control-flow skeleton here instead of copy-pasted into two
/// `Drop`s that could silently drift.
///
/// All four behaviors of the original hand-rolled Drops are preserved:
/// 1. `finished(&arc)` → already dead (a self-terminated watcher); drop `arc`
///    now with no grace, so its cache slot is reclaimed and the next subscribe
///    builds fresh. Infallible local sources pass `|_| false`.
/// 2. `claim(&arc)` lost → another drop already owns the in-flight grace task;
///    just drop our `arc`. The coalescing CAS means at most one grace task
///    exists per kept-alive object, so context-switch churn can't stack
///    detached timers.
/// 3. No tokio runtime (dropping during process shutdown) → `reset(&arc)` to
///    release the claim, then drop `arc` immediately; dying now is correct.
/// 4. Otherwise spawn a task that sleeps the grace window, calls `reset(&arc)`
///    to release the claim *before* dropping `arc` (so a later subscribe/drop
///    cycle can grace again), then drops it.
///
/// `claim` performs the winning side of the coalescing CAS and returns whether
/// it won; `reset` undoes it. Only `reset` and `arc` cross into the spawned
/// task, so only they carry `Send + 'static`.
pub(crate) fn spawn_grace<A>(
    arc: A,
    finished: impl FnOnce(&A) -> bool,
    claim: impl FnOnce(&A) -> bool,
    reset: impl FnOnce(&A) + Send + 'static,
) where
    A: Send + 'static,
{
    if finished(&arc) {
        return;
    }
    if !claim(&arc) {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        // `reset` is `FnOnce`; consuming it here is sound only because this
        // arm diverges (`return`) — so the spawn path below never also moves
        // it. A future edit that drops the `return` would fail to compile,
        // which is the intended tripwire, not a silent hazard.
        reset(&arc);
        return;
    };
    handle.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
        reset(&arc);
        drop(arc);
    });
}

pub mod mux;
pub mod daemon;
pub mod daemon_config;
pub mod describe;
pub mod live_query;
pub mod live_query_dynamic;
pub mod local;
pub mod metrics;
pub mod ops;
pub mod overlay;
pub mod protocol;
pub mod repaint;
pub mod resource_def;
pub mod resource_defs;
pub mod resources;
pub mod server_session;
pub mod session;
pub mod session_env;
pub mod session_actions;
pub mod session_commands;
pub mod session_events;
pub mod shared_poller;
