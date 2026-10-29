//! Per-session process environment, owned by the session and applied to every
//! child process the daemon spawns on that session's behalf.
//!
//! The daemon serves many TUI sessions concurrently, each with its own cloud
//! credentials (`AWS_PROFILE`, `KUBECONFIG`, `PATH`, …). It MUST NOT write these
//! into its *own* process environment: an exec credential plugin re-runs lazily
//! at token-refresh and would read whichever tenant's value was written last,
//! bleeding credentials across sessions. Instead each session carries its env
//! as this value and applies it directly onto the child `Command`s it spawns —
//! the in-process kube client's exec plugin (via the kubeconfig `exec.env`, see
//! the daemon's session module's `client_build`) and every `kubectl`
//! subprocess (describe/yaml/logs/delete fallbacks, the exec PTY, port-forward).
//!
//! Cheap to clone (`Arc` inside): a session builds exactly one `SessionEnv` and
//! hands cheap clones to everything that authenticates on its behalf — the
//! client builder, its `ServerSession`, its `SessionContext`, and each
//! `PortForwardRequest`.

use std::collections::HashMap;
use std::sync::Arc;

/// How many `kubectl` subprocesses one session may have running at once.
///
/// The fan-out is per session because its source is: a confirmed batch
/// sends one command per marked row, and `handle_command` spawns the
/// handler and returns, so the session's 32-slot command channel throttles
/// nothing. Delete three hundred pods on a cluster whose API path refuses
/// them and all three hundred fall back to `kubectl delete` — three hundred
/// forks at once, against the same apiserver, from a process whose own fd
/// and thread budget has to cover every other session.
///
/// Per session rather than daemon-wide on purpose: a global budget would
/// let one session's batch starve another session's interactive describe,
/// and the fan-out it bounds is a per-session act. (The daemon-wide cap on
/// `kube::Client` builds is global for the opposite reason — the resource
/// there is the shared blocking pool, and the storm is across contexts.)
///
/// Eight matches that cap, and it is a *queue*, not a refusal: a batch of
/// three hundred still completes, eight at a time. Refusing instead would
/// strand the client's batch tracker, which waits for one result per
/// target.
pub(crate) const MAX_CONCURRENT_KUBECTL: usize = 8;

/// A TUI session's environment variables, owned by the session. Never written
/// to the daemon's process env.
///
/// Intentionally NOT `Default`: an empty `SessionEnv` is a credential-less
/// session, the invalid-ish state this module exists to prevent — building one
/// must go through [`SessionEnv::new`] with the env the TUI actually sent.
#[derive(Clone)]
pub struct SessionEnv {
    vars: Arc<HashMap<String, String>>,
    /// This session's `kubectl` budget, shared by every clone — the clones
    /// are one session's several holders, not several sessions. See
    /// `MAX_CONCURRENT_KUBECTL` and [`SessionEnv::kubectl_once`].
    slots: Arc<tokio::sync::Semaphore>,
}

impl SessionEnv {
    pub fn new(vars: HashMap<String, String>) -> Self {
        Self {
            vars: Arc::new(vars),
            slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_KUBECTL)),
        }
    }

    /// The raw `name → value` map, for overlaying into a kubeconfig `exec.env`
    /// (see `client_build::bind_session_env`).
    pub fn vars(&self) -> &HashMap<String, String> {
        &self.vars
    }

    /// Apply this session's env onto a child process, overriding any inherited
    /// host value per key — exactly mirroring how kube-rs feeds an exec plugin.
    /// This is THE subprocess-env injection point, so a new `kubectl` spawn site
    /// is one visible `apply_to` call away from being correct.
    pub fn apply_to<C: CommandEnv>(&self, cmd: &mut C) {
        cmd.apply_envs(&self.vars);
    }

    /// A `kubectl` invocation that runs to completion, holding one of this
    /// session's `MAX_CONCURRENT_KUBECTL` slots from here until its
    /// [`output`] returns. Awaits a free slot, so the caller queues rather
    /// than being refused.
    ///
    /// This is THE way the daemon builds a bounded `kubectl` invocation on a
    /// session's behalf. Two things are baked in at construction rather than
    /// left to the caller to remember: the session's env (so you cannot
    /// obtain a session `kubectl` that is missing its credentials) and the
    /// slot (so you cannot obtain one that is uncounted). The returned value
    /// has no `spawn`, so there is no way to hold a child that outlives its
    /// slot, and `kill_on_drop` is set here rather than at each site.
    ///
    /// [`output`]: KubectlOnce::output
    pub(crate) async fn kubectl_once(&self) -> KubectlOnce {
        // `acquire_owned` can only fail on a closed semaphore, and nothing
        // closes this one — it is owned by the `SessionEnv` and dies with it.
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("the session's kubectl semaphore is never closed");
        let mut cmd = tokio::process::Command::new("kubectl");
        self.apply_to(&mut cmd);
        cmd.kill_on_drop(true);
        KubectlOnce { cmd, _slot: slot }
    }

    /// A `kubectl` command pre-seeded with this session's env, for an
    /// invocation that streams for as long as the client watches it
    /// (`logs -f`).
    ///
    /// Deliberately NOT counted against `MAX_CONCURRENT_KUBECTL`: its
    /// lifetime is the user's, not the work's, and a slot held open for the
    /// minutes a log view stays up would starve the bounded calls — eight
    /// open log views and no describe would ever run again. What bounds
    /// these is the substream budget (`k9rs_mux::MAX_INBOUND_SUBSTREAMS`),
    /// because each one rides a substream the client had to open.
    pub fn kubectl_streaming(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new("kubectl");
        self.apply_to(&mut cmd);
        cmd
    }

    /// How many of this session's `kubectl` slots are free — the one
    /// readable number for "is this session saturated". Reported by
    /// [`SessionEnv`]'s `Debug` and asserted by the tests; nothing branches
    /// on it, because a branch on a free-slot count races the slot.
    pub(crate) fn free_kubectl_slots(&self) -> usize {
        self.slots.available_permits()
    }
}

/// A `kubectl` invocation that owns one of its session's subprocess slots.
///
/// Built only by [`SessionEnv::kubectl_once`], and it releases the slot when
/// it is dropped — including on a cancelled task, which `kill_on_drop` turns
/// into a killed child rather than an orphan. The surface is `arg` and
/// `output` on purpose: `output` takes `self`, so the slot is held for
/// exactly the subprocess's life, and there is no door (`spawn`,
/// `DerefMut`) through which a child could outlive it.
pub(crate) struct KubectlOnce {
    cmd: tokio::process::Command,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl KubectlOnce {
    pub(crate) fn arg<S: AsRef<std::ffi::OsStr>>(&mut self, arg: S) -> &mut Self {
        self.cmd.arg(arg);
        self
    }

    /// Run to completion, then release the slot.
    pub(crate) async fn output(mut self) -> std::io::Result<std::process::Output> {
        self.cmd.output().await
    }
}

impl std::fmt::Debug for SessionEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Values are redacted — the env carries credentials (e.g.
        // `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
        f.debug_struct("SessionEnv")
            .field("var_count", &self.vars.len())
            .field("free_kubectl_slots", &self.free_kubectl_slots())
            .finish()
    }
}

/// Bridges the two `Command` flavors the daemon spawns — `tokio`'s async
/// command (kubectl subprocesses) and `std`'s (the PTY exec). Both already
/// expose `envs`; this lets [`SessionEnv::apply_to`] be one method, not two.
pub trait CommandEnv {
    fn apply_envs(&mut self, vars: &HashMap<String, String>);
}

impl CommandEnv for tokio::process::Command {
    fn apply_envs(&mut self, vars: &HashMap<String, String>) {
        self.envs(vars);
    }
}

impl CommandEnv for std::process::Command {
    fn apply_envs(&mut self, vars: &HashMap<String, String>) {
        self.envs(vars);
    }
}

#[cfg(test)]
#[path = "tests/session_env.rs"]
mod tests;
