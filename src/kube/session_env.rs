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
//! [`crate::kube::server_session`]'s `client_build`) and every `kubectl`
//! subprocess (describe/yaml/logs/delete fallbacks, the exec PTY, port-forward).
//!
//! Cheap to clone (`Arc` inside): a session builds exactly one `SessionEnv` and
//! hands cheap clones to everything that authenticates on its behalf — the
//! client builder, its `ServerSession`, its `SessionContext`, and each
//! `PortForwardRequest`.

use std::collections::HashMap;
use std::sync::Arc;

/// A TUI session's environment variables, owned by the session. Never written
/// to the daemon's process env.
///
/// Intentionally NOT `Default`: an empty `SessionEnv` is a credential-less
/// session, the invalid-ish state this module exists to prevent — building one
/// must go through [`SessionEnv::new`] with the env the TUI actually sent.
#[derive(Clone)]
pub struct SessionEnv(Arc<HashMap<String, String>>);

impl SessionEnv {
    pub fn new(vars: HashMap<String, String>) -> Self {
        Self(Arc::new(vars))
    }

    /// The raw `name → value` map, for overlaying into a kubeconfig `exec.env`
    /// (see `client_build::bind_session_env`).
    pub fn vars(&self) -> &HashMap<String, String> {
        &self.0
    }

    /// Apply this session's env onto a child process, overriding any inherited
    /// host value per key — exactly mirroring how kube-rs feeds an exec plugin.
    /// This is THE subprocess-env injection point, so a new `kubectl` spawn site
    /// is one visible `apply_to` call away from being correct.
    pub fn apply_to<C: CommandEnv>(&self, cmd: &mut C) {
        cmd.apply_envs(&self.0);
    }

    /// A `kubectl` command pre-seeded with this session's env. THE way the
    /// daemon builds a `kubectl` invocation on a session's behalf: because the
    /// env is baked in at construction, you cannot obtain a session `kubectl`
    /// command that is missing its credentials — the make-invalid-states-
    /// unrepresentable version of "remember to call `apply_to`". (The PTY exec
    /// path takes a `SessionEnv` as a required argument for the same reason.)
    pub fn kubectl(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new("kubectl");
        self.apply_to(&mut cmd);
        cmd
    }
}

impl std::fmt::Debug for SessionEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Values are redacted — the env carries credentials (e.g.
        // `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
        f.debug_struct("SessionEnv").field("var_count", &self.0.len()).finish()
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
