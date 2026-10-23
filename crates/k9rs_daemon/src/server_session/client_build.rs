//! Per-session `kube::Client` construction with credential isolation.
//!
//! The session's env is pinned *into the selected context's `exec.env`* rather
//! than the daemon's process-global environment, so each session's exec
//! credential plugin reads its own credentials at refresh and concurrent
//! tenants can't contaminate each other. No `std::env::set_var`, no `unsafe`,
//! no serialization task — builds run concurrently (bounded by a daemon-wide
//! semaphore at the call site) and the one genuinely-blocking step (the exec
//! plugin run inside `Client::try_from`) is moved onto a blocking thread.

use std::collections::HashMap;

use k9rs_proto::protocol;
use k9rs_kube::session_env::SessionEnv;

/// Build one TUI session's `kube::Client` from the kubeconfig + env it sent.
///
///   * The daemon never calls `std::env::set_var`, so there is no shared
///     mutable process env — no lock, no `unsafe`, no single-owner builder
///     task; sessions build concurrently.
///   * An exec credential plugin (`aws eks get-token`, gcloud, …) re-runs
///     lazily at token-refresh and reads env from *its own* exec config (see
///     [`bind_session_env`]), never a sibling session's leftover global env.
///   * That exec run is synchronous in kube-rs, so it happens on a blocking
///     thread here — see the `spawn_blocking` below.
pub(super) async fn build_session_client(
    kubeconfig_yaml: &str,
    context: Option<&protocol::ContextName>,
    session_env: &SessionEnv,
) -> anyhow::Result<(kube::Client, kube::Config)> {
    use kube::config::{Config, KubeConfigOptions, Kubeconfig};

    let kubeconfig: Kubeconfig = serde_yaml::from_str(kubeconfig_yaml)
        .map_err(|e| anyhow::anyhow!("Failed to parse kubeconfig YAML: {}", e))?;

    // Resolve the context once — an explicit request wins, else current-context.
    // Drives both context selection and which user's exec auth gets the env.
    let context_name = context
        .map(|c| c.as_str().to_owned())
        .or_else(|| kubeconfig.current_context.clone());

    let kubeconfig = bind_session_env(kubeconfig, context_name.as_deref(), session_env.vars());

    let options = KubeConfigOptions {
        context: context_name,
        ..Default::default()
    };

    let config = Config::from_custom_kubeconfig(kubeconfig, &options).await
        .map_err(|e| anyhow::anyhow!("Failed to create config from kubeconfig: {}", e))?;

    // Field-update, not mutation: longer timeouts for slow APIs / large lists.
    let config = Config {
        read_timeout: Some(std::time::Duration::from_secs(300)),
        connect_timeout: Some(std::time::Duration::from_secs(30)),
        ..config
    };

    // `Client::try_from` eagerly runs the exec credential plugin, which kube-rs
    // executes as a *synchronous* subprocess (`Command::output`) — a blocking
    // `waitpid` with no timeout, on a child kube-rs owns. Run it on a blocking
    // thread so a slow `aws eks get-token` can't stall an async worker.
    //
    // BOUNDED, because that child is the one process in the daemon no `Drop`
    // of ours can reach. An interactive plugin (`kubelogin --grant-type=
    // authcode`, device-code flows) that opens a browser and waits blocks its
    // thread forever; the caller holds one of
    // `MAX_CONCURRENT_CLIENT_BUILDS` semaphore permits while it does, and the
    // TUI's reconnect backoff re-tries — so eight wedged plugins starve every
    // future session on every context, and only `pkill` recovers. A timeout
    // downgrades that to one leaked thread and an actionable message.
    //
    // (The thread itself cannot be cancelled — a blocking closure has no
    // cancellation point — which is why `run_daemon` also bounds runtime
    // shutdown rather than waiting on it.)
    const CLIENT_BUILD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
    let client = {
        let config = config.clone();
        let handle = tokio::task::spawn_blocking(move || kube::Client::try_from(config));
        match tokio::time::timeout(CLIENT_BUILD_TIMEOUT, handle).await {
            Ok(joined) => joined
                .map_err(|e| anyhow::anyhow!("client build task panicked: {}", e))?
                .map_err(summarize_client_error)?,
            Err(_) => anyhow::bail!(
                "credential plugin did not finish within {}s — if it needs a browser \
                 or a prompt, run it once outside k9rs to cache the token",
                CLIENT_BUILD_TIMEOUT.as_secs(),
            ),
        }
    };
    Ok((client, config))
}

/// Turn a `kube::Error` into something a human can act on.
///
/// kube-rs formats a failed credential plugin as
/// `auth exec command '{cmd}' failed with status {status}: {out:?}` — where
/// `cmd` is the command with its ENTIRE environment inlined (PATH, HOME,
/// AWS_PROFILE, every nix store path) and `out` is a `Debug`-printed
/// `Output`. The result is a several-thousand-character line whose only
/// useful content is the plugin's own stderr, and which leaks the session's
/// environment into the log.
///
/// So we match the TYPED variant and keep the part that says what to do:
/// the plugin's stderr. Same boundary discipline as everywhere else —
/// translate a foreign error into our own vocabulary at the point it enters,
/// rather than passing its formatting through to the user.
pub(super) fn summarize_client_error(err: kube::Error) -> anyhow::Error {
    // Nothing is lost — the untouched chain is one `-v` away.
    tracing::debug!("client build failed: {err:?}");
    let kube::Error::Auth(kube::client::AuthError::AuthExecRun { cmd, out, .. }) = &err else {
        return anyhow::Error::new(err);
    };
    // The plugin usually explains itself on stderr; fall back to stdout,
    // then to the exit status, so this is never LESS informative.
    let detail = first_nonempty([
        String::from_utf8_lossy(&out.stderr).trim().to_string(),
        String::from_utf8_lossy(&out.stdout).trim().to_string(),
    ])
    .unwrap_or_else(|| format!("exited with {}", out.status));

    // `cmd` is env-prefixed; the program is the first token that isn't an
    // `NAME=value` assignment.
    let program = cmd
        .split_whitespace()
        .find(|tok| !tok.contains('='))
        .unwrap_or("credential plugin")
        .trim_matches('"');

    match auth_hint(&detail) {
        Some(hint) => anyhow::anyhow!("{program}: {detail} ({hint})"),
        None => anyhow::anyhow!("{program}: {detail}"),
    }
}

fn first_nonempty<const N: usize>(candidates: [String; N]) -> Option<String> {
    candidates.into_iter().find(|c| !c.is_empty())
}

/// The one-line "so do this" for credential failures we recognise. Kept
/// deliberately small: a wrong guess is worse than none, so anything
/// unrecognised gets the plugin's own words and nothing else.
fn auth_hint(detail: &str) -> Option<&'static str> {
    let lower = detail.to_lowercase();
    if lower.contains("sso") && (lower.contains("token") || lower.contains("session")) {
        return Some("run `aws sso login`");
    }
    if lower.contains("expired") {
        return Some("credentials expired — re-authenticate");
    }
    if lower.contains("could not connect to the endpoint")
        || lower.contains("failed to resolve")
    {
        return Some("check your network / VPN");
    }
    None
}

/// Pin a session's credential environment into the kubeconfig it builds from,
/// so the env travels *with the config* rather than through process-global
/// state. Only the `auth_infos` entry backing the selected context is touched,
/// and only if it uses exec auth — token / client-certificate auth spawns no
/// subprocess and has no inherited-env hazard, so it is left exactly as-is.
/// Pure transformation: consumes the kubeconfig, returns the rewritten one.
fn bind_session_env(
    kubeconfig: kube::config::Kubeconfig,
    context: Option<&str>,
    env_vars: &HashMap<String, String>,
) -> kube::config::Kubeconfig {
    use kube::config::{AuthInfo, Kubeconfig, NamedAuthInfo};

    // The user this session authenticates as. Nothing to pin if it can't be
    // resolved (no context, unknown context, or a context with no user).
    let Some(user) = selected_context_user(&kubeconfig, context) else {
        return kubeconfig;
    };

    Kubeconfig {
        auth_infos: kubeconfig.auth_infos.into_iter()
            .map(|named| if named.name == user {
                NamedAuthInfo {
                    auth_info: named.auth_info.map(|ai| AuthInfo {
                        exec: bind_exec_env(ai.exec, env_vars),
                        ..ai
                    }),
                    ..named
                }
            } else {
                named
            })
            .collect(),
        ..kubeconfig
    }
}

/// The `auth_infos` name backing the selected context (its `user`), falling
/// back to the kubeconfig's `current-context` when none was requested.
fn selected_context_user(
    kubeconfig: &kube::config::Kubeconfig,
    context: Option<&str>,
) -> Option<String> {
    let wanted = context.or(kubeconfig.current_context.as_deref())?;
    kubeconfig.contexts.iter()
        .find(|c| c.name == wanted)
        .and_then(|c| c.context.as_ref())
        .and_then(|ctx| ctx.user.clone())
}

/// TakeoverOp the session env onto an exec credential plugin's `env`; non-exec
/// auth (`None`) is returned unchanged. kube-rs runs the plugin with
/// `Command::envs(exec.env)`, which overrides the inherited host env, so the
/// session's values win for every name it carries. `drop_env` is deliberately
/// left unset: kube-rs applies it via `env_remove` *after* the overlay, so
/// listing an injected name there would strip the value we just set.
fn bind_exec_env(
    exec: Option<kube::config::ExecConfig>,
    env_vars: &HashMap<String, String>,
) -> Option<kube::config::ExecConfig> {
    exec.map(|exec| kube::config::ExecConfig {
        env: Some(merge_exec_env(exec.env.as_deref().unwrap_or_default(), env_vars)),
        ..exec
    })
}

/// Merge the session env over a plugin's pre-existing `env` entries (the k8s
/// `{name, value}` shape), session values taking precedence so there is
/// exactly one entry per name.
fn merge_exec_env(
    existing: &[HashMap<String, String>],
    env_vars: &HashMap<String, String>,
) -> Vec<HashMap<String, String>> {
    existing.iter()
        .filter(|entry| entry.get("name").is_none_or(|n| !env_vars.contains_key(n)))
        .cloned()
        .chain(env_vars.iter().map(|(name, value)| exec_env_entry(name, value)))
        .collect()
}

/// One k8s exec `env` entry: `{ "name": <k>, "value": <v> }`.
fn exec_env_entry(name: &str, value: &str) -> HashMap<String, String> {
    HashMap::from([
        ("name".to_owned(), name.to_owned()),
        ("value".to_owned(), value.to_owned()),
    ])
}

/// A stable hash of the credential material in a resolved `AuthInfo` — the
/// watcher-cache identity discriminant (see [`k9rs_proto::protocol::ContextId`]).
///
/// Two sessions with the SAME kubeconfig user *name* on one cluster but
/// DIFFERENT credentials (a different token, or a different exec profile via
/// the injected `exec.env`) must never share a watcher — different RBAC = a
/// cross-tenant data leak. Hashing the resolved auth material, not the user
/// name, is what enforces that; genuinely-identical credentials still hash
/// equal and so still share.
///
/// Determinism: the auth is serialized through `serde_json::Value` (whose
/// objects are `BTreeMap`s → sorted keys, *as long as serde_json's
/// `preserve_order` feature stays off* — pinned by the `serde_json_objects_sorted`
/// test) and the exec `env` array is sorted, so HashMap iteration order can't
/// perturb the hash and needlessly split a watcher equal-credential sessions
/// should share.
///
/// Cross-session sharing is also coupled to the client session's
/// `collect_env_vars` staying a *small, auth-scoped allowlist*: the injected
/// `exec.env` is part of this hash, so forwarding volatile vars (PWD, TERM, …)
/// there would split otherwise-shareable sessions.
///
/// Assumptions (acceptable for k9rs's model — each session sends its own
/// kubeconfig with explicit creds — but stated so they're not a surprise):
///   * A `Default`/empty `AuthInfo` (creds entirely ambient / in-cluster, not
///     in the kubeconfig) hashes the same for everyone → such sessions share;
///     there is no config-level credential to tell them apart.
///   * The hash is over credential *declarations* (token-file path, exec spec),
///     not the bytes they resolve to lazily later — so two sessions pointing at
///     the same `tokenFile` whose contents differ at read time would share.
///
/// Non-portable: `DefaultHasher` output is not stable across Rust versions —
/// fine ONLY because `ContextId` is process-internal (never persisted nor wired).
pub(super) fn fingerprint_auth(auth: &kube::config::AuthInfo) -> u64 {
    use std::hash::{Hash, Hasher};
    // `to_value` on an `AuthInfo` (plain strings/maps + exposable secrets) does
    // not fail in practice; the fallback exists only so a hypothetical failure
    // cannot MERGE sessions it can't tell apart. Fail-CLOSED: mint a unique
    // never-shared fingerprint. Hashing a lossy stand-in (the old
    // `format!("{auth:?}")`) would be fail-open — kube-rs REDACTS secret
    // material in `Debug`, so two auths differing only in secret values would
    // fingerprint equal and share a watcher: the exact cross-tenant merge
    // this fingerprint exists to prevent. A never-shared watcher merely costs
    // one duplicate poll; a wrongly-shared one leaks another tenant's rows.
    let mut value = match serde_json::to_value(auth) {
        Ok(v) => v,
        Err(_) => {
            static UNSHARED: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let nonce = UNSHARED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            ("k9rs-unshareable-auth", nonce).hash(&mut hasher);
            return hasher.finish();
        }
    };
    // Canonicalize the one HashMap-ordered array so identical creds with the
    // exec env listed in a different order still hash equal.
    if let Some(env) = value.pointer_mut("/exec/env").and_then(|e| e.as_array_mut()) {
        env.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.to_string().hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[path = "../tests/server_session/client_build.rs"]
mod auth_env_tests_mirror;
