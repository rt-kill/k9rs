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

use crate::kube::protocol;
use crate::kube::session_env::SessionEnv;

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
    // executes as a *synchronous* subprocess (`Command::output`). Run it on a
    // blocking thread so a slow `aws eks get-token` can't stall an async worker.
    let client = {
        let config = config.clone();
        tokio::task::spawn_blocking(move || kube::Client::try_from(config))
            .await
            .map_err(|e| anyhow::anyhow!("client build task panicked: {}", e))??
    };
    Ok((client, config))
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

/// Overlay the session env onto an exec credential plugin's `env`; non-exec
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
/// watcher-cache identity discriminant (see [`crate::kube::protocol::ContextId`]).
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
/// Cross-session sharing is also coupled to [`crate::kube::client_session`]'s
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
    // can't collapse *every* failing auth onto one fingerprint (fail-open = a
    // cross-tenant merge). `Debug` still varies by the non-secret fields.
    let mut value = serde_json::to_value(auth)
        .unwrap_or_else(|_| serde_json::Value::String(format!("{auth:?}")));
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
mod auth_env_tests {
    use super::*;

    /// Two contexts: one exec-auth user (with a pre-existing `AWS_PROFILE`),
    /// one token-auth user. Lets us prove selection, overlay, and isolation.
    const KUBECONFIG: &str = r#"
apiVersion: v1
kind: Config
current-context: ctx-exec
clusters:
- name: c1
  cluster:
    server: https://example.test
contexts:
- name: ctx-exec
  context:
    cluster: c1
    user: user-exec
- name: ctx-token
  context:
    cluster: c1
    user: user-token
users:
- name: user-exec
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: aws
      args: ["eks", "get-token"]
      env:
      - name: AWS_PROFILE
        value: original
- name: user-token
  user:
    token: secret-token
"#;

    fn parse() -> kube::config::Kubeconfig {
        serde_yaml::from_str(KUBECONFIG).expect("valid kubeconfig")
    }

    fn session_env() -> HashMap<String, String> {
        HashMap::from([
            ("AWS_PROFILE".to_owned(), "session".to_owned()),
            ("PATH".to_owned(), "/session/bin".to_owned()),
        ])
    }

    /// Flatten a user's exec `env` (the `{name,value}` list) into name→value.
    fn exec_env_of(kc: &kube::config::Kubeconfig, user: &str) -> Option<HashMap<String, String>> {
        let ai = kc.auth_infos.iter().find(|a| a.name == user)?.auth_info.as_ref()?;
        let env = ai.exec.as_ref()?.env.as_ref()?;
        Some(env.iter()
            .filter_map(|e| Some((e.get("name")?.clone(), e.get("value")?.clone())))
            .collect())
    }

    fn count_named(kc: &kube::config::Kubeconfig, user: &str, name: &str) -> usize {
        exec_env_of(kc, user)
            .map(|_| kc.auth_infos.iter().find(|a| a.name == user).unwrap()
                .auth_info.as_ref().unwrap().exec.as_ref().unwrap()
                .env.as_ref().unwrap().iter()
                .filter(|e| e.get("name").map(String::as_str) == Some(name)).count())
            .unwrap_or(0)
    }

    #[test]
    fn session_env_overlays_selected_exec_user() {
        let bound = bind_session_env(parse(), Some("ctx-exec"), &session_env());
        let env = exec_env_of(&bound, "user-exec").expect("exec env present");
        // Session value overrides the pre-existing AWS_PROFILE...
        assert_eq!(env.get("AWS_PROFILE").map(String::as_str), Some("session"));
        // ...and a fresh session var is added.
        assert_eq!(env.get("PATH").map(String::as_str), Some("/session/bin"));
        // Exactly one entry per name — session wins, no duplicate.
        assert_eq!(count_named(&bound, "user-exec", "AWS_PROFILE"), 1);
    }

    #[test]
    fn drop_env_is_left_unset() {
        // kube-rs applies drop_env via env_remove AFTER the env overlay, so
        // listing an injected name there would strip the value we just set.
        let bound = bind_session_env(parse(), Some("ctx-exec"), &session_env());
        let exec = bound.auth_infos.iter().find(|a| a.name == "user-exec").unwrap()
            .auth_info.as_ref().unwrap().exec.clone().unwrap();
        assert!(exec.drop_env.is_none());
    }

    #[test]
    fn non_selected_user_is_untouched() {
        let bound = bind_session_env(parse(), Some("ctx-exec"), &session_env());
        let token_ai = bound.auth_infos.iter().find(|a| a.name == "user-token").unwrap()
            .auth_info.as_ref().unwrap();
        assert!(token_ai.exec.is_none(), "non-selected user must not gain exec env");
        assert!(token_ai.token.is_some(), "non-selected user is left intact");
    }

    #[test]
    fn non_exec_selected_user_unchanged() {
        // Select the token context: nothing to pin, exec stays absent.
        let bound = bind_session_env(parse(), Some("ctx-token"), &session_env());
        let token_ai = bound.auth_infos.iter().find(|a| a.name == "user-token").unwrap()
            .auth_info.as_ref().unwrap();
        assert!(token_ai.exec.is_none());
    }

    #[test]
    fn unknown_context_returns_kubeconfig_untouched() {
        let bound = bind_session_env(parse(), Some("does-not-exist"), &session_env());
        // Original AWS_PROFILE preserved, no session env injected anywhere.
        assert_eq!(exec_env_of(&bound, "user-exec").unwrap().get("AWS_PROFILE").map(String::as_str), Some("original"));
        assert!(!exec_env_of(&bound, "user-exec").unwrap().contains_key("PATH"));
    }

    #[test]
    fn selected_context_user_resolves_and_falls_back() {
        let kc = parse();
        assert_eq!(selected_context_user(&kc, Some("ctx-token")).as_deref(), Some("user-token"));
        // None → current-context (ctx-exec → user-exec).
        assert_eq!(selected_context_user(&kc, None).as_deref(), Some("user-exec"));
        assert_eq!(selected_context_user(&kc, Some("nope")), None);
    }

    #[test]
    fn merge_exec_env_dedupes_session_wins() {
        let existing = vec![
            exec_env_entry("AWS_PROFILE", "original"),
            exec_env_entry("KEEP", "kept"),
        ];
        let session = HashMap::from([("AWS_PROFILE".to_owned(), "session".to_owned())]);
        let merged = merge_exec_env(&existing, &session);
        let flat: HashMap<String, String> = merged.iter()
            .filter_map(|e| Some((e.get("name")?.clone(), e.get("value")?.clone())))
            .collect();
        assert_eq!(flat.get("AWS_PROFILE").map(String::as_str), Some("session"));
        assert_eq!(flat.get("KEEP").map(String::as_str), Some("kept"));
        assert_eq!(merged.iter().filter(|e| e.get("name").map(String::as_str) == Some("AWS_PROFILE")).count(), 1);
    }

    #[test]
    fn bind_exec_env_none_stays_none() {
        assert!(bind_exec_env(None, &session_env()).is_none());
    }

    // -- fingerprint_auth -----------------------------------------------------

    fn auth(json: serde_json::Value) -> kube::config::AuthInfo {
        serde_json::from_value(json).expect("valid AuthInfo")
    }

    #[test]
    fn fingerprint_same_creds_equal() {
        let a = auth(serde_json::json!({ "token": "same" }));
        let b = auth(serde_json::json!({ "token": "same" }));
        assert_eq!(fingerprint_auth(&a), fingerprint_auth(&b));
    }

    #[test]
    fn fingerprint_differs_on_token() {
        let a = auth(serde_json::json!({ "token": "aaa" }));
        let b = auth(serde_json::json!({ "token": "bbb" }));
        assert_ne!(fingerprint_auth(&a), fingerprint_auth(&b));
    }

    #[test]
    fn fingerprint_differs_on_exec_profile() {
        // Same exec command, different AWS_PROFILE → different identity.
        let a = auth(serde_json::json!({ "exec": { "command": "aws", "env": [{ "name": "AWS_PROFILE", "value": "team-a" }] } }));
        let b = auth(serde_json::json!({ "exec": { "command": "aws", "env": [{ "name": "AWS_PROFILE", "value": "team-b" }] } }));
        assert_ne!(fingerprint_auth(&a), fingerprint_auth(&b));
    }

    #[test]
    fn fingerprint_stable_across_exec_env_order() {
        // Identical credentials with the exec env listed in different order must
        // hash EQUAL (the canonical sort), so the two sessions still share a
        // watcher instead of needlessly splitting it.
        let a = auth(serde_json::json!({ "exec": { "command": "aws", "env": [{ "name": "A", "value": "1" }, { "name": "B", "value": "2" }] } }));
        let b = auth(serde_json::json!({ "exec": { "command": "aws", "env": [{ "name": "B", "value": "2" }, { "name": "A", "value": "1" }] } }));
        assert_eq!(fingerprint_auth(&a), fingerprint_auth(&b), "env order must not affect the fingerprint");
    }

    #[test]
    fn fingerprint_token_vs_exec_differ() {
        let token = auth(serde_json::json!({ "token": "aaa" }));
        let exec = auth(serde_json::json!({ "exec": { "command": "aws" } }));
        assert_ne!(fingerprint_auth(&token), fingerprint_auth(&exec));
    }

    #[test]
    fn fingerprint_differs_on_impersonate() {
        // Acting-as a different user (`as:`) is a different RBAC identity.
        let a = auth(serde_json::json!({ "token": "t", "as": "admin" }));
        let b = auth(serde_json::json!({ "token": "t", "as": "viewer" }));
        assert_ne!(fingerprint_auth(&a), fingerprint_auth(&b));
    }

    #[test]
    fn fingerprint_differs_on_auth_provider() {
        let a = auth(serde_json::json!({ "auth-provider": { "name": "gcp", "config": { "x": "1" } } }));
        let b = auth(serde_json::json!({ "auth-provider": { "name": "gcp", "config": { "x": "2" } } }));
        assert_ne!(fingerprint_auth(&a), fingerprint_auth(&b));
    }

    #[test]
    fn serde_json_objects_sorted() {
        // fingerprint_auth's determinism relies on serde_json NOT enabling its
        // `preserve_order` feature (Value::Object stays a sorted BTreeMap). If a
        // dependency ever turns it on via feature unification, this fails loudly
        // — before the fingerprint silently starts over-isolating equal creds.
        assert_eq!(serde_json::json!({ "z": 1, "a": 2 }).to_string(), r#"{"a":2,"z":1}"#);
    }
}
