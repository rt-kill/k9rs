//! Unit tests for `client_build` (mirrored out of the source file).

// ---------------------------------------------------------------------------
// auth_env_tests (mirrored out of client_build.rs)
// ---------------------------------------------------------------------------

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
