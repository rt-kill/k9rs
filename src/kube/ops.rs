//! Delete fallback used by the server session.
//!
//! Scale, restart, secret-decode, and cron-trigger operations live inline
//! in `crate::kube::server_session::ops` — their patch bodies are free
//! functions at the call site, not trait methods. Delete needs its own
//! helper here because it has a kubectl-subprocess fallback for the case
//! where the kube-rs API path fails (network blip, RBAC on the discovery
//! endpoint, truly unknown resource).

use crate::kube::protocol::ObjectRef;

/// Execute a delete API call for the given `ObjectRef`.
///
/// Built-ins resolve through the registry's `&'static Gvr` (no HTTP, no
/// allocation). CRDs use their stored GVR if populated, falling back to
/// discovery only when the client sent us an incomplete shape. On any
/// failure, falls back to `kubectl delete` so the user can still delete
/// truly unknown resources.
pub async fn execute_delete(
    client: &::kube::Client,
    target: &ObjectRef,
    context: &crate::kube::protocol::ContextName,
) -> anyhow::Result<()> {
    use ::kube::api::DeleteParams;

    // Try the kube-rs API path first.
    match crate::kube::describe::api_resource_for(client, &target.resource).await {
        Ok((ar, scope)) => {
            let api = crate::kube::describe::dynamic_api_for(client, &ar, scope, &target.namespace);
            match api.delete(&target.name, &DeleteParams::default()).await {
                Ok(_) => return Ok(()),
                Err(e) => tracing::warn!(
                    "kube-rs delete failed for {}, falling back to kubectl: {}",
                    target.kubectl_target(), e,
                ),
            }
        }
        Err(e) => tracing::warn!(
            "API resolution failed for {}, falling back to kubectl: {}",
            target.kubectl_target(), e,
        ),
    }

    // Fallback: kubectl delete.
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("delete").arg(target.kubectl_target());
    if let Some(ns) = target.namespace.as_option() { cmd.arg("-n").arg(ns); }
    if !context.is_empty() { cmd.arg("--context").arg(context.as_str()); }
    cmd.kill_on_drop(true);
    let output = cmd.output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("{}", stderr.trim()));
    }

    Ok(())
}
