use kube::api::{Api, DynamicObject};

use crate::kube::protocol::ResourceScope;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Fetch a describe view for a resource via `kubectl describe`.
/// kubectl's output is comprehensive (events, replica sets, rolling update
/// strategy, etc.) and matches what users expect from k9s.
pub async fn fetch_describe_native(
    _client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    fetch_describe_via_kubectl(resource, name, namespace, context).await
}

/// Fetch YAML for a resource. Tries the kube API first (fast, no subprocess),
/// falls back to kubectl if API discovery fails.
pub async fn fetch_yaml_native(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    match fetch_yaml_via_api(client, resource, name, namespace).await {
        Ok(yaml) => yaml,
        Err(_) => fetch_yaml_via_kubectl(resource, name, namespace, context).await,
    }
}

/// Resolve a resource type string to an ApiResource.
/// Tries built-in lookups first (instant), falls back to API discovery.
pub async fn resolve_api_resource(
    client: &kube::Client,
    resource: &str,
) -> anyhow::Result<(kube::api::ApiResource, ResourceScope)> {
    use kube::api::ApiResource;
    use kube::discovery::{self, Scope};
    use ResourceScope::{Cluster, Namespaced};

    // Try singular name ("deployment"), plural ("deployments"), or alias ("deploy").
    let meta = crate::kube::resource_types::find_by_name(resource)
        .or_else(|| crate::kube::resource_types::find_by_plural(resource))
        .or_else(|| crate::kube::resource_types::find_by_alias(resource));
    if let Some(meta) = meta {
        let gvk = kube::api::GroupVersionKind::gvk(meta.group, meta.version, meta.kind);
        let ar = ApiResource::from_gvk_with_plural(&gvk, meta.plural);
        return Ok((ar, meta.scope));
    }

    // Use API discovery to find the resource by plural name (with or without group).
    let discovery = discovery::Discovery::new(client.clone()).run().await?;

    if resource.contains('.') {
        let parts: Vec<&str> = resource.splitn(2, '.').collect();
        let plural = parts[0];
        let group = parts.get(1).unwrap_or(&"");
        for api_group in discovery.groups() {
            for (ar, caps) in api_group.recommended_resources() {
                if ar.plural == plural && ar.group == *group {
                    let scope = if caps.scope == Scope::Cluster { Cluster } else { Namespaced };
                    return Ok((ar, scope));
                }
            }
        }
    } else {
        let lower = resource.to_lowercase();
        for api_group in discovery.groups() {
            for (ar, caps) in api_group.recommended_resources() {
                if ar.plural.to_lowercase() == lower
                    || ar.kind.to_lowercase() == lower
                    || format!("{}s", ar.kind.to_lowercase()) == lower
                {
                    let scope = if caps.scope == Scope::Cluster { Cluster } else { Namespaced };
                    return Ok((ar, scope));
                }
            }
        }
    }

    Err(anyhow::anyhow!("Resource not found: {}", resource))
}

// ---------------------------------------------------------------------------
// YAML via kube API (no subprocess)
// ---------------------------------------------------------------------------

async fn fetch_yaml_via_api(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<String> {
    let (ar, scope) = resolve_api_resource(client, resource).await?;
    let api: Api<DynamicObject> = match scope {
        ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
        ResourceScope::Namespaced if namespace.is_empty() => Api::default_namespaced_with(client.clone(), &ar),
        ResourceScope::Namespaced => Api::namespaced_with(client.clone(), namespace, &ar),
    };
    let obj = api.get(name).await?;
    let yaml = serde_yaml::to_string(&obj)?;
    Ok(yaml)
}

// ---------------------------------------------------------------------------
// kubectl fallbacks
// ---------------------------------------------------------------------------

async fn fetch_describe_via_kubectl(resource: &str, name: &str, namespace: &str, context: &str) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("describe").arg(resource).arg(name);
    if !context.is_empty() { cmd.arg("--context").arg(context); }
    if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
    match cmd.output().await {
        Ok(output) if output.status.success() => {
            crate::util::strip_ansi(&String::from_utf8_lossy(&output.stdout))
        }
        Ok(output) => format!("Error running kubectl describe:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}

async fn fetch_yaml_via_kubectl(resource: &str, name: &str, namespace: &str, context: &str) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("get").arg(resource).arg(name).arg("-o").arg("yaml");
    if !context.is_empty() { cmd.arg("--context").arg(context); }
    if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
    match cmd.output().await {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).to_string(),
        Ok(output) => format!("Error fetching YAML:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}
