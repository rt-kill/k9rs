/// Fetch a describe-like view for a resource using the kube-rs client directly.
/// Falls back to kubectl describe for resource types that fail native fetch.
pub async fn fetch_describe_native(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    match fetch_describe_via_api(client, resource, name, namespace).await {
        Ok(describe) => describe,
        Err(_) => {
            // Fallback to kubectl describe for complex/unknown resource types
            fetch_describe_via_kubectl(resource, name, namespace, context).await
        }
    }
}

/// Build a describe-like output from a DynamicObject fetched via kube-rs.
async fn fetch_describe_via_api(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<String> {
    use ::kube::api::{Api, DynamicObject};

    let (ar, scope) = resolve_api_resource(client, resource).await?;

    let api: Api<DynamicObject> = if namespace.is_empty() || scope == "Cluster" {
        Api::all_with(client.clone(), &ar)
    } else {
        Api::namespaced_with(client.clone(), namespace, &ar)
    };

    let obj = api.get(name).await?;
    let val = serde_json::to_value(&obj)?;
    Ok(format_describe_output(&val, resource))
}

/// Format a JSON object into a human-readable describe-like output.
fn format_describe_output(val: &serde_json::Value, resource: &str) -> String {
    let mut out = String::new();

    // Name
    if let Some(name) = val.pointer("/metadata/name").and_then(|v| v.as_str()) {
        out.push_str(&format!("Name:         {}\n", name));
    }

    // Namespace
    if let Some(ns) = val.pointer("/metadata/namespace").and_then(|v| v.as_str()) {
        out.push_str(&format!("Namespace:    {}\n", ns));
    }

    // Creation timestamp
    if let Some(ts) = val.pointer("/metadata/creationTimestamp").and_then(|v| v.as_str()) {
        out.push_str(&format!("Created:      {}\n", ts));
    }

    // Labels
    if let Some(labels) = val.pointer("/metadata/labels").and_then(|v| v.as_object()) {
        out.push_str("Labels:\n");
        for (k, v) in labels {
            out.push_str(&format!("              {}={}\n", k, v.as_str().unwrap_or("")));
        }
    } else {
        out.push_str("Labels:       <none>\n");
    }

    // Annotations
    if let Some(annotations) = val.pointer("/metadata/annotations").and_then(|v| v.as_object()) {
        out.push_str("Annotations:\n");
        for (k, v) in annotations {
            out.push_str(&format!("              {}={}\n", k, v.as_str().unwrap_or("")));
        }
    } else {
        out.push_str("Annotations:  <none>\n");
    }

    out.push('\n');

    // Resource-specific sections
    match resource {
        "pod" => format_describe_pod(val, &mut out),
        "deployment" => format_describe_deployment(val, &mut out),
        "service" => format_describe_service(val, &mut out),
        "node" => format_describe_node(val, &mut out),
        _ => format_describe_generic(val, &mut out),
    }

    out
}

fn format_describe_pod(val: &serde_json::Value, out: &mut String) {
    // Status
    if let Some(phase) = val.pointer("/status/phase").and_then(|v| v.as_str()) {
        out.push_str(&format!("Status:       {}\n", phase));
    }

    // Node
    if let Some(node) = val.pointer("/spec/nodeName").and_then(|v| v.as_str()) {
        out.push_str(&format!("Node:         {}\n", node));
    }

    // IP
    if let Some(ip) = val.pointer("/status/podIP").and_then(|v| v.as_str()) {
        out.push_str(&format!("IP:           {}\n", ip));
    }

    // Service Account
    if let Some(sa) = val.pointer("/spec/serviceAccountName").and_then(|v| v.as_str()) {
        out.push_str(&format!("Service Account: {}\n", sa));
    }

    out.push('\n');

    // Containers
    if let Some(containers) = val.pointer("/spec/containers").and_then(|v| v.as_array()) {
        out.push_str("Containers:\n");
        let statuses = val.pointer("/status/containerStatuses").and_then(|v| v.as_array());
        for container in containers {
            let cname = container.get("name").and_then(|v| v.as_str()).unwrap_or("?");
            let image = container.get("image").and_then(|v| v.as_str()).unwrap_or("?");
            out.push_str(&format!("  {}:\n", cname));
            out.push_str(&format!("    Image:    {}\n", image));

            // Ports
            if let Some(ports) = container.get("ports").and_then(|v| v.as_array()) {
                let port_strs: Vec<String> = ports.iter().map(|p| {
                    let port = p.get("containerPort").and_then(|v| v.as_u64()).unwrap_or(0);
                    let proto = p.get("protocol").and_then(|v| v.as_str()).unwrap_or("TCP");
                    format!("{}/{}", port, proto)
                }).collect();
                out.push_str(&format!("    Ports:    {}\n", port_strs.join(", ")));
            }

            // Resource requests/limits
            if let Some(resources) = container.get("resources") {
                if let Some(limits) = resources.get("limits").and_then(|v| v.as_object()) {
                    let parts: Vec<String> = limits.iter().map(|(k, v)| {
                        format!("{}={}", k, v.as_str().unwrap_or(""))
                    }).collect();
                    out.push_str(&format!("    Limits:   {}\n", parts.join(", ")));
                }
                if let Some(requests) = resources.get("requests").and_then(|v| v.as_object()) {
                    let parts: Vec<String> = requests.iter().map(|(k, v)| {
                        format!("{}={}", k, v.as_str().unwrap_or(""))
                    }).collect();
                    out.push_str(&format!("    Requests: {}\n", parts.join(", ")));
                }
            }

            // Container status from status.containerStatuses
            if let Some(statuses) = statuses {
                if let Some(status) = statuses.iter().find(|s| {
                    s.get("name").and_then(|v| v.as_str()) == Some(cname)
                }) {
                    let ready = status.get("ready").and_then(|v| v.as_bool()).unwrap_or(false);
                    let restart_count = status.get("restartCount").and_then(|v| v.as_u64()).unwrap_or(0);
                    out.push_str(&format!("    Ready:    {}\n", ready));
                    out.push_str(&format!("    Restarts: {}\n", restart_count));

                    // State
                    if let Some(state) = status.get("state").and_then(|v| v.as_object()) {
                        for (state_name, state_detail) in state {
                            out.push_str(&format!("    State:    {}\n", state_name));
                            if let Some(detail_obj) = state_detail.as_object() {
                                for (dk, dv) in detail_obj {
                                    out.push_str(&format!("      {}:  {}\n", dk, format_json_value_inline(dv)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Init containers
    if let Some(init_containers) = val.pointer("/spec/initContainers").and_then(|v| v.as_array()) {
        if !init_containers.is_empty() {
            out.push_str("\nInit Containers:\n");
            for container in init_containers {
                let cname = container.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                let image = container.get("image").and_then(|v| v.as_str()).unwrap_or("?");
                out.push_str(&format!("  {}:\n", cname));
                out.push_str(&format!("    Image:    {}\n", image));
            }
        }
    }

    out.push('\n');

    // Conditions
    if let Some(conditions) = val.pointer("/status/conditions").and_then(|v| v.as_array()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                 Status\n");
        out.push_str("  ----                 ------\n");
        for cond in conditions {
            let ctype = cond.get("type").and_then(|v| v.as_str()).unwrap_or("?");
            let status = cond.get("status").and_then(|v| v.as_str()).unwrap_or("?");
            out.push_str(&format!("  {:<22}{}\n", ctype, status));
        }
    }

    // Volumes
    if let Some(volumes) = val.pointer("/spec/volumes").and_then(|v| v.as_array()) {
        out.push_str("\nVolumes:\n");
        for vol in volumes {
            let vname = vol.get("name").and_then(|v| v.as_str()).unwrap_or("?");
            out.push_str(&format!("  {}:\n", vname));
            // Show volume type (first non-name key)
            if let Some(obj) = vol.as_object() {
                for (k, v) in obj {
                    if k != "name" {
                        out.push_str(&format!("    Type: {}\n", k));
                        if let Some(inner) = v.as_object() {
                            for (ik, iv) in inner {
                                out.push_str(&format!("    {}:  {}\n", ik, format_json_value_inline(iv)));
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    // Tolerations
    if let Some(tolerations) = val.pointer("/spec/tolerations").and_then(|v| v.as_array()) {
        out.push_str("\nTolerations:\n");
        for tol in tolerations {
            let key = tol.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let op = tol.get("operator").and_then(|v| v.as_str()).unwrap_or("Equal");
            let effect = tol.get("effect").and_then(|v| v.as_str()).unwrap_or("");
            let value = tol.get("value").and_then(|v| v.as_str()).unwrap_or("");
            if key.is_empty() {
                out.push_str(&format!("  op={}\n", op));
            } else if value.is_empty() {
                out.push_str(&format!("  {}:{} for {}\n", key, op, if effect.is_empty() { "NoSchedule" } else { effect }));
            } else {
                out.push_str(&format!("  {}={}:{}\n", key, value, if effect.is_empty() { "NoSchedule" } else { effect }));
            }
        }
    }
}

fn format_describe_deployment(val: &serde_json::Value, out: &mut String) {
    // Replicas
    if let Some(spec_replicas) = val.pointer("/spec/replicas").and_then(|v| v.as_u64()) {
        let ready = val.pointer("/status/readyReplicas").and_then(|v| v.as_u64()).unwrap_or(0);
        let available = val.pointer("/status/availableReplicas").and_then(|v| v.as_u64()).unwrap_or(0);
        let updated = val.pointer("/status/updatedReplicas").and_then(|v| v.as_u64()).unwrap_or(0);
        out.push_str(&format!("Replicas:     {} desired | {} updated | {} total | {} available | {} ready\n",
            spec_replicas, updated, spec_replicas, available, ready));
    }

    // Strategy
    if let Some(strategy) = val.pointer("/spec/strategy/type").and_then(|v| v.as_str()) {
        out.push_str(&format!("Strategy:     {}\n", strategy));
    }

    // Selector
    if let Some(match_labels) = val.pointer("/spec/selector/matchLabels").and_then(|v| v.as_object()) {
        let parts: Vec<String> = match_labels.iter().map(|(k, v)| {
            format!("{}={}", k, v.as_str().unwrap_or(""))
        }).collect();
        out.push_str(&format!("Selector:     {}\n", parts.join(",")));
    }

    out.push('\n');

    // Pod template containers
    if let Some(containers) = val.pointer("/spec/template/spec/containers").and_then(|v| v.as_array()) {
        out.push_str("Pod Template:\n");
        for container in containers {
            let cname = container.get("name").and_then(|v| v.as_str()).unwrap_or("?");
            let image = container.get("image").and_then(|v| v.as_str()).unwrap_or("?");
            out.push_str(&format!("  {}:\n", cname));
            out.push_str(&format!("    Image:    {}\n", image));

            if let Some(ports) = container.get("ports").and_then(|v| v.as_array()) {
                let port_strs: Vec<String> = ports.iter().map(|p| {
                    let port = p.get("containerPort").and_then(|v| v.as_u64()).unwrap_or(0);
                    let proto = p.get("protocol").and_then(|v| v.as_str()).unwrap_or("TCP");
                    format!("{}/{}", port, proto)
                }).collect();
                out.push_str(&format!("    Ports:    {}\n", port_strs.join(", ")));
            }

            if let Some(env) = container.get("env").and_then(|v| v.as_array()) {
                out.push_str("    Environment:\n");
                for e in env {
                    let ename = e.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    if let Some(value) = e.get("value").and_then(|v| v.as_str()) {
                        out.push_str(&format!("      {}={}\n", ename, value));
                    } else if e.get("valueFrom").is_some() {
                        out.push_str(&format!("      {} (from ref)\n", ename));
                    }
                }
            }
        }
    }

    out.push('\n');

    // Conditions
    if let Some(conditions) = val.pointer("/status/conditions").and_then(|v| v.as_array()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                 Status  Reason\n");
        out.push_str("  ----                 ------  ------\n");
        for cond in conditions {
            let ctype = cond.get("type").and_then(|v| v.as_str()).unwrap_or("?");
            let status = cond.get("status").and_then(|v| v.as_str()).unwrap_or("?");
            let reason = cond.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            out.push_str(&format!("  {:<22}{:<8}{}\n", ctype, status, reason));
        }
    }
}

fn format_describe_service(val: &serde_json::Value, out: &mut String) {
    if let Some(svc_type) = val.pointer("/spec/type").and_then(|v| v.as_str()) {
        out.push_str(&format!("Type:         {}\n", svc_type));
    }
    if let Some(cluster_ip) = val.pointer("/spec/clusterIP").and_then(|v| v.as_str()) {
        out.push_str(&format!("ClusterIP:    {}\n", cluster_ip));
    }
    if let Some(external_ips) = val.pointer("/spec/externalIPs").and_then(|v| v.as_array()) {
        let ips: Vec<&str> = external_ips.iter().filter_map(|v| v.as_str()).collect();
        out.push_str(&format!("ExternalIPs:  {}\n", ips.join(", ")));
    }
    if let Some(lb_ip) = val.pointer("/status/loadBalancer/ingress").and_then(|v| v.as_array()) {
        let ips: Vec<String> = lb_ip.iter().map(|i| {
            i.get("ip").or(i.get("hostname")).and_then(|v| v.as_str()).unwrap_or("").to_string()
        }).collect();
        if !ips.is_empty() {
            out.push_str(&format!("LoadBalancer: {}\n", ips.join(", ")));
        }
    }

    // Selector
    if let Some(selector) = val.pointer("/spec/selector").and_then(|v| v.as_object()) {
        let parts: Vec<String> = selector.iter().map(|(k, v)| {
            format!("{}={}", k, v.as_str().unwrap_or(""))
        }).collect();
        out.push_str(&format!("Selector:     {}\n", parts.join(",")));
    }

    // Ports
    if let Some(ports) = val.pointer("/spec/ports").and_then(|v| v.as_array()) {
        out.push_str("\nPorts:\n");
        for port in ports {
            let pname = port.get("name").and_then(|v| v.as_str()).unwrap_or("<unnamed>");
            let p = port.get("port").and_then(|v| v.as_u64()).unwrap_or(0);
            let tp = port.get("targetPort");
            let proto = port.get("protocol").and_then(|v| v.as_str()).unwrap_or("TCP");
            let target_str = match tp {
                Some(serde_json::Value::Number(n)) => n.to_string(),
                Some(serde_json::Value::String(s)) => s.clone(),
                _ => "?".to_string(),
            };
            out.push_str(&format!("  {} {}/{} -> {}\n", pname, p, proto, target_str));
        }
    }

    // Session affinity
    if let Some(affinity) = val.pointer("/spec/sessionAffinity").and_then(|v| v.as_str()) {
        out.push_str(&format!("Session Affinity: {}\n", affinity));
    }
}

fn format_describe_node(val: &serde_json::Value, out: &mut String) {
    // Roles (from labels)
    if let Some(labels) = val.pointer("/metadata/labels").and_then(|v| v.as_object()) {
        let roles: Vec<&str> = labels.keys()
            .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/"))
            .collect();
        if !roles.is_empty() {
            out.push_str(&format!("Roles:        {}\n", roles.join(", ")));
        }
    }

    // Addresses
    if let Some(addresses) = val.pointer("/status/addresses").and_then(|v| v.as_array()) {
        out.push_str("Addresses:\n");
        for addr in addresses {
            let atype = addr.get("type").and_then(|v| v.as_str()).unwrap_or("?");
            let address = addr.get("address").and_then(|v| v.as_str()).unwrap_or("?");
            out.push_str(&format!("  {}: {}\n", atype, address));
        }
    }

    // Capacity
    if let Some(capacity) = val.pointer("/status/capacity").and_then(|v| v.as_object()) {
        out.push_str("Capacity:\n");
        for (k, v) in capacity {
            out.push_str(&format!("  {}: {}\n", k, v.as_str().unwrap_or("")));
        }
    }

    // Allocatable
    if let Some(allocatable) = val.pointer("/status/allocatable").and_then(|v| v.as_object()) {
        out.push_str("Allocatable:\n");
        for (k, v) in allocatable {
            out.push_str(&format!("  {}: {}\n", k, v.as_str().unwrap_or("")));
        }
    }

    // System info
    if let Some(info) = val.pointer("/status/nodeInfo").and_then(|v| v.as_object()) {
        out.push_str("\nSystem Info:\n");
        let fields = ["kubeletVersion", "containerRuntimeVersion", "osImage", "architecture", "operatingSystem", "kernelVersion"];
        for field in &fields {
            if let Some(v) = info.get(*field).and_then(|v| v.as_str()) {
                out.push_str(&format!("  {}: {}\n", field, v));
            }
        }
    }

    out.push('\n');

    // Conditions
    if let Some(conditions) = val.pointer("/status/conditions").and_then(|v| v.as_array()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                   Status  Reason                   Message\n");
        out.push_str("  ----                   ------  ------                   -------\n");
        for cond in conditions {
            let ctype = cond.get("type").and_then(|v| v.as_str()).unwrap_or("?");
            let status = cond.get("status").and_then(|v| v.as_str()).unwrap_or("?");
            let reason = cond.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let message = cond.get("message").and_then(|v| v.as_str()).unwrap_or("");
            out.push_str(&format!("  {:<24}{:<8}{:<25}{}\n", ctype, status, reason, message));
        }
    }
}

fn format_describe_generic(val: &serde_json::Value, out: &mut String) {
    // For any resource type, print spec and status as indented key-value pairs
    if let Some(spec) = val.get("spec") {
        out.push_str("Spec:\n");
        format_json_indented(spec, out, 2);
        out.push('\n');
    }

    if let Some(status) = val.get("status") {
        out.push_str("Status:\n");
        format_json_indented(status, out, 2);
        out.push('\n');
    }
}

/// Format a JSON value as indented key-value lines (describe-style).
fn format_json_indented(val: &serde_json::Value, out: &mut String, indent: usize) {
    let prefix = " ".repeat(indent);
    match val {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                match v {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        out.push_str(&format!("{}{}:\n", prefix, k));
                        format_json_indented(v, out, indent + 2);
                    }
                    _ => {
                        out.push_str(&format!("{}{}: {}\n", prefix, k, format_json_value_inline(v)));
                    }
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, item) in arr.iter().enumerate() {
                match item {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        out.push_str(&format!("{}- [{}]:\n", prefix, i));
                        format_json_indented(item, out, indent + 2);
                    }
                    _ => {
                        out.push_str(&format!("{}- {}\n", prefix, format_json_value_inline(item)));
                    }
                }
            }
        }
        _ => {
            out.push_str(&format!("{}{}\n", prefix, format_json_value_inline(val)));
        }
    }
}

/// Format a simple JSON value for inline display.
fn format_json_value_inline(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "<none>".to_string(),
        other => other.to_string(),
    }
}

async fn fetch_describe_via_kubectl(
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("describe").arg(resource).arg(name);
    if !context.is_empty() {
        cmd.arg("--context").arg(context);
    }
    if !namespace.is_empty() {
        cmd.arg("-n").arg(namespace);
    }
    match cmd.output().await {
        Ok(output) => {
            if output.status.success() {
                let raw = String::from_utf8_lossy(&output.stdout).to_string();
                crate::util::strip_ansi(&raw)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                format!("Error running kubectl describe:\n{}", stderr)
            }
        }
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}

/// Fetch YAML for a resource using the kube-rs client directly.
/// Falls back to kubectl for complex resource types.
pub async fn fetch_yaml_native(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    // Try native kube-rs fetch first
    match fetch_yaml_via_discovery(client, resource, name, namespace).await {
        Ok(yaml) => yaml,
        Err(_) => {
            // Fallback to kubectl for complex/unknown resource types
            fetch_yaml_via_kubectl(resource, name, namespace, context).await
        }
    }
}

async fn fetch_yaml_via_discovery(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<String> {
    use ::kube::api::{Api, DynamicObject};

    // Map common resource type strings to their API group/version/plural
    let (ar, scope) = resolve_api_resource(client, resource).await?;

    let api: Api<DynamicObject> = if namespace.is_empty() || scope == "Cluster" {
        Api::all_with(client.clone(), &ar)
    } else {
        Api::namespaced_with(client.clone(), namespace, &ar)
    };

    let obj = api.get(name).await?;
    let yaml = serde_yaml::to_string(&obj)?;
    Ok(yaml)
}

/// Resolve a resource type string (e.g. "pod", "deployment", "service") to an ApiResource.
pub async fn resolve_api_resource(
    client: &::kube::Client,
    resource: &str,
) -> anyhow::Result<(::kube::api::ApiResource, String)> {
    use ::kube::api::ApiResource;
    use ::kube::discovery::{self, Scope};

    // Common built-in resources — avoid discovery roundtrip
    let (group, version, kind, plural, scope) = match resource {
        "pod" => ("", "v1", "Pod", "pods", "Namespaced"),
        "deployment" => ("apps", "v1", "Deployment", "deployments", "Namespaced"),
        "service" => ("", "v1", "Service", "services", "Namespaced"),
        "configmap" => ("", "v1", "ConfigMap", "configmaps", "Namespaced"),
        "secret" => ("", "v1", "Secret", "secrets", "Namespaced"),
        "statefulset" => ("apps", "v1", "StatefulSet", "statefulsets", "Namespaced"),
        "daemonset" => ("apps", "v1", "DaemonSet", "daemonsets", "Namespaced"),
        "job" => ("batch", "v1", "Job", "jobs", "Namespaced"),
        "cronjob" => ("batch", "v1", "CronJob", "cronjobs", "Namespaced"),
        "replicaset" => ("apps", "v1", "ReplicaSet", "replicasets", "Namespaced"),
        "ingress" => ("networking.k8s.io", "v1", "Ingress", "ingresses", "Namespaced"),
        "networkpolicy" => ("networking.k8s.io", "v1", "NetworkPolicy", "networkpolicies", "Namespaced"),
        "serviceaccount" => ("", "v1", "ServiceAccount", "serviceaccounts", "Namespaced"),
        "namespace" => ("", "v1", "Namespace", "namespaces", "Cluster"),
        "node" => ("", "v1", "Node", "nodes", "Cluster"),
        "pv" => ("", "v1", "PersistentVolume", "persistentvolumes", "Cluster"),
        "pvc" => ("", "v1", "PersistentVolumeClaim", "persistentvolumeclaims", "Namespaced"),
        "storageclass" => ("storage.k8s.io", "v1", "StorageClass", "storageclasses", "Cluster"),
        "role" => ("rbac.authorization.k8s.io", "v1", "Role", "roles", "Namespaced"),
        "clusterrole" => ("rbac.authorization.k8s.io", "v1", "ClusterRole", "clusterroles", "Cluster"),
        "rolebinding" => ("rbac.authorization.k8s.io", "v1", "RoleBinding", "rolebindings", "Namespaced"),
        "clusterrolebinding" => ("rbac.authorization.k8s.io", "v1", "ClusterRoleBinding", "clusterrolebindings", "Cluster"),
        "hpa" => ("autoscaling", "v1", "HorizontalPodAutoscaler", "horizontalpodautoscalers", "Namespaced"),
        "endpoints" => ("", "v1", "Endpoints", "endpoints", "Namespaced"),
        "limitrange" => ("", "v1", "LimitRange", "limitranges", "Namespaced"),
        "resourcequota" => ("", "v1", "ResourceQuota", "resourcequotas", "Namespaced"),
        "poddisruptionbudget" => ("policy", "v1", "PodDisruptionBudget", "poddisruptionbudgets", "Namespaced"),
        "event" => ("", "v1", "Event", "events", "Namespaced"),
        "customresourcedefinition" => ("apiextensions.k8s.io", "v1", "CustomResourceDefinition", "customresourcedefinitions", "Cluster"),
        _ => {
            // CRD or unknown — resource string is already "plural.group" format
            // e.g. "clickhouseinstallations.clickhouse.altinity.com"
            if resource.contains('.') {
                let parts: Vec<&str> = resource.splitn(2, '.').collect();
                let plural = parts[0];
                let group = parts.get(1).unwrap_or(&"");
                // Use discovery to find the version
                let discovery = discovery::Discovery::new(client.clone()).run().await?;
                for api_group in discovery.groups() {
                    for (ar, caps) in api_group.recommended_resources() {
                        if ar.plural == plural && ar.group == *group {
                            let scope = if caps.scope == Scope::Cluster { "Cluster" } else { "Namespaced" };
                            return Ok((ar, scope.to_string()));
                        }
                    }
                }
                return Err(anyhow::anyhow!("Resource not found: {}", resource));
            }
            return Err(anyhow::anyhow!("Unknown resource type: {}", resource));
        }
    };

    let gvk = ::kube::api::GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    Ok((ar, scope.to_string()))
}

async fn fetch_yaml_via_kubectl(
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("get").arg(resource).arg(name).arg("-o").arg("yaml");
    if !context.is_empty() {
        cmd.arg("--context").arg(context);
    }
    if !namespace.is_empty() {
        cmd.arg("-n").arg(namespace);
    }
    match cmd.output().await {
        Ok(output) => {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).to_string()
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                format!("Error fetching YAML:\n{}", stderr)
            }
        }
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}
