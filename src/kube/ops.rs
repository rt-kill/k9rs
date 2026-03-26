use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::event::{AppEvent, ResourceUpdate};

/// Try to copy text to the system clipboard using available tools.
/// Returns `true` on success.
pub fn try_copy_to_clipboard(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let tools: &[(&str, &[&str])] = &[
        ("xclip", &["-selection", "clipboard"]),
        ("xsel", &["--clipboard", "--input"]),
        ("wl-copy", &[]),
        ("pbcopy", &[]),
    ];

    for (tool, args) in tools {
        if let Ok(mut child) = Command::new(tool)
            .args(*args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            if let Some(ref mut stdin) = child.stdin {
                let _ = stdin.write_all(text.as_bytes());
            }
            if let Ok(status) = child.wait() {
                if status.success() {
                    return true;
                }
            }
        }
    }
    false
}

/// Execute a delete API call for the given resource.
pub async fn execute_delete(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<()> {
    use k8s_openapi::api::{
        apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
        autoscaling::v1::HorizontalPodAutoscaler,
        batch::v1::{CronJob, Job},
        core::v1::{
            ConfigMap, Endpoints, LimitRange, Namespace, Node, PersistentVolume,
            PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
        },
        networking::v1::{Ingress, NetworkPolicy},
        policy::v1::PodDisruptionBudget,
        rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
        storage::v1::StorageClass,
    };
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use ::kube::api::DeleteParams;
    use ::kube::Api;

    let dp = DeleteParams::default();

    macro_rules! delete_namespaced {
        ($k8s_type:ty) => {{
            let api: Api<$k8s_type> = if namespace.is_empty() {
                Api::default_namespaced(client.clone())
            } else {
                Api::namespaced(client.clone(), namespace)
            };
            api.delete(name, &dp).await?;
        }};
    }

    macro_rules! delete_cluster {
        ($k8s_type:ty) => {{
            let api: Api<$k8s_type> = Api::all(client.clone());
            api.delete(name, &dp).await?;
        }};
    }

    match resource {
        "pod" => delete_namespaced!(Pod),
        "deployment" => delete_namespaced!(Deployment),
        "service" => delete_namespaced!(Service),
        "configmap" => delete_namespaced!(ConfigMap),
        "secret" => delete_namespaced!(Secret),
        "statefulset" => delete_namespaced!(StatefulSet),
        "daemonset" => delete_namespaced!(DaemonSet),
        "job" => delete_namespaced!(Job),
        "cronjob" => delete_namespaced!(CronJob),
        "replicaset" => delete_namespaced!(ReplicaSet),
        "ingress" => delete_namespaced!(Ingress),
        "networkpolicy" => delete_namespaced!(NetworkPolicy),
        "serviceaccount" => delete_namespaced!(ServiceAccount),
        "pvc" => delete_namespaced!(PersistentVolumeClaim),
        "role" => delete_namespaced!(Role),
        "rolebinding" => delete_namespaced!(RoleBinding),
        "hpa" => delete_namespaced!(HorizontalPodAutoscaler),
        "endpoints" => delete_namespaced!(Endpoints),
        "limitrange" => delete_namespaced!(LimitRange),
        "resourcequota" => delete_namespaced!(ResourceQuota),
        "poddisruptionbudget" => delete_namespaced!(PodDisruptionBudget),
        "namespace" => delete_cluster!(Namespace),
        "node" => delete_cluster!(Node),
        "pv" => delete_cluster!(PersistentVolume),
        "storageclass" => delete_cluster!(StorageClass),
        "clusterrole" => delete_cluster!(ClusterRole),
        "clusterrolebinding" => delete_cluster!(ClusterRoleBinding),
        "customresourcedefinition" => delete_cluster!(CustomResourceDefinition),
        "event" => delete_namespaced!(k8s_openapi::api::core::v1::Event),
        other => {
            // Dynamic CRD resources: use kubectl delete as fallback since we
            // can't construct typed Api objects for arbitrary CRDs.
            let mut cmd = tokio::process::Command::new("kubectl");
            cmd.arg("delete").arg(other).arg(name);
            if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
            let output = cmd.output().await?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow::anyhow!("{}", stderr.trim()));
            }
        }
    }

    Ok(())
}

pub async fn restart_via_patch(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<()> {
    use ::kube::api::{Api, DynamicObject, Patch, PatchParams};

    let (ar, scope) = crate::kube::describe::resolve_api_resource(client, resource).await?;

    let api: Api<DynamicObject> = if namespace.is_empty() || scope == "Cluster" {
        Api::all_with(client.clone(), &ar)
    } else {
        Api::namespaced_with(client.clone(), namespace, &ar)
    };

    let patch = serde_json::json!({
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": chrono::Utc::now().to_rfc3339()
                    }
                }
            }
        }
    });

    api.patch(name, &PatchParams::apply("k9rs"), &Patch::Merge(&patch)).await?;
    Ok(())
}

/// Spawn a kubectl logs streaming task, returning a `JoinHandle` that can be
/// cancelled to stop the stream.  This centralises the command-building logic
/// that was previously duplicated across four call-sites.
pub fn spawn_log_stream(
    tx: mpsc::Sender<AppEvent>,
    target: String,
    namespace: String,
    container: String,
    context: String,
    follow: bool,
    tail: Option<u64>,
    since: Option<String>,
    previous: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::process::Command;

        let mut cmd = Command::new("kubectl");
        cmd.arg("logs");

        if follow {
            cmd.arg("-f");
        }
        if previous {
            cmd.arg("--previous");
        }

        cmd.arg(&target);

        if !namespace.is_empty() {
            cmd.arg("-n").arg(&namespace);
        }

        if !container.is_empty() && container != "all" {
            cmd.arg("-c").arg(&container);
        } else if container == "all" {
            cmd.arg("--all-containers=true");
        }

        if let Some(ref s) = since {
            cmd.arg(format!("--since={}", s));
        } else if let Some(t) = tail {
            cmd.arg("--tail").arg(t.to_string());
        }

        if !context.is_empty() {
            cmd.arg("--context").arg(&context);
        }

        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let end_label = if previous {
            "--- Previous logs ended ---"
        } else {
            "--- Stream ended ---"
        };

        match cmd.spawn() {
            Ok(mut child) => {
                // Read stderr in a separate task so errors appear in the UI
                if let Some(stderr) = child.stderr.take() {
                    let stderr_tx = tx.clone();
                    tokio::spawn(async move {
                        let reader = BufReader::new(stderr);
                        let mut lines = reader.lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let clean = crate::util::strip_ansi(&line);
                            let _ = stderr_tx
                                .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                                    format!("[stderr] {}", clean),
                                )))
                                .await;
                        }
                    });
                }
                if let Some(stdout) = child.stdout.take() {
                    let reader = BufReader::new(stdout);
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let clean = crate::util::strip_ansi(&line);
                        if tx
                            .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(clean)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                let _ = tx.send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                    end_label.to_string()
                ))).await;
                // kill_on_drop(true) handles the abort path; this covers
                // normal completion where the stream ends before cancellation.
                let _ = child.kill().await;
            }
            Err(e) => {
                let _ = tx
                    .send(AppEvent::ResourceUpdate(ResourceUpdate::LogLine(
                        format!("Failed to start kubectl logs: {}", e),
                    )))
                    .await;
            }
        }
    })
}
