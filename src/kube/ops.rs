/// Execute a delete API call for the given resource.
pub async fn execute_delete(
    client: &::kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> anyhow::Result<()> {
    use k8s_openapi::api::{
        apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
        autoscaling::v2::HorizontalPodAutoscaler,
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

    /// Generates a match that dispatches each resource name to a typed delete
    /// call. Each arm returns `Ok(())` after a successful delete.
    macro_rules! delete_resource {
        ($res:expr, $client:expr, $ns:expr, $name:expr, $dp:expr,
         $(ns: $($np:pat_param)|+ => $nk:ty),+,
         $(cluster: $($cp:pat_param)|+ => $ck:ty),+ $(,)?
        ) => {
            match $res {
                $(
                    $($np)|+ => {
                        let api: Api<$nk> = if $ns.is_empty() {
                            Api::default_namespaced($client.clone())
                        } else {
                            Api::namespaced($client.clone(), $ns)
                        };
                        api.delete($name, &$dp).await?;
                        return Ok(());
                    }
                )+
                $(
                    $($cp)|+ => {
                        let api: Api<$ck> = Api::all($client.clone());
                        api.delete($name, &$dp).await?;
                        return Ok(());
                    }
                )+
                _ => {}
            }
        };
    }

    delete_resource!(resource, client, namespace, name, dp,
        ns: "pod"                   => Pod,
        ns: "deployment"            => Deployment,
        ns: "service"               => Service,
        ns: "configmap"             => ConfigMap,
        ns: "secret"                => Secret,
        ns: "statefulset"           => StatefulSet,
        ns: "daemonset"             => DaemonSet,
        ns: "job"                   => Job,
        ns: "cronjob"               => CronJob,
        ns: "replicaset"            => ReplicaSet,
        ns: "ingress"               => Ingress,
        ns: "networkpolicy"         => NetworkPolicy,
        ns: "serviceaccount"        => ServiceAccount,
        ns: "pvc"                   => PersistentVolumeClaim,
        ns: "role"                  => Role,
        ns: "rolebinding"           => RoleBinding,
        ns: "hpa"                   => HorizontalPodAutoscaler,
        ns: "endpoints"             => Endpoints,
        ns: "limitrange"            => LimitRange,
        ns: "resourcequota"         => ResourceQuota,
        ns: "poddisruptionbudget"   => PodDisruptionBudget,
        ns: "event"                 => k8s_openapi::api::core::v1::Event,
        cluster: "namespace"                => Namespace,
        cluster: "node"                     => Node,
        cluster: "pv"                       => PersistentVolume,
        cluster: "storageclass"             => StorageClass,
        cluster: "clusterrole"              => ClusterRole,
        cluster: "clusterrolebinding"       => ClusterRoleBinding,
        cluster: "customresourcedefinition" => CustomResourceDefinition,
    );

    // Fallback for dynamic CRD resources: use kubectl delete since we can't
    // construct typed Api objects for arbitrary CRDs.
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("delete").arg(resource).arg(name);
    if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
    if !context.is_empty() { cmd.arg("--context").arg(context); }
    let output = cmd.output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("{}", stderr.trim()));
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
    use crate::kube::protocol::ResourceScope;

    let (ar, scope) = crate::kube::describe::resolve_api_resource(client, resource).await?;

    let api: Api<DynamicObject> = match scope {
        ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
        ResourceScope::Namespaced if namespace.is_empty() => Api::default_namespaced_with(client.clone(), &ar),
        ResourceScope::Namespaced => Api::namespaced_with(client.clone(), namespace, &ar),
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

    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
}
