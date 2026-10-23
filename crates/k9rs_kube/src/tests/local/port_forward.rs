use super::*;
use k9rs_proto::protocol::{ContextName, Namespace, ObjectRef, ResourceId};
use k9rs_proto::resource_def::BuiltInKind;

/// A client pointed at a port nothing listens on. The forward's supervised
/// tunnel will fail to resolve and back off, which is irrelevant here — the
/// entry and its label exist the moment `create` returns.
fn offline_client() -> kube::Client {
    let config = kube::Config::new("http://127.0.0.1:1".parse().expect("a valid uri"));
    kube::Client::try_from(config).expect("offline client build")
}

fn request(context: &str, local_port: u16) -> PortForwardRequest {
    PortForwardRequest {
        target: ObjectRef {
            resource: ResourceId::BuiltIn(BuiltInKind::Pod),
            namespace: Namespace::Named("default".into()),
            name: "canarypod".into(),
        },
        context: ContextName::new(context).expect("non-empty"),
        kubectl_target: "pods/canarypod".into(),
        namespace: Namespace::Named("default".into()),
        local_port,
        remote_port: 8080,
        client: offline_client(),
    }
}

fn label_of(source: &PortForwardSource, id: u64) -> String {
    source.entries.get(&id).expect("entry exists").entry.context.as_str().to_string()
}

#[tokio::test]
async fn each_forward_records_the_context_that_created_it() {
    // Two kubeconfig contexts pointing at ONE cluster with ONE set of
    // credentials resolve to the same `ContextId` — the context NAME is
    // deliberately excluded from that identity, so watchers are shared by
    // actual endpoint rather than by a label that collides across kubeconfig
    // files. They therefore share this source, which is correct.
    //
    // What was NOT correct: the label. `create` stamped each entry from a
    // `bound_context` field on the SOURCE, i.e. whichever context happened to
    // attach first — and `PortForwardEntry` is `Serialize`, so that wrong name
    // reached the user on the describe/yaml view of every forward created from
    // the other context. The name now rides the request, where the session
    // that actually knows it puts it.
    let source = PortForwardSource::for_context();
    let from_a = source.create(request("ctx-a", 15001));
    let from_b = source.create(request("ctx-b", 15002));

    assert_eq!(label_of(&source, from_a), "ctx-a");
    assert_eq!(
        label_of(&source, from_b),
        "ctx-b",
        "the second context's forward must not inherit the first's name",
    );
}
