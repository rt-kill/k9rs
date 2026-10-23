//! Reading the kubeconfig — the one place the client side parses it.
//!
//! The TUI used to call `kube::config::Kubeconfig::read()` from `app::client`
//! and `cli::contexts` called it a second time, which made kube-rs a
//! dependency of both layers for the sake of three strings. Nothing else up
//! there named it, so this module is what lets the claim in [`crate`]'s doc
//! be a manifest fact: above `k9rs_kube` and `k9rs_daemon` there is no
//! kube-rs to name.
//!
//! The type is called `Kubeconfig` like kube-rs's, because that is what it
//! is; theirs is written out in full (`kube::config::Kubeconfig`) at the two
//! places it appears, which is also the shortest way to see that it appears
//! nowhere else.

use anyhow::Result;

use k9rs_proto::protocol::{ClusterIdentity, ContextName};

/// One context, as the picker and the header need it.
///
/// A nameless entry cannot be built: [`ContextName::new`] refuses the empty
/// string, and the reader drops what it refuses rather than listing a blank
/// row the cursor can land on.
#[derive(Debug, Clone)]
pub struct KubeContext {
    pub name: ContextName,
    pub identity: ClusterIdentity,
    pub is_current: bool,
}

/// The kubeconfig, read once and projected into our own vocabulary.
///
/// Three consumers, one disk read and one parse: `current` and `yaml` are the
/// daemon's Init payload, `contexts` is what the picker lists, and
/// [`Kubeconfig::identity_of`] answers the header's question about whichever
/// context is in play.
pub struct Kubeconfig {
    /// Which context this session is aiming at, or `None` when the file names
    /// none usable.
    ///
    /// `and_then(ContextName::new)` is the load-bearing bit: `kubectl config
    /// unset current-context` does not remove the key, it writes
    /// `current-context: ""`, so kube-rs hands back `Some("")`. Absence has to
    /// collapse to `None` HERE — the one place a name enters the process — or
    /// it travels on as a context named "" and the daemon fails on a name
    /// that cannot exist.
    pub current: Option<ContextName>,
    /// Every named context, in file order.
    pub contexts: Vec<KubeContext>,
    /// The file re-serialized, for shipping to the daemon.
    pub yaml: String,
}

impl Kubeconfig {
    /// Read and parse the kubeconfig, preferring `requested` (the `--context`
    /// flag) over the file's own `current-context`.
    pub fn read(requested: Option<&ContextName>) -> Result<Self> {
        let raw = kube::config::Kubeconfig::read()
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

        let current = requested
            .cloned()
            .or_else(|| raw.current_context.clone().and_then(ContextName::new));

        let contexts = raw
            .contexts
            .iter()
            .filter_map(|nc| {
                let name = ContextName::new(nc.name.as_str())?;
                let identity = nc
                    .context
                    .as_ref()
                    .map(|c| {
                        ClusterIdentity::new(c.cluster.clone(), c.user.clone().unwrap_or_default())
                    })
                    .unwrap_or_default();
                Some(KubeContext {
                    is_current: current.as_ref() == Some(&name),
                    name,
                    identity,
                })
            })
            .collect();

        let yaml = serde_yaml::to_string(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to serialize kubeconfig: {}", e))?;

        Ok(Self { current, contexts, yaml })
    }

    /// The cluster + user names for `context`.
    ///
    /// A context this file does not name — or one whose entry names no user —
    /// yields an all-unknown [`ClusterIdentity`], which every reader must
    /// destructure rather than read as a blank string.
    pub fn identity_of(&self, context: &ContextName) -> ClusterIdentity {
        self.contexts
            .iter()
            .find(|c| c.name == *context)
            .map(|c| c.identity.clone())
            .unwrap_or_default()
    }
}
