pub mod backend;
pub mod cache;
pub mod daemon;
pub mod describe;
pub mod metrics;
pub mod ops;
pub mod protocol;
pub mod resources;
pub mod watcher;

use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Client, Config};

/// A wrapper around the kube-rs Client that tracks the active context.
pub struct KubeClient {
    client: Client,
    context: String,
}

impl KubeClient {
    /// Creates a new KubeClient.
    ///
    /// If `context` is `Some`, connects using that kubeconfig context.
    /// If `None`, uses the current-context from the default kubeconfig,
    /// or falls back to in-cluster config.
    /// Accepts an optional pre-read kubeconfig to avoid redundant file reads.
    pub async fn new(context: Option<&str>, kubeconfig: Option<Kubeconfig>) -> anyhow::Result<Self> {
        let (client, ctx_name) = match context {
            Some(ctx) => {
                let kc = match kubeconfig {
                    Some(kc) => kc,
                    None => Kubeconfig::read()?,
                };
                let options = KubeConfigOptions {
                    context: Some(ctx.to_string()),
                    ..Default::default()
                };
                let config = Config::from_custom_kubeconfig(kc, &options).await?;
                let client = Client::try_from(config)?;
                (client, ctx.to_string())
            }
            None => {
                let kc = match kubeconfig {
                    Some(kc) => kc,
                    None => match Kubeconfig::read() {
                        Ok(kc) => kc,
                        Err(_) => {
                            let config = Config::incluster()?;
                            let client = Client::try_from(config)?;
                            return Ok(Self { client, context: "in-cluster".to_string() });
                        }
                    },
                };
                let current = kc.current_context.clone().unwrap_or_default();
                let options = KubeConfigOptions {
                    context: Some(current.clone()),
                    ..Default::default()
                };
                let config = Config::from_custom_kubeconfig(kc, &options).await?;
                let client = Client::try_from(config)?;
                (client, current)
            }
        };

        Ok(Self {
            client,
            context: ctx_name,
        })
    }

    /// Returns a reference to the underlying kube-rs Client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns the name of the currently active context.
    pub fn context(&self) -> &str {
        &self.context
    }

    /// Builds a `Config` from the default kubeconfig for the given context name.
    async fn config_for_context(context: &str) -> anyhow::Result<Config> {
        let kubeconfig = Kubeconfig::read()?;
        let options = KubeConfigOptions {
            context: Some(context.to_string()),
            ..Default::default()
        };
        let config = Config::from_custom_kubeconfig(kubeconfig, &options).await?;
        Ok(config)
    }

    /// Creates a new kube Client for the given context without mutating self.
    /// This can be called from a background task.
    pub async fn create_client_for_context(context: &str) -> anyhow::Result<Client> {
        let config = Self::config_for_context(context).await?;
        let client = Client::try_from(config)?;
        Ok(client)
    }

    /// Replace the internal client and context (used after background context switch).
    pub fn set_client(&mut self, client: Client, context: &str) {
        self.client = client;
        self.context = context.to_string();
    }

    /// Returns (cluster_name, user_name) for ALL contexts from kubeconfig in one read.
    pub fn all_context_info() -> std::collections::HashMap<String, (String, String)> {
        let kubeconfig = match Kubeconfig::read() {
            Ok(kc) => kc,
            Err(_) => return std::collections::HashMap::new(),
        };
        let mut map = std::collections::HashMap::new();
        for named_ctx in &kubeconfig.contexts {
            if let Some(ref ctx) = named_ctx.context {
                map.insert(
                    named_ctx.name.clone(),
                    (ctx.cluster.clone(), ctx.user.clone().unwrap_or_default()),
                );
            }
        }
        map
    }

    /// Returns (cluster_name, user_name) for a given context from kubeconfig.
    /// Returns empty strings if not found.
    pub fn context_info(context: &str) -> (String, String) {
        let kubeconfig = match Kubeconfig::read() {
            Ok(kc) => kc,
            Err(_) => return (String::new(), String::new()),
        };
        for named_ctx in &kubeconfig.contexts {
            if named_ctx.name.as_str() == context {
                if let Some(ref ctx) = named_ctx.context {
                    return (
                        ctx.cluster.clone(),
                        ctx.user.clone().unwrap_or_default(),
                    );
                }
            }
        }
        (String::new(), String::new())
    }
}

