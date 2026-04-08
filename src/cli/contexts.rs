use anyhow::Result;

/// List available contexts from kubeconfig.
pub fn run() -> Result<()> {
    let kubeconfig = ::kube::config::Kubeconfig::read()
        .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

    let current = kubeconfig.current_context.as_deref().unwrap_or("");

    if kubeconfig.contexts.is_empty() {
        println!("No contexts found in kubeconfig");
        return Ok(());
    }

    for named_ctx in &kubeconfig.contexts {
        let marker = if named_ctx.name == current { "*" } else { " " };
        let (cluster, user) = named_ctx.context.as_ref()
            .map(|c| (c.cluster.as_str(), c.user.as_deref().unwrap_or("")))
            .unwrap_or(("", ""));
        println!("{} {:<30} cluster={:<30} user={}", marker, named_ctx.name, cluster, user);
    }

    Ok(())
}
