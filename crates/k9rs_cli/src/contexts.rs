use anyhow::Result;

/// List available contexts from kubeconfig.
pub fn run() -> Result<()> {
    // Through the shared reader, not `kube::config::Kubeconfig::read()`: this
    // was the second of the two calls that kept kube-rs in the layers above
    // `k9rs_kube`, and the projection it does (drop nameless entries, collapse
    // a blank `current-context` to absent) is the same one the TUI needs.
    let kubeconfig = k9rs_kube::kubeconfig::Kubeconfig::read(None)?;

    if kubeconfig.contexts.is_empty() {
        println!("No contexts found in kubeconfig");
        return Ok(());
    }

    for ctx in &kubeconfig.contexts {
        println!(
            "{} {:<30} cluster={:<30} user={}",
            if ctx.is_current { "*" } else { " " },
            ctx.name,
            ctx.identity.cluster().unwrap_or("n/a"),
            ctx.identity.user().unwrap_or("n/a"),
        );
    }

    Ok(())
}
