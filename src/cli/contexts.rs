use anyhow::Result;

/// List cached contexts from disk cache files at `~/.cache/k9rs/*.json`.
pub async fn run() -> Result<()> {
    let cache_dir = match crate::kube::cache::cache_dir() {
        Some(d) => d,
        None => {
            println!("Cache directory not found");
            return Ok(());
        }
    };

    let entries = match std::fs::read_dir(&cache_dir) {
        Ok(e) => e,
        Err(_) => {
            println!("No cache directory at {}", cache_dir.display());
            return Ok(());
        }
    };

    let mut found = false;
    for entry in entries.flatten() {
        if entry.path().extension().map_or(false, |x| x == "json") {
            if let Ok(data) = std::fs::read_to_string(entry.path()) {
                if let Ok(dc) =
                    serde_json::from_str::<crate::kube::cache::DiscoveryCache>(&data)
                {
                    println!(
                        "{}: {} namespaces, {} crds (updated {})",
                        dc.context,
                        dc.namespaces.len(),
                        dc.crds.len(),
                        dc.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                    found = true;
                }
            }
        }
    }

    if !found {
        println!("No cached contexts found in {}", cache_dir.display());
    }

    Ok(())
}
