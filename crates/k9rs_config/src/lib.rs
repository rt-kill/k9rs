//! Where configuration comes from.
//!
//! ONE file (`~/.config/k9rs/config.yaml`), ONE root key (`k9rs`), and
//! sections with different owners: the TUI interprets `keys`/`ui`/`readOnly`
//! (see `k9rs_app::config`), and machinery inside `k9rs_kube` interprets
//! `daemon`. Neither of those is this crate's business. What *is* its
//! business is the one question both of them used to answer separately —
//! which file, which root, and what a missing section means versus a broken
//! one.
//!
//! It is a crate rather than a module because of where the reader used to
//! live: `k9rs_kube::daemon_config::load_section`, so the TUI read its own
//! config through the DAEMON's module, and the path lived in the crate least
//! entitled to decide it. Down here, neither layer owns it.
//!
//! The I/O is separated from the parse on purpose. [`section_of`] is pure, so
//! the contract below is testable; [`load_section`] is the only function in
//! the tree that knows the path. Before the split there was no test of the
//! `Ok(None)`-versus-`Err` distinction at all, because the only way to reach
//! it was to change `$HOME`.

use std::path::PathBuf;

/// The config file. The one place this path is written.
pub fn path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/k9rs/config.yaml"))
}

/// Extract and deserialize one section of an already-read config file.
///
/// - `Ok(None)` — the `k9rs` root or the requested section is ABSENT.
///   Expected; callers use their defaults.
/// - `Err` — something IS there and is invalid (YAML syntax error, or strict
///   deserialization failed). The user wrote something and it is being
///   rejected, so callers must never silently collapse this into "use
///   defaults": a one-letter typo in `keys:` would revert the whole config
///   (`readOnly` included) with zero feedback, since the TUI's tracing writes
///   to a sink without `--log-file`.
///
/// `section` is `""` for the `k9rs` root itself, or a key beneath it.
pub fn section_of<T: serde::de::DeserializeOwned>(
    yaml: &str,
    section: &str,
    // Named only so errors can say WHICH file; this function reads nothing.
    label: &str,
) -> Result<Option<T>, String> {
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|e| format!("{label}: YAML syntax error: {e}"))?;
    let Some(root) = parsed.get("k9rs") else { return Ok(None) };
    let value = if section.is_empty() {
        root.clone()
    } else {
        match root.get(section) {
            Some(v) => v.clone(),
            None => return Ok(None),
        }
    };
    serde_yaml::from_value(value).map(Some).map_err(|e| {
        let named = if section.is_empty() { "k9rs" } else { section };
        format!("{label}: invalid '{named}' section: {e}")
    })
}

/// Read [`path`] and hand the requested section to [`section_of`].
///
/// A missing file is `Ok(None)`, same as a missing section — there is nothing
/// to reject. An unreadable one is an `Err`: it exists and we cannot honour
/// it.
pub fn load_section<T: serde::de::DeserializeOwned>(section: &str) -> Result<Option<T>, String> {
    let Some(path) = path() else { return Ok(None) };
    let yaml = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {}: {}", path.display(), e)),
    };
    section_of(&yaml, section, &path.display().to_string())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
