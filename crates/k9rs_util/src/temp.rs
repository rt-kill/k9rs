//! A per-process `0700` temp directory, and the only safe way to write into
//! it.
//!
//! `O_CREAT | O_EXCL` so a planted symlink cannot divert a write, and
//! [`TempDirGuard`] reclaims the directory on process exit — `remove_dir`,
//! never recursive, so a file someone else is still using survives rather
//! than being deleted out from under them.

// ---------------------------------------------------------------------------
// Per-process safe temp directory
// ---------------------------------------------------------------------------

/// Path to the per-process temp directory, created on first use with mode
/// `0700`. Used by every "save this to disk" path (edit YAML, save logs,
/// save table) so we never dump predictable filenames into world-writable
/// `/tmp`. Files written into this dir use `O_CREAT | O_EXCL` so a symlink
/// planted in advance can't divert the write — see [`safe_create_temp`].
pub fn process_temp_dir() -> std::io::Result<std::path::PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    let dir = std::env::temp_dir().join(format!("k9rs-{}", std::process::id()));
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // The dir already exists — but `env::temp_dir()` is typically
            // world-writable `/tmp`, so another local user who won the PID
            // race could have pre-created it and now controls files we
            // write there (swapping the YAML we're about to apply, or
            // capturing saved output). Refuse unless it is a REAL
            // directory (not a symlink), owned by us, mode exactly 0700 —
            // the same check the daemon socket path already makes.
            verify_owned_private_dir(&dir)?;
        }
        Err(e) => return Err(e),
    }
    Ok(dir)
}

/// RAII owner of the per-process temp directory.
///
/// `TempFile` removes a FILE; nothing owned the DIRECTORY, so every run that
/// edited a resource or saved a table left a `k9rs-<pid>/` behind forever
/// (there was not one `remove_dir` in the tree). Worse than accumulation: a
/// run killed while an edit file exists leaves that file, and when the OS
/// later recycles the pid the per-process filename counter restarts at 0 —
/// so `create_new` hits EEXIST and `:e` on that resource fails with "File
/// exists" until the user cleans `/tmp` by hand.
///
/// Deliberately `remove_dir`, NOT a recursive delete: `SaveTable` and
/// `SaveLogs` write here on purpose and tell the user the path. Removing the
/// directory only when it is EMPTY reclaims exactly the files we own (edit
/// temp files, which `TempFile::drop` has already taken) and never touches
/// something the user asked us to write.
pub struct TempDirGuard;

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let Ok(dir) = process_temp_dir() else { return };
        reclaim_if_empty(&dir);
    }
}

/// Remove `dir` only if it holds nothing. Split out so the policy is
/// testable without touching the real process directory.
fn reclaim_if_empty(dir: &std::path::Path) {
    // `remove_dir` fails with ENOTEMPTY rather than deleting, which is the
    // whole point — a recursive delete here would take files the user asked
    // us to save.
    let _ = std::fs::remove_dir(dir);
}

/// Assert `dir` is a directory (not a symlink), owned by the current uid,
/// with mode `0700` — rejecting a hostile pre-created temp dir.
fn verify_owned_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    // `symlink_metadata` does NOT follow a final symlink, so a planted
    // symlink-to-attacker-dir is caught (it is not itself a dir).
    let md = std::fs::symlink_metadata(dir)?;
    let deny = |msg: &str| Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, msg));
    if !md.file_type().is_dir() {
        return deny("temp dir is not a directory (possible symlink attack)");
    }
    // SAFETY: getuid is always successful and thread-safe.
    let our_uid = unsafe { libc::getuid() };
    if md.uid() != our_uid {
        return deny("temp dir is owned by another user");
    }
    if md.mode() & 0o777 != 0o700 {
        return deny("temp dir has unexpected permissions");
    }
    Ok(())
}

/// Atomically create a fresh file inside [`process_temp_dir`] and return
/// its path + an open `File` handle. Refuses to follow symlinks or
/// overwrite an existing entry. Caller is responsible for writing content
/// and dropping the handle.
pub fn safe_create_temp(filename: &str) -> std::io::Result<(std::path::PathBuf, std::fs::File)> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = process_temp_dir()?;
    let path = dir.join(filename);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    Ok((path, f))
}

/// Convenience wrapper: create + write the entire payload. Returns the
/// final path.
pub fn safe_write_temp(filename: &str, content: &[u8]) -> std::io::Result<std::path::PathBuf> {
    use std::io::Write as _;
    let (path, mut f) = safe_create_temp(filename)?;
    // `safe_create_temp` used `O_CREAT|O_EXCL`, so the file EXISTS on disk
    // from here on. A bare `?` on the write left it behind with no owner —
    // and because the per-process name counter restarts at 0 in a later run,
    // a zero-byte orphan makes the next `create_new` for that resource fail
    // with EEXIST. Clean up what we created before reporting the failure.
    if let Err(e) = f.write_all(content) {
        drop(f);
        let _ = std::fs::remove_file(&path);
        return Err(e);
    }
    Ok(path)
}

#[cfg(test)]
#[path = "tests/temp.rs"]
mod tests;
