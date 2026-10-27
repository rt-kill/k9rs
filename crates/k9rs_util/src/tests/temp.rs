//! Foreign resources `Drop` has to reach for.


// ---------------------------------------------------------------------------
// Foreign resources Drop has to reach for (2026-09 audit)
// ---------------------------------------------------------------------------

/// The process temp directory is reclaimed — but only when it holds nothing.
///
/// `TempFile::drop` removed a FILE; nothing owned the DIRECTORY, so every run
/// that edited a resource left a `k9rs-<pid>/` behind forever. The cleanup
/// must not be recursive: `SaveTable` and `SaveLogs` write there on purpose
/// and tell the user the path.
#[test]
fn the_temp_dir_is_reclaimed_only_when_empty() {
    let base = std::env::temp_dir().join(format!("k9rs-reclaim-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir(&base).expect("scratch dir");

    // Something the user asked us to save → left alone.
    let saved = base.join("pods-20260914.txt");
    std::fs::write(&saved, b"rows").expect("write");
    super::reclaim_if_empty(&base);
    assert!(base.exists(), "a directory with saved output survives");
    assert!(saved.exists(), "…and so does the file in it");

    // Once nothing of ours is left, the directory goes too.
    std::fs::remove_file(&saved).expect("remove");
    super::reclaim_if_empty(&base);
    assert!(!base.exists(), "an empty temp directory is reclaimed");
}
