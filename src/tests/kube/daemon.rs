//! Unit tests for `daemon` (mirrored out of the source file).

use super::*;

/// `exe_identity` has to notice a REPLACED file at the same path, because
/// that is how cargo installs a new build: write a temp file, rename it over
/// the old one. The running process keeps the old (now unlinked) inode.
///
/// This exists because the first version of the self-exit asked
/// `std::env::current_exe()` on every tick instead of stat'ing a remembered
/// path. `current_exe()` reads `/proc/self/exe`, which resolves to the
/// running inode — so after the rename it answers `"…/k9rs (deleted)"`, that
/// stats to nothing, `exe_identity` returned `None`, and the "an error is not
/// a change" rule then swallowed the one event it was meant to catch. The
/// daemon never exited. Verified by hand against a real daemon afterwards;
/// this is the part that can be held in the suite.
#[test]
fn a_replaced_file_at_the_same_path_is_a_different_identity() {
    let dir = std::env::temp_dir().join(format!("k9rs-exe-id-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let path = dir.join("binary");

    std::fs::write(&path, b"original").expect("write");
    let before = exe_identity(&path).expect("a file that exists has an identity");

    // Same path, new file, installed by rename — exactly cargo's move.
    let staged = dir.join("binary.new");
    std::fs::write(&staged, b"a different build").expect("write");
    std::fs::rename(&staged, &path).expect("rename over");

    let after = exe_identity(&path).expect("the new file at that path");
    assert_ne!(before, after, "a replaced binary must read as a different identity");

    // And stable while nothing changes — otherwise the daemon would exit on
    // its own every tick.
    assert_eq!(after, exe_identity(&path).unwrap());

    let _ = std::fs::remove_dir_all(&dir);
}

/// A path that does not exist is "cannot tell", NOT "changed". The daemon
/// acts only on a definite difference, so an unreadable binary must never
/// make it shut itself down.
#[test]
fn an_unreadable_path_has_no_identity() {
    let missing = std::env::temp_dir().join("k9rs-definitely-not-here-7f3a");
    let _ = std::fs::remove_file(&missing);
    assert!(exe_identity(&missing).is_none());
}
