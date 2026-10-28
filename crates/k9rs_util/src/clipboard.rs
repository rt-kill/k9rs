//! Putting text on the system clipboard, if there is one.
//!
//! No test mirror, deliberately. Everything here is `Command::new(tool)`
//! against whatever the machine has installed: a test would either depend on
//! `wl-copy`/`pbcopy`/`xclip`/`xsel` being present (so it would pass or fail
//! on the runner rather than on the code), or assert the literal tool list
//! back at itself. The one thing that could deadlock — writing to a child's
//! stdin and then waiting without closing the pipe, which `xclip` reads to
//! EOF — cannot: `std::process::Child::wait` takes and drops `stdin` before
//! it waits, which is documented behaviour and not an accident of this code.

/// Try to copy text to the system clipboard using available tools.
/// Returns `true` on success.
pub fn try_copy_to_clipboard(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let tools: &[(&str, &[&str])] = &[
        ("wl-copy", &[]),       // Wayland-native (preferred on Wayland)
        ("pbcopy", &[]),        // macOS
        ("xclip", &["-selection", "clipboard"]),  // X11
        ("xsel", &["--clipboard", "--input"]),    // X11 fallback
    ];

    for (tool, args) in tools {
        if let Ok(mut child) = Command::new(tool)
            .args(*args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            if let Some(ref mut stdin) = child.stdin {
                let _ = stdin.write_all(text.as_bytes());
            }
            if let Ok(status) = child.wait() {
                if status.success() {
                    return true;
                }
            }
        }
    }
    false
}
