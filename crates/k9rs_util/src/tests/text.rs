//! What is visible, and what must never reach a terminal.

use super::*;

#[test]
fn sanitize_terminal_drops_escapes_keeps_tab() {
    // The OSC 52 clipboard payload and a cursor-report both vanish.
    assert_eq!(sanitize_terminal("a\x1b]52;c;Zm9v\x07b"), "a]52;c;Zm9vb");
    assert_eq!(sanitize_terminal("x\x1b[6ny"), "x[6ny");
    // Tab survives; CR/BEL/DEL/C1 do not.
    assert_eq!(sanitize_terminal("a\tb\r\x07\x7f\u{0090}c"), "a\tbc");
    // Plain text is unchanged.
    assert_eq!(sanitize_terminal("pod-1 Running"), "pod-1 Running");
}
