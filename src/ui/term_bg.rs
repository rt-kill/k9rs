//! Terminal background detection — what `ui.theme: auto` is built on.
//!
//! Terminals don't publish their color scheme, so we ask, in order:
//!
//! 1. **OSC 11** (`ESC ] 11 ; ? ST`) — the terminal answers with its actual
//!    background color. Supported by xterm, kitty, foot, wezterm, alacritty,
//!    iTerm2, Ghostty, Terminal.app and tmux.
//! 2. **`COLORFGBG`** — exported by urxvt/rxvt/konsole (and some shell
//!    setups); its trailing field is the background's ANSI palette index.
//! 3. Dark, when nothing answers — the historical default.
//!
//! Two properties matter for correctness:
//!
//! - The query runs BEFORE the TUI enables raw mode and the alternate
//!   screen (the theme is built in `App::new`, which main calls first), and
//!   it drains its own reply from the tty. Nothing leaks into crossterm's
//!   input stream as a phantom keypress.
//! - It is bounded: a DA1 chaser rides behind the OSC query, so a terminal
//!   that ignores OSC 11 still says something and we stop waiting
//!   immediately instead of burning the whole timeout. Startup pays the
//!   full [`REPLY_TIMEOUT`] only on a terminal that answers nothing at all.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Whether the terminal is showing dark text on light, or the reverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Appearance {
    Dark,
    Light,
}

/// Upper bound on how long startup may block waiting for the terminal.
const REPLY_TIMEOUT: Duration = Duration::from_millis(100);

/// Detect the terminal's appearance, defaulting to [`Appearance::Dark`].
///
/// The terminal is queried at most once per process: the answer can't
/// change under us mid-session in any way we'd act on, and the query costs
/// a tty round-trip.
pub fn detect() -> Appearance {
    static CACHED: OnceLock<Appearance> = OnceLock::new();
    *CACHED.get_or_init(|| {
        query_osc11()
            .or_else(|| std::env::var("COLORFGBG").ok().and_then(|v| classify_colorfgbg(&v)))
            .unwrap_or(Appearance::Dark)
    })
}

/// Ask the terminal for its background color over `/dev/tty`.
///
/// `/dev/tty` rather than stdout/stdin so the query still works when either
/// is redirected, and so we read exactly the bytes we provoked.
fn query_osc11() -> Option<Appearance> {
    let mut tty = OpenOptions::new().read(true).write(true).open("/dev/tty").ok()?;
    let fd = tty.as_raw_fd();
    // SAFETY: `fd` is owned by `tty` and open for the whole call.
    if unsafe { libc::isatty(fd) } != 1 {
        return None;
    }
    // Canonical mode would hold the reply until a newline that never comes,
    // and echo would paint it on the screen.
    let _raw = RawGuard::new(fd)?;

    // OSC 11 query, then a DA1 (`ESC [ c`) chaser: every terminal answers
    // DA1, and always after the OSC reply if one is coming, so a DA1 answer
    // with no color in front of it is a definitive "unsupported".
    tty.write_all(b"\x1b]11;?\x1b\\\x1b[c").ok()?;
    tty.flush().ok()?;

    let deadline = Instant::now() + REPLY_TIMEOUT;
    let mut buf = Vec::with_capacity(64);
    let mut chunk = [0u8; 64];
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || !wait_readable(fd, left) {
            return None;
        }
        match tty.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
        match classify_reply(&buf) {
            ReplyState::Color(rgb) => return Some(appearance_of(rgb)),
            ReplyState::Unsupported => return None,
            ReplyState::NeedMore => continue,
        }
    }
}

/// What the bytes received so far amount to.
#[derive(Debug, PartialEq, Eq)]
enum ReplyState {
    /// The terminal answered OSC 11 with this background color.
    Color((u8, u8, u8)),
    /// The terminal finished answering DA1 without sending a color: it
    /// doesn't support the query, and waiting longer is pointless.
    Unsupported,
    /// Still mid-reply — read more.
    NeedMore,
}

/// Decide whether the bytes read so far settle the question. Called after
/// every read, so it must tolerate a reply split across arbitrary chunks.
fn classify_reply(buf: &[u8]) -> ReplyState {
    if let Some(rgb) = parse_osc11(buf) {
        return ReplyState::Color(rgb);
    }
    // DA1 reply: `ESC [ ? … c`. The terminating 'c' is the signal — it can
    // only arrive after any OSC 11 answer the terminal was going to send.
    if buf.last() == Some(&b'c') && buf.windows(3).any(|w| w == b"\x1b[?") {
        return ReplyState::Unsupported;
    }
    ReplyState::NeedMore
}

/// Parse an OSC 11 reply: `ESC ] 11 ; <color> BEL` (or `… ST`).
///
/// Returns `None` while the reply is still incomplete — the terminator is
/// required, so a half-read `rgb:2e2e` is never mistaken for a color.
fn parse_osc11(buf: &[u8]) -> Option<(u8, u8, u8)> {
    let s = String::from_utf8_lossy(buf);
    let body = &s[s.find("]11;")? + 4..];
    // BEL, or the ESC that opens the string terminator.
    let end = body.find(['\x07', '\x1b'])?;
    parse_color(&body[..end])
}

/// Parse an X11 color spec: `rgb:RRRR/GGGG/BBBB` (the OSC 11 form),
/// `rgba:…` (some terminals), or `#RRGGBB`.
fn parse_color(spec: &str) -> Option<(u8, u8, u8)> {
    let spec = spec.trim();
    if let Some(hex) = spec.strip_prefix('#') {
        // Equal-width components: #rgb, #rrggbb, #rrrgggbbb, #rrrrggggbbbb.
        if hex.is_empty() || hex.len() % 3 != 0 || hex.len() > 12 {
            return None;
        }
        let w = hex.len() / 3;
        return Some((
            scale(&hex[..w])?,
            scale(&hex[w..2 * w])?,
            scale(&hex[2 * w..])?,
        ));
    }
    let spec = spec
        .strip_prefix("rgba:")
        .or_else(|| spec.strip_prefix("rgb:"))?;
    let mut parts = spec.split('/');
    Some((
        scale(parts.next()?)?,
        scale(parts.next()?)?,
        scale(parts.next()?)?,
    ))
}

/// Scale a 1–4 digit hex component to 8 bits: `f`, `ff` and `ffff` are all
/// full intensity.
fn scale(hex: &str) -> Option<u8> {
    if hex.is_empty() || hex.len() > 4 {
        return None;
    }
    let v = u32::from_str_radix(hex, 16).ok()?;
    let max = (1u32 << (4 * hex.len() as u32)) - 1;
    Some((v * 255 / max) as u8)
}

/// Classify a background color by perceived brightness (ITU-R BT.601 luma).
fn appearance_of((r, g, b): (u8, u8, u8)) -> Appearance {
    let luma = 0.299 * r as f32 + 0.587 * g as f32 + 0.114 * b as f32;
    if luma > 127.5 {
        Appearance::Light
    } else {
        Appearance::Dark
    }
}

/// Classify a `COLORFGBG` value (`"15;0"`, `"0;default;15"`, …).
///
/// The LAST field is the background's index into the 16-color palette:
/// 0–6 and 8 are its dark half, 7 and 9–15 the light half. A non-numeric
/// field (`default`) means the terminal declined to say.
fn classify_colorfgbg(value: &str) -> Option<Appearance> {
    let idx: u8 = value.rsplit(';').next()?.trim().parse().ok()?;
    Some(if matches!(idx, 0..=6 | 8) {
        Appearance::Dark
    } else {
        Appearance::Light
    })
}

/// Puts a tty into cbreak mode (no canonical line buffering, no echo,
/// non-blocking reads) and restores the original settings on drop.
struct RawGuard {
    fd: i32,
    original: libc::termios,
}

impl RawGuard {
    fn new(fd: i32) -> Option<Self> {
        // SAFETY: `fd` is a valid open tty descriptor owned by the caller
        // for the guard's lifetime; termios is a plain C struct.
        unsafe {
            let mut termios: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut termios) != 0 {
                return None;
            }
            let original = termios;
            termios.c_lflag &= !(libc::ICANON | libc::ECHO);
            // VMIN=0/VTIME=0: read() returns whatever has arrived instead of
            // blocking. `poll` upstream of it does the waiting.
            termios.c_cc[libc::VMIN] = 0;
            termios.c_cc[libc::VTIME] = 0;
            if libc::tcsetattr(fd, libc::TCSANOW, &termios) != 0 {
                return None;
            }
            Some(Self { fd, original })
        }
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        // SAFETY: same fd, still open (the caller holds the `File`).
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.original);
        }
    }
}

/// Wait until `fd` has bytes to read, or `timeout` elapses.
fn wait_readable(fd: i32, timeout: Duration) -> bool {
    let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    // SAFETY: `pfd` is a valid single-element pollfd array.
    unsafe { libc::poll(&mut pfd, 1, ms) > 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_osc11_replies() {
        // xterm's canonical form: 16-bit components, ST-terminated.
        assert_eq!(
            parse_osc11(b"\x1b]11;rgb:ffff/ffff/ffff\x1b\\"),
            Some((255, 255, 255))
        );
        // BEL-terminated, 8-bit components (kitty, foot).
        assert_eq!(parse_osc11(b"\x1b]11;rgb:2e/34/40\x07"), Some((46, 52, 64)));
        // Alpha channel appended (some VTE builds).
        assert_eq!(
            parse_osc11(b"\x1b]11;rgba:0000/0000/0000/ffff\x07"),
            Some((0, 0, 0))
        );
        assert_eq!(parse_osc11(b"\x1b]11;#fdf6e3\x07"), Some((253, 246, 227)));
    }

    #[test]
    fn incomplete_reply_is_not_a_color() {
        // Still streaming in: no terminator yet, so committing to these
        // digits would read #2e2e2e as the background.
        assert_eq!(parse_osc11(b"\x1b]11;rgb:2e2e/34"), None);
        assert_eq!(parse_osc11(b"\x1b]11;"), None);
        assert_eq!(parse_osc11(b"\x1b[?62;c"), None);
    }

    #[test]
    fn scales_components_to_eight_bits() {
        assert_eq!(scale("f"), Some(255));
        assert_eq!(scale("ffff"), Some(255));
        assert_eq!(scale("0"), Some(0));
        assert_eq!(scale("8080"), Some(128));
        assert_eq!(scale("fffff"), None);
        assert_eq!(scale("zz"), None);
    }

    #[test]
    fn classifies_real_terminal_backgrounds() {
        // Nord, Solarized dark, black.
        assert_eq!(appearance_of((46, 52, 64)), Appearance::Dark);
        assert_eq!(appearance_of((0, 43, 54)), Appearance::Dark);
        assert_eq!(appearance_of((0, 0, 0)), Appearance::Dark);
        // Solarized light, white, GitHub light gray.
        assert_eq!(appearance_of((253, 246, 227)), Appearance::Light);
        assert_eq!(appearance_of((255, 255, 255)), Appearance::Light);
        assert_eq!(appearance_of((214, 216, 219)), Appearance::Light);
    }

    #[test]
    fn reads_background_index_from_colorfgbg() {
        assert_eq!(classify_colorfgbg("15;0"), Some(Appearance::Dark));
        assert_eq!(classify_colorfgbg("0;15"), Some(Appearance::Light));
        assert_eq!(classify_colorfgbg("15;default;0"), Some(Appearance::Dark));
        assert_eq!(classify_colorfgbg("0;default;7"), Some(Appearance::Light));
        // Bright black is still a dark background.
        assert_eq!(classify_colorfgbg("15;8"), Some(Appearance::Dark));
        // Nothing usable — fall through to the default.
        assert_eq!(classify_colorfgbg("15;default"), None);
        assert_eq!(classify_colorfgbg(""), None);
    }

    /// Feed a reply to the state machine one byte at a time — the read loop
    /// sees whatever chunks the tty hands it, and must not settle early.
    fn drive(reply: &[u8]) -> (ReplyState, usize) {
        let mut buf = Vec::new();
        for (i, byte) in reply.iter().enumerate() {
            buf.push(*byte);
            match classify_reply(&buf) {
                ReplyState::NeedMore => {}
                settled => return (settled, i + 1),
            }
        }
        (ReplyState::NeedMore, reply.len())
    }

    #[test]
    fn a_color_reply_settles_the_query() {
        // xterm answers the color, then DA1. We stop at the color and never
        // look at the DA1 bytes behind it.
        let (state, consumed) = drive(b"\x1b]11;rgb:2e2e/3434/4040\x1b\\\x1b[?62;1;6c");
        assert_eq!(state, ReplyState::Color((46, 52, 64)));
        assert_eq!(consumed, 24, "should settle on the OSC terminator, not read past it");
    }

    #[test]
    fn da1_alone_means_the_terminal_cannot_answer() {
        // A terminal that ignores OSC 11 still answers DA1 — that's what
        // keeps startup from burning the whole timeout.
        assert_eq!(drive(b"\x1b[?62;1;6c").0, ReplyState::Unsupported);
        // ... but only once its reply is complete.
        assert_eq!(drive(b"\x1b[?62;1;6").0, ReplyState::NeedMore);
    }

    #[test]
    fn partial_color_reply_keeps_waiting() {
        assert_eq!(drive(b"\x1b]11;rgb:2e2e/3434/40").0, ReplyState::NeedMore);
    }
}
