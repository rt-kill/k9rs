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
//! - The query runs BEFORE the caller enables raw mode and the alternate
//!   screen, and it drains its own reply from the tty. Nothing leaks into
//!   crossterm's input stream as a phantom keypress. That ordering is a
//!   contract on the caller, not something this module can enforce; the
//!   type is what makes it hard to get wrong, since [`Appearance`] has no
//!   `Auto` and so a palette cannot be built without resolving first.
//! - It is bounded: a DA1 chaser rides behind the OSC query, so a terminal
//!   that ignores OSC 11 still says something and we stop waiting
//!   immediately instead of burning the whole timeout. Startup pays the
//!   full `REPLY_TIMEOUT` (100ms) only on a terminal that answers nothing
//!   at all.

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
    // with no color in front of it is a definitive "unsupported" — AND its
    // arrival is what tells us the tty is drained of everything we provoked.
    // A terminal that answers the color but not DA1 costs the full
    // `REPLY_TIMEOUT` here; leaving its reply behind costs stray keystrokes.
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
            ReplyState::Done(color) => return color.map(appearance_of),
            ReplyState::NeedMore => continue,
        }
    }
}

/// What the bytes received so far amount to.
#[derive(Debug, PartialEq, Eq)]
enum ReplyState {
    /// The terminal has finished answering BOTH queries. Carries the
    /// background color if OSC 11 supplied one, `None` if it didn't.
    Done(Option<(u8, u8, u8)>),
    /// Still mid-reply — read more.
    NeedMore,
}

/// Decide whether the bytes read so far settle the question. Called after
/// every read, so it must tolerate a reply split across arbitrary chunks.
///
/// The DA1 terminator is the ONLY stopping point, even once a color has
/// parsed. DA1 is the chaser: it can only arrive after whatever OSC 11 answer
/// was coming, so its arrival means the terminal has said everything it is
/// going to say. Returning early on the color — which is what this used to do
/// — left DA1's own reply sitting unread in `/dev/tty`, and the TUI starts
/// reading that same tty moments later. `ESC [ ? 6 4 ; 1 ; 2 c` shredded into
/// keystrokes is a `?` (help) and a trailing `c` (copy) arriving from nowhere
/// during the first frames. The resume sieve can't help: it runs on
/// suspend/resume, and this is startup.
fn classify_reply(buf: &[u8]) -> ReplyState {
    if buf.last() == Some(&b'c') && buf.windows(3).any(|w| w == b"\x1b[?") {
        return ReplyState::Done(parse_osc11(buf));
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
#[path = "tests/term_bg.rs"]
mod mirror_tests;
