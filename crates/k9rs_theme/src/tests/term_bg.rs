//! Terminal-background detection — the pure half.
//!
//! `detect()` itself needs a tty and can't run here, but every decision it
//! makes is a pure function over bytes, and those are exactly the parts that
//! are easy to get subtly wrong: a reply split across reads, a terminal that
//! answers DA1 but not OSC 11, a `COLORFGBG` that says `default`.

use super::*;

#[test]
fn an_osc11_reply_is_parsed_in_every_shape_terminals_send() {
    // 4-digit components (xterm, kitty, foot), BEL- and ST-terminated.
    assert_eq!(parse_osc11(b"\x1b]11;rgb:2e2e/3434/4040\x07"), Some((46, 52, 64)));
    assert_eq!(parse_osc11(b"\x1b]11;rgb:2e2e/3434/4040\x1b\\"), Some((46, 52, 64)));
    // Short and long forms scale to 8 bits: f, ff and ffff are all full.
    assert_eq!(parse_osc11(b"\x1b]11;rgb:f/f/f\x07"), Some((255, 255, 255)));
    assert_eq!(parse_osc11(b"\x1b]11;rgb:ffff/ffff/ffff\x07"), Some((255, 255, 255)));
    // `rgba:` (some terminals) and `#rrggbb`.
    assert_eq!(parse_osc11(b"\x1b]11;rgba:ffff/ffff/ffff/ffff\x07"), Some((255, 255, 255)));
    assert_eq!(parse_osc11(b"\x1b]11;#ffffff\x07"), Some((255, 255, 255)));
}

#[test]
fn a_half_read_reply_is_never_mistaken_for_a_colour() {
    // The terminator is required — reads arrive in arbitrary chunks, and a
    // truncated `rgb:2e2e` must not classify as a very dark background.
    assert_eq!(parse_osc11(b"\x1b]11;rgb:2e2e"), None);
    assert_eq!(parse_osc11(b"\x1b]11;rgb:2e2e/3434"), None);
    assert_eq!(classify_reply(b"\x1b]11;rgb:2e2e/3434"), ReplyState::NeedMore);
}

#[test]
fn reading_stops_at_the_da1_chaser_not_at_the_colour() {
    // A COMPLETE colour is still not a stopping point. DA1 is the chaser and
    // always arrives last, so its reply is still in flight here — and
    // `/dev/tty` is the same tty the TUI starts reading from moments later.
    // Stopping on the colour left `ESC [ ? 6 4 ; 1 ; 2 c` behind, which
    // shreds into a `?` (help) and a trailing `c` (copy) from nowhere. The
    // resume sieve cannot cover it: that runs on suspend/resume, this is
    // startup.
    assert_eq!(
        classify_reply(b"\x1b]11;rgb:ffff/ffff/ffff\x07"),
        ReplyState::NeedMore,
        "the colour parsed, but the chaser we sent has not answered yet",
    );
    assert_eq!(
        classify_reply(b"\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?62;c"),
        ReplyState::Done(Some((255, 255, 255))),
        "both answers in hand: the tty is drained and the colour stands",
    );
}

#[test]
fn a_da1_answer_with_no_colour_means_unsupported() {
    // THE bound on startup: a terminal that ignores OSC 11 still answers
    // DA1, so we stop waiting immediately instead of burning the timeout.
    assert_eq!(classify_reply(b"\x1b[?62;c"), ReplyState::Done(None));
    // A DA1 arriving BEHIND a colour must not mask it — the colour wins.
    assert_eq!(
        classify_reply(b"\x1b]11;rgb:0000/0000/0000\x07\x1b[?62;c"),
        ReplyState::Done(Some((0, 0, 0))),
    );
}

#[test]
fn brightness_decides_appearance() {
    assert_eq!(appearance_of((255, 255, 255)), Appearance::Light);
    assert_eq!(appearance_of((0, 0, 0)), Appearance::Dark);
    // Nord's own background is dark; a typical light-theme cream is light.
    assert_eq!(appearance_of((46, 52, 64)), Appearance::Dark);
    assert_eq!(appearance_of((253, 246, 227)), Appearance::Light);
    // Luma is weighted, not a mean: saturated green reads far lighter than
    // saturated blue at the same numeric value.
    assert_eq!(appearance_of((0, 255, 0)), Appearance::Light);
    assert_eq!(appearance_of((0, 0, 255)), Appearance::Dark);
}

#[test]
fn colorfgbg_uses_the_last_field_and_declines_politely() {
    // urxvt/konsole style: the trailing field is the background's index.
    assert_eq!(classify_colorfgbg("15;0"), Some(Appearance::Dark));
    assert_eq!(classify_colorfgbg("0;15"), Some(Appearance::Light));
    // Three fields — still the last one.
    assert_eq!(classify_colorfgbg("0;default;15"), Some(Appearance::Light));
    // The dark half of the 16-colour palette is 0..=6 and 8.
    for idx in [0, 1, 2, 3, 4, 5, 6, 8] {
        assert_eq!(classify_colorfgbg(&format!("7;{idx}")), Some(Appearance::Dark), "{idx}");
    }
    for idx in [7, 9, 10, 11, 12, 13, 14, 15] {
        assert_eq!(classify_colorfgbg(&format!("0;{idx}")), Some(Appearance::Light), "{idx}");
    }
    // "It declined to say" is None, not a guess — the caller falls through
    // to its own default rather than inventing one.
    assert_eq!(classify_colorfgbg("15;default"), None);
    assert_eq!(classify_colorfgbg(""), None);
}

// ---------------------------------------------------------------------------
// tests (mirrored out of term_bg.rs)
// ---------------------------------------------------------------------------


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
    fn a_color_reply_is_read_through_to_the_chaser() {
        // xterm answers the color, then DA1.
        //
        // This test used to assert the opposite — "we stop at the color and
        // never look at the DA1 bytes behind it" — which pinned a bug as if it
        // were a feature. Those unread bytes stay in `/dev/tty`, which is the
        // tty the TUI begins reading milliseconds later, where
        // `ESC [ ? 6 2 ; 1 ; 6 c` shreds into a stray `?` and `c`.
        let reply = b"\x1b]11;rgb:2e2e/3434/4040\x1b\\\x1b[?62;1;6c";
        let (state, consumed) = drive(reply);
        assert_eq!(state, ReplyState::Done(Some((46, 52, 64))));
        assert_eq!(
            consumed,
            reply.len(),
            "every byte we provoked is consumed, not just the ones we wanted",
        );
    }

    #[test]
    fn da1_alone_means_the_terminal_cannot_answer() {
        // A terminal that ignores OSC 11 still answers DA1 — that's what
        // keeps startup from burning the whole timeout.
        assert_eq!(drive(b"\x1b[?62;1;6c").0, ReplyState::Done(None));
        // ... but only once its reply is complete.
        assert_eq!(drive(b"\x1b[?62;1;6").0, ReplyState::NeedMore);
    }

    #[test]
    fn partial_color_reply_keeps_waiting() {
        assert_eq!(drive(b"\x1b]11;rgb:2e2e/3434/40").0, ReplyState::NeedMore);
    }
