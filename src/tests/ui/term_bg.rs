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
    // …and once it completes, it resolves.
    assert_eq!(
        classify_reply(b"\x1b]11;rgb:ffff/ffff/ffff\x07"),
        ReplyState::Color((255, 255, 255)),
    );
}

#[test]
fn a_da1_answer_with_no_colour_means_unsupported() {
    // THE bound on startup: a terminal that ignores OSC 11 still answers
    // DA1, so we stop waiting immediately instead of burning the timeout.
    assert_eq!(classify_reply(b"\x1b[?62;c"), ReplyState::Unsupported);
    // A DA1 arriving BEHIND a colour must not mask it — the colour wins.
    assert_eq!(
        classify_reply(b"\x1b]11;rgb:0000/0000/0000\x07\x1b[?62;c"),
        ReplyState::Color((0, 0, 0)),
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
