//! Palette + skin-layering tests. In the `src/tests/` mirror per the
//! repo convention (2026-07-27) rather than inline, which is where the
//! upstream PR had them.

use super::*;

/// WCAG relative luminance.
fn luminance(c: Color) -> f64 {
    let Color::Rgb(r, g, b) = c else {
        panic!("palette colors are always RGB, got {c:?}")
    };
    let lin = |v: u8| {
        let v = v as f64 / 255.0;
        if v <= 0.04045 { v / 12.92 } else { ((v + 0.055) / 1.055).powf(2.4) }
    };
    0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)
}

/// WCAG contrast ratio between two colors (1.0 = identical, 21.0 = black
/// on white).
fn contrast(a: Color, b: Color) -> f64 {
    let (x, y) = (luminance(a), luminance(b));
    let (hi, lo) = if x > y { (x, y) } else { (y, x) };
    (hi + 0.05) / (lo + 0.05)
}

/// The light palette's whole point: it has to stay readable on a light
/// terminal. Every foreground clears WCAG AA (4.5:1) against both pure
/// white and the dialog fill — `muted` (borders, timestamps) is the
/// floor, which is why it is a mid gray and not a pale one.
#[test]
fn light_palette_foregrounds_are_readable_on_light_backgrounds() {
    let p = &LIGHT;
    let fgs = [
        ("text", p.text), ("text_bright", p.text_bright), ("muted", p.muted),
        ("blue", p.blue), ("teal", p.teal), ("green", p.green),
        ("yellow", p.yellow), ("red", p.red), ("mauve", p.mauve),
    ];
    for (name, fg) in fgs {
        for (bg_name, bg) in [("white", Color::Rgb(255, 255, 255)), ("dialog", p.dialog_bg)] {
            let ratio = contrast(fg, bg);
            assert!(ratio >= 4.5, "light {name} on {bg_name}: {ratio:.2}:1 < 4.5:1");
        }
    }
}

/// Filled bars (row cursor, breadcrumbs, search hits) carry their own
/// foreground, so they must be legible against THAT, not the terminal.
/// The light palette is held to AA (4.5:1); the dark pairings are Nord's
/// own, kept as-is for fidelity with the k9s "foot" skin, and its mauve
/// active crumb is the floor at 4.4:1.
#[test]
fn filled_bars_are_readable_in_both_palettes() {
    for (name, p, floor) in [("dark", &DARK, 4.0), ("light", &LIGHT, 4.5)] {
        for (what, fg, bg) in [
            ("cursor", p.cursor_fg, p.cursor_bg),
            ("crumb", p.crumb_fg, p.crumb_bg),
            ("crumb_active", p.crumb_active_fg, p.crumb_active_bg),
            ("search_match", p.match_fg, p.match_bg),
        ] {
            let ratio = contrast(fg, bg);
            assert!(ratio >= floor, "{name} {what}: {ratio:.2}:1 < {floor}:1");
        }
    }
}

#[test]
fn light_and_dark_are_actually_different_themes() {
    let (dark, light) = (Theme::dark(), Theme::light());
    assert_ne!(dark.row_normal.fg, light.row_normal.fg);
    assert_ne!(dark.dialog_fill, light.dialog_fill);
    // Default stays dark: the historical behavior, and what non-rendering
    // callers (help line counting, tests) get.
    assert_eq!(Theme::default().row_normal.fg, dark.row_normal.fg);
}

#[test]
fn theme_mode_parses_from_config() {
    assert_eq!(serde_yaml::from_str::<ThemeMode>("auto").unwrap(), ThemeMode::Auto);
    assert_eq!(serde_yaml::from_str::<ThemeMode>("dark").unwrap(), ThemeMode::Dark);
    assert_eq!(serde_yaml::from_str::<ThemeMode>("light").unwrap(), ThemeMode::Light);
    assert!(serde_yaml::from_str::<ThemeMode>("solarized").is_err());
    assert_eq!(ThemeMode::default(), ThemeMode::Auto);
}

/// A skin overrides individual colors ON TOP of the mode's palette —
/// picking light doesn't discard the skin, and a skin that only sets a
/// few keys doesn't drag the rest back to the dark defaults.
#[test]
fn skin_overrides_layer_over_the_selected_palette() {
    let path = std::env::temp_dir()
        .join(format!("k9rs-skin-test-{}.yaml", std::process::id()));
    std::fs::write(
        &path,
        "k9s:\n  frame:\n    border:\n      focusColor: \"#ff00ff\"\n",
    )
    .unwrap();

    let light = Theme::from_skin_file(&path, Appearance::Light).unwrap();
    let dark = Theme::from_skin_file(&path, Appearance::Dark).unwrap();
    let _ = std::fs::remove_file(&path);

    // The skinned color wins in both.
    assert_eq!(light.border_focused.fg, Some(Color::Rgb(255, 0, 255)));
    assert_eq!(dark.border_focused.fg, Some(Color::Rgb(255, 0, 255)));
    // Everything the skin left alone still comes from the palette.
    assert_eq!(light.row_normal.fg, Some(LIGHT.teal));
    assert_eq!(dark.row_normal.fg, Some(DARK.teal));
}

// ---------------------------------------------------------------------------
// Integration with this repo's config conventions
// ---------------------------------------------------------------------------

// Two tests moved to `src/tests/app/theme_integration.rs`: one parses
// `ui.theme` through the real `AppConfig` loader, the other checks that a
// test-constructed `App` is pinned dark. Both assert how the APP consumes a
// theme, from the theme crate's own mirror — which would make
// `cargo test -p k9rs_theme` build the whole TUI.

// ---------------------------------------------------------------------------
// A broken skin is reported, not swallowed
// ---------------------------------------------------------------------------

/// A directory that removes itself — four tests do not earn a dependency.
struct TempSkins(std::path::PathBuf);

impl TempSkins {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos();
        let dir = std::env::temp_dir()
            .join(format!("k9rs-skin-test-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("fixture dir");
        Self(dir)
    }
    fn write(&self, name: &str, body: &str) -> std::path::PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, body).expect("write skin");
        path
    }
}

impl Drop for TempSkins {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `Theme` has no `Debug`, so `expect_err` is unavailable — and a theme is
/// not a thing worth printing anyway.
fn skin_error(path: &std::path::Path, why: &str) -> String {
    match Theme::from_skin_file(path, Appearance::Dark) {
        Err(e) => e,
        Ok(_) => panic!("{why}"),
    }
}

/// ONE mistyped key discards the whole skin — and now says which.
///
/// Every `Skin*` struct carries `deny_unknown_fields`, so `fgcolor` for
/// `fgColor` fails the deserialize, which used to become `None` and then the
/// default palette with no output anywhere. The user saw stock colours and
/// no reason, from the same config directory whose three sibling loaders
/// refuse to "silently collapse into use-defaults".
#[test]
fn a_mistyped_skin_key_is_reported_rather_than_discarded() {
    let dir = TempSkins::new("typo");
    let path = dir.write("bad.yaml", "k9s:\n  body:\n    fgcolor: red\n");
    let err = skin_error(&path, "a typo must not be silently accepted");
    assert!(
        err.contains("fgcolor"),
        "the refusal must name the key it did not know: {err}",
    );
}

/// The other three ways a skin file can fail each say what happened.
#[test]
fn every_skin_failure_names_itself() {
    let dir = TempSkins::new("shapes");

    let not_yaml = dir.write("a.yaml", "k9s:\n  body: [unclosed\n");
    let err = skin_error(&not_yaml, "invalid YAML");
    assert!(err.contains("YAML"), "{err}");

    let no_section = dir.write("b.yaml", "something_else:\n  body: {}\n");
    let err = skin_error(&no_section, "no k9s section");
    assert!(err.contains("k9s"), "{err}");

    let err = skin_error(&dir.0.join("nope.yaml"), "absent file");
    assert!(err.contains("cannot read"), "{err}");
}

/// A VALID skin still loads, and still layers over the chosen palette —
/// the fix must not turn a working skin into a complaint.
#[test]
fn a_valid_skin_loads_without_complaint() {
    let dir = TempSkins::new("good");
    // The same shape the layering test uses, so this asserts "it still
    // works" against a mapping that is already known to hold.
    let path = dir.write(
        "ok.yaml",
        "k9s:\n  frame:\n    border:\n      focusColor: \"#ff00ff\"\n",
    );
    let Ok(theme) = Theme::from_skin_file(&path, Appearance::Dark) else {
        panic!("a valid skin loads");
    };
    assert_eq!(
        theme.border_focused.fg,
        Some(ratatui::style::Color::Rgb(0xff, 0x00, 0xff)),
        "the override took effect, so `Result` did not break the happy path",
    );
}

/// No skin configured is not a problem — the stock palette is the answer,
/// and `load_reporting` says nothing.
#[test]
fn no_skin_configured_is_not_a_complaint() {
    for name in [None, Some(""), Some("   ")] {
        let (_, problem) = Theme::load_reporting(name, Appearance::Dark);
        match name {
            // A whitespace name is a NAMED skin that will not be found —
            // the filter only drops the empty string.
            Some(n) if !n.is_empty() => assert!(problem.is_some(), "{n:?}"),
            _ => assert!(problem.is_none(), "{name:?} is not a skin request: {problem:?}"),
        }
    }
}
