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

/// `ui.theme` must round-trip through the REAL config loader, which is strict
/// (`deny_unknown_fields`) — a field that parses in isolation but is rejected
/// by `AppConfig` would fail loudly at startup and take every other setting
/// with it.
#[test]
fn ui_theme_parses_through_the_real_config() {
    let cfg: crate::app::AppConfig =
        serde_yaml::from_str("k9s:\n  ui:\n    theme: light\n")
            .or_else(|_| serde_yaml::from_str::<crate::app::AppConfig>("ui:\n  theme: light\n"))
            .expect("ui.theme is accepted by the strict loader");
    assert_eq!(cfg.ui.theme, crate::ui::theme::ThemeMode::Light);

    // Omitted → auto, and every other ui setting keeps its default.
    let bare: crate::app::AppConfig = serde_yaml::from_str("ui:\n  skin: nord\n").unwrap();
    assert_eq!(bare.ui.theme, crate::ui::theme::ThemeMode::Auto);
}

/// `Auto` must not be constructible as a rendered palette: resolving it does
/// terminal I/O, and the theme is built inside `App::new`, which is
/// deliberately hermetic. The type enforces this — `for_appearance` takes an
/// `Appearance`, which has no `Auto`.
#[test]
fn resolution_happens_before_the_theme_is_built() {
    use crate::ui::term_bg::Appearance;
    assert_eq!(ThemeMode::Dark.resolve(), Appearance::Dark);
    assert_eq!(ThemeMode::Light.resolve(), Appearance::Light);
    // A test-constructed App is pinned dark and never touches a terminal.
    let app = crate::app::App::new_for_test();
    assert_eq!(app.ui.theme.dialog_fill, Theme::dark().dialog_fill);
}
