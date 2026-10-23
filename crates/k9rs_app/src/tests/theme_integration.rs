//! How the app consumes a theme.
//!
//! These two were in `k9rs_theme`'s own mirror. They assert APP behaviour —
//! that `ui.theme` survives `AppConfig`'s strict loader, and that a
//! test-constructed `App` is pinned dark — so from down there they were a
//! dev-dependency on the entire TUI.

/// `ui.theme` must round-trip through the REAL config loader, which is strict
/// (`deny_unknown_fields`) — a field that parses in isolation but is rejected
/// by `AppConfig` would fail loudly at startup and take every other setting
/// with it.
#[test]
fn ui_theme_parses_through_the_real_config() {
    let cfg: crate::AppConfig =
        serde_yaml::from_str("k9s:\n  ui:\n    theme: light\n")
            .or_else(|_| serde_yaml::from_str::<crate::AppConfig>("ui:\n  theme: light\n"))
            .expect("ui.theme is accepted by the strict loader");
    assert_eq!(cfg.ui.theme, k9rs_theme::ThemeMode::Light);

    // Omitted → auto, and every other ui setting keeps its default.
    let bare: crate::AppConfig = serde_yaml::from_str("ui:\n  skin: nord\n").unwrap();
    assert_eq!(bare.ui.theme, k9rs_theme::ThemeMode::Auto);
}

/// `Auto` must not be constructible as a rendered palette: resolving it does
/// terminal I/O, and the theme is built inside `App::new`, which is
/// deliberately hermetic. The type enforces this — `for_appearance` takes an
/// `Appearance`, which has no `Auto`.
#[test]
fn resolution_happens_before_the_theme_is_built() {
    use k9rs_theme::term_bg::Appearance;
    assert_eq!(k9rs_theme::ThemeMode::Dark.resolve(), Appearance::Dark);
    assert_eq!(k9rs_theme::ThemeMode::Light.resolve(), Appearance::Light);
    // A test-constructed App is pinned dark and never touches a terminal.
    let app = crate::App::new_for_test();
    assert_eq!(app.ui.theme.dialog_fill, k9rs_theme::Theme::dark().dialog_fill);
}
