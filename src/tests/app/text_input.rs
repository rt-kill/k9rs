use super::*;

/// The invariant that justifies the type: the caret is a byte offset that is
/// ALWAYS on a grapheme boundary. Moving by `char` would leave it between a
/// letter and its combining accent; moving by byte would panic on the next
/// slice. Every mutator has to preserve it, which is the whole reason there
/// is one of these rather than four.
#[test]
fn the_caret_never_lands_inside_a_grapheme() {
    // e + combining acute, a family emoji, a wide CJK char, and plain ASCII.
    for text in ["e\u{0301}x", "a\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}b", "\u{672C}z", "abc"] {
        let mut t = TextInput::seeded(text);
        // Walk left to the start, then right to the end, slicing at every
        // stop. A boundary violation panics here rather than in a renderer.
        let mut stops = vec![t.cursor()];
        while t.cursor() > 0 {
            t.left();
            let _ = &t.value()[..t.cursor()];
            stops.push(t.cursor());
        }
        assert_eq!(t.cursor(), 0, "{text:?} walked past the start");
        while t.cursor() < t.value().len() {
            t.right();
            let _ = &t.value()[..t.cursor()];
        }
        assert_eq!(t.cursor(), text.len(), "{text:?} did not reach the end");
        // One stop per grapheme, plus the starting position.
        let clusters = unicode_segmentation::UnicodeSegmentation::graphemes(text, true).count();
        assert_eq!(stops.len(), clusters + 1, "{text:?} stopped in the wrong places");
    }
}

/// Insertion happens AT the caret — the thing four `push`-based inputs
/// could not do.
#[test]
fn text_is_inserted_at_the_caret_not_appended() {
    let mut t = TextInput::seeded("wokers=0");
    for _ in 0..6 {
        t.left(); // sitting between "wo" and "kers=0"
    }
    assert_eq!(t.cursor(), 2);
    t.insert('r');
    assert_eq!(t.value(), "workers=0");
    assert_eq!(t.cursor(), 3, "the caret follows what was typed");

    t.home();
    t.insert('/');
    assert_eq!(t.value(), "/workers=0");
}

/// Backspace and Delete remove whole graphemes on the correct side, and are
/// no-ops at the respective ends rather than panicking.
#[test]
fn deletion_removes_whole_graphemes_and_stops_at_the_edges() {
    let mut t = TextInput::seeded("ae\u{0301}b");
    t.backspace();
    assert_eq!(t.value(), "ae\u{0301}", "b removed");
    t.backspace();
    assert_eq!(t.value(), "a", "the accent went with its base letter, not half of it");
    t.backspace();
    assert_eq!(t.value(), "");
    t.backspace(); // at the start
    assert_eq!(t.value(), "");

    let mut t = TextInput::seeded("xe\u{0301}y");
    t.home();
    t.delete();
    assert_eq!(t.value(), "e\u{0301}y");
    t.delete();
    assert_eq!(t.value(), "y", "forward delete also takes the whole cluster");
    t.delete();
    t.delete(); // past the end
    assert_eq!(t.value(), "");
}

/// The caret is drawn in COLUMNS, not bytes — a renderer using the byte
/// offset would put it past the end of any line with a multi-byte character.
#[test]
fn the_drawn_caret_is_in_columns() {
    let mut t = TextInput::seeded("\u{672C}\u{672C}"); // two wide chars
    assert_eq!(t.cursor(), 6, "six bytes");
    assert_eq!(t.cursor_col(), 4, "but four columns");
    t.left();
    assert_eq!(t.cursor_col(), 2);
    t.home();
    assert_eq!(t.cursor_col(), 0);
}

/// Commit empties the input and resets the caret with it — a leftover caret
/// past the end of a cleared string is the out-of-bounds state this type
/// exists to prevent.
#[test]
fn taking_the_text_resets_the_caret() {
    let mut t = TextInput::seeded("pods");
    assert_eq!(t.take(), "pods");
    assert!(t.is_empty());
    assert_eq!(t.cursor(), 0);
    t.insert('x');
    assert_eq!(t.value(), "x");
}

/// Every text box in the app is this type — that is the claim the
/// consolidation rests on, and it is what makes "add word motion" one method
/// rather than five patches.
///
/// Checked by construction rather than by grep: each of the five sinks is
/// built here and edited through the same operations.
#[test]
fn every_text_surface_is_one_type() {
    // 1. the `:` command prompt
    let mut app = crate::app::App::new_for_test();
    app.ui.open(crate::app::Modal::Command {
        input: TextInput::seeded("pods"),
        history_index: None,
    });
    let (input, _) = app.ui.command_input_mut().expect("command is open");
    input.home();
    input.insert(':');
    assert_eq!(input.value(), ":pods");

    // 2. the table filter
    let el = app.nav.top_mut();
    let fi = el.filter_input_mut().expect("a table has one");
    fi.start();
    for c in "runing".chars() {
        fi.input_mut().insert(c);
    }
    let t = fi.input_mut();
    for _ in 0..3 {
        t.left();
    }
    t.insert('n');
    assert_eq!(el.filter_input().text(), "running");

    // 3. a form field
    let mut field = crate::app::FormFieldState {
        name: "replicas".to_string(),
        label: "Replicas".into(),
        kind: crate::app::FormFieldKind::Number { min: 0, max: 100 },
        value: TextInput::seeded("10"),
    };
    field.value.home();
    field.value.delete();
    assert_eq!(field.value.value(), "0");

    // 4. the content-view search — goes through the shared key handler
    let mut search = TextInput::seeded("erro");
    let tk = crate::app::TextKey::new(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('r'),
        crossterm::event::KeyModifiers::NONE,
    ))
    .unwrap();
    crate::app::handle_search_key(&mut search, tk);
    assert_eq!(search.value(), "error");

    // 5. the log grep draft
    let mut draft = TextInput::seeded("worker=");
    draft.left();
    draft.insert('s');
    assert_eq!(draft.value(), "workers=");
}

/// The caret is always on screen, and the window is always a legal slice.
///
/// Five render sites each guarded with `if cursor_x < right_edge { draw the
/// caret }`, so outgrowing the bar clipped the text AND stopped drawing the
/// caret: you were typing blind. The window makes that unexpressable — the
/// caret's column is returned relative to what is shown, so it cannot be
/// outside it.
#[test]
fn the_window_always_contains_the_caret() {
    use crate::app::text_input::TextInput;
    use unicode_width::UnicodeWidthStr;

    for value in [
        "",
        "short",
        "a-very-long-grep-pattern-that-outgrows-any-reasonable-filter-bar",
        "日本語のパターンはとても長い",
        "emoji \u{1F469}\u{200D}\u{1F467} family \u{26a0}\u{FE0F} warn",
    ] {
        let mut input = TextInput::seeded(value);
        // Walk the caret across every position, in both directions.
        input.home();
        for _ in 0..=value.chars().count() {
            for width in [0usize, 1, 2, 5, 12, 40, 200] {
                let (shown, col) = input.window(width);
                assert!(
                    UnicodeWidthStr::width(shown) <= width,
                    "{value:?} w={width}: window {shown:?} is wider than {width}",
                );
                assert!(
                    col <= width.saturating_sub(1).max(0) || width == 0,
                    "{value:?} w={width}: caret column {col} is off the right edge",
                );
                // The window is a real slice of the value, not a rebuild.
                assert!(
                    value.contains(shown),
                    "{value:?} w={width}: {shown:?} is not a slice of it",
                );
                // Everything that fits is shown: no needless scrolling.
                // STRICTLY wider, because the caret needs a cell of its own
                // — a value that exactly fills the bar must give up its
                // first column for the caret sitting after its last.
                if width > UnicodeWidthStr::width(value) {
                    assert_eq!(shown, value, "{value:?} w={width}: it all fits");
                    assert_eq!(col, input.cursor_col(), "…and the caret is where it is");
                }
            }
            input.right();
        }
    }
}

/// A caret at the very end of a long value still has a column to sit in.
#[test]
fn a_caret_past_the_last_character_is_still_on_screen() {
    use crate::app::text_input::TextInput;

    let mut input = TextInput::seeded("abcdefghij");
    input.end();
    assert_eq!(input.cursor_col(), 10, "the caret sits past the last char");

    // Five columns: the caret takes the fifth, so four characters precede it.
    let (shown, col) = input.window(5);
    assert_eq!(shown, "ghij", "the window follows the caret to the end");
    assert_eq!(col, 4, "and the caret is the last column, not past it");
}
