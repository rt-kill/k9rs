//! The `:` word declaration, checked where it is declared.
//!
//! This file had no mirror: its one pin — that each documented word is one
//! the GRAMMAR accepts — lives in `k9rs_session`, where the parser is, and
//! rightly so. What is checkable HERE is the declaration's own consistency
//! and how it prints, which is what three hand-written copies of this list
//! used to get wrong in three different ways.

use super::*;

/// A word takes an argument or it does not, and `display` says which —
/// `ns <name>` for every spelling of it, `q / quit / exit / q!` for none.
#[test]
fn display_puts_the_placeholder_on_every_spelling() {
    let with_arg = COMMAND_WORDS
        .iter()
        .find(|d| d.words.contains(&"ns"))
        .expect("`ns` is declared");
    assert_eq!(with_arg.display(), "ns <name> / namespace <name>");

    let without = COMMAND_WORDS
        .iter()
        .find(|d| d.words.contains(&"q"))
        .expect("`q` is declared");
    assert_eq!(without.display(), "q / quit / exit / q!");
}

/// No word is declared twice. Two entries claiming one word means the
/// grammar's linear match silently picks the first and the completer offers
/// both — the exact drift three parallel lists produced.
#[test]
fn no_command_word_is_declared_twice() {
    let mut seen: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for doc in COMMAND_WORDS {
        for word in doc.words {
            if let Some(prev) = seen.insert(word, doc.label) {
                panic!("`:{word}` is declared by both {prev:?} and {:?}", doc.label);
            }
        }
    }
}

/// Every entry is complete: words to type, something to say about it, and a
/// probe for the grammar test to feed the parser.
#[test]
fn every_declared_word_is_usable() {
    assert!(!COMMAND_WORDS.is_empty());
    for doc in COMMAND_WORDS {
        assert!(!doc.words.is_empty(), "{:?} has no words", doc.label);
        assert!(!doc.label.is_empty(), "{:?} has no label", doc.words);
        assert!(!doc.probe.is_empty(), "{:?} has no probe", doc.words);
        for word in doc.words {
            assert!(
                !word.is_empty() && !word.starts_with(':'),
                "{word:?}: a word is typed AFTER the colon, bare",
            );
            assert!(
                !word.contains(' '),
                "{word:?}: a word with a space in it is two tokens to the parser",
            );
        }
        // The probe must be an invocation OF this entry, so the grammar
        // test over in `k9rs_session` is feeding the parser the right word.
        let first_token = doc.probe.split_whitespace().next().unwrap_or_default();
        assert!(
            doc.words.contains(&first_token),
            "{:?}'s probe {:?} does not start with one of its own words",
            doc.label,
            doc.probe,
        );
        // And it carries an argument exactly when the entry takes one.
        let has_arg = doc.probe.split_whitespace().count() > 1;
        assert_eq!(
            has_arg,
            doc.arg.is_some(),
            "{:?}: probe {:?} and arg {:?} disagree",
            doc.label,
            doc.probe,
            doc.arg,
        );
    }
}
