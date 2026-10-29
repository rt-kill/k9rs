//! The suspension drain: what happens to daemon events while the user is
//! sitting in `$EDITOR`.

use super::*;
use k9rs_app::AppEvent;

/// Events that arrive while a subprocess is running must be APPLIED, not
/// queued behind a loop that isn't running.
///
/// The link does not pause because a human is thinking. With nobody reading,
/// the client's channel fills, its reader tasks stop reading their substreams,
/// the yamux windows fill, the daemon's writes stall, and its 60s
/// `connection_write_timeout` tears the connection down — a long edit used to
/// come back to a dead session. The flash below is a stand-in for any event:
/// what is being pinned is that the channel is drained at all.
#[tokio::test]
async fn events_arriving_during_a_suspension_are_applied_not_stranded() {
    let mut app = App::new_for_test();
    let (tx, mut rx) = mpsc::channel::<AppEvent>(8);

    // "The subprocess": finishes only once told to, so the drain has to be
    // concurrent rather than after-the-fact.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let sender = tokio::spawn(async move {
        tx.send(AppEvent::NoContextConfigured).await.expect("channel open");
        // Give the drain a chance to consume it before the work completes,
        // so a passing result can't come from a post-hoc drain.
        tokio::task::yield_now().await;
        let _ = done_tx.send(());
    });

    super::draining(&mut app, &mut rx, async {
        let _ = done_rx.await;
    })
    .await;
    sender.await.expect("sender should not panic");

    assert!(
        app.kube.context.is_none(),
        "the NoContextConfigured event reached app state",
    );
    assert!(
        app.ui.flash.is_some(),
        "…and was applied while the work was still running, not dropped",
    );
}

/// A closed channel must not stall the thing being awaited. `select!` disables
/// a branch whose pattern fails to match, and `recv()` on a closed channel
/// yields `None` forever — get this wrong and the editor hangs after a
/// disconnect instead of returning.
#[tokio::test]
async fn a_closed_event_channel_does_not_strand_the_subprocess() {
    let mut app = App::new_for_test();
    let (tx, mut rx) = mpsc::channel::<AppEvent>(1);
    drop(tx);

    let out = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        super::draining(&mut app, &mut rx, async { 42u8 }),
    )
    .await
    .expect("the work must still complete with no daemon behind it");
    assert_eq!(out, 42);
}

// ---------------------------------------------------------------------------
// parser_tests (mirrored out of session_commands.rs)
// ---------------------------------------------------------------------------

    use k9rs_proto::protocol::Namespace;
    use k9rs_proto::resource_def::BuiltInKind;

    fn make_app() -> App {
        App::new_for_test()
    }

    fn parse(input: &str) -> ParsedCommand {
        let app = make_app();
        parse_command_input(input, &input.to_lowercase(), &app)
    }

    /// Every documented `:` word really parses to what it claims.
    ///
    /// `COMMAND_WORDS` exists so the aliases view does not hand-write its own
    /// copy of this grammar — which is exactly what `Ctrl-a` used to do, and
    /// why its key list had drifted from the keymap. A declaration the parser
    /// does not read needs a probe, so this is the same arrangement the
    /// keymap has: documentation beside a check that it is true.
    #[test]
    fn every_documented_command_word_is_one_the_parser_accepts() {
        for doc in k9rs_app::COMMAND_WORDS {
            let parsed = parse(doc.probe);
            assert!(
                (doc.accepts)(&parsed),
                "`:{}` is documented as `{}` but parsed as {:?}",
                doc.probe,
                doc.label,
                parsed,
            );
            // The declared words must include the probe's first token, or
            // the row documents one thing and the test checks another.
            let head = doc.probe.split(' ').next().unwrap_or(doc.probe);
            assert!(
                doc.words.contains(&head),
                "`{}` lists {:?}, which does not include the probe `{}`",
                doc.label,
                doc.words,
                head,
            );
            // And a word that takes an argument must be probed WITH one,
            // or the probe exercises a different grammar rule.
            assert_eq!(
                doc.arg.is_some(),
                doc.probe.contains(' '),
                "`{}`: the probe and the declared arity disagree",
                doc.label,
            );
        }
    }

    #[test]
    fn static_words_classify() {
        assert!(matches!(parse("q"), ParsedCommand::Quit));
        assert!(matches!(parse("quit"), ParsedCommand::Quit));
        assert!(matches!(parse("help"), ParsedCommand::Help));
        assert!(matches!(parse("?"), ParsedCommand::Help));
        // `:alias` resolves as the ordinary client-owned RESOURCE it is now.
        assert!(matches!(
            parse("alias"),
            ParsedCommand::Resource(ResourceId::Local(
                k9rs_proto::local_types::LocalResourceKind::Alias
            ))
        ));
        assert!(matches!(parse("home"), ParsedCommand::Overview));
    }

    /// `:ctx` is not a special word any more — it resolves through the same
    /// alias path as `:pods` and `:pf`, which is what makes it RESET the nav
    /// stack (contexts are a root, not a drill) and pick up grep, sort and
    /// the rest of the table machinery for free.
    #[test]
    fn ctx_resolves_as_an_ordinary_resource_alias() {
        for word in ["ctx", "context", "contexts"] {
            match parse(word) {
                ParsedCommand::Resource(rid) => assert_eq!(
                    rid,
                    k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id(),
                    "`:{word}` must name the contexts resource",
                ),
                _ => panic!("`:{word}` should resolve to a resource"),
            }
        }
    }

    #[test]
    fn ctx_switch_keeps_case() {
        match parse("ctx prod-us-east") {
            ParsedCommand::ContextSwitch(name) => assert_eq!(name.as_str(), "prod-us-east"),
            _ => panic!("expected ContextSwitch"),
        }
    }

    #[test]
    fn ns_switch_respects_all_and_named() {
        match parse("ns kube-system") {
            ParsedCommand::NamespaceSwitch(Namespace::Named(n)) => assert_eq!(n, "kube-system"),
            _ => panic!("expected NamespaceSwitch(Named)"),
        }
        match parse("ns all") {
            ParsedCommand::NamespaceSwitch(Namespace::All) => {}
            _ => panic!("expected NamespaceSwitch(All)"),
        }
    }

    #[test]
    fn bare_resource_alias_resolves() {
        match parse("pods") {
            ParsedCommand::Resource(rid) => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Pod));
            }
            _ => panic!("expected Resource"),
        }
    }

    #[test]
    fn resource_with_filter_runs_before_resource_with_ns() {
        // The pass-11 bug: `pods /Nginx` used to parse as namespace `/Nginx`.
        // The filter rule must come first.
        match parse("pods /Nginx") {
            ParsedCommand::ResourceFilter { rid, filter } => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Pod));
                assert_eq!(filter, "Nginx", "filter must preserve case");
            }
            other => panic!("expected ResourceFilter, got {:?}", match other {
                ParsedCommand::NamespaceSwitch(_) => "NamespaceSwitch",
                ParsedCommand::ResourceInNamespace { .. } => "ResourceInNamespace",
                _ => "other",
            }),
        }
    }

    #[test]
    fn resource_in_namespace_classifies() {
        match parse("deploy kube-system") {
            ParsedCommand::ResourceInNamespace { rid, namespace } => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Deployment));
                assert_eq!(namespace, Namespace::Named("kube-system".into()));
            }
            _ => panic!("expected ResourceInNamespace"),
        }
    }

    #[test]
    fn cluster_scoped_rejects_ns_arg() {
        // Nodes are cluster-scoped — "nodes foo" isn't a ns-switch, so it
        // falls through to the CRD-candidate fallback.
        match parse("nodes foo") {
            ParsedCommand::CrdCandidate(_) => {}
            _ => panic!("expected CrdCandidate fallback for cluster-scoped + ns"),
        }
    }

    #[test]
    fn unknown_input_falls_through_to_crd_candidate() {
        match parse("clickhouseinstallation") {
            ParsedCommand::CrdCandidate(name) => assert_eq!(name, "clickhouseinstallation"),
            _ => panic!("expected CrdCandidate fallback"),
        }
    }

    #[test]
    fn bare_ns_alias_opens_namespace_resource() {
        // `:ns` alone is the resource alias for Namespace (matches rule 4,
        // "plain resource alias"). The `:ns <name>` form only triggers
        // when there's an explicit namespace argument.
        match parse("ns") {
            ParsedCommand::Resource(rid) => {
                assert_eq!(rid.built_in_kind(), Some(BuiltInKind::Namespace));
            }
            _ => panic!("expected Resource(Namespace)"),
        }
    }

/// …and a `TextKey` really does type into the sinks.
#[test]
fn a_text_key_types_into_the_filter_and_the_command_bar() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use k9rs_app::TextKey;
    let text = |c: char| {
        TextKey::new(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)).expect("text")
    };

    let mut app = App::new_for_test();
    app.nav.top_mut().filter_input_mut().expect("table takes a filter").start();
    assert!(handle_filter_key(&mut app, text('c')));
    assert_eq!(app.nav.top().filter_input().text(), "c");

    let mut app = App::new_for_test();
    app.ui.open(k9rs_app::Modal::Command { input: k9rs_app::TextInput::new(), history_index: None });
    assert!(matches!(
        edit_command_key(&mut app, text('g')),
        CommandKey::Consumed,
    ));
    assert_eq!(app.ui.command_input().map(|(i, _)| i.to_string()), Some("g".to_string()));
}

/// Every text sink moves its caret on the arrows — the command bar
/// included, which it did not.
///
/// `edit_command_key`'s or-pattern admitted `Backspace | Delete | Home |
/// End | Char(_)` and not the arrows, so the `KeyCode::Left`/`Right` arms
/// written inside it could never fire: the key fell to the outer `_ => {}`,
/// was reported CONSUMED, and the caret stayed put. Nothing warned —
/// `unreachable_pattern` cannot fire on a well-typed `KeyCode` arm — and the
/// existing test typed only a `Char`.
///
/// Driven across the sinks together, because "all the text boxes behave the
/// same way" is the actual claim and the command bar was the one exception.
#[test]
fn the_arrows_move_the_caret_in_every_text_sink() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use k9rs_app::TextKey;
    let key = |code: KeyCode| {
        TextKey::new(KeyEvent::new(code, KeyModifiers::NONE)).expect("a text key")
    };

    // --- the command bar ---
    let mut app = App::new_for_test();
    app.ui.open(k9rs_app::Modal::Command {
        input: k9rs_app::TextInput::seeded("pods"),
        history_index: None,
    });
    let caret =
        |app: &App| app.ui.command_prompt().expect("open").1.cursor_col();
    assert_eq!(caret(&app), 4, "seeded with the caret at the end");

    for _ in 0..2 {
        assert!(matches!(edit_command_key(&mut app, key(KeyCode::Left)), CommandKey::Consumed));
    }
    assert_eq!(caret(&app), 2, "Left moved it — this is what was swallowed");
    assert!(matches!(edit_command_key(&mut app, key(KeyCode::Right)), CommandKey::Consumed));
    assert_eq!(caret(&app), 3, "and Right moves it back");

    // Typing lands AT the caret, which is the point of having one.
    assert!(matches!(edit_command_key(&mut app, key(KeyCode::Char('X'))), CommandKey::Consumed));
    assert_eq!(
        app.ui.command_prompt().map(|(_, t)| t.value().to_string()),
        Some("podXs".to_string()),
        "inserted mid-word, not appended",
    );

    // --- the table filter, which already worked: the same assertions, so
    // the two cannot drift apart again without one of them failing.
    let mut app = App::new_for_test();
    {
        let fi = app.nav.top_mut().filter_input_mut().expect("a table filters");
        fi.start();
        *fi.input_mut() = k9rs_app::TextInput::seeded("pods");
    }
    assert!(handle_filter_key(&mut app, key(KeyCode::Left)));
    assert!(handle_filter_key(&mut app, key(KeyCode::Left)));
    assert_eq!(app.nav.top().filter_input().caret_col(), 2);
    assert!(handle_filter_key(&mut app, key(KeyCode::Char('X'))));
    assert_eq!(app.nav.top().filter_input().text(), "poXds");
}
