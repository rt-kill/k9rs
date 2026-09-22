use super::*;
use crate::kube::protocol::ExecPlaceholder;

fn pod_target() -> ExecTarget {
    ExecTarget::Pod {
        pod: "my-pod".into(),
        namespace: "default".into(),
        container: "nginx".into(),
    }
}

fn node_target() -> ExecTarget {
    ExecTarget::Node { node: "worker-1".into() }
}

#[test]
fn resolve_namespace_on_pod() {
    let result = resolve_placeholder(&ExecPlaceholder::Namespace, &pod_target());
    assert_eq!(result, Some("default".into()));
}

#[test]
fn resolve_namespace_on_node_returns_none() {
    let result = resolve_placeholder(&ExecPlaceholder::Namespace, &node_target());
    assert_eq!(result, None);
}

#[test]
fn resolve_node_name_on_node() {
    let result = resolve_placeholder(&ExecPlaceholder::NodeName, &node_target());
    assert_eq!(result, Some("node/worker-1".into()));
}

#[test]
fn resolve_pod_name_on_node_returns_none() {
    let result = resolve_placeholder(&ExecPlaceholder::PodName, &node_target());
    assert_eq!(result, None);
}

#[test]
fn resolve_container_on_pod_with_empty_container() {
    let target = ExecTarget::Pod {
        pod: "p".into(),
        namespace: "ns".into(),
        container: String::new(),
    };
    let result = resolve_placeholder(&ExecPlaceholder::Container, &target);
    assert_eq!(result, Some(String::new()), "empty container is valid — ConditionalPair skips it");
}

// ---------------------------------------------------------------------------
// Resume-window debris classifier (2026-08 audit: the old blanket drain ate
// real type-ahead; the sieve must swallow shredded terminal responses ONLY)
// ---------------------------------------------------------------------------

fn key(code: crossterm::event::KeyCode) -> CtEvent {
    CtEvent::Key(crossterm::event::KeyEvent::new(
        code,
        crossterm::event::KeyModifiers::NONE,
    ))
}

fn ch(c: char) -> CtEvent {
    key(crossterm::event::KeyCode::Char(c))
}

/// Feed events through the classifier the way `drain_stale_input` does,
/// including its end-of-window flush of an unconfirmed Esc.
fn sieve(events: Vec<CtEvent>) -> Vec<CtEvent> {
    let mut kept = Vec::new();
    let mut state = DebrisState::Idle;
    for ev in events {
        state = debris_step(state, ev, &mut kept);
    }
    if let DebrisState::PendingEsc(esc) = state {
        kept.push(esc);
    }
    kept
}

#[test]
fn cursor_report_shred_is_swallowed_typing_after_it_survives() {
    use crossterm::event::KeyCode;
    // ESC [ 1 5 ; 4 2 R — a cursor-position report crossterm shredded —
    // followed by a real 'j'. The spurious trailing letter (the class of
    // the old stray-'r' restart dialog) must die; the 'j' must live.
    let kept = sieve(vec![
        key(KeyCode::Esc),
        ch('['), ch('1'), ch('5'), ch(';'), ch('4'), ch('2'), ch('R'),
        ch('j'),
    ]);
    assert_eq!(kept, vec![ch('j')]);
}

#[test]
fn device_attributes_reply_is_swallowed() {
    use crossterm::event::KeyCode;
    // ESC [ ? 6 4 ; 4 c — a DA1 reply.
    let kept = sieve(vec![
        key(KeyCode::Esc),
        ch('['), ch('?'), ch('6'), ch('4'), ch(';'), ch('4'), ch('c'),
    ]);
    assert!(kept.is_empty());
}

#[test]
fn plain_typeahead_is_never_eaten() {
    use crossterm::event::KeyCode;
    let typed = vec![ch('j'), ch('k'), key(KeyCode::Enter), ch('5'), ch('R'), ch('/')];
    assert_eq!(sieve(typed.clone()), typed, "digits/letters outside a run are real keys");
}

#[test]
fn real_esc_is_released_when_no_introducer_follows() {
    use crossterm::event::KeyCode;
    // A user pressing Esc then 'j' in the settle window: the Esc is held
    // until the next event proves it wasn't a shred — both survive.
    let kept = sieve(vec![key(KeyCode::Esc), ch('j')]);
    assert_eq!(kept, vec![key(KeyCode::Esc), ch('j')]);
}

#[test]
fn trailing_esc_is_kept_at_window_end() {
    use crossterm::event::KeyCode;
    // Debris Esc is always followed by its body inside the window; a
    // window ending on a bare Esc means the user pressed Esc.
    let kept = sieve(vec![ch('x'), key(KeyCode::Esc)]);
    assert_eq!(kept, vec![ch('x'), key(KeyCode::Esc)]);
}

#[test]
fn malformed_shred_tail_stops_consuming() {
    use crossterm::event::KeyCode;
    // ESC [ 5 <Up>: the arrow can't be sequence body (crossterm parses
    // KNOWN sequences whole), so consumption stops and the arrow is real.
    let kept = sieve(vec![key(KeyCode::Esc), ch('['), ch('5'), key(KeyCode::Up)]);
    assert_eq!(kept, vec![key(KeyCode::Up)]);
}

#[test]
fn focus_and_resize_churn_is_dropped_without_breaking_a_run() {
    use crossterm::event::KeyCode;
    // Transition noise interleaved into a shred neither survives nor
    // derails the body consumption.
    let kept = sieve(vec![
        key(KeyCode::Esc),
        ch('['),
        CtEvent::Resize(80, 24),
        ch('1'), ch('R'),
        ch('q'),
    ]);
    assert_eq!(kept, vec![ch('q')]);
}

// ---------------------------------------------------------------------------
// OSC / DCS replies (2026-09-14, user-reported): `:q` out of the editor and
// the command bar contained `:2f2f/3434/3f3fg`.
// ---------------------------------------------------------------------------

fn alt(c: char) -> CtEvent {
    CtEvent::Key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char(c),
        crossterm::event::KeyModifiers::ALT,
    ))
}

fn ctrl(c: char) -> CtEvent {
    CtEvent::Key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char(c),
        crossterm::event::KeyModifiers::CONTROL,
    ))
}

/// The reported bug, byte for byte.
///
/// `ESC ] 1 1 ; r g b : 2 f 2 f / 3 4 3 4 / 3 f 3 f BEL` — the terminal's
/// answer to an OSC 11 background-colour query, which editors send on startup
/// (nvim does) and which can land in OUR stdin if it arrives after the editor
/// exits. The sieve only knew CSI (`ESC [`) and SS3 (`ESC O`), so every byte
/// of this leaked through as a keystroke: the `:` of `rgb:` opened command
/// mode and `2f2f/3434/3f3f` was typed into it.
///
/// The old doc comment even predicted it — "an exotic response type (OSC/DCS
/// replies, never provoked by our transitions) could still leak keys". The
/// parenthetical stopped being true the moment a subprocess did the provoking.
#[test]
fn an_osc_colour_reply_is_swallowed_whole() {
    use crossterm::event::KeyCode;
    let kept = sieve(vec![
        key(KeyCode::Esc),
        ch(']'), ch('1'), ch('1'), ch(';'), ch('r'), ch('g'), ch('b'), ch(':'),
        ch('2'), ch('f'), ch('2'), ch('f'), ch('/'),
        ch('3'), ch('4'), ch('3'), ch('4'), ch('/'),
        ch('3'), ch('f'), ch('3'), ch('f'),
        ctrl('g'), // BEL — crossterm delivers 0x07 as Ctrl-G
        ch('j'),   // a real keypress after it
    ]);
    assert_eq!(kept, vec![ch('j')], "only the real key survives");
}

/// Same reply, but crossterm folded `ESC ]` into one Alt-modified event
/// because both bytes landed in the same read — and terminated with `ST`
/// (`ESC \`) rather than `BEL`. Both shapes occur; both must be caught.
#[test]
fn an_osc_reply_is_swallowed_when_folded_and_st_terminated() {
    use crossterm::event::KeyCode;
    let kept = sieve(vec![
        alt(']'),
        ch('1'), ch('1'), ch(';'), ch('r'), ch('g'), ch('b'), ch(':'),
        ch('2'), ch('f'), ch('2'), ch('f'),
        key(KeyCode::Esc), ch('\\'), // ST
        ch('k'),
    ]);
    assert_eq!(kept, vec![ch('k')]);

    // ST can itself arrive folded as Alt-\.
    let kept = sieve(vec![alt(']'), ch('1'), ch('1'), alt('\\'), ch('k')]);
    assert_eq!(kept, vec![ch('k')]);
}

/// A string that never terminates must not eat the rest of the window.
///
/// This is the trade the CSI sieve was built around — real typing is never
/// eaten — and a state that consumes until a terminator could break it. The
/// body cap is what keeps the failure bounded.
#[test]
fn an_unterminated_string_gives_up_instead_of_eating_typing() {
    use crossterm::event::KeyCode;
    let mut events = vec![key(KeyCode::Esc), ch(']')];
    events.extend(std::iter::repeat_with(|| ch('x')).take(super::STRING_BODY_CAP + 1));
    events.push(ch('j'));
    let kept = sieve(events);
    assert!(
        kept.contains(&ch('j')),
        "past the cap the sieve starts keeping events again: {kept:?}",
    );
}

/// DCS / PM / APC use the same string grammar and the same terminator.
#[test]
fn the_other_string_introducers_are_covered_too() {
    use crossterm::event::KeyCode;
    for introducer in ['P', '^', '_'] {
        let kept = sieve(vec![
            key(KeyCode::Esc),
            ch(introducer), ch('1'), ch('$'), ch('r'),
            key(KeyCode::Esc), ch('\\'),
            ch('j'),
        ]);
        assert_eq!(kept, vec![ch('j')], "ESC {introducer} … ST");
    }
}

/// A real Esc followed by `]` typed by hand still reaches the app as an Esc.
/// The introducer check must not make Esc-then-bracket unreachable — though
/// the run after it is consumed, which is the same bounded trade CSI makes.
#[test]
fn a_bare_esc_not_followed_by_an_introducer_is_still_real() {
    use crossterm::event::KeyCode;
    let kept = sieve(vec![key(KeyCode::Esc), ch('j')]);
    assert_eq!(kept, vec![key(KeyCode::Esc), ch('j')]);
}

/// THE key-routing invariant, and a live regression against it: if
/// `handler::handle_key_event` has an answer for a key, the input-mode pass
/// in front of it must not swallow that key first.
///
/// `try_handle_input_mode` ends in a blanket `if app.ui.is_modal() { return
/// true }`. That guard was added to stop a modal NOBODY handles from leaking
/// keys to the table underneath it. But every modal IS handled — the overlay
/// match in `handle_key_event` is exhaustive — so the guard does not catch a
/// gap, it preempts the handlers. With `?` help up, the five input sinks all
/// decline, `is_modal()` returns true, and Esc/q/j/k never reach
/// `handle_help_view_keys` at all: the overlay cannot be closed.
#[test]
fn a_modal_with_a_key_handler_is_not_swallowed_before_reaching_it() {
    use crate::app::{Modal, Overlay, TextKey};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    let target = crate::kube::protocol::ObjectRef::new(
        crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
        "p".to_string(),
        crate::kube::protocol::Namespace::Named("default".to_string()),
    );
    // EVERY modal, not just the one the user reported: they were all broken
    // by the same blanket guard, and a new one must not be able to join them.
    let modals: Vec<(&str, Modal)> = vec![
        (
            "help",
            Modal::Overlay(Overlay::ContainerSelect {
                target: target.clone(),
                choices: vec![crate::app::ContainerChoice {
                    label: "web".into(),
                    container: crate::kube::protocol::LogContainer::Named("web".into()),
                }],
                selected: 0,
                action: crate::app::ContainerAction::Logs,
            }),
        ),
        (
            "container picker",
            Modal::Overlay(Overlay::ContainerSelect {
                target: target.clone(),
                choices: vec![crate::app::ContainerChoice {
                    label: "web".into(),
                    container: crate::kube::protocol::LogContainer::Named("web".into()),
                }],
                selected: 0,
                action: crate::app::ContainerAction::Logs,
            }),
        ),
        (
            "edit flow",
            Modal::Overlay(Overlay::Edit {
                target: target.clone(),
                state: crate::app::EditState::AwaitingYaml,
            }),
        ),
        (
            "shell connect",
            Modal::Overlay(Overlay::Shell(Box::new(crate::app::ShellState {
                title: "p".to_string(),
                stream: None,
                connect_state: crate::app::ShellConnectState::Connecting,
                pending_output: Vec::new(),
            }))),
        ),
        (
            "delete confirmation",
            Modal::Confirm(crate::app::ConfirmDialog {
                message: "Delete pod p?".to_string(),
                action_label: "Delete".to_string(),
                pending: crate::app::PendingAction::Single {
                    op: crate::app::SingleOp::Delete,
                    target,
                },
                action_focused: false,
            }),
        ),
    ];

    for (name, modal) in modals {
        let mut app = App::new_for_test();
        app.ui.open(modal);

        // Downstream HAS an answer for Esc…
        assert!(
            crate::event::handler::handle_key_event(&app, esc).is_some(),
            "{name}: handle_key_event must answer Esc",
        );

        // …but none of the input sinks that can run without a live session
        // claims the key, and neither Command nor Form is open, so the
        // blanket `is_modal()` fall-through is what decides — and it
        // swallows.
        let tk = TextKey::new(esc).expect("Esc is not a Ctrl chord");
        assert!(!crate::kube::session_commands::handle_filter_key(&mut app, tk), "{name}");
        let tk = TextKey::new(esc).unwrap();
        assert!(!super::handle_log_filter_key(&mut app, tk), "{name}");
        let tk = TextKey::new(esc).unwrap();
        assert!(!super::handle_content_search_key(&mut app, tk), "{name}");
        assert!(app.ui.form_dialog().is_none(), "{name}");

        assert!(
            !app.ui.modal_without_key_handler(),
            "{name}: the routing backstop must not preempt a modal's own handler",
        );
    }
}
