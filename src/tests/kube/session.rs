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
