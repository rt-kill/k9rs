use tokio::sync::mpsc;

use crate::app::App;
use crate::app::nav::rid;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;
use crate::kube::protocol::{self, ObjectRef};
use crate::kube::resource_def::BuiltInKind;
use crate::app::element::Element;
use crate::app::store::RowPredicate;
use crate::kube::session::{ds_try, ActionResult};


// ---------------------------------------------------------------------------
// Main dispatcher — thin routing table, delegates to focused sub-functions
// ---------------------------------------------------------------------------

pub(crate) fn handle_action(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;

    // Optimization, not a correctness gate: selection reads are total
    // (cold cache → fresh order derive inside `selected_key`), but warming
    // the memo here lets the action's resolution and the next paint share
    // ONE derive, and gives relative moves a length to clamp against
    // pre-paint. Cheap + no-op for non-table tops.
    let level = app.ui.column_level;
    let max_col_width = app.config.ui.max_column_width;
    app.nav.top_mut().ensure_view_cached(level, max_col_width);

    match action {
        // --- Lifecycle ---
        Action::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        Action::Back => {
            // ONE undo path: close the overlay if any, else pop the top
            // element (reviving its data if the subscription died while
            // covered). At the root: no-op.
            if app.ui.takeover().is_some() {
                app.ui.close_modal();
            } else if app.nav.pop().is_some() {
                app.nav.ensure_top_live(data_source);
            }
        }
        Action::Help => {
            // `?` pushes an ELEMENT, so help gets grep, scroll, sort, column
            // movement and copy for free, and Esc is the ordinary nav pop —
            // no bespoke key path to remember, which is what let every modal
            // lose its Esc when a routing guard was added upstream of them.
            if app.nav.top().is_keybindings_view() {
                app.nav.pop();
            } else {
                let class = crate::event::keymap::ViewClass::of(app.nav.top());
                let caps = Some(app.current_capabilities());
                let markable = app.nav.top().markable();
                let store =
                    app.core.seed_keybindings(caps, app.config.keys, class, markable);
                let rid = crate::kube::local::LocalResourceKind::Keybinding.to_resource_id();
                app.nav.push(crate::app::element::Element::ResourceList(
                    crate::app::element::ResourceList::client(
                        crate::app::element::QuerySpec {
                            rid,
                            namespace: crate::kube::protocol::Namespace::All,
                            filter: None,
                        },
                        store,
                        &app.kube.metrics,
                        "keys".to_string(),
                    ),
                ));
            }
        }

        // --- Tab switching ---
        //
        // Refused with no context: every resource root would open a
        // subscription against a session that doesn't exist, landing the user
        // on a permanently empty table — and, because it `nav.reset`s, taking
        // the picker with it. The picker is the ROOT precisely so it can't be
        // escaped; Tab was walking around that.
        Action::NextTab | Action::PrevTab
            if matches!(
                crate::app::Liveness::of_link(&app.conn),
                crate::app::Liveness::NoContext
            ) =>
        {
            app.ui.flash = Some(crate::app::FlashMessage::warn(
                "Select a context first".to_string(),
            ));
        }
        Action::NextTab => {
            let new_rid = app.next_tab();
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, new_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
        }
        Action::PrevTab => {
            let new_rid = app.prev_tab();
            let root = App::root_list_element(
                data_source, &app.kube.metrics,
                &app.core, new_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
        }

        // --- Scroll / in-view navigation ---
        a @ (Action::NextItem | Action::PrevItem | Action::PageUp | Action::PageDown
            | Action::Home | Action::End | Action::ScrollUp(_) | Action::ScrollDown(_)
            | Action::ColLeft | Action::ColRight | Action::PageLeft | Action::PageRight
            | Action::ColFirst | Action::ColLast) => {
            handle_scroll(app, a);
        }

        // --- Already-extracted resource actions ---
        Action::Enter => return handle_enter(app, data_source),
        Action::Describe => handle_describe(app, data_source),
        Action::Yaml => handle_yaml(app, data_source),
        Action::Logs => handle_logs(app, data_source),
        Action::PreviousLogs => handle_previous_logs(app, data_source),

        // --- Resource CRUD / mutation operations ---
        a @ (Action::Shell | Action::Delete | Action::Edit | Action::Scale
            | Action::Restart | Action::ForceKill | Action::DecodeSecret
            | Action::TriggerCronJob | Action::SuspendCronJob) => {
            return handle_resource_op(app, a, data_source);
        }

        // --- Batch operations (produced only by the select-mode gate) ---
        a @ (Action::BatchDelete | Action::BatchRestart | Action::BatchForceKill) => {
            handle_batch_op(app, a);
        }
        Action::BatchLogs => handle_batch_logs(app, data_source),

        // --- Confirmation dialog ---
        Action::Confirm => return handle_confirm_action(app, data_source),
        // Cancel is the ordinary pop now — the dialog is on the stack.
        Action::Cancel => {
            if matches!(app.nav.top(), crate::app::element::Element::Confirm(_)) {
                app.nav.pop();
            }
        }
        Action::ToggleDialogButton => {
            if let crate::app::element::Element::Confirm(d) = app.nav.top_mut() {
                d.action_focused = !d.action_focused;
            }
        }

        // --- Filter & search ---
        a @ (Action::Filter(_) | Action::ColumnFilter | Action::ClearFilter
            | Action::ToggleFaultFilter | Action::SearchStart | Action::SearchExec(_)
            | Action::SearchNext | Action::SearchPrev | Action::SearchClear) => {
            handle_filter_search(app, a, data_source);
        }

        // --- Log view actions ---
        a @ (Action::ToggleLogFollow | Action::ToggleLogWrap | Action::ToggleLogTimestamps
            | Action::ClearLogs | Action::LogSince(_)
            | Action::SaveLogs) => {
            handle_log_action(app, a, data_source);
        }

        // --- Drill-down navigation ---
        a @ (Action::ShowNode | Action::UsedBy | Action::JumpToOwner | Action::NodeShell
            | Action::ShowEvents | Action::OverlayCapability(_)) => {
            return handle_drill(app, a, data_source);
        }

        // --- Clipboard / file I/O ---
        a @ (Action::Copy | Action::BatchCopy | Action::SaveTable) => {
            handle_io(app, a, event_tx, data_source);
        }

        // --- Simple inline actions ---
        Action::SwitchNamespace(ns) => do_switch_namespace(app, data_source, ns),
        Action::SwitchContext(ctx) => begin_context_switch(app, &ctx),
        Action::CommandMode => {
            app.ui.open(crate::app::Modal::Command { input: crate::app::text_input::TextInput::new(), history_index: None });
        }
        Action::Sort(target) => app.sort_by(target),
        Action::ToggleSortDirection => app.toggle_sort_direction(),
        Action::ToggleHeader => { app.ui.show_header = !app.ui.show_header; }
        Action::ToggleWide => {
            app.ui.column_level = app.ui.column_level.next();
            app.ui.flash = Some(crate::app::FlashMessage::info(
                format!("Columns: {}", app.ui.column_level.label())
            ));
        }
        Action::ToggleMark => match app.toggle_mark() {
            crate::app::element::MarkOutcome::Toggled => {}
            crate::app::element::MarkOutcome::RowGone => {
                app.ui.flash = Some(crate::app::FlashMessage::warn(
                    "No row under the cursor".to_string(),
                ));
            }
            crate::app::element::MarkOutcome::Unsupported => {
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    "Marking isn't available in this view".to_string(),
                ));
            }
        },
        Action::SpanMark => match app.span_mark() {
            crate::app::element::SpanOutcome::Applied => {}
            crate::app::element::SpanOutcome::NoAnchor => {
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    "No visible mark to span from — Space marks the row".to_string(),
                ));
            }
            crate::app::element::SpanOutcome::Unsupported => {
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    "Marking isn't available in this view".to_string(),
                ));
            }
        },
        Action::ClearMarks => {
            if !app.clear_marks() {
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    "Marking isn't available in this view".to_string(),
                ));
            }
        }
        Action::ToggleLastView => {
            if let Some(crate::app::nav::RootSpec::Resource(last_rid)) = app.nav.prev_root().cloned() {
                let root = App::root_list_element(
                    data_source, &app.kube.metrics,
                &app.core, last_rid, app.kube.selected_ns.clone(),
                );
                app.nav.reset(root);
            }
        }
        Action::ShowPortForwards => handle_show_port_forwards(app, data_source),
        Action::PortForward => {
            if !app.current_capabilities().supports(crate::kube::protocol::OperationKind::PortForward) {
                return ActionResult::None;
            }
            let schema = &crate::kube::protocol::FormSchema::PORT_FORWARD;
            let row = app.nav.top().selected_row();
            let headers = app.nav.top().headers_snapshot();
            if let Some(info) = get_selected_resource_info(app) {
                if let Some(dialog) = build_form_from_schema(schema, crate::kube::protocol::OperationKind::PortForward, info, row.as_ref(), &headers) {
                    app.nav.push(crate::app::element::Element::Form(Box::new(dialog)));
                } else {
                    app.ui.flash = Some(crate::app::FlashMessage::error(
                        "No target found for port-forward".to_string()
                    ));
                }
            } else {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    "No target found for port-forward".to_string()
                ));
            }
        }
        Action::Refresh => handle_refresh(app, data_source),
        Action::ShowAliases => handle_show_aliases(app, data_source),
        Action::FlashInfo(msg) => {
            app.ui.flash = Some(crate::app::FlashMessage::info(msg));
        }
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Scroll / in-view navigation
// ---------------------------------------------------------------------------

fn handle_scroll(app: &mut App, action: crate::app::actions::Action) {
    use crate::app::actions::Action;
    use crate::app::element::Element;

    // Overlays capture scroll keys first (help sheet, container picker).
    // The one overlay that still takes scroll keys. `if let` rather than a
    // `match` with a `_ => {}`: after help became a nav element there is only
    // one arm left, and a wildcard would hide the next one that needs adding.
    if let Element::Picker(p) = app.nav.top_mut() {
        let (choices, selected) = (&p.choices, &mut p.selected);
        match action {
            Action::NextItem => {
                if !choices.is_empty() && *selected + 1 < choices.len() {
                    *selected += 1;
                }
            }
            Action::PrevItem => *selected = selected.saturating_sub(1),
            _ => {}
        }
        return;
    }


    match app.nav.top_mut() {
        // Log views: scroll over the derived visible-line set.
        el @ (Element::LogSession(_) | Element::LogFilter(_)) => {
            // Pure intent — the log widget owns the physical (wrap-expanded)
            // geometry and publishes it back into the viewport each frame, so
            // the action layer never guesses a height or a line-to-row count.
            let Some(view) = el.log_view_mut() else { return };
            match action {
                Action::PageUp => view.viewport.page_up(),
                Action::PageDown => view.viewport.page_down(),
                Action::Home => view.viewport.home(),
                Action::End => view.viewport.end(),
                Action::ScrollUp(n) => view.viewport.line_up(n),
                Action::ScrollDown(n) => view.viewport.line_down(n),
                // The other axis, same rule: intent only. `Pan` holds the
                // step and the bound, both published by the last render.
                Action::ColLeft => view.pan.left(),
                Action::ColRight => view.pan.right(),
                Action::ColFirst => view.pan.home(),
                Action::ColLast => view.pan.end(),
                Action::PageLeft => view.pan.page_left(),
                Action::PageRight => view.pan.page_right(),
                _ => {}
            }
        }
        // A confirm box has nothing to scroll; the picker moves its own
        // cursor in the arm above, before this dispatch.
        Element::Confirm(_) | Element::Picker(_) | Element::Form(_) => {}
        // Content views: pure intent — the render publishes the (non-wrapping)
        // extent back into the viewport, so no geometry is guessed here.
        Element::ContentView(cv) => match action {
            Action::NextItem => cv.state.viewport.line_down(1),
            Action::PrevItem => cv.state.viewport.line_up(1),
            Action::PageUp => cv.state.viewport.page_up(),
            Action::PageDown => cv.state.viewport.page_down(),
            Action::Home => cv.state.viewport.home(),
            Action::End => cv.state.viewport.end(),
            _ => {}
        }
        // Row-bearing table views: cursor lives on the element's
        // TableInteraction.
        Element::ResourceList(_) | Element::RowFilter(_) | Element::DerivedRows(_)
        | Element::Overview(_) => match action {
            Action::NextItem => app.select_next(),
            Action::PrevItem => app.select_prev(),
            Action::PageUp => app.page_up(),
            Action::PageDown => app.page_down(),
            Action::Home => app.go_home(),
            Action::End => app.go_end(),
            Action::ColLeft => app.col_left(),
            Action::ColRight => app.col_right(),
            Action::ColFirst => app.col_first(),
            Action::ColLast => app.col_last(),
            _ => {}
        },
    }
}

// ---------------------------------------------------------------------------
// Resource CRUD / mutation operations
// ---------------------------------------------------------------------------

fn handle_resource_op(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;

    // Derived views (container table, etc.) only support Shell — all other
    // mutating operations (delete, edit, scale, restart) don't apply to
    // derived rows. Shell is handled specially below (uses nav.source).
    if app.nav.top().derived_kind().is_some() && !matches!(action, Action::Shell) {
        return ActionResult::None;
    }

    match action {
        Action::Shell => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::Shell) {
                // Derived view (container table): shell directly into
                // the selected container; the element's origin IS the
                // parent pod.
                if app.nav.top().derived_kind().is_some() {
                    let origin = app.nav.top().origin().cloned();
                    let item = app.nav.top().selected_row();
                    if let (Some(origin), Some(item)) = (origin, item) {
                        // Only a Named namespace can be shelled into; `All` is
                        // structurally excluded (and a namespace literally named
                        // "all" is `Named("all")`, so it works correctly — the
                        // old `ns != "all"` magic-string guard wrongly rejected it).
                        if let crate::kube::protocol::Namespace::Named(ns) = &origin.namespace {
                            return ActionResult::Exec {
                                op: crate::kube::protocol::OperationKind::Shell,
                                target: crate::kube::session::ExecTarget::Pod {
                                    pod: origin.name.clone(),
                                    namespace: ns.clone(),
                                    container: item.name.clone(),
                                },
                            };
                        }
                    }
                    return ActionResult::None;
                }
                let Some(current_rid) = app.nav.resource_id().cloned() else { return ActionResult::None; };
                if let Some(row) = app.nav.top().selected_row() {
                    let Some(pod_ns) = row.namespace.clone() else {
                        app.ui.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has no resolved namespace", row.name)
                        ));
                        return ActionResult::None;
                    };
                    if pod_ns.is_empty() {
                        app.ui.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has empty namespace", row.name)
                        ));
                        return ActionResult::None;
                    }
                    let pod_name = row.name.clone();
                    let containers = &row.containers;
                    let target = crate::kube::protocol::ObjectRef::new(
                        current_rid,
                        pod_name.clone(),
                        crate::kube::protocol::Namespace::from_row(&pod_ns),
                    );
                    if containers.len() > 1 {
                        app.ui.close_modal();
                        let action = crate::app::ContainerAction::Shell;
                        app.nav.push(crate::app::element::Element::Picker(Box::new(crate::app::ContainerPicker {
                            target,
                            choices: crate::app::ContainerChoice::list(action, containers),
                            selected: 0,
                            action,
                        })));
                    } else {
                        let container = containers.first().map(|c| c.name.clone()).unwrap_or_default();
                        return ActionResult::Exec { op: crate::kube::protocol::OperationKind::Shell, target: crate::kube::session::ExecTarget::Pod {
                            pod: pod_name,
                            namespace: pod_ns,
                            container,
                        } };
                    }
                }
            }
        }
        // Single-target by construction: the select-mode gate transforms
        // these into Batch* while marks are active, so no "marked set
        // non-empty?" branch exists here to silently substitute batch
        // semantics (or, stale-frame-wise, single semantics).
        Action::Delete => {
            if let Some(info) = require_selected(app) {
                app.nav.push(crate::app::element::Element::Confirm(Box::new(crate::app::ConfirmDialog {
                    message: format!("Delete {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Delete".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, target: info },
                    action_focused: false,
                })));
            }
        }
        Action::Edit => {
            if matches!(app.ui.takeover(), Some(crate::app::TakeoverOp::Edit { .. })) {
                app.ui.flash = Some(crate::app::FlashMessage::warn("Edit already in progress".to_string()));
                return ActionResult::None;
            }
            if let Some(info) = require_selected(app) {
                ds_try!(app, data_source.yaml(&info));
                app.ui.close_modal();
                app.ui.close_modal();
                app.ui.open(crate::app::Modal::Takeover(crate::app::TakeoverOp::Edit {
                    target: info,
                    state: crate::app::EditState::AwaitingYaml,
                }));
            }
        }
        Action::Scale => {
            if let Some(info) = require_selected(app) {
                let schema = &crate::kube::protocol::FormSchema::SCALE;
                let row = app.nav.top().selected_row();
                let headers = app.nav.top().headers_snapshot();
                if let Some(dialog) = build_form_from_schema(schema, crate::kube::protocol::OperationKind::Scale, info, row.as_ref(), &headers) {
                    app.nav.push(crate::app::element::Element::Form(Box::new(dialog)));
                }
            }
        }
        Action::Restart => {
            if let Some(info) = require_selected(app) {
                app.nav.push(crate::app::element::Element::Confirm(Box::new(crate::app::ConfirmDialog {
                    message: format!("Restart {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Restart".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, target: info },
                    action_focused: false,
                })));
            }
        }
        Action::ForceKill => {
            if let Some(info) = require_selected(app) {
                app.nav.push(crate::app::element::Element::Confirm(Box::new(crate::app::ConfirmDialog {
                    message: format!("Force-kill {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Force Kill".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, target: info },
                    action_focused: false,
                })));
            }
        }
        Action::DecodeSecret => {
            if let Some(info) = require_selected(app) {
                ds_try!(app, data_source.decode_secret(&info));
                // Its OWN spec kind, not `Describe`: the decoded bytes must
                // never land in the describe cache, and Ctrl-R must
                // re-decode. Fetching → the honest spinner (the old version
                // painted fake "Decoding..." text as if it were content).
                let el = crate::app::element::Element::ContentView(
                    crate::app::element::ContentView::new(
                        crate::app::element::ContentSpec::DecodedSecret(info),
                        crate::app::ContentViewState::default(),
                        crate::app::element::ContentPhase::Fetching,
                    ),
                );
                app.nav.push(el);
            }
        }
        Action::TriggerCronJob => {
            if let Some(info) = require_selected(app) {
                let name = info.name.clone();
                ds_try!(app, data_source.trigger_cronjob(&info));
                app.ui.flash = Some(crate::app::FlashMessage::info(format!("Triggering CronJob: {}", name)));
            }
        }
        Action::SuspendCronJob => {
            if let Some(info) = require_selected(app) {
                ds_try!(app, data_source.toggle_suspend_cronjob(&info));
            }
        }
        _ => {}
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Batch operations (select mode)
// ---------------------------------------------------------------------------

/// Raise the confirm dialog for a batch operation. Targets are captured
/// HERE, at keypress: marks ∩ present rows (`get_marked_resource_infos`
/// intersects at read). During the refresh window — marks survive
/// `clear()`, rows unknown — that capture is honestly empty and we
/// refuse instead of operating on unverifiable identities.
pub(crate) fn handle_batch_op(app: &mut App, action: crate::app::actions::Action) {
    use crate::app::actions::Action;

    // ONE batch at a time. The tracker is cleared the moment its last
    // result lands (or is taken on disconnect/context-switch), so
    // `Some` here means results are genuinely outstanding — dispatching
    // another batch would silently replace the tracker, duplicate the
    // sends (marks are still set until results arrive), and let the
    // first batch's late results clobber the second's summary flash.
    if app.pending_batch.is_some() {
        app.ui.flash = Some(crate::app::FlashMessage::warn(
            "A batch is still in flight — wait for its summary".to_string(),
        ));
        return;
    }

    let targets = get_marked_resource_infos(app);
    if targets.is_empty() {
        app.ui.flash = Some(crate::app::FlashMessage::warn(
            "No marked rows present (still refreshing?)".to_string(),
        ));
        return;
    }

    let (verb, label, pending) = match action {
        Action::BatchDelete => ("Delete", "Delete", crate::app::PendingAction::BatchDelete(targets.clone())),
        Action::BatchRestart => ("Restart", "Restart", crate::app::PendingAction::BatchRestart(targets.clone())),
        Action::BatchForceKill => {
            // Namespace guard BEFORE anything else — a refused batch must
            // leave the selection untouched (the old flow cleared marks
            // first, destroying the selection it then refused to act on).
            if let Some(bad) = targets.iter().find(|t| t.namespace.as_option().is_none()) {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("Batch force-kill refused: pod/{} has no resolved namespace", bad.name)
                ));
                return;
            }
            ("Force-kill", "Force Kill", crate::app::PendingAction::BatchForceKill(targets.clone()))
        }
        _ => return,
    };

    app.nav.push(crate::app::element::Element::Confirm(Box::new(crate::app::ConfirmDialog {
        message: batch_confirm_message(verb, &targets),
        action_label: label.to_string(),
        pending,
        action_focused: false,
    })));
}

/// The batch confirm text names its targets — marks live on the shared
/// store and may be hidden by the current filter, so a bare count would
/// let rows the user cannot see die on a `y`. The decision point is
/// where explicitness must land.
fn batch_confirm_message(verb: &str, targets: &[crate::kube::protocol::ObjectRef]) -> String {
    const LISTED: usize = 5;
    let noun = targets[0].resource.display_label();
    let names: Vec<&str> = targets.iter().take(LISTED).map(|t| t.name.as_str()).collect();
    let mut msg = format!(
        "{} {} {}{}?\n{}",
        verb,
        targets.len(),
        noun,
        if targets.len() == 1 { "" } else { "s" },
        names.join(", "),
    );
    if targets.len() > LISTED {
        msg.push_str(&format!(" …and {} more", targets.len() - LISTED));
    }
    msg
}

/// Select mode's log key: ONE merged log view over every marked pod.
///
/// The only `BatchSupport::Aggregate` action, and the reason that stance
/// exists — `BatchDelete` fans out to N independent commands with N results,
/// while this collapses to a single `LogInit` the daemon fans out and merges
/// back. N log views would answer a question nobody asked.
///
/// Two things it refuses rather than guesses:
/// Each marked row resolves through the SAME `row_log_target` a single
/// `Ctrl-L` uses, so a hovered deployment and a marked one can't disagree
/// about which pods they mean; the union is what lets a mixed selection —
/// two deployments, a job and a bare pod — be one stream. Inside a
/// container projection the selection lands on the other axis instead
/// (see [`batch_container_log_spec`]).
///
/// Two things it still refuses rather than guesses:
/// - **A selection spanning namespaces.** One stream carries one namespace
///   (`LogInit.namespace`), which is the same constraint `kubectl logs` has.
/// - **Rows with no logs at all** — a namespace, a configmap. A row earns a
///   place in the union by knowing where its pods are, not by being a pod.
fn handle_batch_logs(app: &mut App, data_source: &mut ClientSession) {
    match batch_log_spec(app, app.config.ui.logs.tail_lines) {
        Ok(spec) => push_log_session(app, data_source, spec),
        Err(why) => app.ui.flash = Some(crate::app::FlashMessage::warn(why)),
    }
}

/// The merged log stream a marked selection defines — or, as `Err`, the
/// reason there isn't one. Separated from the handler so the decision is
/// testable without a live session; the handler only pushes or flashes.
pub(crate) fn batch_log_spec(app: &App, tail: u64) -> Result<protocol::LogInit, String> {
    // A projection's marked rows aren't resources in a store, so the store
    // path below cannot read them — and the match is exhaustive, so a new
    // projection kind has to decide what its marked rows mean rather than
    // silently inheriting the parent store's marks.
    if let Some(kind) = app.nav.top().derived_kind() {
        return match kind {
            crate::app::view::DerivedViewKind::Containers => {
                batch_container_log_spec(app, tail)
            }
        };
    }

    let Some(store) = app.nav.top().data_store() else {
        return Err("Nothing here to log".to_string());
    };
    let Some(rid) = app.nav.top().rid().cloned() else {
        return Err("Nothing here to log".to_string());
    };

    // Marks ∩ present rows, read once — same honesty as `handle_batch_op`:
    // during the Ctrl-R window marks survive but rows don't, so this is
    // empty rather than acting on identities the store can't vouch for.
    let marked: Vec<crate::kube::resources::row::ResourceRow> = store.with_read(|inner| {
        inner
            .rows
            .iter()
            .filter(|row| inner.marked().contains(&crate::app::store::row_key(row)))
            .cloned()
            .collect()
    });

    if marked.is_empty() {
        return Err("No marked rows present (still refreshing?)".to_string());
    }
    let ns = marked[0].namespace.clone().unwrap_or_default();
    if marked.iter().any(|r| r.namespace.clone().unwrap_or_default() != ns) {
        return Err(
            "Marked rows span namespaces — one log stream covers one namespace".to_string()
        );
    }

    // Each row says where ITS logs are, through the same resolution a single
    // `Ctrl-L` uses — so a marked deployment and a hovered one can't disagree
    // about which pods that is. The union is what makes a MIXED selection
    // expressible: two deployments, a job and a bare pod is four targets of
    // three different shapes.
    let mut targets = Vec::with_capacity(marked.len());
    for row in &marked {
        match row_log_target(row, &rid) {
            Some(t) => targets.push(t),
            None => {
                return Err(format!(
                    "{} has no logs — it names neither containers nor pods",
                    row.name,
                ))
            }
        }
    }

    // `Default` keeps single-container pods on the daemon's API path (no
    // subprocess each). Anything else in the set — a sidecar, or a workload
    // whose replicas we haven't seen — forces `--all-containers`, the only
    // shape that names every container it finds.
    let container = if marked.iter().all(|r| r.containers.len() == 1) {
        protocol::LogContainer::Default
    } else {
        protocol::LogContainer::All
    };

    // A one-row selection is just that row, so it says so; `plural()` is
    // already plural and must not be pluralized again.
    let label = match targets.as_slice() {
        [only] => only.display(),
        many => format!("{} {}", many.len(), rid.plural()),
    };
    Ok(protocol::LogInit {
        target: protocol::LogTarget::Union { label, targets },
        namespace: protocol::Namespace::from_row(&ns),
        container,
        follow: true,
        tail: Some(tail),
        since: None,
        previous: false,
    })
}

/// Batch logs inside a container projection: ONE pod (the projection's
/// origin), the marked containers of it.
///
/// The other axis of the same idea — and the reason [`protocol::LogContainer`]
/// grew a `Set`. `All` would not do: it is `--all-containers`, every
/// container of the pod, which is precisely what you said no to by marking
/// three of the eight.
fn batch_container_log_spec(app: &App, tail: u64) -> Result<protocol::LogInit, String> {
    let Some(origin) = app.nav.top().origin().cloned() else {
        return Err("Nothing here to log".to_string());
    };
    // `marked_keys` is already intersected with the live projection, and
    // returns them in VIEW order so the header reads the way the table did.
    let names: Vec<String> = app.nav.top().marked_keys().into_iter().map(|k| k.name).collect();
    if names.is_empty() {
        return Err("No marked containers present".to_string());
    }
    Ok(protocol::LogInit {
        target: protocol::LogTarget::One(origin.name.clone()),
        namespace: origin.namespace.clone(),
        container: protocol::LogContainer::Set(names),
        follow: true,
        tail: Some(tail),
        since: None,
        previous: false,
    })
}

/// Dispatch a confirmed batch: re-anchor on the CURRENT marked set, send
/// one command per survivor, and install the tracker that correlates the
/// per-target results (unmark per-success, retain marks on failure, one
/// aggregate flash).
///
/// The dialog is key-modal — every key while it is open maps to
/// Confirm/Cancel/Toggle — so the only thing that can have changed the
/// marked set since capture is the data prune itself (rows deleted
/// externally). `captured ∩ current-marks` honors exactly that: gone
/// rows are skipped, not blindly re-targeted at whatever replaced them.
fn dispatch_batch(
    app: &mut App,
    data_source: &mut ClientSession,
    batch: Vec<crate::kube::protocol::ObjectRef>,
    verb: &'static str,
    op: crate::kube::protocol::OperationKind,
    send: impl Fn(&mut ClientSession, &crate::kube::protocol::ObjectRef) -> anyhow::Result<()>,
) {
    use crate::app::BatchTracker;

    let current: std::collections::HashSet<crate::kube::protocol::ObjectKey> =
        app.nav.top().marked_keys().into_iter().collect();
    let total = batch.len();
    let live: Vec<_> = batch
        .into_iter()
        .filter(|t| current.contains(&BatchTracker::key_of(t)))
        .collect();
    let skipped = total - live.len();
    if live.is_empty() {
        app.ui.flash = Some(crate::app::FlashMessage::warn(
            "Nothing to do — the marked rows are gone".to_string(),
        ));
        return;
    }

    let noun = live[0].resource.display_label().to_string();
    let rid = live[0].resource.clone();
    let store = app
        .nav
        .top()
        .data_store()
        .map(std::sync::Arc::downgrade)
        .unwrap_or_default();
    let mut tracker = BatchTracker::new(verb, noun, rid, &live, skipped, store, op);

    for item in &live {
        // A send failure is this item's result (recorded, loop continues)
        // — not a bail that would strand the rest of the batch unsent
        // and untracked.
        if let Err(e) = send(data_source, item) {
            tracker.fail_send(item, format!("send failed: {e}"));
        }
    }

    if tracker.is_done() {
        // Every send failed synchronously — summarize now.
        app.ui.flash = Some(tracker.summary());
    } else {
        app.pending_batch = Some(tracker);
    }
}

// ---------------------------------------------------------------------------
// Confirmation dialog
// ---------------------------------------------------------------------------

fn handle_confirm_action(
    app: &mut App,
    data_source: &mut ClientSession,
) -> ActionResult {
    // Pop FIRST, then act: the dialog is the element being answered, and
    // taking it off the stack is what "answered" means. It also means the
    // operation dispatches against the view the question was asked from.
    let dialog = match app.nav.top() {
        crate::app::element::Element::Confirm(_) => match app.nav.pop() {
            Some(crate::app::element::Element::Confirm(d)) => Some(*d),
            _ => None,
        },
        _ => None,
    };
    if let Some(dialog) = dialog {
        app.kube.kubectl_cache.clear();
        match dialog.pending {
            crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, ref target } => {
                ds_try!(app, data_source.delete(target));
            }
            crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, ref target } => {
                ds_try!(app, data_source.restart(target));
            }
            crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, ref target } => {
                if target.namespace.as_option().is_none() {
                    app.ui.flash = Some(crate::app::FlashMessage::error(
                        format!("Force-kill refused: pod/{} has no resolved namespace", target.name)
                    ));
                    return ActionResult::None;
                }
                ds_try!(app, data_source.force_kill(target));
            }
            // Marks are NOT cleared here: each Ok result unmarks its row
            // via the tracker; failures keep theirs so the user can retry
            // exactly the failed set.
            crate::app::PendingAction::BatchDelete(batch) => {
                dispatch_batch(app, data_source, batch, "Deleted", crate::kube::protocol::OperationKind::Delete, |ds, t| ds.delete(t));
            }
            crate::app::PendingAction::BatchRestart(batch) => {
                dispatch_batch(app, data_source, batch, "Restarted", crate::kube::protocol::OperationKind::Restart, |ds, t| ds.restart(t));
            }
            crate::app::PendingAction::BatchForceKill(batch) => {
                // Namespaces were validated at capture (before the dialog
                // ever opened) and are immutable on the captured refs.
                dispatch_batch(app, data_source, batch, "Force-killed", crate::kube::protocol::OperationKind::ForceKill, |ds, t| ds.force_kill(t));
            }
        }
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Filter & search
// ---------------------------------------------------------------------------

fn handle_filter_search(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;

    match action {
        Action::Filter(_) => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.draft = Some(crate::app::text_input::TextInput::new());
            } else if let Some(fi) = app.nav.top_mut().filter_input_mut() {
                fi.start();
            }
        }
        Action::ColumnFilter => {
            {
                // Data index — the predicate indexes the full `cells` array,
                // not the visible subset. Read from the element's own last
                // view (the same visible set the renderer used), so the
                // mapping cannot drift. The header rides along for the crumb.
                if let Some((col, header)) = app.nav.top().selected_data_col() {
                    if let Some(fi) = app.nav.top_mut().filter_input_mut() {
                        fi.start_column(col, header);
                    }
                }
            }
        }
        Action::ClearFilter => {
            // Esc, unified: cancel a draft → close the overlay → pop the
            // top element. This handler never touches marks — in select mode
            // `select_gate` has already shadowed Esc into `ClearMarks`, so
            // reaching here at all means the mode is off.
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                if view.is_filtering() {
                    view.draft = None;
                    return;
                }
            }
            if app.ui.takeover().is_some() {
                app.ui.close_modal();
            } else if app.nav.pop().is_some() {
                // Pop = drop: the popped element's stream RSTs, its
                // backward Arcs release. The revealed element still owns
                // its cursor, sort, and draft — nothing to restore. Only
                // its subscription may have died while covered; revive it.
                app.nav.ensure_top_live(data_source);
            }
            // At the root there is nothing to pop — Esc is a no-op.
        }
        Action::ToggleFaultFilter => {
            // Strict LIFO: Ctrl-Z pops the fault filter when it is the
            // TOP; a fault filter buried under later refinements can't be
            // spliced out from the middle (children's predicate chains
            // are value-copies — a splice would not update them anyway).
            if app.nav.top_is_fault() {
                app.nav.pop();
                app.nav.ensure_top_live(data_source);
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            } else if app.nav.any_fault() {
                app.ui.flash = Some(crate::app::FlashMessage::warn(
                    "Fault filter active below — Esc back to it",
                ));
            } else if let Ok(el) = Element::derive_filter(app.nav.top(), RowPredicate::Fault) {
                app.nav.push(el);
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            }
        }
        Action::SearchStart => {
            match app.nav.top_mut() {
                crate::app::element::Element::ContentView(cv) => {
                    cv.state.search_input_active = true;
                    cv.state.search_input.clear();
                }
                el => {
                    if let Some(view) = el.log_view_mut() {
                        view.draft = Some(crate::app::text_input::TextInput::new());
                    }
                }
            }
        }
        Action::SearchExec(term) => {
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.search = Some(term.clone());
                cv.state.update_search();
            }
        }
        Action::SearchNext => {
            // Detail views (yaml/describe) only. Logs have no separate search —
            // their `/` grep IS the filter — and the old log branch compared a
            // logical line index against a PHYSICAL wrap-offset (broken under
            // wrap, and already muddled with filters); n/N is unbound in logs.
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.next_match();
            }
        }
        Action::SearchPrev => {
            // Detail views only — see SearchNext. The log n/N branch was removed.
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.prev_match();
            }
        }
        Action::SearchClear => {
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.clear_search();
            }
            // Log views: committed filters are elements — cleared via Esc.
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Log view actions
// ---------------------------------------------------------------------------

fn handle_log_action(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;
    use crate::app::element::Element;

    match action {
        Action::ToggleLogFollow => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.viewport.toggle_follow();
            }
        }
        Action::ToggleLogWrap => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.wrap = !view.wrap;
            }
        }

        Action::ToggleLogTimestamps => {
            // ONE key cycles how much chrome is shed, in `Role::SHED_ORDER`:
            // everything, then without the timestamp, then without the
            // source tag either. A key per role meant two bindings for three
            // meaningful states, a fourth that nothing could produce, and a
            // new binding for every role added later.
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                use crate::kube::protocol::Role;
                view.shed = (view.shed + 1) % Role::levels();
                let showing: Vec<&str> = Role::SHED_ORDER
                    .iter()
                    .filter(|r: &&Role| r.shown(view.shed))
                    .map(|r| match r {
                        Role::Timestamp => "time",
                        Role::Source => "source",
                        Role::Body => "body",
                    })
                    .collect();
                let msg = if showing.is_empty() {
                    "Fields: message only".to_string()
                } else {
                    format!("Fields: {} + message", showing.join(" + "))
                };
                app.ui.flash = Some(crate::app::FlashMessage::info(msg));
            }
        }

        Action::ClearLogs => {
            // Clears the SESSION's store, then resets the greps derived from
            // it. The filters own their lines now, so emptying the parent
            // does nothing to them on its own — this comment used to claim
            // "every filter over it empties with it, honestly", which
            // stopped being true the moment `retained` was introduced and
            // left `Shift-C` flashing "Logs cleared" over an unchanged view.
            if let Some(store) = app.nav.top().log_root_store().cloned() {
                store.clear();
                app.nav.reset_log_filters_of(&store);
                app.ui.flash = Some(crate::app::FlashMessage::info("Logs cleared".to_string()));
            }
        }

        Action::LogSince(ref since) => {
            // Range keys re-run the OWNING session's spec. Under a filter,
            // the owner is found by store pointer identity — the same
            // named-walk discipline as subscription revival.
            let Some(store) = app.nav.top().log_root_store().cloned() else { return };
            let since_label = since.as_deref().unwrap_or("tail");
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            app.nav.with_log_session_of(&store, |session_el| {
                session_el.restart_range(data_source, since.clone());
            });
            // The session store was just emptied and refilled from a new
            // range; a grep over it is still holding the OLD range's matches.
            app.nav.reset_log_filters_of(&store);
        }
        Action::SaveLogs => {
            let Element::LogSession(ref s) = app.nav.top() else {
                // Saving from a filter view saves what you see — the
                // filtered lines of the top element.
                save_visible_logs(app);
                return;
            };
            let (what, container, _) = s.header();
            let safe = |x: &str| x.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
            let filename = format!(
                "logs-{}-{}-{}.log",
                safe(&what), safe(&container),
                chrono::Utc::now().format("%Y%m%d-%H%M%S"),
            );
            let content: String = s.store().with_read(|i| {
                i.lines.iter()
                    .map(|l| l.flat_text())
                    .collect::<Vec<_>>()
                    .join("\n")
            });
            write_log_file(app, &filename, &content);
        }
        _ => {}
    }
}

/// Save the top log element's VISIBLE (filtered) lines.
fn save_visible_logs(app: &mut App) {
    let Some(indices) = app.nav.top_mut().log_visible() else { return };
    let Some(store) = app.nav.top().log_store() else { return };
    let content: String = store.with_read(|i| {
        indices.iter()
            .filter_map(|&idx| i.lines.get(idx))
            .map(|l| l.flat_text())
            .collect::<Vec<_>>()
            .join("\n")
    });
    let filename = format!("logs-filtered-{}.log", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
    write_log_file(app, &filename, &content);
}

fn write_log_file(app: &mut App, filename: &str, content: &str) {
    match crate::util::safe_write_temp(filename, content.as_bytes()) {
        Ok(path) => {
            let count = content.lines().count();
            app.ui.flash = Some(crate::app::FlashMessage::info(
                format!("Saved {} lines to {}", count, path.display())
            ));
        }
        Err(e) => {
            app.ui.flash = Some(crate::app::FlashMessage::error(
                format!("Save failed: {}", e)
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Drill-down navigation (ShowNode, UsedBy, JumpToOwner, NodeShell)
// ---------------------------------------------------------------------------

fn handle_drill(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;

    match action {
        Action::ShowNode => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::ShowNode) {
                if let Some(row) = app.nav.top().selected_row() {
                    let node = row.node.clone().unwrap_or_default();
                    if !node.is_empty() {
                        // "The node with the ip of this pod": the row's node
                        // IS the whole definition — nodes are cluster-scoped,
                        // so nothing namespace-ish enters construction.
                        let selector = crate::app::nav::K8sFieldSelector::MetadataName(node.clone());
                        let el = app.list_element_from_top(
                            data_source,
                            rid(BuiltInKind::Node),
                            protocol::Namespace::All,
                            Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                            format!("Nodes/{}", node),
                        );
                        app.nav.push(el);
                    }
                }
            }
        }
        // The events ABOUT this object — "why is this Pending", which is the
        // question `:events` exists to answer and could not, because it
        // showed the whole cluster's events and left you grepping a name by
        // eye.
        //
        // A global Table binding rather than an `OperationKind`: every
        // namespaced object has events, so declaring the capability in
        // thirty `operations()` lists would carve an exception where the
        // framework already has a shape for this (`J`, `U`).
        Action::ShowEvents => {
            if let Some(info) = require_selected(app) {
                let selector = crate::kube::resources::row::K8sFieldSelector::InvolvedObject {
                    kind: info.resource.kind_str().to_string(),
                    name: info.name.clone(),
                    namespace: info.namespace.as_option().unwrap_or_default().to_string(),
                };
                let breadcrumb = selector.breadcrumb();
                // Scoped to ALL namespaces on purpose: a cluster-scoped
                // object's events land in whatever namespace the reporting
                // component chose, and the selector above already pins the
                // subject exactly. Narrowing twice would only lose events.
                let el = app.list_element_from_top(
                    data_source,
                    rid(BuiltInKind::Event),
                    protocol::Namespace::All,
                    Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                    format!("events({breadcrumb})"),
                );
                app.nav.push(el);
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    format!("Events for {breadcrumb}"),
                ));
            }
        }
        // The pods that actually NAME this object.
        //
        // This used to grep the pod list for the resource's name, which found
        // `data-postgres-0` → `postgres-0` by luck of naming convention and
        // missed a secret mounted by `api-7d9f` entirely — reporting the miss
        // as an empty table, which reads like an answer. The references are
        // now extracted from the pod spec at conversion
        // (`ResourceRow::references`) and the drill is a filter over them.
        Action::UsedBy => {
            if let Some(info) = require_selected(app) {
                // Gated on the CLOSED set a pod spec can actually name (see
                // `referenceable_kinds`). Accepting every built-in meant `U`
                // on a Deployment, Node or Service opened a confidently empty
                // table — a miss dressed as an answer, which is what this
                // lookup exists to stop doing.
                let kind = info.resource.built_in_kind().filter(|k| {
                    crate::kube::resources::pods::referenceable_kinds().contains(k)
                });
                let Some(kind) = kind else {
                    app.ui.flash = Some(crate::app::FlashMessage::warn(format!(
                        "Nothing references a {} — pods name claims, secrets, configmaps and service accounts",
                        info.resource.display_label(),
                    )));
                    return ActionResult::None;
                };
                let label = format!("{}/{}", info.resource.plural(), info.name);
                let el = app.list_element_from_top(
                    data_source,
                    rid(BuiltInKind::Pod),
                    info.namespace.clone(),
                    Some(protocol::SubscriptionFilter::References {
                        kind,
                        name: info.name.clone(),
                    }),
                    format!("uses({label})"),
                );
                app.nav.push(el);
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    format!("Pods that reference {label}"),
                ));
            }
        }
        Action::JumpToOwner => {
            if let Some(row) = app.nav.top().selected_row() {
                if let Some(owner) = row.owner_refs.first() {
                    let owner_kind = owner.kind.clone();
                    let owner_name = owner.name.clone();
                    let owner_rid = crate::kube::protocol::ResourceId::from_alias(&owner_kind.to_lowercase())
                        .unwrap_or_else(|| {
                            crate::kube::protocol::ResourceId::CrdUnresolved(owner_kind.to_lowercase())
                        });
                    let selector = crate::app::nav::K8sFieldSelector::MetadataName(owner_name.clone());
                    let el = app.list_element_from_top(
                        data_source,
                        owner_rid.clone(),
                        app.kube.selected_ns.clone(),
                        Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                        format!("{}/{}", owner_rid.short_label(), owner_name),
                    );
                    app.nav.push(el);
                    app.ui.flash = Some(crate::app::FlashMessage::info(
                        format!("Owner: {}/{}", owner_kind.to_lowercase(), owner_name)
                    ));
                } else {
                    app.ui.flash = Some(crate::app::FlashMessage::warn("No owner found".to_string()));
                }
            }
        }
        Action::NodeShell => {
            if let Some(row) = app.nav.top().selected_row() {
                let node = row.name.clone();
                return ActionResult::Exec {
                    op: crate::kube::protocol::OperationKind::NodeShell,
                    target: crate::kube::session::ExecTarget::Node { node },
                };
            }
        }
        Action::OverlayCapability(ref cap_name) => {
            let Some(plural) = app.nav.top().rid().map(|r| r.plural().to_owned()) else {
                return ActionResult::None;
            };
            let overlay = crate::kube::overlay::overlay_for(&plural);
            let cap = overlay.and_then(|o| o.capabilities.get(cap_name));
            match cap {
                Some(crate::kube::overlay::OverlayCapability::Drill { target, column }) => {
                    let target_rid = crate::kube::protocol::ResourceId::from_alias(target)
                        .unwrap_or_else(|| crate::kube::protocol::ResourceId::CrdUnresolved(target.clone()));
                    let headers = app.nav.top().headers_snapshot();
                    let filter_value = app.nav.top().selected_row().and_then(|row| {
                        let col_idx = headers.iter().position(|h| h.eq_ignore_ascii_case(column))?;
                        row.cells.get(col_idx).map(|c| c.to_string())
                    });
                    if let Some(value) = filter_value.filter(|v| !v.is_empty()) {
                        let selector = crate::app::nav::K8sFieldSelector::MetadataName(value.clone());
                        let el = app.list_element_from_top(
                            data_source,
                            target_rid,
                            app.kube.selected_ns.clone(),
                            Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                            format!("{}/{}", target, value),
                        );
                        app.nav.push(el);
                        app.ui.flash = Some(crate::app::FlashMessage::info(
                            format!("{} matching: {}", target, value)
                        ));
                    } else {
                        app.ui.flash = Some(crate::app::FlashMessage::warn(
                            format!("No value in column '{}' for '{}'", column, cap_name)
                        ));
                    }
                }
                None => {
                    app.ui.flash = Some(crate::app::FlashMessage::warn(
                        format!("Unknown overlay capability: {}", cap_name)
                    ));
                }
            }
        }
        _ => {}
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Clipboard / file I/O
// ---------------------------------------------------------------------------

fn handle_io(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;

    match action {
        Action::Copy => {
            use crate::app::element::Element;
            let (text, label) = match app.nav.top_mut() {
                Element::ContentView(cv) => {
                    if cv.state.content().is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = cv.state.line_count();
                        (cv.state.content().to_string(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                el @ (Element::LogSession(_) | Element::LogFilter(_)) => {
                    match el.log_lines() {
                        None => (String::new(), String::new()),
                        Some((indices, _)) if indices.is_empty() => {
                            (String::new(), String::new())
                        }
                            Some((indices, store)) => {
                            let joined: String = store.with_read(|i| {
                                indices.iter()
                                    .filter_map(|&idx| i.lines.get(idx))
                                    .map(|l| l.flat_text())
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            });
                                let count = indices.len();
                            (joined, format!("Copied {} lines to clipboard", count))
                        }
                    }
                }
                _ => {
                    let dump = build_table_dump(app);
                    if dump.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let count = dump.lines().count().saturating_sub(1); // exclude header
                        (dump, format!("Copied {} rows to clipboard", count))
                    }
                }
            };
            send_to_clipboard(data_source, event_tx, text, label);
        }

        // Select mode's `c`. Distinct from `Copy` because the whole point of
        // the mode is that single and batch are never the same action: with
        // rows marked, `Copy` used to fall through and dump the ENTIRE visible
        // table, so marking three pods and copying gave you all forty-seven.
        Action::BatchCopy => {
            let marked = app.nav.top().marked_snapshot();
            let text = table_dump(app, Some(&marked));
            let count = text.lines().count().saturating_sub(1); // exclude header
            let label = format!("Copied {} marked row{} to clipboard",
                count, if count == 1 { "" } else { "s" });
            send_to_clipboard(data_source, event_tx, text, label);
        }
        Action::SaveTable => {
            let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
            let filename = format!(
                "{}-{}.txt",
                safe(app.nav.top().title()),
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            );
            let content = build_table_dump(app);
            let tx = event_tx.clone();
            data_source.track_task(async move {
                let result = tokio::task::spawn_blocking(move || {
                    crate::util::safe_write_temp(&filename, content.as_bytes())
                }).await;
                match result {
                    Ok(Ok(path)) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Saved to {}", path.display())))).await;
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Save failed: {}", e)))).await;
                    }
                    Err(e) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Save failed: {}", e)))).await;
                    }
                }
            });
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Standalone helpers
// ---------------------------------------------------------------------------

fn handle_show_port_forwards(app: &mut App, data_source: &mut ClientSession) {
    let pf_rid = crate::kube::protocol::ResourceId::Local(
        crate::kube::local::LocalResourceKind::PortForward,
    );
    let label = pf_rid.short_label().to_lowercase();
    let el = app.list_element_from_top(
        data_source, pf_rid, app.kube.selected_ns.clone(), None, label,
    );
    app.nav.push(el);
}

fn handle_refresh(app: &mut App, data_source: &mut ClientSession) {
    use crate::app::element::{ContentSpec, Element};

    // Inside a content element: re-issue ITS OWN fetch directly — each
    // kind re-runs its own operation (a decoded secret re-DECODES; the old
    // Describe-labeled decode view re-described here, silently replacing
    // the decoded bytes with plain describe text).
    if let Element::ContentView(cv) = app.nav.top_mut() {
        let result = match &cv.kind {
            ContentSpec::Describe(target) => Some(data_source.describe(target)),
            ContentSpec::Yaml(target) => Some(data_source.yaml(target)),
            ContentSpec::DecodedSecret(target) => Some(data_source.decode_secret(target)),
        };
        if let Some(result) = result {
            app.kube.kubectl_cache.clear();
            match result {
                Ok(()) => {
                    if let Element::ContentView(cv) = app.nav.top_mut() {
                        cv.state = crate::app::ContentViewState::default();
                        cv.phase = crate::app::element::ContentPhase::Fetching;
                    }
                    app.ui.flash = Some(crate::app::FlashMessage::info("Refreshing..."));
                }
                Err(_) => {
                    app.ui.flash = Some(crate::app::FlashMessage::error("Failed to refresh"));
                }
            }
        }
        return;
    }

    // Derived views are client-side projections — no subscription to refresh.
    if app.nav.top().derived_kind().is_some() {
        app.ui.flash = Some(crate::app::FlashMessage::info("Pop back to refresh the parent view"));
        return;
    }
    app.kube.kubectl_cache.clear();
    // Re-run the owning element's OWN query spec with a forced watcher.
    // The old refresh re-subscribed with the AMBIENT namespace, silently
    // narrowing all-namespace drills — unrepresentable now.
    app.nav.refresh_top_query(data_source);
    app.ui.flash = Some(crate::app::FlashMessage::info("Refreshed"));
}

fn handle_show_aliases(app: &mut App, data_source: &crate::kube::client_session::ClientSession) {
    // The same ROOT element `:alias` resolves to, through the same
    // client-owned routing — `Ctrl-a` is a shortcut for the command, not a
    // second way of building the view. Resets rather than pushes, for the
    // reason every root-level resource command does: this is a reference
    // view you go TO, not a sheet you consult about where you are.
    let rid = crate::kube::local::LocalResourceKind::Alias.to_resource_id();
    app.nav.reset(crate::app::App::root_list_element(
        data_source,
        &app.kube.metrics,
        &app.core,
        rid,
        protocol::Namespace::All,
    ));
}

// ===========================================================================
// Action handlers (merged from session_handlers.rs)
// ===========================================================================

/// Map a container name string from a row to the typed `LogContainer`.
/// Empty -> `Default` (let kubectl pick); a real container name -> `Named`.
fn log_container_from_str(name: &str) -> protocol::LogContainer {
    if name.is_empty() {
        protocol::LogContainer::Default
    } else {
        protocol::LogContainer::Named(name.to_string())
    }
}

pub(crate) fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::kube::protocol::ResourceId;

    // Derived view Enter: open logs for the selected container.
    if app.nav.top().derived_kind().is_some() {
        open_logs_from_derived(app, data_source, false);
        return ActionResult::None;
    }

    // A pick: open logs or a shell for the chosen container. Copied OUT of
    // the element before the stack is touched again, so no borrow is live
    // across the pop and the flash.
    let pick = match app.nav.top() {
        Element::Picker(p) => Some((
            p.target.clone(),
            p.choices.get(p.selected).cloned(),
            p.selected,
            p.action,
        )),
        _ => None,
    };
    if let Some((target, picked, selected, action)) = pick {
        let pod_ns_str = target.namespace.display().to_string();
        // The dialog captured its choices at construction.
        let Some(picked) = picked else {
            app.ui.flash = Some(crate::app::FlashMessage::error(
                format!("Pod {}/{} no longer has a container at index {}", pod_ns_str, target.name, selected)
            ));
            app.ui.close_modal();
            return ActionResult::None;
        };
        app.ui.close_modal();

        if matches!(action, crate::app::ContainerAction::Shell) {
            let protocol::LogContainer::Named(container) = picked.container else {
                // A shell picker contains only `Named` rows — `ContainerChoice::list`
                // offers "all containers" for the log actions alone, because there
                // is no exec into every container at once.
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    "Pick one container to shell into".to_string(),
                ));
                return ActionResult::None;
            };
            return ActionResult::Exec { op: crate::kube::protocol::OperationKind::Shell, target: crate::kube::session::ExecTarget::Pod {
                pod: target.name,
                namespace: pod_ns_str,
                container,
            } };
        }

        let previous = matches!(action, crate::app::ContainerAction::PreviousLogs);
        push_log_session(
            app,
            data_source,
            crate::kube::protocol::LogInit {
                target: crate::kube::protocol::LogTarget::One(target.name.clone()),
                namespace: target.namespace.clone(),
                container: picked.container,
                follow: !previous,
                tail: Some(app.config.ui.logs.tail_lines),
                since: None,
                previous,
            },
        );
        return ActionResult::None;
    }

    // Handle Enter: read the row's drill_target and act on it.
    use crate::kube::resources::row::DrillTarget;
    let row_data = app.nav.top().selected_row();

    let Some(row) = row_data else {
        handle_describe(app, data_source);
        return ActionResult::None;
    };

    match row.drill_target.clone() {
        Some(DrillTarget::PodsInNamespace(ns)) => {
            drill_to_pods_in_namespace(app, data_source, ns);
        }
        Some(DrillTarget::SwitchContext(name)) => {
            // Not a drill: the nav stack below belongs to the context we
            // are leaving, so `begin_context_switch` resets to home at once
            // (a failed switch lands on the fallback context's home with an
            // error flash, not back on this row).
            begin_context_switch(app, &name);
        }
        Some(DrillTarget::BrowseCrd(crd_ref)) => {
            let kind_label = crd_ref.kind.clone();
            let crd_rid = ResourceId::Crd(crd_ref);
            let label = crd_rid.short_label().to_lowercase();
            let el = app.list_element_from_top(
                data_source, crd_rid, app.kube.selected_ns.clone(), None, label,
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Browsing CRD: {}", kind_label)));
        }
        Some(DrillTarget::PodsByLabels { labels, breadcrumb }) => {
            // Scope to the *source* object's namespace, not the parent view's —
            // from an all-namespaces parent these labels would otherwise match
            // pods cluster-wide.
            let source_ns = crate::kube::protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            drill_to_pods_by_labels(app, data_source, labels, source_ns, &breadcrumb);
        }
        Some(DrillTarget::PodsByOwner { uid, kind, name }) => {
            let source_ns = crate::kube::protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            drill_to_pods_by_owner(app, data_source, &uid, kind, &name, source_ns);
        }
        Some(DrillTarget::PodsByField(selector)) => {
            let breadcrumb = selector.breadcrumb();
            // The element's OWN scope is intrinsic to the drill: a node
            // hosts pods from EVERY namespace, so a spec.nodeName drill is
            // cluster-wide; name/phase drills keep the session scope as a
            // construction input. Because the element owns its columns,
            // the cluster-wide view shows NAMESPACE regardless of the
            // ambient selector.
            let ns = match &selector {
                crate::app::nav::K8sFieldSelector::SpecNodeName(_) => protocol::Namespace::All,
                _ => app.kube.selected_ns.clone(),
            };
            let el = app.list_element_from_top(
                data_source,
                rid(BuiltInKind::Pod),
                ns,
                Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                breadcrumb.clone(),
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods filtered by {}", breadcrumb)));
        }
        Some(DrillTarget::PodsByNameGrep(name)) => {
            drill_to_pods_by_grep(app, data_source, &name);
        }
        Some(DrillTarget::JobsByOwner { uid, kind, name }) => {
            let kind_str = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind;
            let kind_lower = kind_str.to_lowercase();
            // Scope to the SOURCE object's namespace — its jobs live there.
            let source_ns = protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            let el = app.list_element_from_top(
                data_source,
                rid(BuiltInKind::Job),
                source_ns,
                Some(protocol::SubscriptionFilter::OwnerUid(uid)),
                format!("{}/{}", kind_str, name),
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Jobs for {}/{}", kind_lower, name)));
        }
        Some(DrillTarget::Derived(kind)) => {
            // "Containers on this pod": the row's identity + the top's
            // live source ARE the definition. The projection is LIVE — it
            // re-derives as the parent row changes and empties honestly
            // if the pod disappears.
            if let Ok(el) = Element::derive_projection(app.nav.top(), &row, kind) {
                app.nav.push(el);
            }
        }
        None => {
            handle_describe(app, data_source);
        }
    }
    ActionResult::None
}

pub(crate) fn handle_describe(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::element::{ContentPhase, ContentSpec, ContentView, Element};
    if let Some(info) = require_selected(app) {
        if let Some(lines) = app.kube.kubectl_cache.get_describe_lines(&info) {
            let mut state = crate::app::ContentViewState::default();
            state.set_describe_lines(lines);
            app.nav.push(Element::ContentView(ContentView::new(
                ContentSpec::Describe(info),
                state,
                ContentPhase::Ready,
            )));
            return;
        }

        ds_try!(app, data_source.describe(&info));
        app.nav.push(Element::ContentView(ContentView::new(
            ContentSpec::Describe(info),
            crate::app::ContentViewState::default(),
            ContentPhase::Fetching,
        )));
    }
}

pub(crate) fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::element::{ContentPhase, ContentSpec, ContentView, Element};
    if let Some(info) = require_selected(app) {
        if let Some(cached) = app.kube.kubectl_cache.get(&info, crate::app::ContentKind::Yaml) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.nav.push(Element::ContentView(ContentView::new(
                ContentSpec::Yaml(info),
                state,
                ContentPhase::Ready,
            )));
            return;
        }

        ds_try!(app, data_source.yaml(&info));
        app.nav.push(Element::ContentView(ContentView::new(
            ContentSpec::Yaml(info),
            crate::app::ContentViewState::default(),
            ContentPhase::Fetching,
        )));
    }
}

pub(crate) fn handle_logs(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    open_logs(app, data_source, false);
}

pub(crate) fn handle_previous_logs(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    open_logs(app, data_source, true);
}

/// Push a `LogSession` element for `spec` — "logs of this container" is
/// the element's whole self-definition.
fn push_log_session(
    app: &mut App,
    data_source: &mut ClientSession,
    spec: protocol::LogInit,
) {
    let el = crate::app::element::Element::LogSession(Box::new(
        crate::app::element::LogSession::open(data_source, spec, &app.config.ui.logs),
    ));
    app.nav.push(el);
}

/// Core log-open flow shared by live and previous-logs actions.
fn open_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    previous: bool,
) {
    // Derived view (e.g., container table): the selected row IS the
    // container, and the element's origin IS the parent pod.
    if app.nav.top().derived_kind().is_some() {
        open_logs_from_derived(app, data_source, previous);
        return;
    }

    let Some(info) = require_selected(app) else { return; };
    let name = info.name.clone();
    let namespace_typed = info.namespace.clone();

    let containers = app.nav.top().selected_row()
        .map(|row| row.containers.clone())
        .unwrap_or_default();

    if containers.len() > 1 {
        let action = if previous {
            crate::app::ContainerAction::PreviousLogs
        } else {
            crate::app::ContainerAction::Logs
        };
        app.ui.close_modal();
        app.nav.push(crate::app::element::Element::Picker(Box::new(crate::app::ContainerPicker {
            target: info.clone(),
            choices: crate::app::ContainerChoice::list(action, &containers),
            selected: 0,
            action,
        })));
        return;
    }

    let (target, container) = if !containers.is_empty() {
        // The row IS a pod (it lists containers), with exactly one of them
        // after the multi-container branch above.
        let container = containers.first().map(|ci| ci.name.clone()).unwrap_or_default();
        (protocol::LogTarget::One(name.clone()), log_container_from_str(&container))
    } else {
        // Not a pod — a workload. Its logs are every pod it owns, found via
        // the SAME drill target Enter uses, so "logs of this deployment" and
        // "pods of this deployment" can never disagree about which pods those
        // are. `--all-containers` because a replica set's pods routinely carry
        // sidecars and there is no one container to pick across N of them.
        (workload_log_target(app, &info), protocol::LogContainer::All)
    };

    push_log_session(
        app,
        data_source,
        protocol::LogInit {
            target,
            namespace: namespace_typed,
            container,
            follow: !previous,
            tail: Some(app.config.ui.logs.tail_lines),
            since: None,
            previous,
        },
    );
}

/// The log target for a non-pod row, read off its drill target.
///
/// The fallback — the row's `kubectl`-style ref as a single target — is the
/// pre-fan-out behaviour, kept for the shapes nothing can resolve (a cronjob
/// owns jobs, not pods; a name-grep isn't a selector): `kubectl logs` accepts
/// `cronjobs/foo` and picks a pod, or fails with its own message, exactly as
/// it did before.
pub(crate) fn workload_log_target(app: &App, info: &ObjectRef) -> protocol::LogTarget {
    let rid = info.resource.clone();
    app.nav
        .top()
        .selected_row()
        .and_then(|row| row_log_target(&row, &rid))
        .unwrap_or_else(|| protocol::LogTarget::One(info.kubectl_target()))
}

/// What ONE row's logs are: itself if it is a pod, otherwise the pods its
/// drill target names.
///
/// The single resolution behind both `Ctrl-L` on a row and `Ctrl-L` over a
/// marked set, so the hovered deployment and the marked one can't disagree
/// about which pods they mean. `None` = this row has no logs at all
/// (a namespace, a configmap): it names neither containers nor pods.
pub(crate) fn row_log_target(
    row: &crate::kube::resources::row::ResourceRow,
    rid: &crate::kube::protocol::ResourceId,
) -> Option<protocol::LogTarget> {
    use crate::kube::resources::row::DrillTarget;
    let label = format!("{}/{}", rid.plural(), row.name);
    // Listing containers is what makes a row a pod.
    if !row.containers.is_empty() {
        return Some(protocol::LogTarget::One(row.name.clone()));
    }
    match row.drill_target.clone() {
        Some(DrillTarget::PodsByLabels { labels, .. }) => {
            Some(protocol::LogTarget::Selector { labels, label })
        }
        Some(DrillTarget::PodsByOwner { uid, .. }) => {
            Some(protocol::LogTarget::Owner { uid, label })
        }
        // Nothing here resolves to a pod set, but the resource still
        // declares logs (a cronjob does) — hand the ref to kubectl, which is
        // exactly what happened before any of this existed.
        _ if rid.capabilities().supports(protocol::OperationKind::StreamLogs) => {
            Some(protocol::LogTarget::One(label))
        }
        _ => None,
    }
}

/// Open logs from a derived view (e.g., container table). The selected
/// row's name is the container; the element's origin is the parent pod.
fn open_logs_from_derived(
    app: &mut App,
    data_source: &mut ClientSession,
    previous: bool,
) {
    let Some(origin) = app.nav.top().origin().cloned() else { return; };
    let Some(item) = app.nav.top().selected_row() else { return; };

    let container_name = item.name.clone();
    let pod_name = origin.name.clone();
    let namespace = origin.namespace.clone();
    let container = protocol::LogContainer::Named(container_name.clone());

    push_log_session(
        app,
        data_source,
        protocol::LogInit {
            target: protocol::LogTarget::One(pod_name),
            namespace,
            container,
            follow: !previous,
            tail: Some(app.config.ui.logs.tail_lines),
            since: None,
            previous,
        },
    );
}

/// Build a tab-separated text dump of the currently visible resource table.
/// Reads the element's own last-materialized view: same columns (the
/// element's OWN NAMESPACE decision — not the ambient selector), same
/// filter and sort, same effective (metrics-overlaid) cell strings the
/// user is looking at. What you copy is exactly what you see.
/// Put `text` on the clipboard in the background, flashing `label` on success
/// and a single install hint on failure. Shared by `Copy` and `BatchCopy` so
/// the two can't drift on the failure message.
fn send_to_clipboard(
    data_source: &mut ClientSession,
    event_tx: &mpsc::Sender<AppEvent>,
    text: String,
    label: String,
) {
    if text.is_empty() {
        return;
    }
    let tx = event_tx.clone();
    data_source.track_task(async move {
        let ok = tokio::task::spawn_blocking(move || {
            crate::util::try_copy_to_clipboard(&text)
        })
        .await
        .unwrap_or(false);
        if ok {
            let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(label))).await;
        } else {
            let _ = tx
                .send(AppEvent::Flash(crate::app::FlashMessage::warn(
                    "No clipboard tool found (install xclip, xsel, wl-copy, or pbcopy)",
                )))
                .await;
        }
    });
}

pub(crate) fn build_table_dump(app: &App) -> String {
    table_dump(app, None)
}

/// The table as text, optionally restricted to a set of row identities.
///
/// `only` is `Some` for select mode's copy: `PreparedView.keys` runs parallel
/// to `rows`, so the marked set filters by identity rather than by screen
/// position — a delta reordering the table between the mark and the copy
/// cannot hand back a different row. The header line is kept either way; a
/// bare column of values is not something you can paste anywhere useful.
fn table_dump(app: &App, only: Option<&std::collections::HashSet<crate::kube::protocol::ObjectKey>>) -> String {
    let Some(view) = app.nav.top().last_view() else { return String::new() };
    if view.rows.is_empty() && view.total_rows == 0 {
        return String::new();
    }
    let mut lines = vec![view.headers.join("\t")];
    for (i, row) in view.rows.iter().enumerate() {
        if let Some(keys) = only {
            // A row with no identity at this index can't be matched against
            // the marks, so it is not one of them.
            match view.keys.get(i) {
                Some(key) if keys.contains(key) => {}
                _ => continue,
            }
        }
        lines.push(row.join("\t"));
    }
    // Header only = nothing matched; report that as empty rather than
    // putting a lone header row on the clipboard.
    if lines.len() == 1 {
        return String::new();
    }
    lines.join("\n")
}

/// Build a `FormDialog` from a declarative `FormSchema`.
///
/// Each `FormFieldSchema` in the schema maps to a `FormFieldState`:
/// - `Number { default_column }` reads the default from the row's cell
///   (splitting on '/' to get the denominator, e.g. "2/3" -> "3").
/// - `Port` uses the first port from `row.pf_ports`, or 8080.
/// - `DynamicSelect` builds a `Select` from `row.pf_ports` if non-empty,
///   otherwise falls back to a `Port` input.
///
/// Title is derived from `title_template` with `{{kind}}` and `{{name}}`
/// substitutions. Subtitle comes from the target namespace.
pub(crate) fn build_form_from_schema(
    schema: &crate::kube::protocol::FormSchema,
    op: crate::kube::protocol::OperationKind,
    target: ObjectRef,
    row: Option<&crate::kube::resources::row::ResourceRow>,
    headers: &[String],
) -> Option<crate::app::FormDialog> {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState, FormSubmit};
    use crate::kube::protocol::{FormFieldSchemaKind, DynamicSelectFallback};

    let submit = FormSubmit::from_operation(op)?;

    let title = schema.title_template
        .replace("{{kind}}", &target.resource.short_label().to_lowercase())
        .replace("{{name}}", &target.name);

    let subtitle = match &target.namespace {
        crate::kube::protocol::Namespace::Named(ns) if !ns.is_empty() => {
            format!("namespace: {}", ns)
        }
        _ => String::new(),
    };

    let fields: Vec<FormFieldState> = schema.fields.iter().map(|field_schema| {
        match &field_schema.kind {
            FormFieldSchemaKind::Number { min, max, default_column } => {
                let default_value = default_column
                    .and_then(|col_name| {
                        let col_idx = headers.iter().position(|h| h.eq_ignore_ascii_case(col_name))?;
                        let r = row?;
                        let cell = r.cells.get(col_idx)?;
                        let s = cell.to_string();
                        let denom = s.split('/').nth(1)?.trim().to_string();
                        if denom.is_empty() { None } else { Some(denom) }
                    })
                    .unwrap_or_default();
                FormFieldState {
                    name: field_schema.name.into(),
                    label: field_schema.label.into(),
                    kind: FormFieldKind::Number { min: *min, max: *max },
                    value: crate::app::text_input::TextInput::seeded(default_value),
                }
            }
            FormFieldSchemaKind::Port => {
                let default_port = row
                    .and_then(|r| r.pf_ports.first().copied())
                    .unwrap_or(8080);
                FormFieldState {
                    name: field_schema.name.into(),
                    label: field_schema.label.into(),
                    kind: FormFieldKind::Port,
                    value: crate::app::text_input::TextInput::seeded(default_port.to_string()),
                }
            }
            FormFieldSchemaKind::DynamicSelect { fallback } => {
                let ports = row.map(|r| &r.pf_ports[..]).unwrap_or(&[]);
                if ports.is_empty() {
                    let default_port = match fallback {
                        DynamicSelectFallback::Port => 8080,
                    };
                    FormFieldState {
                        name: field_schema.name.into(),
                        label: field_schema.label.into(),
                        kind: FormFieldKind::Port,
                        value: crate::app::text_input::TextInput::seeded(default_port.to_string()),
                    }
                } else {
                    let options: Vec<crate::app::SelectOption> = ports
                        .iter()
                        .map(|p| crate::app::SelectOption::new(p.to_string(), p.to_string()))
                        .collect();
                    FormFieldState {
                        name: field_schema.name.into(),
                        label: field_schema.label.into(),
                        kind: FormFieldKind::Select { options, selected: 0 },
                        value: crate::app::text_input::TextInput::new(),
                    }
                }
            }
        }
    }).collect();

    Some(FormDialog {
        submit,
        title,
        subtitle,
        target,
        fields,
        focused: 0,
    })
}

/// The row under the cursor for a SELECTION-DRIVEN action, or an honest
/// flash when there is none (empty view / non-resource top). Every
/// keypress that names a single target routes here — a miss must say so,
/// not silently swallow the key.
pub(crate) fn require_selected(app: &mut App) -> Option<ObjectRef> {
    // A view whose resource declares NO operations is documentation, not a
    // list of things to act on — help and the contexts table both. Most
    // single-target keys are gated on a specific capability, but `e` (Edit)
    // and `E` (Events) are STRUCTURAL and were gated on nothing: pressing `e`
    // on a help row sent the daemon a yaml request for `Local(Keybinding)`,
    // got back the string "Unknown local resource type", wrote it to a temp
    // file and opened `$EDITOR` on it. Gating here rather than at each key is
    // what makes that true of the next structural key too.
    if app.nav.top().capabilities().operations.is_empty() {
        app.ui.flash = Some(crate::app::FlashMessage::info(
            "Nothing to act on here — this view is a reference, not a resource".to_string(),
        ));
        return None;
    }
    let info = get_selected_resource_info(app);
    if info.is_none() {
        app.ui.flash =
            Some(crate::app::FlashMessage::warn("No resource selected".to_string()));
    }
    info
}

pub(crate) fn get_selected_resource_info(app: &App) -> Option<ObjectRef> {
    use crate::kube::protocol::Namespace;

    let current_rid = app.nav.top().rid()?.clone();
    let row = app.nav.top().selected_row()?;
    Some(ObjectRef::new(
        current_rid,
        row.name.clone(),
        Namespace::from_row(row.namespace.as_deref().unwrap_or("")),
    ))
}

/// Resource refs for all marked rows on the top element's data,
/// intersected with the rows PRESENT in the store at read time.
///
/// Marks are IDENTITY-keyed on the shared store, so this is the marked
/// set as data — deliberately independent of the current refinement (a
/// filter hides rows from view, not from the selection; the confirm
/// dialog names every target so hidden ones are visible at the decision
/// point). The row intersection is the other half of the contract:
/// under `Ready` it is a no-op (marks are pruned atomically with row
/// removals and inserts are presence-checked), and while `Initializing`
/// — the Ctrl-R window where marks survive `clear()` but rows are
/// unknown — it is honestly empty, so nothing can operate on an
/// identity the store cannot vouch for. Iterating rows also gives the
/// output a stable (store) order.
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::protocol::Namespace;

    let Some(current_rid) = app.nav.top().rid().cloned() else { return Vec::new(); };
    if !app.nav.top().has_marks() {
        return Vec::new();
    }
    let Some(store) = app.nav.top().data_store() else { return Vec::new(); };
    store.with_read(|inner| {
        inner
            .rows
            .iter()
            .map(crate::app::store::row_key)
            .filter(|k| inner.marked().contains(k))
            .map(|key| {
                ObjectRef::new(
                    current_rid.clone(),
                    key.name,
                    Namespace::from_row(&key.namespace),
                )
            })
            .collect()
    })
}

// ===========================================================================
// Navigation helpers (merged from session_nav.rs)
// ===========================================================================

/// Drill down to pods filtered by label selector. `namespace` is the
/// SOURCE object's namespace — intrinsic to the drill, carried on the
/// element's own query spec.
pub(crate) fn drill_to_pods_by_labels(
    app: &mut App,
    data_source: &mut ClientSession,
    labels: std::collections::BTreeMap<String, String>,
    namespace: crate::kube::protocol::Namespace,
    description: &str,
) {
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        namespace,
        Some(protocol::SubscriptionFilter::Labels(labels)),
        description.to_string(),
    );
    app.nav.push(el);
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", description)));
}

/// Drill down to pods filtered by name prefix (fallback when no
/// selector_labels): a pods list element plus a grep refinement over it.
pub(crate) fn drill_to_pods_by_grep(
    app: &mut App,
    data_source: &mut ClientSession,
    name: &str,
) {
    use crate::app::nav::CompiledGrep;
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        app.kube.selected_ns.clone(),
        None,
        "pods".to_string(),
    );
    app.nav.push(el);
    let predicate = RowPredicate::Grep(CompiledGrep::new(format!("{}-", regex::escape(name))));
    if let Ok(el) = Element::derive_filter(app.nav.top(), predicate) {
        app.nav.push(el);
    }
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
}

/// Drill down to pods owned by a resource (via ownerReferences chain).
/// `namespace` is the source object's namespace — intrinsic scope.
pub(crate) fn drill_to_pods_by_owner(
    app: &mut App,
    data_source: &mut ClientSession,
    uid: &str,
    kind: BuiltInKind,
    name: &str,
    namespace: crate::kube::protocol::Namespace,
) {
    let kind_str = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind;
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        namespace,
        Some(protocol::SubscriptionFilter::OwnerUid(uid.to_string())),
        format!("{}/{}", kind_str, name),
    );
    app.nav.push(el);
    let kind_lower = kind_str.to_lowercase();
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods for {}/{}",
        kind_lower, name
    )));
}

/// Drill from a namespace row into the pods running in that namespace.
/// Entering a namespace also makes it the session's active scope (matching
/// k9s), so sibling tabs inherit it. The pushed Pod step carries no filter,
/// so `apply_nav_change` falls back to `selected_ns` for the fresh
/// subscription — scoping the pods to the namespace we just selected. Esc
/// pops back to the (cluster-scoped) namespace list, whose cached rows
/// survive the namespaced-cache clear below.
pub(crate) fn drill_to_pods_in_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
) {
    // Entering a namespace also makes it the session's active SELECTOR
    // (k9s parity) — an explicit selector write, not something the
    // element reads back. The pushed element carries the namespace as
    // its OWN scope.
    app.kube.selected_ns = ns.clone();
    app.kube.kubectl_cache.clear();

    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        ns.clone(),
        None,
        "pods".to_string(),
    );
    app.nav.push(el);
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods in namespace: {}",
        ns.display()
    )));
}

/// Begin a context switch.
/// Takes no `ClientSession`: a switch only *requests* one. The main loop's
/// rebuild choke point is what drops the old session and builds the new one,
/// so this function is pure app-state mutation — which is also what makes the
/// whole switch FLOW testable without a daemon.
pub(crate) fn begin_context_switch(
    app: &mut App,
    ctx_name: &crate::kube::protocol::ContextName,
) {
    if !app.kube.context_switch.is_stable() {
        app.ui.flash = Some(crate::app::FlashMessage::error(
            "Context switch already in progress".to_string(),
        ));
        return;
    }

    // `app.kube.context` / `identity` are NOT set here — they mean "last
    // CONFIRMED", updated only by `ConnectionEstablished`; the target being
    // switched TO lives in `context_switch` (below). This keeps the previous
    // good context available for the fallback. See `ContextSwitchState`.
    app.conn.switch_requested();
    // Land on home NOW, not when the connection completes.
    //
    // A context switch is a ROOT-level change: everything on the stack
    // describes a cluster you have just asked to leave, and sitting on
    // "pods (prod)" while staging connects is the same disorientation as
    // sitting on the picker after you have already picked. Every entry point
    // does this — `:ctx <name>`, Enter on a context row — so the two can't
    // drift apart again.
    //
    // The teardown used to be deferred here so a FAILED switch could put you
    // back on your previous view. That is the cost: a failure now leaves you
    // on the fallback context's home with the error flash, rather than the
    // exact view you left. Worth it — the deferral bought one rare case and
    // charged for it on every switch. `core.clear()` and the namespace reset
    // still wait for confirmation (see `dispatch_app_event`), because unlike
    // the nav stack they would blank chrome the fallback still needs.
    app.nav.reset(crate::app::element::Element::Overview(
        crate::app::element::Overview,
    ));
    app.kube.kubectl_cache.clear();
    app.ui.close_modal();
    app.kube.metrics.clear();

    // An in-flight batch dies with the old session — its remaining
    // results will never arrive (the rebuild drops the old channels
    // WITHOUT a DaemonDisconnected, so the disconnect arm can't clean
    // this up). A stranded tracker would silently consume same-named
    // results from the NEW cluster; take it and fold the partial tally
    // into the switch flash instead.
    let batch_note = app
        .pending_batch
        .take()
        .map(|b| format!(" ({})", b.interrupted_summary().message))
        .unwrap_or_default();

    app.kube.context_switch = crate::app::ContextSwitchState::Requested(ctx_name.clone());
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...{}",
        ctx_name, batch_note
    )));
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
pub(crate) fn do_switch_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
) {
    app.kube.selected_ns = ns.clone();

    // Rebuild unconditionally — identical to the `:<res> <ns>` command path.
    // For a cluster-scoped tab the daemon forces effective_ns=All, so the
    // rebuild yields byte-identical data; the old early-return here made `0`
    // (all-namespaces) a silent no-op on cluster-scoped tabs — including CRDs
    // that resolve to Cluster scope — while the equivalent command still worked.
    app.ui.close_modal();
    app.kube.kubectl_cache.clear();

    // Same root recipe, re-scoped: a fresh element with a fresh store —
    // the old namespace's rows drop with the old stack (no cache to
    // scrub, no tracker to reset).
    let root_rid = match app.nav.root_spec() {
        Some(crate::app::nav::RootSpec::Resource(r)) => r,
        None => rid(BuiltInKind::Pod),
    };
    let root = App::root_list_element(data_source, &app.kube.metrics,
                &app.core, root_rid, ns.clone());
    app.nav.reset(root);

    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns.display()
    )));
}

#[cfg(test)]
#[path = "../tests/kube/session_actions.rs"]
mod dump_tests;
