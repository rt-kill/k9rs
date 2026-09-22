
/// A keybinding entry for the help overlay.
struct HelpEntry {
    key: String,
    /// `Cow` so a line can name what it acts on. The select-mode section
    /// is derived from the CURRENT resource's operations, and a screen
    /// that hardcodes which keys batch goes stale the moment an operation
    /// changes its `batch_support()` stance.
    description: std::borrow::Cow<'static, str>,
}

/// A section of keybindings.
struct HelpSection {
    title: &'static str,
    entries: Vec<HelpEntry>,
}

/// Help overlay widget.
/// NOT a widget — it renders nothing. Help is a nav element now
/// (`LocalResourceKind::Keybinding`), so this only projects the CONTENT the
/// table is seeded from. The `Widget` impl and the dialog geometry that used
/// to live here were dead after that move and invisible to clippy, because a
/// trait impl is never dead-code-linted.
pub struct HelpContent {
    caps: Option<crate::kube::protocol::ResourceCapabilities>,
    /// User key rebindings — help shows EFFECTIVE bindings, not defaults.
    keys: crate::app::KeysConfig,
    /// Which view the user pressed `?` in. Help is CONTEXTUAL now: a
    /// context-free dump had to describe `s` and `Ctrl-d` without saying
    /// which view it meant, so it was wrong in at least one of them.
    class: crate::event::keymap::ViewClass,
}

impl HelpContent {
    /// Project the sections from the ONE keymap declaration plus the
    /// capability path.
    ///
    /// This used to be ~250 lines of hand-written entries covering every view
    /// at once — so it showed `s` (follow, in logs) and `Ctrl-d` (half-page in
    /// detail, DELETE in a table) with no indication of which view they meant,
    /// omitted `J`/`U`/`~` entirely, and advertised a `Ctrl-l` feature that had
    /// been deleted. Now help shows the keys for the view you are actually in,
    /// and [`crate::event::keymap`]'s agreement test makes each one true.
    /// The help content as TABLE ROWS — the projection that lets help be an
    /// ordinary resource instead of a bespoke overlay.
    ///
    /// The grouping that used to be section headers becomes a SECTION
    /// column, which is strictly more useful: it filters and sorts like any
    /// other, so `/log` narrows to the log keys and `S` on SECTION regroups.
    /// The content itself is unchanged — same `keymap`, same capabilities,
    /// same effective bindings — because the thing being replaced was the
    /// rendering, not the knowledge.
    pub fn rows(
        caps: Option<crate::kube::protocol::ResourceCapabilities>,
        keys: crate::app::KeysConfig,
        class: crate::event::keymap::ViewClass,
    ) -> (Vec<String>, Vec<crate::kube::resources::row::ResourceRow>) {
        use crate::kube::resources::row::{CellValue, ResourceRow};
        let tmp = HelpContent { caps, keys, class };
        let mut rows = Vec::new();
        for section in tmp.sections() {
            for entry in section.entries {
                // A key-less line is prose belonging to its section (e.g.
                // "every other single-row key is disabled"); it keeps its
                // row so the statement is not lost, with an empty KEY.
                rows.push(ResourceRow {
                    cells: vec![
                        // SECTION first: the table's default sort is the
                        // first column, so the grouping the old sheet drew
                        // with headers survives as the default ORDER — and
                        // `S` on KEY regroups it the other way, which the
                        // headers could never do.
                        CellValue::Text(section.title.to_string()),
                        CellValue::Text(entry.key.clone()),
                        CellValue::Text(entry.description.into_owned()),
                    ],
                    name: format!("{}/{}", section.title, entry.key),
                    namespace: None,
                    ..Default::default()
                });
            }
        }
        (
            vec!["SECTION".to_string(), "KEY".to_string(), "DESCRIPTION".to_string()],
            rows,
        )
    }

    fn sections(&self) -> Vec<HelpSection> {
        use crate::event::keymap;

        let to_entries = |bindings: Vec<keymap::Binding>| -> Vec<HelpEntry> {
            bindings
                .into_iter()
                .map(|b| HelpEntry {
                    key: b.display_keys(&self.keys),
                    description: b.label.into(),
                })
                .collect()
        };

        let mut sections = vec![HelpSection {
            title: self.class.title(),
            entries: to_entries(keymap::for_class(self.class)),
        }];

        // Per-resource operations: label from the descriptor, key from the
        // EFFECTIVE binding. Only tables have capabilities.
        let resource_actions = self.resource_actions_section();
        if !resource_actions.entries.is_empty() {
            sections.push(resource_actions);
        }

        if self.class == keymap::ViewClass::Table {
            let mut entries = vec![
                HelpEntry {
                    key: "Space".into(),
                    description: "Toggle the mark on the cursor row".into(),
                },
                HelpEntry {
                    key: "Ctrl-Space".into(),
                    description: "Span-mark to/from the nearest mark".into(),
                },
                HelpEntry {
                    key: "Esc".into(),
                    description: "Clear the selection (then Esc pops as usual)".into(),
                },
                HelpEntry {
                    key: "Ctrl-\\".into(),
                    description: "Clear all marks".into(),
                },
                HelpEntry { key: "c".into(), description: "Copy ONLY the marked rows".into() },
            ];
            entries.extend(self.batch_entries());
            entries.push(HelpEntry {
                key: "".into(),
                description: "every other single-row key is disabled".into(),
            });
            sections.push(HelpSection {
                title: "Select mode (while rows are marked)",
                entries,
            });
        }

        sections.push(HelpSection {
            title: "Everywhere",
            entries: to_entries(keymap::global()),
        });

        // `/` is offered in every view class, and its syntax is the one
        // thing about it nobody can guess: `[a|b]` is a set of characters,
        // not a choice between two words, in vim as much as here.
        sections.push(HelpSection {
            title: "Search & filter syntax (vim magic)",
            entries: vec![
                HelpEntry {
                    key: r"foo\|bar".into(),
                    description: "either — a BARE | is a literal pipe".into(),
                },
                HelpEntry {
                    key: r"\(ab\)\+".into(),
                    description: r"group and repeat — bare ( ) + ? { } are literal".into(),
                },
                HelpEntry {
                    key: ". *  ^ $  [abc]".into(),
                    description: "special as usual (^ and $ anchor at the ends)".into(),
                },
                HelpEntry {
                    key: "foo".into(),
                    description: "smartcase: any capital makes it case-sensitive".into(),
                },
                // THE rule the four lines above are instances of, and the
                // one nobody guesses: backslash flips specialness, it does
                // not escape. `shard\=` is `shar` + an optional `d`, not a
                // literal `=` — and `=` never needed escaping to begin with.
                HelpEntry {
                    key: r"\= \? \+".into(),
                    description: "backslash makes punctuation SPECIAL, not literal".into(),
                },
                HelpEntry {
                    key: "= : - / @".into(),
                    description: "already literal — escaping one CHANGES its meaning".into(),
                },
            ],
        });
        sections
    }

    /// What the batch-capable keys do in select mode, read off the SAME
    /// `batch_support()` manifest the select gate transforms on — so a
    /// future operation that changes stance can't leave this screen
    /// claiming otherwise. Only operations THIS resource declares appear
    /// (a node has no Restart to batch).
    fn batch_entries(&self) -> Vec<HelpEntry> {
        use crate::kube::protocol::BatchSupport;
        let Some(ref caps) = self.caps else { return Vec::new() };
        caps.operations
            .iter()
            .filter_map(|op| {
                let key = self.keys.op_key(op).map(|c| c.label()).unwrap_or_else(|| {
                    // Structural chords: no descriptor default, always live.
                    match op {
                        crate::kube::protocol::OperationKind::Delete => "Ctrl-d".into(),
                        crate::kube::protocol::OperationKind::ForceKill => "Ctrl-k".into(),
                        _ => String::new(),
                    }
                });
                let label = op.descriptor().label;
                match op.batch_support() {
                    BatchSupport::PerItem => Some(HelpEntry {
                        key,
                        description: format!("{label} — EVERY marked row").into(),
                    }),
                    BatchSupport::Aggregate => Some(HelpEntry {
                        key,
                        description: format!("{label} — ONE merged stream over the marked rows")
                            .into(),
                    }),
                    BatchSupport::SingleOnly => None,
                }
            })
            .collect()
    }

    /// Build the "Resource Actions" section dynamically from the current
    /// resource's capabilities: label from the descriptor, key from the
    /// EFFECTIVE binding. With no caps (non-resource views) the section
    /// is empty.
    fn resource_actions_section(&self) -> HelpSection {
        let mut entries = Vec::new();

        if let Some(ref caps) = self.caps {
            // Data-driven: label from the descriptor, key from the
            // EFFECTIVE binding (user override or descriptor default).
            // Skip always-on ops (Describe, Yaml, Delete) — they're in
            // the Actions section already. Ops with no effective chord
            // (structural-only, like port-forward's Shift-F) are listed
            // via the hardcoded lines below.
            // EVERY declared operation, including Describe/Yaml/Delete.
            // They used to be skipped here because a hand-written "Actions"
            // section listed them — that section is gone, and skipping them
            // now would drop `d` and `y` out of help entirely.
            for op in &caps.operations {
                let desc = op.descriptor();
                if let Some(combo) = self.keys.op_key(op) {
                    entries.push(HelpEntry {
                        key: combo.label(),
                        description: desc.label.into(),
                    });
                }
            }
            // Structural chords with no descriptor default. `op_key` returns
            // `None` for these unless the user rebound them, in which case the
            // loop above already listed the override.
            if caps.supports(crate::kube::protocol::OperationKind::Delete)
                && self.keys.op_key(&crate::kube::protocol::OperationKind::Delete).is_none()
            {
                entries.push(HelpEntry { key: "Ctrl-d".into(), description: "Delete".into() });
            }
            if caps.supports(crate::kube::protocol::OperationKind::ForceKill) {
                entries.push(HelpEntry { key: "Ctrl-k".into(), description: "Force kill".into() });
            }
            if caps.supports(crate::kube::protocol::OperationKind::PortForward) {
                entries.push(HelpEntry { key: "Shift-F".into(), description: "Port forward".into() });
            }
        }

        HelpSection {
            title: "Resource Actions",
            entries,
        }
    }

}
