    use super::*;

    // -- RenderPredicate tests --------------------------------------------------

    #[test]
    fn predicate_parse_gte() {
        let p = RenderPredicate::parse_when(">= 90").unwrap();
        assert!(p.matches("95%"));
        assert!(p.matches("90"));
        assert!(!p.matches("89"));
    }

    #[test]
    fn predicate_parse_gt() {
        let p = RenderPredicate::parse_when("> 90").unwrap();
        assert!(p.matches("95"));
        assert!(!p.matches("90"));
        assert!(!p.matches("85"));
    }

    #[test]
    fn predicate_parse_lt() {
        let p = RenderPredicate::parse_when("< 10").unwrap();
        assert!(p.matches("5"));
        assert!(!p.matches("15"));
    }

    #[test]
    fn predicate_parse_invalid() {
        assert!(RenderPredicate::parse_when("bad predicate").is_none());
    }

    #[test]
    fn predicate_contains() {
        let p = RenderPredicate::Contains("CrashLoop".into());
        assert!(p.matches("CrashLoopBackOff"));
        assert!(!p.matches("Running"));
    }

    #[test]
    fn predicate_numeric_strips_units() {
        let p = RenderPredicate::parse_when(">= 70").unwrap();
        assert!(p.matches("72%"));
        assert!(p.matches("800m"));
        assert!(!p.matches("50%"));
    }

    // -- build_column_rules + evaluate tests ----------------------------------

    #[test]
    fn build_and_evaluate_column_rules() {
        let headers = vec!["STATUS".to_string(), "%CPU/R".to_string()];
        let _rules = build_column_rules(&headers, "pods");
        // Without a loaded overlay for "pods" in tests, rules are empty.
        // Directly test ColumnRenderRules evaluation instead.
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::Contains("CrashLoopBackOff".into()),
                    style: RowHealth::Failed,
                },
            ],
        };
        assert_eq!(col.evaluate("CrashLoopBackOff"), Some(RowHealth::Failed));
        assert_eq!(col.evaluate("Running"), None);
    }

    #[test]
    fn column_rules_numeric_evaluation() {
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 90").unwrap(),
                    style: RowHealth::Failed,
                },
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 70").unwrap(),
                    style: RowHealth::Pending,
                },
            ],
        };
        assert_eq!(col.evaluate("95%"), Some(RowHealth::Failed));
        assert_eq!(col.evaluate("72%"), Some(RowHealth::Pending));
        assert_eq!(col.evaluate("50%"), None);
    }

    #[test]
    fn column_rules_first_match_wins() {
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 90").unwrap(),
                    style: RowHealth::Failed,
                },
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 70").unwrap(),
                    style: RowHealth::Pending,
                },
            ],
        };
        // 95% matches both rules, but first match (Failed) wins.
        assert_eq!(col.evaluate("95%"), Some(RowHealth::Failed));
    }

    #[test]
    fn predicate_na_value_does_not_match() {
        // Metrics placeholder "n/a" should never match numeric predicates.
        let p = RenderPredicate::parse_when(">= 70").unwrap();
        assert!(!p.matches("n/a"));
        assert!(!p.matches(""));
    }

    #[test]
    fn build_column_rules_missing_column_is_empty() {
        // Overlay references a column that doesn't exist in headers.
        let headers = vec!["NAME".to_string(), "STATUS".to_string()];
        let rules = build_column_rules(&headers, "nonexistent-resource");
        // No overlay for this resource → all rules empty.
        assert!(rules.iter().all(|r| r.rules.is_empty()));
    }

    #[test]
    fn deserialize_overlay_yaml() {
        let yaml = r#"
resource: nodeclaims
capabilities:
  show-node:
    type: drill
    target: nodes
    column: NODE
bindings:
  o: show-node
columns:
  - header: "INSTANCE TYPE"
    jsonpath: ".spec.instanceType"
coloring:
  - column: "STATUS"
    rules:
      - match: "NotReady"
        health: failed
"#;
        let overlay: ResourceOverlay = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(overlay.resource, "nodeclaims");
        assert_eq!(overlay.bindings.get(&'o'), Some(&"show-node".to_string()));
        assert!(overlay.capabilities.contains_key("show-node"));
        let OverlayCapability::Drill { ref target, ref column } = overlay.capabilities["show-node"];
        assert_eq!(target, "nodes");
        assert_eq!(column, "NODE");
        assert_eq!(overlay.columns.len(), 1);
        assert_eq!(overlay.columns[0].header, "INSTANCE TYPE");
        assert_eq!(overlay.coloring.len(), 1);
    }

    // -- Overlay key canonicalization -------------------------------------------

    /// An overlay written against an ALIAS must attach to the resource.
    ///
    /// Overlays were stored under the raw `resource:` string and looked up by
    /// plural, so `resource: po` filed under `po` and never matched `pods`.
    /// What made it invisible: the load-time validation resolves aliases (it
    /// calls `REGISTRY.by_alias`), so the file passed every check and the
    /// startup log said "loaded 1 overlay(s): po" while every column, binding
    /// and colour rule in it was dead.
    #[test]
    fn an_overlay_named_by_alias_attaches_to_the_resource() {
        use crate::kube::resource_def::BuiltInKind;
        let pods = crate::kube::protocol::ResourceId::BuiltIn(BuiltInKind::Pod);

        for written_as in ["po", "pod", "pods", "PODS", "Po"] {
            assert_eq!(
                super::canonical_key(written_as),
                pods.plural().to_lowercase(),
                "an overlay written as {written_as:?} must key on the canonical plural",
            );
        }
    }

    /// A CRD is not in the registry, so its name is kept as written — that is
    /// how CRD overlays have always been looked up.
    #[test]
    fn an_unknown_resource_keeps_the_name_it_was_given() {
        assert_eq!(super::canonical_key("NodeClaims"), "nodeclaims");
        assert_eq!(super::canonical_key("widgets.example.io"), "widgets.example.io");
    }

    // -- Coloring rules must name a column that exists ---------------------------

    fn coloring_for(columns: &[&str]) -> ResourceOverlay {
        let rules = columns
            .iter()
            .map(|c| format!("  - column: \"{c}\"\n    rules:\n      - when: \">= 90\"\n        health: failed"))
            .collect::<Vec<_>>()
            .join("\n");
        serde_yaml::from_str(&format!("resource: nodes\ncoloring:\n{rules}\n"))
            .expect("overlay yaml")
    }

    /// A coloring rule naming a column the resource doesn't have is a SILENT
    /// no-op: `build_column_rules` resolves by header and yields empty rules.
    ///
    /// That is not hypothetical. A node overlay colouring `CPU%`/`MEM%` sat
    /// dead indefinitely — the real headers are `%CPU`/`%MEM` — while the
    /// `STATUS` rules in the same file worked, so it looked like util
    /// colouring was unsupported rather than misspelled.
    #[test]
    fn a_coloring_rule_for_a_missing_column_is_reported() {
        let known = ["NAME", "STATUS", "%CPU", "%MEM"];

        // The real typo, both directions.
        let overlay = coloring_for(&["CPU%", "MEM%"]);
        assert_eq!(
            super::unknown_coloring_columns(&overlay, &known),
            vec!["CPU%", "MEM%"],
        );

        // The corrected spelling is accepted.
        let overlay = coloring_for(&["%CPU", "%MEM", "STATUS"]);
        assert!(super::unknown_coloring_columns(&overlay, &known).is_empty());

        // Case differences already work at lookup time, so they must not warn.
        let overlay = coloring_for(&["status"]);
        assert!(super::unknown_coloring_columns(&overlay, &known).is_empty());
    }

    /// A column the overlay itself declares counts as known — that is the
    /// whole point of CRD overlays adding columns and then colouring them.
    #[test]
    fn an_overlay_may_colour_a_column_it_adds_itself() {
        let overlay: ResourceOverlay = serde_yaml::from_str(
            "resource: nodeclaims\n\
             columns:\n  - header: \"INSTANCE TYPE\"\n    jsonpath: \".spec.instanceType\"\n\
             coloring:\n  - column: \"INSTANCE TYPE\"\n    rules:\n      - match: \"spot\"\n        health: pending\n",
        )
        .expect("overlay yaml");
        let known: Vec<&str> = ["NAME"]
            .into_iter()
            .chain(overlay.columns.iter().map(|c| c.header.as_str()))
            .collect();
        assert!(super::unknown_coloring_columns(&overlay, &known).is_empty());
    }


/// The typo check must read the MANDATORY header list, not the optional
/// tagging hook.
///
/// It read `column_defs()`, which only 4 of 34 built-ins override, so it
/// warned about 30 resources' perfectly valid rules — with an empty column
/// list, since there was nothing to list. The original test passed a
/// hand-built `known` and so never touched the registry; this one goes
/// through it, which is the only way to catch the mistake.
#[test]
fn a_real_resources_columns_come_from_its_headers() {
    let deployments = crate::kube::resource_defs::REGISTRY
        .by_alias("deployments")
        .expect("deployments is a built-in");

    let headers = deployments.default_headers();
    assert!(
        headers.iter().any(|h| h == "READY"),
        "deployments really do have a READY column: {headers:?}",
    );
    assert!(
        deployments.column_defs().is_empty(),
        "…and no column_defs at all, which is why reading those was wrong",
    );

    let known: Vec<&str> = headers.iter().map(String::as_str).collect();

    // The overlay the user actually has: colouring READY on deployments.
    let real: ResourceOverlay = serde_yaml::from_str(
        "resource: deployments\ncoloring:\n  - column: \"READY\"\n    rules:\n      - match: \"0/\"\n        health: failed\n",
    )
    .expect("overlay yaml");
    assert!(
        unknown_coloring_columns(&real, &known).is_empty(),
        "a rule on a column the resource HAS must not be reported as unknown",
    );

    // And a genuine typo still is.
    let typo: ResourceOverlay = serde_yaml::from_str(
        "resource: deployments\ncoloring:\n  - column: \"REDY\"\n    rules:\n      - match: \"x\"\n        health: failed\n",
    )
    .expect("overlay yaml");
    assert_eq!(unknown_coloring_columns(&typo, &known), vec!["REDY"]);
}
