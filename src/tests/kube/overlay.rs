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
