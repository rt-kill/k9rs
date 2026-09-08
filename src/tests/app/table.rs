use super::*;

fn table(n: usize) -> StatefulTable<usize> {
    let mut t = StatefulTable::new();
    t.set_items((0..n).collect());
    t
}

#[test]
fn cursor_moves_and_clamps() {
    let mut t = table(3);
    t.next();
    t.next();
    assert_eq!(t.selected(), 2);
    t.next(); // at end: stays
    assert_eq!(t.selected(), 2);
    t.previous();
    assert_eq!(t.selected(), 1);
    t.home();
    assert_eq!(t.selected(), 0);
    t.end();
    assert_eq!(t.selected(), 2);
}

#[test]
fn paging_respects_bounds() {
    let mut t = table(100);
    t.set_page_size(10);
    t.page_down();
    assert_eq!(t.selected(), 10);
    t.page_up();
    assert_eq!(t.selected(), 0);
    t.page_up(); // at start: stays
    assert_eq!(t.selected(), 0);
}

#[test]
fn set_items_clamps_selection_and_sets_ready() {
    let mut t = table(10);
    t.end();
    assert_eq!(t.selected(), 9);
    t.set_items(vec![1, 2, 3]);
    assert_eq!(t.selected(), 2);
    assert_eq!(t.data_state, TableDataState::Ready);
    t.set_items(Vec::new());
    assert_eq!(t.selected(), 0);
    assert!(t.selected_item().is_none());
}

#[test]
fn visible_items_windows_by_offset_and_page() {
    let mut t = table(50);
    t.set_page_size(5);
    t.end();
    let visible: Vec<usize> = t.visible_items().into_iter().copied().collect();
    assert_eq!(visible, vec![45, 46, 47, 48, 49]);
    assert_eq!(t.offset(), 45);
}

#[test]
fn empty_table_is_safe_everywhere() {
    let mut t: StatefulTable<usize> = StatefulTable::new();
    t.next();
    t.previous();
    t.page_down();
    t.end();
    assert_eq!(t.selected(), 0);
    assert!(t.visible_items().is_empty());
    assert!(t.selected_item().is_none());
}
