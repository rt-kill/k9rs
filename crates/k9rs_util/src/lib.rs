//! Small things every layer needs, and nothing of ours.
//!
//! Depends on no other `k9rs_*` crate, and deliberately not on ratatui —
//! `parse_ansi_line` lived here and dragged the TUI's rendering library
//! toward the daemon, so it moved to `k9rs_ui::ansi`. The escape tokenizer
//! stayed, because every text consumer needs it.
//!
//! One module per concern. This was a single 1176-line `lib.rs` holding seven
//! of them, with the four width functions split across its two ends and 500
//! lines of regex-dialect translation in the middle. Two submodules already
//! existed (`atomic_option`, `one_task`), so the pattern was there; the rest
//! had simply never been moved.
//!
//! TYPES are re-exported, because a name like `SearchPattern` or
//! `AbortOnDrop` already says what it is. FUNCTIONS are not: `truncate`,
//! `format_age` and `sanitize_terminal` each need the module to supply the
//! missing noun, so call sites say `width::truncate`, `units::format_age`,
//! `text::sanitize_terminal`.

pub(crate) mod atomic_option;
pub mod clipboard;
pub mod one_task;
pub(crate) mod search;
pub mod task;
pub mod temp;
pub mod text;
pub mod units;
pub mod width;

pub use atomic_option::AtomicOption;
pub use search::{CompiledGrep, MatchMode, SearchPattern};
pub use task::AbortOnDrop;
pub use temp::TempDirGuard;
