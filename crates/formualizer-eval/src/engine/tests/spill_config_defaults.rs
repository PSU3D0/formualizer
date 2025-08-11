use crate::engine::{
    EvalConfig, SpillBoundsPolicy, SpillBufferMode, SpillCancellationPolicy, SpillConflictPolicy,
    SpillTiebreaker, SpillVisibility,
};

#[test]
fn spill_config_defaults() {
    let cfg = EvalConfig::default();
    assert_eq!(cfg.spill.conflict_policy, SpillConflictPolicy::Error);
    assert_eq!(cfg.spill.tiebreaker, SpillTiebreaker::FirstWins);
    assert_eq!(cfg.spill.bounds_policy, SpillBoundsPolicy::Strict);
    assert_eq!(cfg.spill.buffer_mode, SpillBufferMode::ShadowBuffer);
    assert_eq!(cfg.spill.memory_budget_bytes, None);
    assert_eq!(cfg.spill.cancellation, SpillCancellationPolicy::Cooperative);
    assert_eq!(cfg.spill.visibility, SpillVisibility::OnCommit);
}
