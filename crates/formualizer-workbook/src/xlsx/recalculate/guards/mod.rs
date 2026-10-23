//! Strict source/evaluation guards before source-preserving publication.
//! These checks do not mutate source bytes or their formula marks.
pub(in crate::xlsx) mod inert;
pub(in crate::xlsx) mod visibility;
#[cfg(not(feature = "system-clock"))]
pub(in crate::xlsx) mod wall_clock;
