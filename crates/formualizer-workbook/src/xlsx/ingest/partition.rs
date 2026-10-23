//! Convert source-family evidence into engine partition proposals.
//! Exact legacy members and exclusion accounting are retained without source edits.
use super::evidence::{FragmentedFamilyProposal, SourceExclusion};
use formualizer_eval::engine::{
    ExplicitPartitionLegacyMembers, PartitionLegacyMember, PartitionLegacyMemberKind,
    PartitionReconciliation, PartitionedSourceFormulaFamily,
};

pub(crate) fn convert(
    proposal: FragmentedFamilyProposal,
) -> Result<PartitionedSourceFormulaFamily, &'static str> {
    let mut members: Vec<_> = proposal
        .fallback_members
        .into_iter()
        .map(|coord| PartitionLegacyMember {
            coord,
            kind: PartitionLegacyMemberKind::SharedFamilyMember,
        })
        .collect();
    let mut ordinary_exceptions = 0u64;
    let mut holes = 0u64;
    for exclusion in proposal.exclusions {
        match exclusion {
            SourceExclusion::Hole(_) => holes = holes.saturating_add(1),
            SourceExclusion::OrdinaryFormula(coord) => {
                ordinary_exceptions = ordinary_exceptions.saturating_add(1);
                members.push(PartitionLegacyMember {
                    coord,
                    kind: PartitionLegacyMemberKind::OrdinaryException,
                });
            }
        }
    }
    Ok(PartitionedSourceFormulaFamily {
        source_id: proposal.source_id,
        source_order: proposal.source_order,
        template_origin0: proposal.anchor_coord0,
        template_text: proposal.anchor_text,
        declared: proposal.declared,
        surviving_member_count: proposal.member_count,
        fragments: proposal.fragments,
        legacy_members: ExplicitPartitionLegacyMembers::try_new(members)?,
        reconciliation: PartitionReconciliation {
            shared_members: proposal.member_count,
            ordinary_exceptions,
            holes,
        },
    })
}
