//! Reverse routing for observed grid reads, including empty producer cells.
//! Reuses the authority's geometric index; slots are recycled on retargeting.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_route_slots_recycle_across_index_levels() {
        let mut routes = ObservedRoutes::default();
        let mut records: Vec<_> = (0..130)
            .map(|r| {
                let reads = vec![(0, r, 1, r, 1)];
                let slots = routes.insert(VertexId(r), &reads);
                (reads, slots)
            })
            .collect();
        for r in (0..130).step_by(2) {
            routes.remove(&records[r].0, &records[r].1);
        }
        for n in 0..300 {
            let reads = vec![(0, 1000 + n, 2, 1000 + n, 2)];
            let slots = routes.insert(VertexId(1000), &reads);
            let mut out = Vec::new();
            routes.query(0, &Rect::cell(1000 + n, 2), &mut out);
            assert_eq!(out, vec![VertexId(1000)]);
            routes.remove(&reads, &slots);
            assert!(routes.sheets[&0].readers.len() <= 130);
        }
        for r in (1..130).step_by(2) {
            let mut out = Vec::new();
            routes.query(0, &Rect::cell(r as u32, 1), &mut out);
            assert_eq!(out, vec![VertexId(r as u32)]);
            let (reads, slots) = std::mem::take(&mut records[r]);
            routes.remove(&reads, &slots);
        }
        assert!(routes.is_empty());
    }
}
use super::geom::Rect;
use super::level_index::{LevelIndex, NONE};
use crate::engine::VertexId;
use rustc_hash::FxHashMap;

#[derive(Debug, Default)]
struct SheetRoutes {
    index: LevelIndex,
    locations: Vec<u32>,
    readers: Vec<VertexId>,
    free: Vec<u32>,
}
#[derive(Debug, Default)]
pub(crate) struct ObservedRoutes {
    sheets: FxHashMap<u16, SheetRoutes>,
}
impl ObservedRoutes {
    pub(super) fn is_empty(&self) -> bool {
        self.sheets.is_empty()
    }

    pub(super) fn insert(
        &mut self,
        reader: VertexId,
        reads: &[(u16, u32, u32, u32, u32)],
    ) -> Vec<u32> {
        reads
            .iter()
            .map(|&(sheet, r0, c0, r1, c1)| {
                let s = self.sheets.entry(sheet).or_default();
                let id = if let Some(id) = s.free.pop() {
                    s.readers[id as usize] = reader;
                    id
                } else {
                    let id = u32::try_from(s.readers.len()).expect("observed route slot capacity");
                    s.readers.push(reader);
                    s.locations.push(NONE);
                    id
                };
                s.index
                    .insert(Rect::new(r0, c0, r1, c1).as_box(), id, &mut s.locations);
                id
            })
            .collect()
    }

    pub(super) fn remove(&mut self, reads: &[(u16, u32, u32, u32, u32)], slots: &[u32]) {
        debug_assert_eq!(reads.len(), slots.len());
        for (&(sheet, _, _, _, _), &id) in reads.iter().zip(slots) {
            let s = self.sheets.get_mut(&sheet).expect("indexed observed sheet");
            s.index.remove(id, &mut s.locations);
            s.free.push(id);
            s.index.settle(&mut s.locations);
            if s.index.live() == 0 {
                self.sheets.remove(&sheet);
            }
        }
    }

    pub(super) fn query(&self, sheet: u16, rect: &Rect, out: &mut Vec<VertexId>) {
        if let Some(s) = self.sheets.get(&sheet) {
            s.index
                .query(&rect.as_box(), &mut |id| out.push(s.readers[id as usize]));
        }
    }
}
