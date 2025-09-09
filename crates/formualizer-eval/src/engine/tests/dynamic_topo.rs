use crate::engine::topo::pk::{DynamicTopo, GraphView, PkConfig};
use crate::engine::vertex::VertexId;
use rustc_hash::FxHashMap;

struct SimpleGraph {
    succ: FxHashMap<VertexId, Vec<VertexId>>,
    pred: FxHashMap<VertexId, Vec<VertexId>>,
}

impl GraphView<VertexId> for SimpleGraph {
    fn successors(&self, n: VertexId, out: &mut Vec<VertexId>) {
        out.clear();
        if let Some(v) = self.succ.get(&n) {
            out.extend(v.iter().copied());
        }
    }
    fn predecessors(&self, n: VertexId, out: &mut Vec<VertexId>) {
        out.clear();
        if let Some(v) = self.pred.get(&n) {
            out.extend(v.iter().copied());
        }
    }
    fn exists(&self, _n: VertexId) -> bool {
        true
    }
}

#[test]
fn pk_basic_insert_and_layers() {
    let mut g = SimpleGraph {
        succ: FxHashMap::default(),
        pred: FxHashMap::default(),
    };
    let nodes = vec![VertexId(1), VertexId(2), VertexId(3), VertexId(4)];
    let mut pk = DynamicTopo::new(nodes.clone(), PkConfig::default());
    pk.rebuild_full(&g);

    // Establish edges: 1->2, 2->3 (conceptual)
    g.succ.entry(VertexId(1)).or_default().push(VertexId(2));
    g.pred.entry(VertexId(2)).or_default().push(VertexId(1));
    assert!(pk.try_add_edge(&g, VertexId(1), VertexId(2)).is_ok());

    g.succ.entry(VertexId(2)).or_default().push(VertexId(3));
    g.pred.entry(VertexId(3)).or_default().push(VertexId(2));
    assert!(pk.try_add_edge(&g, VertexId(2), VertexId(3)).is_ok());

    // layers for subset
    let layers = pk.layers_for(&g, &[VertexId(1), VertexId(2), VertexId(3)], None);
    assert_eq!(layers.len(), 3);
    assert_eq!(layers[0], vec![VertexId(1)]);
    assert_eq!(layers[1], vec![VertexId(2)]);
    assert_eq!(layers[2], vec![VertexId(3)]);
}

#[test]
fn pk_cycle_detection() {
    let mut g = SimpleGraph {
        succ: FxHashMap::default(),
        pred: FxHashMap::default(),
    };
    let nodes = vec![VertexId(1), VertexId(2)];
    let mut pk = DynamicTopo::new(nodes.clone(), PkConfig::default());
    pk.rebuild_full(&g);

    // 1->2
    g.succ.entry(VertexId(1)).or_default().push(VertexId(2));
    g.pred.entry(VertexId(2)).or_default().push(VertexId(1));
    assert!(pk.try_add_edge(&g, VertexId(1), VertexId(2)).is_ok());

    // Adding 2->1 should cycle
    g.succ.entry(VertexId(2)).or_default().push(VertexId(1));
    g.pred.entry(VertexId(1)).or_default().push(VertexId(2));
    assert!(pk.try_add_edge(&g, VertexId(2), VertexId(1)).is_err());
}
