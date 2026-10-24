//! Calculation-name dependency analysis shared by workbook sources.
//! Only strongly connected components are cyclic; downstream names are not.
use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct CalculationNameAnalysis {
    pub(crate) dependencies: Vec<Vec<usize>>,
    pub(crate) cyclic: HashSet<usize>,
}

pub(crate) fn analyze(
    dependencies: Vec<Vec<usize>>,
    cancelled: impl Fn() -> bool,
) -> CalculationNameAnalysis {
    let mut reverse = vec![Vec::new(); dependencies.len()];
    let mut seen = vec![false; dependencies.len()];
    let mut finish = Vec::with_capacity(dependencies.len());
    for (root, deps) in dependencies.iter().enumerate() {
        if cancelled() {
            return CalculationNameAnalysis::default();
        }
        for &j in deps {
            reverse[j].push(root);
        }
        if seen[root] {
            continue;
        }
        seen[root] = true;
        let mut stack = vec![(root, 0)];
        while let Some((i, next)) = stack.last_mut() {
            if cancelled() {
                return CalculationNameAnalysis::default();
            }
            if let Some(&j) = dependencies[*i].get(*next) {
                *next += 1;
                if !seen[j] {
                    seen[j] = true;
                    stack.push((j, 0));
                }
            } else {
                finish.push(*i);
                stack.pop();
            }
        }
    }
    seen.fill(false);
    let mut cyclic = HashSet::new();
    for root in finish.into_iter().rev() {
        if cancelled() {
            return CalculationNameAnalysis::default();
        }
        if seen[root] {
            continue;
        }
        seen[root] = true;
        let mut component = Vec::new();
        let mut stack = vec![root];
        while let Some(i) = stack.pop() {
            if cancelled() {
                return CalculationNameAnalysis::default();
            }
            component.push(i);
            for &j in &reverse[i] {
                if !seen[j] {
                    seen[j] = true;
                    stack.push(j);
                }
            }
        }
        if component.len() > 1 || dependencies[root].contains(&root) {
            cyclic.extend(component);
        }
    }
    CalculationNameAnalysis {
        dependencies,
        cyclic,
    }
}
