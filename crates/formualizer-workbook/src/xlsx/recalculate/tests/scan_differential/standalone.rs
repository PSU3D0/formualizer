//! Standalone scanner boundary: pre-inflated, immutable XML in memory;
//! no ZIP, Calamine, normalization, oracle comparisons or per-event timers.
use super::*;
use std::hint::black_box;

#[test]
#[ignore = "standalone scanner benchmark; requires FZ_SCAN_INPUT_LIST"]
fn standalone_scanner() {
    exclusive(|| {
        let arm = std::env::var("FZ_SCAN_ARM").unwrap();
        let iterations: usize = std::env::var("FZ_SCAN_ITERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        let cells: usize = std::env::var("FZ_SCAN_CELL_COUNT")
            .unwrap()
            .parse()
            .unwrap();
        let list = std::fs::read_to_string(std::env::var("FZ_SCAN_INPUT_LIST").unwrap()).unwrap();
        let inputs: Vec<_> = list
            .lines()
            .map(|path| std::fs::read(path).unwrap())
            .collect();
        let options = XlsxRecalculateOptions::default();
        let (mut serialized, mut literals) = (0usize, 0usize);
        let started = std::time::Instant::now();
        for _ in 0..iterations {
            for input in &inputs {
                let result = match arm.as_str() {
                    "old" => sheet::scan_uncounted(
                        black_box(input),
                        &options,
                        sheet::ScanStrategy::Plain,
                        &mut 0,
                        &mut 0,
                    ),
                    "machine" => sheet::scanner::scan_emitting(
                        black_box(input),
                        &options,
                        sheet::ScanStrategy::Plain,
                        &mut 0,
                        &mut 0,
                        |literal| {
                            literals += 1;
                            black_box(literal);
                            Ok(())
                        },
                    ),
                    _ => panic!("unknown scanner arm"),
                }
                .unwrap();
                let sheet::Scanned::Done(scan) = result else {
                    panic!("benchmark requires scalar worksheets")
                };
                serialized += scan.serialized;
                black_box(scan);
            }
        }
        let ns = started.elapsed().as_nanos();
        assert_eq!(serialized, cells * iterations);
        black_box(literals);
        eprintln!(
            "STANDALONE arm={arm} iterations={iterations} cells={serialized} literals={literals} ns={ns} ns_per_cell={:.6}",
            ns as f64 / serialized as f64
        );
    });
}
