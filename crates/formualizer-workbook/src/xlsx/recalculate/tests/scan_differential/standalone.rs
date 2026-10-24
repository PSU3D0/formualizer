//! Standalone scanner boundary: pre-inflated, immutable XML in memory;
//! no ZIP, Calamine, normalization, oracle comparisons or per-event timers.
use super::*;
use crate::xlsx::options::{ReadOptions, ReadPolicy, ScanRetention};
use std::hint::black_box;

#[derive(Default)]
struct Events {
    literals: usize,
    formulas: usize,
}
impl sheet::events::Sink for Events {
    fn literal(&mut self, event: sheet::events::Literal<'_>) -> Result<(), IoError> {
        self.literals += 1;
        black_box(event);
        Ok(())
    }
    fn formula(&mut self, event: sheet::events::Formula<'_>) -> Result<(), IoError> {
        self.formulas += 1;
        black_box(event);
        Ok(())
    }
}

fn control(c: &mut Option<(std::fs::File, std::io::BufReader<std::fs::File>)>, command: &[u8]) {
    use std::io::{BufRead, Write};
    if let Some((writer, reader)) = c {
        writer.write_all(command).unwrap();
        let mut ack = String::new();
        reader.read_line(&mut ack).unwrap();
        // perf may include the C-string terminator after the previous ack.
        assert_eq!(ack.trim_matches('\0'), "ack\n");
    }
}

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
        let mut read = ReadOptions::from(&options);
        read.policy = if arm == "compatible" {
            ReadPolicy::Compatible
        } else {
            ReadPolicy::Strict
        };
        let request = sheet::ScanRequest {
            strategy: sheet::ScanStrategy::Plain,
            retention: ScanRetention {
                values: true,
                formula_events: true,
                marks: false,
                geometry: false,
            },
        };
        let mut events = Events::default();
        let mut ctl = std::env::var("FZ_SCAN_CONTROL").ok().map(|path| {
            (
                std::fs::OpenOptions::new().write(true).open(path).unwrap(),
                std::io::BufReader::new(
                    std::fs::File::open(std::env::var("FZ_SCAN_ACK").unwrap()).unwrap(),
                ),
            )
        });
        let (mut serialized, mut literals) = (0usize, 0usize);
        control(&mut ctl, b"enable\n");
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
                    "strict" | "compatible" => sheet::scanner::scan_with(
                        black_box(input),
                        &read,
                        request,
                        &mut 0,
                        &mut 0,
                        &mut events,
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
        control(&mut ctl, b"disable\n");
        literals += events.literals;
        black_box(events.formulas);
        assert_eq!(serialized, cells * iterations);
        black_box(literals);
        eprintln!(
            "STANDALONE arm={arm} iterations={iterations} cells={serialized} literals={literals} ns={ns} ns_per_cell={:.6}",
            ns as f64 / serialized as f64
        );
    });
}
