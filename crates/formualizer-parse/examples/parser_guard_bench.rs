//! Release parser+drop throughput; preparation and semantic checks are untimed.
//! Each invocation emits CSV; run independent processes, keep every sample.
use formualizer_parse::{ASTNode, parse, parser::BatchParser};
use std::{hint::black_box, time::Instant};

// The old baseline builder has no capacity method; its cache is unbounded.
// An inherent method in the guarded builder takes precedence over this fallback.
// This lets the identical harness isolate cache policy via an untimed override.
#[allow(dead_code)]
trait BaselineCacheCapacity: Sized {
    fn cache_capacity(self, _entries: usize, _bytes: usize) -> Self {
        self
    }
}
impl<T> BaselineCacheCapacity for T {}

struct Case {
    name: &'static str,
    formulas: Vec<String>,
    iterations: usize,
    batch: bool,
}

fn corpus() -> Vec<Case> {
    let distinct = |f: fn(usize) -> String| (1..=1024).map(f).collect();
    let mut nested = "A1".to_owned();
    for n in 1..=12 {
        nested = format!("IF(B{n}>0,{nested},0)");
    }
    let wide = format!(
        "=SUM({})",
        (1..=64)
            .map(|i| format!("A{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let array = format!(
        "={{{}}}",
        (0..8)
            .map(|r| (0..8)
                .map(|c| (r * 8 + c).to_string())
                .collect::<Vec<_>>()
                .join(","))
            .collect::<Vec<_>>()
            .join(";")
    );
    let chain = format!(
        "={}",
        (1..=128)
            .map(|i| format!("A{i}"))
            .collect::<Vec<_>>()
            .join("+")
    );
    vec![
        Case {
            name: "short_distinct",
            formulas: distinct(|r| format!("=A{r}+B{r}*$C$1")),
            iterations: 300_000,
            batch: false,
        },
        Case {
            name: "finance_arithmetic",
            formulas: distinct(|r| format!("=($B$2*C{r}+D{r})*(1+$E$1)^$F$1")),
            iterations: 200_000,
            batch: false,
        },
        Case {
            name: "lookup_aggregate",
            formulas: distinct(|r| {
                format!(
                    "=IFERROR(INDEX(Data!$D$2:$D$1000,MATCH(A{r},Data!$A$2:$A$1000,0)),SUMIFS($E$2:$E$1000,$B$2:$B$1000,B{r}))"
                )
            }),
            iterations: 75_000,
            batch: false,
        },
        Case {
            name: "structured_quoted_unicode",
            formulas: vec![
                "=SUM(Table1[[#Data],[Amount]])+'Jan 24:Mar 24'!B5".into(),
                "=IF(Table1[@[Amount]]>0,'Données été'!$A$1,0)".into(),
            ],
            iterations: 100_000,
            batch: false,
        },
        Case {
            name: "nested_if_12",
            formulas: vec![format!("={nested}")],
            iterations: 30_000,
            batch: false,
        },
        Case {
            name: "wide_sum_64",
            formulas: vec![wide],
            iterations: 30_000,
            batch: false,
        },
        Case {
            name: "array_8x8",
            formulas: vec![array],
            iterations: 30_000,
            batch: false,
        },
        Case {
            name: "left_chain_128",
            formulas: vec![chain],
            iterations: 10_000,
            batch: false,
        },
        Case {
            name: "powers_parenthesized",
            formulas: vec!["=2^3^2+2^(3^2)+(-2)^2".into()],
            iterations: 200_000,
            batch: false,
        },
        Case {
            name: "batch_repeat_64",
            formulas: (1..=64).map(|r| format!("=A{r}+B{r}*$C$1")).collect(),
            iterations: 300_000,
            batch: true,
        },
        Case {
            name: "batch_workingset_4096",
            formulas: (1..=4096).map(|r| format!("=A{r}+B{r}*$C$1")).collect(),
            iterations: 300_000,
            batch: true,
        },
        Case {
            name: "batch_unique_stream",
            formulas: (1..=50_000).map(|r| format!("=A{r}+B{r}*$C$1")).collect(),
            iterations: 50_000,
            batch: true,
        },
    ]
}

fn semantic_digest(formulas: &[String]) -> u64 {
    // Outside timing; include every source, not only a first representative.
    let mut h = 0xcbf2_9ce4_8422_2325_u64;
    for formula in formulas {
        let ast = parse(formula).unwrap_or_else(|e| panic!("{formula}: {e}"));
        h ^= ast.fingerprint();
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn consume(ast: ASTNode) {
    black_box(&ast);
    drop(ast);
}

fn main() {
    println!("workload,iterations,sources,elapsed_ns,ns_per_parse,digest");
    for c in corpus() {
        let digest = semantic_digest(&c.formulas);
        let builder = BatchParser::builder();
        let builder = if let Ok(entries) = std::env::var("BENCH_CACHE_ENTRIES") {
            builder.cache_capacity(entries.parse().unwrap(), usize::MAX)
        } else {
            builder
        };
        let mut batch = builder.build();
        // Warm allocator, code and the full working set. Pre-existing token
        // cache warmup is retained intentionally to reveal eviction thrashing.
        let unique = c.name == "batch_unique_stream";
        let warm_count = if unique {
            4096
        } else {
            c.formulas.len().max(4096)
        };
        for i in 0..warm_count {
            let other = unique.then(|| format!("=X{i}+Y{i}*$C$1"));
            let f = other
                .as_deref()
                .unwrap_or(&c.formulas[i % c.formulas.len()]);
            consume(if c.batch {
                batch.parse(f).unwrap()
            } else {
                parse(f).unwrap()
            });
        }
        let start = Instant::now();
        for i in 0..c.iterations {
            let f = black_box(&c.formulas[i % c.formulas.len()]);
            consume(if c.batch {
                batch.parse(f).unwrap()
            } else {
                parse(f).unwrap()
            });
        }
        let elapsed = start.elapsed().as_nanos();
        println!(
            "{},{},{},{},{:.3},{:016x}",
            c.name,
            c.iterations,
            c.formulas.len(),
            elapsed,
            elapsed as f64 / c.iterations as f64,
            digest
        );
    }
}
