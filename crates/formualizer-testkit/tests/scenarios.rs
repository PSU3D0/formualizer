use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_testkit::run::XlsxReader;
use formualizer_testkit::scenario::ladder::{by_rows_or_class, covering_set};
use formualizer_testkit::{
    materialize::WorkbookRoute,
    run::{Materializer, Recorder, run},
    scenario::{
        Expect, ExpectedFailureSpec, FailureFingerprint, Filter, Provenance, ScenarioSpec, Step,
        StructureExpect, built_in_registry,
    },
};
use formualizer_workbook::WorkbookConfig;
use libtest_mimic::{Arguments, Failed, Trial};
use std::{env, str::FromStr, sync::Arc};

fn config(mode: FormulaPlaneMode) -> WorkbookConfig {
    let mut config = WorkbookConfig::interactive().with_formula_plane_mode(mode);
    config.eval.enable_parallel = false;
    config
}
fn execute_both(
    spec: &ScenarioSpec,
    mode: FormulaPlaneMode,
    record: bool,
    filter: Option<&Filter>,
) -> Result<(), Failed> {
    let size = spec.sizes[0];
    let recorder = record.then(Recorder::default);
    if filter.is_none_or(|filter| filter.allows_provenance(Provenance::WorkbookApi)) {
        let api = Materializer::workbook_api(WorkbookRoute::SetValuesSetFormulas, config(mode));
        run(spec, mode, size, api, recorder.as_ref())
            .into_result()
            .map_err(Failed::from)?;
    }
    for (provenance, reader) in [
        (Provenance::XlsxCalamine, XlsxReader::Calamine),
        (Provenance::XlsxUmya, XlsxReader::Umya),
    ] {
        if filter.is_none_or(|filter| filter.allows_provenance(provenance)) {
            let path =
                env::temp_dir().join(format!("fz-scenario-{}-{mode:?}-{reader:?}.xlsx", spec.id));
            let xlsx = Materializer::xlsx(path, reader, config(mode));
            run(spec, mode, size, xlsx, recorder.as_ref())
                .into_result()
                .map_err(Failed::from)?;
        }
    }
    Ok(())
}
fn take_custom_args() -> (
    Arguments,
    bool,
    Option<Filter>,
    Option<String>,
    Option<String>,
) {
    let mut args = Vec::new();
    let mut record = false;
    let mut tag = None;
    let mut mode = None;
    let mut size = None;
    let mut input = env::args();
    args.push(input.next().unwrap());
    while let Some(arg) = input.next() {
        if arg == "--record" {
            record = true;
        } else if arg == "--tag-filter" {
            tag = Some(
                Filter::from_str(&input.next().expect("--tag-filter requires a value"))
                    .expect("invalid --tag-filter"),
            );
        } else if arg == "--mode" {
            mode = Some(input.next().expect("--mode requires a value"));
        } else if arg == "--rung" {
            size = Some(input.next().expect("--rung requires rows, class, or all"));
        } else {
            args.push(arg);
        }
    }
    if tag.is_none() {
        tag = Filter::from_env().expect("invalid FZ_SCENARIO_FILTER");
    }
    (Arguments::from_iter(args), record, tag, mode, size)
}
fn main() {
    let (arguments, record, filter, selected_mode, selected_size) = take_custom_args();
    // A witness rung owns its row-bound models and goldens.
    let requested = selected_size.as_deref().unwrap_or("256");
    let mut rungs =
        by_rows_or_class(requested).unwrap_or_else(|| panic!("unknown --rung {requested:?}"));
    // Large and Nightly are never implicit: any --rung value is an explicit
    // request; without one only the 256-row default is enumerated. The env
    // switch is retained for automation that supplies class filters.
    let _nightly_enabled = env::var("FZ_SCENARIO_NIGHTLY").ok().as_deref() == Some("1");
    if selected_size.is_none() {
        rungs.retain(|r| r.rows == 256);
    }
    let mut registry = built_in_registry(200);
    // M0 pins are fixed-size behavioral scenarios; they run at the default
    // rung only.
    if selected_size.is_none() {
        registry.extend(formualizer_testkit::pins::pin_registry());
    }
    let first_witness = registry.len();
    registry.extend(covering_set(rungs));
    let mut trials = Vec::new();
    for (position, spec) in registry
        .iter()
        .enumerate()
        .filter(|(_, spec)| filter.as_ref().is_none_or(|filter| filter.matches(spec)))
    {
        let is_witness = position >= first_witness;
        for &mode in &spec.modes {
            if selected_mode
                .as_ref()
                .is_some_and(|selected| !mode_matches(mode, selected))
            {
                continue;
            }
            let spec = spec.clone();
            let run_filter = filter.clone();
            // Witness structural goldens are event-derived and always record.
            let record = record || is_witness;
            trials.push(Trial::test(
                format!("{}.{}", spec.id, mode_name(mode)),
                move || execute_both(&spec, mode, record, run_filter.as_ref()),
            ));
        }
    }
    trials.push(Trial::test(
        "framework.recorder-no-bleed",
        recorder_no_bleed,
    ));
    // M2 span-internal: these check span placement goldens (active spans,
    // placed/demoted families); with the FormulaPlane mode ignored
    // (unified_authority) no span is placed, so they are reported ignored.
    let spans_ignored = formualizer_testkit::run::formula_plane_mode_ignored();
    trials.push(
        Trial::test("framework.wrong-structure-golden", wrong_structure_golden)
            .with_ignored_flag(spans_ignored),
    );
    trials.push(
        Trial::test("framework.structure-goldens", structure_goldens)
            .with_ignored_flag(spans_ignored),
    );
    trials.push(Trial::test("framework.parity", parity_expectation));
    trials.push(Trial::test("framework.filters-by-tag", filters_by_tag));
    trials.push(Trial::test(
        "framework.expected-failure-earlier-failure-fails",
        expected_failure_earlier_failure_fails,
    ));
    trials.push(Trial::test(
        "framework.expected-failure-other-mismatch-fails",
        expected_failure_other_mismatch_fails,
    ));
    trials.push(Trial::test(
        "framework.expected-failure-exact-is-known",
        expected_failure_exact_is_known,
    ));
    trials.push(Trial::test(
        "framework.expected-failure-unexpected-pass-fails",
        expected_failure_unexpected_pass_fails,
    ));
    libtest_mimic::run(&arguments, trials).exit();
}
fn mode_matches(mode: FormulaPlaneMode, selected: &str) -> bool {
    match mode {
        FormulaPlaneMode::Off => selected == "off",
        FormulaPlaneMode::Shadow => selected == "shadow",
        FormulaPlaneMode::AuthoritativeExperimental => matches!(
            selected,
            "authoritative" | "auth" | "authoritative-experimental"
        ),
    }
}
fn mode_name(mode: FormulaPlaneMode) -> &'static str {
    match mode {
        FormulaPlaneMode::Off => "off",
        FormulaPlaneMode::Shadow => "shadow",
        FormulaPlaneMode::AuthoritativeExperimental => "authoritative",
    }
}
fn recorder_no_bleed() -> Result<(), Failed> {
    let spec = built_in_registry(200).remove(1);
    let recorder = Recorder::default();
    let size = spec.sizes[0];
    let materializer = || {
        Materializer::workbook_api(
            WorkbookRoute::SetValuesSetFormulas,
            config(FormulaPlaneMode::AuthoritativeExperimental),
        )
    };
    run(
        &spec,
        FormulaPlaneMode::AuthoritativeExperimental,
        size,
        materializer(),
        Some(&recorder),
    )
    .into_result()
    .map_err(Failed::from)?;
    let signature =
        |buckets: std::collections::BTreeMap<usize, formualizer_testkit::run::StepBucket>| {
            buckets
                .into_iter()
                .map(|(step, bucket)| {
                    (
                        step,
                        bucket
                            .events
                            .into_iter()
                            .map(|event| event.name)
                            .collect::<Vec<_>>(),
                        bucket.spans,
                    )
                })
                .collect::<Vec<_>>()
        };
    let first = signature(recorder.buckets());
    run(
        &spec,
        FormulaPlaneMode::AuthoritativeExperimental,
        size,
        materializer(),
        Some(&recorder),
    )
    .into_result()
    .map_err(Failed::from)?;
    if first != signature(recorder.buckets()) {
        return Err("recorder buckets bled between runs".into());
    }
    Ok(())
}
fn wrong_structure_golden() -> Result<(), Failed> {
    let mut spec = built_in_registry(200).remove(0);
    spec.expects.push((
        2,
        formualizer_testkit::scenario::Expect::Structure(StructureExpect {
            active_spans: Some(1),
            ..Default::default()
        }),
    ));
    let recorder = Recorder::default();
    let report = run(
        &spec,
        FormulaPlaneMode::AuthoritativeExperimental,
        spec.sizes[0],
        Materializer::workbook_api(
            WorkbookRoute::SetValuesSetFormulas,
            config(FormulaPlaneMode::AuthoritativeExperimental),
        ),
        Some(&recorder),
    );
    match report.failure {
        Some(message) if message.contains("step 2") && message.contains("active_spans") => Ok(()),
        other => Err(format!("wrong golden did not name step and field: {other:?}").into()),
    }
}
fn structure_goldens() -> Result<(), Failed> {
    for spec in built_in_registry(200) {
        let recorder = Recorder::default();
        let report = run(
            &spec,
            FormulaPlaneMode::AuthoritativeExperimental,
            spec.sizes[0],
            Materializer::workbook_api(
                WorkbookRoute::SetValuesSetFormulas,
                config(FormulaPlaneMode::AuthoritativeExperimental),
            ),
            Some(&recorder),
        )
        .into_result()
        .map_err(Failed::from)?;
        let stats = report.steps[2].stats.unwrap();
        let events: Vec<_> = recorder
            .buckets()
            .into_values()
            .flat_map(|bucket| bucket.events)
            .collect();
        let placed: Vec<_> = events
            .iter()
            .filter(|event| event.name == "fz.family.placed")
            .collect();
        let demoted: Vec<_> = events
            .iter()
            .filter(|event| event.name == "fz.span.demoted")
            .collect();
        match spec.id.as_str() {
            "coupled" => {
                if placed.len() != 2
                    || demoted.len() != 2
                    || stats.formula_plane_active_span_count != 0
                {
                    return Err(format!(
                        "coupled golden: placed={}, demoted={}, active={}, events={:?}",
                        placed.len(),
                        demoted.len(),
                        stats.formula_plane_active_span_count,
                        events.iter().map(|e| e.name.as_str()).collect::<Vec<_>>()
                    )
                    .into());
                }
                if demoted.iter().any(|event| {
                    event.fields.get("reason").map(|v| v.trim_matches('"')) != Some("CycleMember")
                }) {
                    return Err("coupled demotion reason was not CycleMember".into());
                }
            }
            "independent" => {
                if placed.len() != 1 || stats.formula_plane_active_span_count != 1 {
                    return Err(format!(
                        "independent golden: placed={}, active={}",
                        placed.len(),
                        stats.formula_plane_active_span_count
                    )
                    .into());
                }
            }
            "fixed-absolute-sum" => {
                if placed.len() != 1 || stats.formula_plane_active_span_count != 1 {
                    return Err(format!(
                        "fixed golden: placed={}, active={}",
                        placed.len(),
                        stats.formula_plane_active_span_count
                    )
                    .into());
                }
                if placed[0].fields.get("constant_result").map(String::as_str) != Some("true") {
                    return Err(format!("fixed constant_result: {:?}", placed[0].fields).into());
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
fn parity_expectation() -> Result<(), Failed> {
    let mut spec = built_in_registry(200).remove(1);
    spec.expects
        .push((2, formualizer_testkit::scenario::Expect::Parity));
    run(
        &spec,
        FormulaPlaneMode::Off,
        spec.sizes[0],
        Materializer::workbook_api(
            WorkbookRoute::SetValuesSetFormulas,
            config(FormulaPlaneMode::Off),
        ),
        None,
    )
    .into_result()
    .map(|_| ())
    .map_err(Failed::from)
}
fn filters_by_tag() -> Result<(), Failed> {
    let specs = built_in_registry(200);
    let coupled = Filter::from_str("family:coupled purpose:behavioral").unwrap();
    let selected: Vec<_> = specs
        .iter()
        .filter(|spec| coupled.matches(spec))
        .map(|spec| spec.id.as_str())
        .collect();
    if selected != ["coupled"] {
        return Err(format!("unexpected tag selection: {selected:?}").into());
    }
    let provenance = Filter::from_str("provenance:xlsx-calamine").unwrap();
    if !provenance.matches(&specs[0])
        || !provenance.allows_provenance(Provenance::XlsxCalamine)
        || provenance.allows_provenance(Provenance::WorkbookApi)
    {
        return Err("run-time provenance filter failed".into());
    }
    let either = Filter::from_str("family:coupled,independent").unwrap();
    if specs.iter().filter(|spec| either.matches(spec)).count() != 2 {
        return Err("OR-within-dimension filter failed".into());
    }
    Ok(())
}

/// A passing built-in scenario with a tracked defect marked at its last step:
/// that step's expectation fails with "tracked mismatch" when `defect` is set.
fn marked_spec(defect: bool) -> (ScenarioSpec, usize) {
    let mut spec = built_in_registry(200).remove(0);
    let last = spec.script.0.len() - 1;
    if defect {
        spec.expects.push((
            last,
            Expect::Oracle(Arc::new(|_| Err("tracked mismatch".into()))),
        ));
    }
    spec.expected_failures.push(ExpectedFailureSpec {
        mode: FormulaPlaneMode::Off,
        provenance: None,
        failure: FailureFingerprint::expectation(last, "tracked mismatch"),
        reason: "tracked test defect".into(),
    });
    (spec, last)
}
fn run_off(spec: &ScenarioSpec) -> formualizer_testkit::run::RunReport {
    run(
        spec,
        FormulaPlaneMode::Off,
        spec.sizes[0],
        Materializer::workbook_api(
            WorkbookRoute::SetValuesSetFormulas,
            config(FormulaPlaneMode::Off),
        ),
        None,
    )
}
fn expect_unmatched(report: formualizer_testkit::run::RunReport, got: &str) -> Result<(), Failed> {
    match (&report.failure, &report.known_failure) {
        (Some(message), None)
            if message.contains("expected failure did not match")
                && message.contains("tracked mismatch")
                && message.contains(got) =>
        {
            Ok(())
        }
        other => Err(format!("marker did not reject an unrelated failure: {other:?}").into()),
    }
}
fn expected_failure_earlier_failure_fails() -> Result<(), Failed> {
    // An earlier expectation fails.
    let (mut spec, _) = marked_spec(true);
    spec.expects.push((
        2,
        Expect::Oracle(Arc::new(|_| Err("unrelated regression".into()))),
    ));
    expect_unmatched(run_off(&spec), "step 2: unrelated regression")?;
    // An earlier action fails; the marked step keeps its index.
    let (mut spec, last) = marked_spec(true);
    spec.script.0[last - 1] = Step::Custom(Arc::new(|_| Err("action broke".into())));
    expect_unmatched(run_off(&spec), &format!("step {}: action broke", last - 1))
}
fn expected_failure_other_mismatch_fails() -> Result<(), Failed> {
    let (mut spec, last) = marked_spec(false);
    spec.expects.push((
        last,
        Expect::Oracle(Arc::new(|_| Err("different mismatch".into()))),
    ));
    expect_unmatched(run_off(&spec), &format!("step {last}: different mismatch"))
}
fn expected_failure_exact_is_known() -> Result<(), Failed> {
    let (spec, last) = marked_spec(true);
    let report = run_off(&spec);
    match (&report.failure, &report.known_failure) {
        (None, Some(known)) if known.ends_with(&format!("step {last}: tracked mismatch")) => Ok(()),
        other => Err(format!("exact fingerprint was not KNOWN: {other:?}").into()),
    }
}
fn expected_failure_unexpected_pass_fails() -> Result<(), Failed> {
    let (spec, _) = marked_spec(false);
    match run_off(&spec).failure {
        Some(message) if message.contains("expected failure did not occur") => Ok(()),
        other => Err(format!("unexpected pass was accepted: {other:?}").into()),
    }
}
