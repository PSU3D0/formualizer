use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_testkit::scenario::ladder::{by_rows_or_class, covering_set};
use formualizer_testkit::{
    materialize::WorkbookRoute,
    run::{Materializer, Recorder, run},
    scenario::{Filter, Provenance, ScenarioSpec, StructureExpect, built_in_registry},
};
use formualizer_workbook::WorkbookConfig;
use libtest_mimic::{Arguments, Failed, Trial};
use std::{env, str::FromStr};

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
    if filter.is_none_or(|filter| filter.allows_provenance(Provenance::Xlsx)) {
        let path = env::temp_dir().join(format!("fz-scenario-{}-{mode:?}.xlsx", spec.id));
        let xlsx = Materializer::xlsx(path, config(mode));
        run(spec, mode, size, xlsx, recorder.as_ref())
            .into_result()
            .map_err(Failed::from)?;
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
    trials.push(Trial::test(
        "framework.wrong-structure-golden",
        wrong_structure_golden,
    ));
    trials.push(Trial::test(
        "framework.structure-goldens",
        structure_goldens,
    ));
    trials.push(Trial::test("framework.parity", parity_expectation));
    trials.push(Trial::test("framework.filters-by-tag", filters_by_tag));
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
    let provenance = Filter::from_str("provenance:xlsx").unwrap();
    if !provenance.matches(&specs[0])
        || !provenance.allows_provenance(Provenance::Xlsx)
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
