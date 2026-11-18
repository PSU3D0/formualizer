use super::{FunctionRefEntry, RuntimeArgMeta, SandboxExample};
use anyhow::{Context, Result, bail, ensure};
use formualizer_common::{CellAddress, LiteralValue};
use formualizer_eval::engine::inspect::{SnapshotOptions, SpillRole};
use formualizer_parse::parser::{ASTNode, ASTNodeType, RefView, parse};
use formualizer_workbook::Workbook;
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

const SIGNATURES: &str = include_str!("../function-signatures.txt");
const EXAMPLES: &str = include_str!("../function-examples.txt");
const DIFFERENCES: &str = include_str!("../function-excel-differences.txt");
const DETAILS: &str = include_str!("../function-details.txt");
const SHEET: &str = "Sheet1";
const OUTPUT: u32 = 100;

#[derive(Debug)]
struct Signature<'a> {
    syntax: &'a str,
    slots: Vec<(&'a str, bool)>,
    min: usize,
    max: Option<usize>,
    step: usize,
}

impl Signature<'_> {
    fn allows(&self, arity: usize) -> bool {
        arity >= self.min
            && self.max.is_none_or(|max| arity <= max)
            && (arity - self.min).is_multiple_of(self.step)
    }
}

fn signatures() -> Result<BTreeMap<&'static str, Signature<'static>>> {
    let token = Regex::new(r"\[|\]|\(|\)|,|\.\.\.|[a-z][a-z0-9_]*")?;
    let mut signatures = BTreeMap::new();
    for line in SIGNATURES
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (names, syntax) = line
            .split_once(':')
            .context("expected names and signature")?;
        let syntax = syntax.trim();
        let mut depth = 0;
        let mut matched_bytes = 0;
        let mut slots = Vec::new();
        for item in token.find_iter(syntax) {
            matched_bytes += item.len();
            match item.as_str() {
                "[" => depth += 1,
                "]" => {
                    ensure!(depth > 0, "unmatched bracket in {names}");
                    depth -= 1;
                }
                "(" | ")" | "," | "..." => (),
                name => {
                    ensure!(
                        !name
                            .strip_prefix("arg")
                            .is_some_and(|tail| tail.chars().all(|c| c.is_ascii_digit())),
                        "placeholder in the signature for {names}: {name}"
                    );
                    ensure!(
                        !slots.iter().any(|(slot, _)| *slot == name),
                        "duplicate slot in {names}: {name}"
                    );
                    slots.push((name, depth == 0));
                }
            }
        }
        ensure!(
            depth == 0
                && matched_bytes == syntax.bytes().filter(|b| !b.is_ascii_whitespace()).count(),
            "invalid signature for {names}: {syntax}"
        );
        let min = slots.iter().filter(|(_, required)| *required).count();
        let max = (!syntax.contains("...")).then_some(slots.len());
        for name in names.split_whitespace() {
            let step = match name {
                "LET" | "IFS" | "SUMIFS" | "AVERAGEIFS" | "COUNTIFS" | "MINIFS" | "MAXIFS" => 2,
                _ => 1,
            };
            ensure!(
                signatures
                    .insert(
                        name,
                        Signature {
                            syntax,
                            slots: slots.clone(),
                            min,
                            max,
                            step
                        }
                    )
                    .is_none(),
                "duplicate signature for {name}"
            );
        }
    }
    Ok(signatures)
}

fn empty_arg() -> RuntimeArgMeta {
    RuntimeArgMeta {
        name: String::new(),
        kinds: vec!["value".to_string()],
        required: false,
        shape: "scalar".to_string(),
        by_ref: false,
        coercion: "None".to_string(),
        max: None,
        repeating: None,
        has_default: false,
        default: None,
    }
}

fn set_signature(entry: &mut FunctionRefEntry, signature: &Signature<'_>) -> Result<()> {
    let name = &entry.function_name;
    ensure!(
        entry.min_args == Some(signature.min)
            || matches!(name.as_str(), "SUM" | "COUNT" | "CEILING" | "FLOOR")
                && entry.min_args == Some(signature.min - 1),
        "{name}: documented minimum {} disagrees with engine minimum {:?}",
        signature.min,
        entry.min_args
    );

    // Most central schemas enforce their own maximum, except single-slot
    // repeated schemas and implementations that validate in dispatch.
    if entry.args.len() > 1
        && !matches!(name.as_str(), "CHOOSE" | "GROUPBY" | "PIVOTBY" | "SORTBY")
        && !entry.args.iter().any(|arg| arg.repeating.is_some())
    {
        ensure!(
            signature.max == Some(entry.args.len()),
            "{name}: signature maximum {:?} disagrees with the {}-slot schema",
            signature.max,
            entry.args.len()
        );
    }

    let schema = std::mem::take(&mut entry.args);
    let repeated = (schema.len() == 1 && signature.slots.len() > 1).then(|| &schema[0]);
    for (index, (slot, required)) in signature.slots.iter().enumerate() {
        let mut arg = schema
            .get(index)
            .or(repeated)
            .cloned()
            .unwrap_or_else(empty_arg);
        arg.name = (*slot).to_string();
        arg.required = *required;
        entry.args.push(arg);
    }
    entry.min_args = Some(signature.min);
    entry.max_args = signature.max;
    entry.variadic = Some(signature.max.is_none());
    entry.signature = Some(if matches!(name.as_str(), "TRUE" | "FALSE") {
        format!("={name}")
    } else {
        format!("={name}({})", signature.syntax)
    });
    entry.arg_schema = Some(
        entry.args.iter().map(|arg| format!(
            "{}{{kinds={},required={},shape={},by_ref={},coercion={},max={},repeating={},default={}}}",
            arg.name,
            arg.kinds.join("|"),
            arg.required,
            arg.shape,
            arg.by_ref,
            arg.coercion,
            arg.max.as_deref().unwrap_or("None"),
            arg.repeating.as_deref().unwrap_or("None"),
            arg.has_default,
        )).collect::<Vec<_>>().join("; ")
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Case {
    Ordinary,
    Alternate,
    ExpectedEqual,
    Boundary,
    Error,
}

#[derive(Clone, Debug)]
struct AuthoredExample {
    case: Case,
    example: SandboxExample,
}

fn authored_examples() -> Result<BTreeMap<String, Vec<AuthoredExample>>> {
    let mut examples = BTreeMap::<String, Vec<AuthoredExample>>::new();
    let mut names = Vec::new();
    let mut grid = BTreeMap::new();
    for (line_number, line) in EXAMPLES.lines().enumerate() {
        let location = || format!("function-examples.txt:{}", line_number + 1);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(header) = line.strip_suffix(':') {
            names = header.split_whitespace().collect();
            grid.clear();
            continue;
        }
        if let Some(cells) = line.strip_prefix("grid ") {
            grid = serde_json::from_str(cells).with_context(location)?;
            continue;
        }
        let mut fields = line.splitn(3, " | ");
        let case = match fields.next() {
            Some("ordinary") => Case::Ordinary,
            Some("alternate") => Case::Alternate,
            Some("expected-equal") => Case::ExpectedEqual,
            Some("boundary") => Case::Boundary,
            Some("error") => Case::Error,
            _ => bail!("{}: invalid example case", location()),
        };
        let formula = fields.next().with_context(location)?;
        let title = fields.next().with_context(location)?;
        ensure!(
            !names.is_empty() && !title.is_empty() && formula.starts_with('='),
            "{}",
            location()
        );
        for name in &names {
            examples
                .entry((*name).to_string())
                .or_default()
                .push(AuthoredExample {
                    case,
                    example: SandboxExample {
                        formula: formula.replace("<FUNCTION>", name),
                        title: Some(title.to_string()),
                        expected: None,
                        grid: grid.clone(),
                    },
                });
        }
    }
    Ok(examples)
}

fn check_calls(
    node: &ASTNode,
    function: &str,
    signatures: &BTreeMap<&str, Signature<'_>>,
    supplied: &mut BTreeSet<usize>,
    found: &mut bool,
    volatile: &mut bool,
) -> Result<()> {
    match &node.node_type {
        ASTNodeType::Literal(LiteralValue::Boolean(value)) => {
            *found |= if *value {
                function == "TRUE"
            } else {
                function == "FALSE"
            };
        }
        ASTNodeType::Function { name, args } => {
            if let Some(signature) = signatures.get(name.as_str()) {
                ensure!(
                    signature.allows(args.len()),
                    "{name} has {} arguments",
                    args.len()
                );
            }
            if name.eq_ignore_ascii_case(function) {
                *found = true;
                supplied.extend(args.iter().enumerate().filter_map(|(index, arg)| {
                    (!matches!(arg.node_type, ASTNodeType::Omitted)).then_some(index)
                }));
            }
            // These two are volatile for dependency tracking, but given the
            // same workbook inputs they always produce the same result.
            *volatile |= !matches!(name.as_str(), "INDIRECT" | "OFFSET")
                && formualizer_eval::function_registry::get("", name).is_some_and(|f| {
                    f.caps()
                        .contains(formualizer_eval::function::FnCaps::VOLATILE)
                });
            for node in args {
                check_calls(node, function, signatures, supplied, found, volatile)?;
            }
        }
        ASTNodeType::Call { callee, args } => {
            check_calls(callee, function, signatures, supplied, found, volatile)?;
            for node in args {
                check_calls(node, function, signatures, supplied, found, volatile)?;
            }
        }
        ASTNodeType::UnaryOp { expr, .. } => {
            check_calls(expr, function, signatures, supplied, found, volatile)?
        }
        ASTNodeType::BinaryOp { left, right, .. } => {
            check_calls(left, function, signatures, supplied, found, volatile)?;
            check_calls(right, function, signatures, supplied, found, volatile)?;
        }
        ASTNodeType::Array(rows) => {
            for node in rows.iter().flatten() {
                check_calls(node, function, signatures, supplied, found, volatile)?;
            }
        }
        _ => (),
    }
    Ok(())
}

/// Store values in the generated MDX using spreadsheet literal notation so
/// text, formulas, numbers, logicals, and blank cells stay distinct.
pub(super) fn encode_cell(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) if s.starts_with('=') => s.clone(),
        serde_json::Value::String(s) => format!("\"{}\"", s.replace('"', "\"\"")),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        serde_json::Value::Null => "(blank)".to_string(),
        other => other.to_string(),
    }
}

fn example_workbook(grid: &BTreeMap<String, serde_json::Value>) -> Result<Workbook> {
    let mut workbook = Workbook::new();
    workbook.add_sheet(SHEET)?;
    // Keep Sheet1 first and match the browser sandbox's formula location.
    let cell_pattern = Regex::new(r"(?i)^([a-z]+)([1-9][0-9]*)$")?;
    let mut sheets = BTreeSet::from([SHEET]);
    for (address, value) in grid {
        let (sheet, cell) = address.split_once('!').unwrap_or((SHEET, address));
        if sheets.insert(sheet) {
            workbook.add_sheet(sheet)?;
        }
        let position = cell_pattern
            .captures(cell)
            .with_context(|| format!("bad cell: {address}"))?;
        let row = position[2].parse::<u32>()?;
        let col = position[1]
            .to_ascii_uppercase()
            .bytes()
            .fold(0_u32, |col, ch| col * 26 + u32::from(ch - b'A' + 1));
        ensure!(
            sheet != SHEET || (row != OUTPUT || col != OUTPUT),
            "input conflicts with the output cell"
        );
        let scalar = match value {
            serde_json::Value::Null => continue,
            serde_json::Value::String(s) if s.starts_with('=') => {
                parse(s).with_context(|| format!("bad input formula in {address}"))?;
                workbook.set_formula(sheet, row, col, s)?;
                continue;
            }
            serde_json::Value::String(s) => LiteralValue::Text(s.clone()),
            serde_json::Value::Bool(b) => LiteralValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                LiteralValue::Number(n.as_f64().context("invalid number")?)
            }
            _ => bail!("invalid input cell {address}: {value}"),
        };
        workbook.set_value(sheet, row, col, scalar)?;
    }
    Ok(workbook)
}

fn format_number(number: f64) -> String {
    if number == 0.0 {
        return "0".to_string();
    }
    if !number.is_finite() {
        return number.to_string();
    }
    let scientific = format!("{number:.14e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    if !(-6..=14).contains(&exponent) {
        return format!(
            "{}E{exponent:+}",
            mantissa.trim_end_matches('0').trim_end_matches('.')
        );
    }
    let decimals = (14 - exponent) as usize;
    let rounded = format!("{number:.decimals$}");
    if decimals == 0 {
        rounded
    } else {
        rounded
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn complex_display(text: &str) -> String {
    let Some(suffix) = text.chars().last().filter(|c| matches!(c, 'i' | 'j')) else {
        return text
            .parse::<f64>()
            .map_or_else(|_| text.to_string(), format_number);
    };
    let coefficients = &text[..text.len() - 1];
    let split = coefficients
        .as_bytes()
        .windows(2)
        .position(|pair| !matches!(pair[0], b'e' | b'E') && matches!(pair[1], b'+' | b'-'))
        .map(|index| index + 1);
    let (real, imaginary) = split.map_or(("", coefficients), |index| coefficients.split_at(index));
    let real = real
        .parse::<f64>()
        .ok()
        .map_or_else(|| real.to_string(), format_number);
    let imaginary = imaginary.parse::<f64>().map_or_else(
        |_| imaginary.to_string(),
        |n| {
            format!(
                "{}{}",
                if n >= 0.0 && !real.is_empty() {
                    "+"
                } else {
                    ""
                },
                format_number(n)
            )
        },
    );
    format!("{real}{imaginary}{suffix}")
}

fn spreadsheet_value(value: &LiteralValue, complex: bool) -> String {
    match value {
        LiteralValue::Boolean(true) => "TRUE".to_string(),
        LiteralValue::Boolean(false) => "FALSE".to_string(),
        LiteralValue::Error(error) => error.kind.to_string(),
        LiteralValue::Empty => "(blank)".to_string(),
        LiteralValue::Text(text) if complex => complex_display(text),
        LiteralValue::Number(_)
        | LiteralValue::Int(_)
        | LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => format_number(value.as_serial_number().unwrap()),
        LiteralValue::Array(rows) if rows.len() == 1 && rows[0].len() == 1 => {
            spreadsheet_value(&rows[0][0], complex)
        }
        LiteralValue::Array(rows) => {
            let rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| {
                            if let LiteralValue::Text(text) = value {
                                let text = if complex {
                                    complex_display(text)
                                } else {
                                    text.clone()
                                };
                                format!("\"{}\"", text.replace('"', "\"\""))
                            } else {
                                spreadsheet_value(value, complex)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", rows.join("; "))
        }
        _ => value.to_string(),
    }
}

fn address(cell: &str) -> Option<(&str, u32, u32)> {
    let (sheet, cell) = cell.split_once('!').unwrap_or((SHEET, cell));
    let (letters, digits) = cell.split_at(cell.find(|c: char| !c.is_ascii_alphabetic())?);
    let col = letters.bytes().fold(0, |col, ch| {
        col * 26 + u32::from(ch.to_ascii_uppercase() - b'A' + 1)
    });
    Some((sheet, digits.parse().ok()?, col))
}

fn reads_cell(ast: &ASTNode, sheet: &str, row: u32, col: u32) -> bool {
    ast.refs().any(|reference| match reference {
        RefView::Cell {
            sheet: source,
            row: r,
            col: c,
            ..
        } => source.unwrap_or(SHEET) == sheet && (r, c) == (row, col),
        RefView::Range {
            sheet: source,
            start_row,
            end_row,
            start_col,
            end_col,
            ..
        } => {
            source.unwrap_or(SHEET) == sheet
                && (start_row.unwrap_or(1)..=end_row.unwrap_or(u32::MAX)).contains(&row)
                && (start_col.unwrap_or(1)..=end_col.unwrap_or(u32::MAX)).contains(&col)
        }
        _ => true,
    })
}

fn example_inputs(ast: &ASTNode, example: &SandboxExample) -> BTreeMap<String, serde_json::Value> {
    // Dynamic references can read cells the parser cannot identify. A grid
    // formula can also have its own dependencies, so follow those too.
    let mut formulas = vec![ast.clone()];
    let mut inputs = BTreeMap::new();
    while let Some(ast) = formulas.pop() {
        let dynamic = ["INDIRECT(", "OFFSET("]
            .iter()
            .any(|name| example.formula.contains(name));
        for (cell, value) in &example.grid {
            if !inputs.contains_key(cell)
                && (dynamic
                    || address(cell)
                        .is_some_and(|(sheet, row, col)| reads_cell(&ast, sheet, row, col)))
            {
                inputs.insert(cell.clone(), value.clone());
                if let Some(source) = value.as_str().filter(|value| value.starts_with('='))
                    && let Ok(ast) = parse(source)
                {
                    formulas.push(ast);
                }
            }
        }
    }
    inputs
}

fn note_map(text: &'static str) -> Result<BTreeMap<&'static str, &'static str>> {
    let mut notes = BTreeMap::new();
    for line in text
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (names, note) = line
            .split_once(" | ")
            .context("expected function names and note")?;
        for name in names.split_whitespace() {
            ensure!(
                notes.insert(name, note).is_none(),
                "duplicate note for {name}"
            );
        }
    }
    Ok(notes)
}

fn evaluate(
    example: &mut SandboxExample,
    function: &str,
    signatures: &BTreeMap<&str, Signature<'_>>,
    authored: bool,
) -> Result<(String, bool, BTreeSet<usize>)> {
    static BOOLEANS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i)"(?:[^"]|"")*"|\b(TRUE|FALSE)\s*\(\s*\)"#).unwrap());
    example.formula = BOOLEANS
        .replace_all(&example.formula, |captures: &regex::Captures| {
            if let Some(boolean) = captures.get(1) {
                return boolean.as_str().to_ascii_uppercase();
            }
            captures[0].to_string()
        })
        .into_owned();
    let ast = parse(&example.formula)?;
    let mut found = false;
    let mut volatile = false;
    let mut supplied = BTreeSet::new();
    check_calls(
        &ast,
        function,
        signatures,
        &mut supplied,
        &mut found,
        &mut volatile,
    )?;
    example.grid = example_inputs(&ast, example);
    ensure!(!authored || found, "the example does not call {function}");
    for input in example.grid.values().filter_map(|input| input.as_str()) {
        if input.starts_with('=') {
            check_calls(
                &parse(input)?,
                "",
                signatures,
                &mut BTreeSet::new(),
                &mut false,
                &mut volatile,
            )?;
        }
    }
    let mut workbook = example_workbook(&example.grid)?;
    workbook.set_formula(SHEET, OUTPUT, OUTPUT, &example.formula)?;
    workbook.evaluate_all()?;
    let mut value = workbook.evaluate_cell(SHEET, OUTPUT, OUTPUT)?;
    let engine = workbook.engine();
    let output = CellAddress::new(SHEET, OUTPUT, OUTPUT)?;
    if let Some(SpillRole::Anchor { extent }) = engine
        .inspect_cell(&output, &SnapshotOptions::default())?
        .cell
        .spill
    {
        value = LiteralValue::Array(engine.get_range_values(
            &extent.sheet,
            extent.start_row,
            extent.start_col,
            extent.end_row,
            extent.end_col,
        ));
    }
    let is_error = matches!(value, LiteralValue::Error(_));
    if volatile && !is_error {
        let direct_function = match &ast.node_type {
            ASTNodeType::Function { name, .. } => name.as_str(),
            _ => "",
        };
        let shape = match direct_function {
            "NOW" => "Date and time serial number".to_string(),
            "TODAY" => "Date serial number".to_string(),
            "RAND" => "Decimal from 0 up to 1".to_string(),
            "RANDBETWEEN" => "Integer in the requested range".to_string(),
            "RANDARRAY" => match &value {
                LiteralValue::Array(rows) => format!(
                    "{} × {} array of random numbers",
                    rows.len(),
                    rows.first().map_or(0, Vec::len)
                ),
                _ => "Random number".to_string(),
            },
            _ => match value {
                LiteralValue::Int(_) | LiteralValue::Number(_) => "Number".to_string(),
                LiteralValue::Text(_) => "Text".to_string(),
                LiteralValue::Boolean(_) => "Logical value".to_string(),
                LiteralValue::Date(_) => "Date".to_string(),
                LiteralValue::DateTime(_) => "Date and time".to_string(),
                other => bail!("unexpected shape for a volatile example: {other:?}"),
            },
        };
        return Ok((shape, false, supplied));
    }
    let complex = function == "COMPLEX" || function.starts_with("IM") && function != "IMPLICIT";
    Ok((spreadsheet_value(&value, complex), is_error, supplied))
}

pub(super) fn prepare(entries: &mut [FunctionRefEntry], filtered: bool) -> Result<()> {
    let signatures = signatures()?;
    let mut examples = authored_examples()?;
    let differences = note_map(DIFFERENCES)?;
    let details = note_map(DETAILS)?;
    let mut errors = Vec::new();
    for entry in entries {
        let name = entry.function_name.clone();
        let signature = signatures
            .get(name.as_str())
            .with_context(|| format!("missing signature for {name}"))?;
        if let Err(error) = set_signature(entry, signature) {
            errors.push(error.to_string());
        }
        entry.excel_differences = differences.get(name.as_str()).copied();
        entry.function_detail = details.get(name.as_str()).copied();
        let authored = examples.remove(&name).unwrap_or_default();
        let legacy = std::mem::take(&mut entry.sandboxes);
        let mut seen = BTreeSet::new();
        let mut cases = BTreeSet::new();
        let mut covered = BTreeSet::new();
        let mut ordinary = None;
        for AuthoredExample { case, mut example } in authored {
            match evaluate(&mut example, &name, &signatures, true) {
                Ok((result, is_error, supplied)) if is_error == (case == Case::Error) => {
                    if case == Case::Ordinary {
                        ordinary = Some(result.clone());
                    }
                    if case == Case::Alternate && ordinary.as_ref() == Some(&result) {
                        errors.push(format!(
                            "{name}: alternate equals ordinary: {}",
                            example.formula
                        ));
                    }
                    if case == Case::ExpectedEqual && ordinary.as_ref() != Some(&result) {
                        errors.push(format!(
                            "{name}: expected the ordinary result: {}",
                            example.formula
                        ));
                    }
                    cases.insert(if case == Case::ExpectedEqual {
                        Case::Alternate
                    } else {
                        case
                    });
                    if !is_error {
                        covered.extend(supplied);
                    }
                    example.expected = Some(result);
                    if seen.insert((
                        parse(&example.formula)?.fingerprint(),
                        serde_json::to_string(&example.grid)?,
                    )) {
                        entry.sandboxes.push(example);
                    } else {
                        errors.push(format!(
                            "{name}: duplicate authored example: {}",
                            example.formula
                        ));
                    }
                }
                Ok((result, _, _)) => errors.push(format!(
                    "{name} {case:?} {} returned {result}",
                    example.formula
                )),
                Err(error) => errors.push(format!("{name} {}: {error:#}", example.formula)),
            }
        }
        for mut example in legacy {
            if !retain_legacy(&name, &example) {
                continue;
            }
            match evaluate(&mut example, &name, &signatures, false) {
                Ok((result, is_error, supplied)) => {
                    if !is_error {
                        covered.extend(supplied);
                    }
                    example.expected = Some(result);
                    if seen.insert((
                        parse(&example.formula)?.fingerprint(),
                        serde_json::to_string(&example.grid)?,
                    )) {
                        entry.sandboxes.push(example);
                    }
                }
                Err(error) => errors.push(format!("{name} {}: {error:#}", example.formula)),
            }
        }
        for case in [Case::Ordinary, Case::Alternate, Case::Boundary] {
            if !cases.contains(&case) {
                errors.push(format!("{name}: missing a verified {case:?} example"));
            }
        }
        for (index, (slot, required)) in signature.slots.iter().enumerate() {
            // The validated filter position always refuses at runtime. Keep it
            // in the signature so the later supported positional options line up.
            let unsupported =
                slot == &"filter_array" && matches!(name.as_str(), "GROUPBY" | "PIVOTBY");
            if !required && !unsupported && !covered.contains(&index) {
                errors.push(format!("{name}: optional slot {slot} is uncovered"));
            }
        }
    }
    if !filtered {
        for name in examples.keys() {
            errors.push(format!("unused example group for {name}"));
        }
    }
    if !errors.is_empty() {
        bail!("function reference content errors:\n{}", errors.join("\n"));
    }
    Ok(())
}

fn retain_legacy(name: &str, example: &SandboxExample) -> bool {
    let formula = example.formula.replace(' ', "");
    match name {
        "COUNTBLANK" | "ISFORMULA" => !example.grid.is_empty(),
        "COUNTIF" | "COUNTIFS" | "SUMIF" | "SUMIFS" | "AVERAGEIF" | "AVERAGEIFS" => {
            !formula.contains('{') && !formula.contains("INDIRECT(\"invalid!\")")
        }
        "MAX" | "MIN" | "AVERAGE" | "AVERAGEA" | "MAXA" | "MINA" | "STDEVA" | "STDEVPA"
        | "VARA" | "VARPA" => !formula.contains("\"x\""),
        "LOG" => formula != "=LOG(100,1)",
        "NPV" => !formula.starts_with("=NPV(-1,"),
        "SEC" => formula != "=SEC(PI()/2)",
        "CHISQ.TEST" => !formula.contains(";0,15}"),
        "SUM" | "COUNT" => formula != format!("={name}()"),
        _ => true,
    }
}

// Compare only rendered result fields. Formula, caption and grid drift still
// require regeneration. Transcendental functions can differ in the final
// displayed digit when evaluated by different platform maths libraries.
pub(super) fn same_displayed_page(current: &str, updated: &str) -> bool {
    let result_tokens =
        Regex::new(r#""(?:[^"]|"")*"|[+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?"#).unwrap();
    let complex = updated
        .lines()
        .nth(1)
        .is_some_and(|title| title == "title: \"COMPLEX\"" || title.starts_with("title: \"IM"));
    let mut before = current.lines();
    let mut after = updated.lines();
    loop {
        match (before.next(), after.next()) {
            (None, None) => return true,
            (Some(left), Some(right)) if left == right => {}
            (Some(left), Some(right)) => {
                let value = |line: &str| -> Option<String> {
                    serde_json::from_str(
                        line.trim_start()
                            .strip_prefix("expected={")?
                            .strip_suffix('}')?,
                    )
                    .ok()
                };
                let (Some(left), Some(right)) = (value(left), value(right)) else {
                    return false;
                };
                let is_array = left.starts_with('{') && right.starts_with('{');
                let both_numeric = left.parse::<f64>().is_ok() && right.parse::<f64>().is_ok();
                if !(is_array || both_numeric || complex) {
                    return false;
                }
                let mut left_at = 0;
                let mut right_at = 0;
                let mut right_tokens = result_tokens.find_iter(&right);
                for a in result_tokens.find_iter(&left) {
                    let Some(b) = right_tokens.next() else {
                        return false;
                    };
                    if left[left_at..a.start()] != right[right_at..b.start()] {
                        return false;
                    }
                    if a.as_str() != b.as_str() {
                        let (Ok(a), Ok(b)) = (a.as_str().parse::<f64>(), b.as_str().parse::<f64>())
                        else {
                            return false;
                        };
                        if !a.is_finite()
                            || !b.is_finite()
                            || (a - b).abs() > 2e-15 * a.abs().max(b.abs())
                        {
                            return false;
                        }
                    }
                    left_at = a.end();
                    right_at = b.end();
                }
                if right_tokens.next().is_some() || left[left_at..] != right[right_at..] {
                    return false;
                }
            }
            _ => return false,
        }
    }
}
