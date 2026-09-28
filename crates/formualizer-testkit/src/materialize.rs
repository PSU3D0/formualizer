//! Materializers for [`crate::shape::Shape`].
#[cfg(any(feature = "xlsx", feature = "workbook"))]
use crate::shape::{Cell, Literal};
use crate::shape::{Shape, ShapeError};
use std::{
    fmt,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum MaterializeError {
    Shape(ShapeError),
    Unsupported(&'static str),
    Backend(String),
}
impl fmt::Display for MaterializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(e) => e.fmt(f),
            Self::Unsupported(s) => write!(f, "unsupported materializer: {s}"),
            Self::Backend(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for MaterializeError {}
impl From<ShapeError> for MaterializeError {
    fn from(e: ShapeError) -> Self {
        Self::Shape(e)
    }
}
#[cfg(feature = "workbook")]
#[allow(clippy::large_enum_variant)]
pub enum Artifact {
    Xlsx(PathBuf),
    Workbook(formualizer_workbook::Workbook),
}
#[cfg(not(feature = "workbook"))]
pub enum Artifact {
    Xlsx(PathBuf),
}
pub trait Materialize {
    fn materialize(&mut self, shape: &Shape) -> Result<Artifact, MaterializeError>;
}
pub type AfterXlsxHook = Box<dyn Fn(&Path) + Send + Sync>;
#[cfg(feature = "workbook")]
pub type AfterWorkbookHook = Box<dyn FnMut(&mut formualizer_workbook::Workbook) + Send>;
#[cfg(feature = "workbook")]
#[derive(Default)]
pub struct Hooks {
    pub after_xlsx: Vec<AfterXlsxHook>,
    pub after_workbook: Vec<AfterWorkbookHook>,
}
#[cfg(not(feature = "workbook"))]
#[derive(Default)]
pub struct Hooks {
    pub after_xlsx: Vec<AfterXlsxHook>,
}
#[cfg(feature = "xlsx")]
pub struct Xlsx {
    pub path: PathBuf,
    pub hooks: Hooks,
}
#[cfg(feature = "xlsx")]
impl Xlsx {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            hooks: Hooks::default(),
        }
    }
}
#[cfg(feature = "xlsx")]
impl Materialize for Xlsx {
    fn materialize(&mut self, shape: &Shape) -> Result<Artifact, MaterializeError> {
        if let Some(p) = self.path.parent() {
            std::fs::create_dir_all(p).map_err(|e| MaterializeError::Backend(e.to_string()))?
        }
        let mut wb = umya_spreadsheet::new_file();
        for (index, sheet) in shape.sheets.iter().enumerate() {
            if index > 0 {
                wb.new_sheet(&sheet.name)
                    .map_err(|e| MaterializeError::Backend(e.to_string()))?;
            } else if sheet.name != "Sheet1" {
                wb.get_sheet_by_name_mut("Sheet1")
                    .expect("Sheet1")
                    .set_name(&sheet.name);
            }
        }
        for c in shape.render()? {
            let s = wb
                .get_sheet_by_name_mut(&c.sheet)
                .ok_or_else(|| MaterializeError::Backend("missing sheet".into()))?;
            let cell = s.get_cell_mut((c.col, c.row));
            match c.cell {
                Cell::Formula(f) => {
                    cell.set_formula(f.trim_start_matches('='));
                }
                Cell::Value(Literal::Number(n)) => {
                    cell.set_value_number(n);
                }
                Cell::Value(Literal::Text(t)) => {
                    cell.set_value_string(t);
                }
                Cell::Value(Literal::Bool(b)) => {
                    cell.set_value_bool(b);
                }
                Cell::Value(Literal::Empty) => {}
            }
        }
        umya_spreadsheet::writer::xlsx::write(&wb, &self.path)
            .map_err(|e| MaterializeError::Backend(e.to_string()))?;
        for hook in &self.hooks.after_xlsx {
            hook(&self.path)
        }
        Ok(Artifact::Xlsx(self.path.clone()))
    }
}
#[cfg(feature = "workbook")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkbookRoute {
    #[default]
    SetValuesSetFormulas,
    WriteRange,
    PerCell,
}
#[cfg(feature = "workbook")]
pub struct WorkbookApi {
    pub route: WorkbookRoute,
    pub config: formualizer_workbook::WorkbookConfig,
    pub hooks: Hooks,
}
#[cfg(feature = "workbook")]
impl WorkbookApi {
    pub fn new(config: formualizer_workbook::WorkbookConfig) -> Self {
        Self {
            route: WorkbookRoute::default(),
            config,
            hooks: Hooks::default(),
        }
    }
    pub fn route(mut self, route: WorkbookRoute) -> Self {
        self.route = route;
        self
    }
}
#[cfg(feature = "workbook")]
impl Materialize for WorkbookApi {
    fn materialize(&mut self, shape: &Shape) -> Result<Artifact, MaterializeError> {
        use formualizer_workbook::{CellData, Workbook};
        use std::collections::BTreeMap;
        let mut wb = Workbook::new_with_config(self.config.clone());
        for sheet in &shape.sheets {
            wb.add_sheet(&sheet.name)
                .map_err(|e| MaterializeError::Backend(e.to_string()))?
        }
        let cells = shape.render()?;
        match self.route {
            WorkbookRoute::PerCell => {
                for c in cells {
                    match c.cell {
                        Cell::Formula(f) => wb.set_formula(&c.sheet, c.row, c.col, &f),
                        Cell::Value(v) => wb.set_value(&c.sheet, c.row, c.col, literal(v)),
                    }
                    .map_err(|e| MaterializeError::Backend(e.to_string()))?
                }
            }
            WorkbookRoute::WriteRange => {
                let mut groups: BTreeMap<String, BTreeMap<(u32, u32), CellData>> = BTreeMap::new();
                for c in cells {
                    groups.entry(c.sheet).or_default().insert(
                        (c.row, c.col),
                        match c.cell {
                            Cell::Formula(f) => CellData::from_formula(f),
                            Cell::Value(v) => CellData::from_value(literal(v)),
                        },
                    );
                }
                for (s, c) in groups {
                    wb.write_range(&s, (1, 1), c)
                        .map_err(|e| MaterializeError::Backend(e.to_string()))?
                }
            }
            WorkbookRoute::SetValuesSetFormulas => {
                batch_write(&mut wb, cells)?;
            }
        }
        for hook in &mut self.hooks.after_workbook {
            hook(&mut wb)
        }
        Ok(Artifact::Workbook(wb))
    }
}
#[cfg(feature = "workbook")]
fn batch_write(
    wb: &mut formualizer_workbook::Workbook,
    cells: Vec<crate::shape::RenderedCell>,
) -> Result<(), MaterializeError> {
    use std::collections::BTreeMap;
    #[derive(Clone)]
    enum Entry {
        Value(formualizer_workbook::LiteralValue),
        Formula(String),
    }
    let mut columns: BTreeMap<(String, u32), BTreeMap<u32, Entry>> = BTreeMap::new();
    for cell in cells {
        let entry = match cell.cell {
            Cell::Value(value) => Entry::Value(literal(value)),
            Cell::Formula(formula) => Entry::Formula(formula),
        };
        columns
            .entry((cell.sheet, cell.col))
            .or_default()
            .insert(cell.row, entry);
    }
    for ((sheet, col), entries) in columns {
        let mut run: Vec<(u32, Entry)> = Vec::new();
        let flush = |wb: &mut formualizer_workbook::Workbook,
                     sheet: &str,
                     col: u32,
                     run: &mut Vec<(u32, Entry)>|
         -> Result<(), MaterializeError> {
            if run.is_empty() {
                return Ok(());
            }
            let start = run[0].0;
            match &run[0].1 {
                Entry::Value(_) => {
                    let rows: Vec<Vec<formualizer_workbook::LiteralValue>> = run
                        .drain(..)
                        .map(|(_, entry)| match entry {
                            Entry::Value(v) => vec![v],
                            Entry::Formula(_) => unreachable!(),
                        })
                        .collect();
                    wb.set_values(sheet, start, col, &rows)
                        .map_err(|e| MaterializeError::Backend(e.to_string()))
                }
                Entry::Formula(_) => {
                    let rows: Vec<Vec<String>> = run
                        .drain(..)
                        .map(|(_, entry)| match entry {
                            Entry::Formula(v) => vec![v],
                            Entry::Value(_) => unreachable!(),
                        })
                        .collect();
                    wb.set_formulas(sheet, start, col, &rows)
                        .map_err(|e| MaterializeError::Backend(e.to_string()))
                }
            }
        };
        for (row, entry) in entries {
            let same_kind = run.first().is_none_or(|(_, previous)| {
                matches!(
                    (previous, &entry),
                    (Entry::Value(_), Entry::Value(_)) | (Entry::Formula(_), Entry::Formula(_))
                )
            });
            let contiguous = run.last().is_none_or(|(previous, _)| row == *previous + 1);
            if !same_kind || !contiguous {
                flush(wb, &sheet, col, &mut run)?;
            }
            run.push((row, entry));
        }
        flush(wb, &sheet, col, &mut run)?;
    }
    Ok(())
}
#[cfg(feature = "workbook")]
fn literal(v: Literal) -> formualizer_workbook::LiteralValue {
    match v {
        Literal::Number(n) => formualizer_workbook::LiteralValue::Number(n),
        Literal::Text(s) => formualizer_workbook::LiteralValue::Text(s),
        Literal::Bool(b) => formualizer_workbook::LiteralValue::Boolean(b),
        Literal::Empty => formualizer_workbook::LiteralValue::Empty,
    }
}
pub struct EngineDirect;
impl Materialize for EngineDirect {
    fn materialize(&mut self, _: &Shape) -> Result<Artifact, MaterializeError> {
        Err(MaterializeError::Unsupported("EngineDirect"))
    }
}
