//! Engine-independent descriptions of spreadsheet fixtures.
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::RangeInclusive,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scale {
    pub rows: u32,
    pub cols: u32,
}
impl Scale {
    pub const fn rows(rows: u32) -> Self {
        Self { rows, cols: 1 }
    }
    pub const fn cols(cols: u32) -> Self {
        Self { rows: 1, cols }
    }
    pub const fn new(rows: u32, cols: u32) -> Self {
        Self { rows, cols }
    }
}
/// A scale-relative endpoint for a range axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Extent {
    Abs(u32),
    Rows,
    Cols,
    RowsMinus(u32),
    ColsMinus(u32),
    RowsPlus(u32),
    ColsPlus(u32),
}
impl From<u32> for Extent {
    fn from(value: u32) -> Self {
        Self::Abs(value)
    }
}
impl Extent {
    fn resolve(self, scale: Scale) -> Result<u32, ShapeError> {
        let n = match self {
            Self::Abs(n) => n,
            Self::Rows => scale.rows,
            Self::Cols => scale.cols,
            Self::RowsMinus(k) => scale
                .rows
                .checked_sub(k)
                .ok_or_else(|| ShapeError("row extent outside grid".into()))?,
            Self::ColsMinus(k) => scale
                .cols
                .checked_sub(k)
                .ok_or_else(|| ShapeError("column extent outside grid".into()))?,
            Self::RowsPlus(k) => scale
                .rows
                .checked_add(k)
                .ok_or_else(|| ShapeError("row extent overflow".into()))?,
            Self::ColsPlus(k) => scale
                .cols
                .checked_add(k)
                .ok_or_else(|| ShapeError("column extent overflow".into()))?,
        };
        if n == 0 {
            Err(ShapeError("range extent outside grid".into()))
        } else {
            Ok(n)
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum RangeKind {
    Col {
        col: u32,
        rows: RangeInclusive<Extent>,
    },
    Row {
        row: Extent,
        cols: RangeInclusive<Extent>,
    },
    Rect {
        rows: RangeInclusive<Extent>,
        cols: RangeInclusive<Extent>,
    },
    Cells(Vec<(u32, u32)>),
}
/// An orientation-neutral, lazily-resolved cell range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range {
    kind: RangeKind,
}
impl Range {
    pub fn col<T: Into<Extent> + Clone>(column: impl AsRef<str>, rows: RangeInclusive<T>) -> Self {
        Self {
            kind: RangeKind::Col {
                col: column_index(column.as_ref()).expect("valid column"),
                rows: rows.start().clone().into()..=rows.end().clone().into(),
            },
        }
    }
    pub fn row<T: Into<Extent> + Clone>(row: T, cols: RangeInclusive<T>) -> Self {
        Self {
            kind: RangeKind::Row {
                row: row.into(),
                cols: cols.start().clone().into()..=cols.end().clone().into(),
            },
        }
    }
    pub fn rect<T: Into<Extent> + Clone>(rows: RangeInclusive<T>, cols: RangeInclusive<T>) -> Self {
        Self {
            kind: RangeKind::Rect {
                rows: rows.start().to_owned().into()..=rows.end().to_owned().into(),
                cols: cols.start().to_owned().into()..=cols.end().to_owned().into(),
            },
        }
    }
    pub fn cells(cells: impl IntoIterator<Item = (u32, u32)>) -> Self {
        Self {
            kind: RangeKind::Cells(cells.into_iter().collect()),
        }
    }
    pub fn resolve(&self, scale: Scale) -> Result<Vec<(u32, u32)>, ShapeError> {
        match &self.kind {
            RangeKind::Cells(c) => Ok(c.clone()),
            RangeKind::Col { col, rows } => Ok((rows.start().resolve(scale)?
                ..=rows.end().resolve(scale)?)
                .map(|r| (r, *col))
                .collect()),
            RangeKind::Row { row, cols } => {
                let row = row.resolve(scale)?;
                Ok((cols.start().resolve(scale)?..=cols.end().resolve(scale)?)
                    .map(|c| (row, c))
                    .collect())
            }
            RangeKind::Rect { rows, cols } => {
                let (row_start, row_end) =
                    (rows.start().resolve(scale)?, rows.end().resolve(scale)?);
                let (col_start, col_end) =
                    (cols.start().resolve(scale)?, cols.end().resolve(scale)?);
                Ok((row_start..=row_end)
                    .flat_map(|r| (col_start..=col_end).map(move |c| (r, c)))
                    .collect())
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Value(Literal),
    Formula(String),
}
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Number(f64),
    Text(String),
    Bool(bool),
    Empty,
}
impl Cell {
    pub fn formula(s: impl Into<String>) -> Self {
        Self::Formula(s.into())
    }
    pub fn number(n: f64) -> Self {
        Self::Value(Literal::Number(n))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    PrimaryInput,
    PrimaryFamily,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShapeError(pub String);
impl fmt::Display for ShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ShapeError {}
pub type Generator = Arc<dyn Fn(u32, u32, &Scale) -> Cell + Send + Sync>;
pub mod r#gen {
    use super::*;
    pub fn constant(cell: Cell) -> Generator {
        Arc::new(move |_, _, _| cell.clone())
    }
    pub fn index_f64() -> Generator {
        Arc::new(|r, _, _| Cell::number(r as f64))
    }
    pub fn seeded<F>(seed: u64, f: F) -> Generator
    where
        F: Fn(u64, u32, u32) -> Cell + Send + Sync + 'static,
    {
        Arc::new(move |r, c, _| f(seed, r, c))
    }
    pub fn text(value: impl Into<String>) -> Generator {
        constant(Cell::Value(Literal::Text(value.into())))
    }
    pub fn mixed() -> Generator {
        Arc::new(|r, c, _| {
            if (r + c) % 2 == 0 {
                Cell::number(r as f64)
            } else {
                Cell::Value(Literal::Text(format!("{r}:{c}")))
            }
        })
    }
}
#[derive(Clone)]
pub struct Family {
    pub name: String,
    pub range: Range,
    pub formula: String,
    boundaries: BTreeMap<(u32, u32), String>,
    except: Vec<Range>,
    overrides: BTreeMap<(u32, u32), Cell>,
    gap_every: Option<u32>,
    blocks: Option<(u32, u32)>,
}
impl Family {
    fn selected(&self, scale: Scale) -> Result<Vec<(u32, u32)>, ShapeError> {
        let excluded: BTreeSet<_> = self
            .except
            .iter()
            .map(|r| r.resolve(scale))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(self
            .range
            .resolve(scale)?
            .into_iter()
            .enumerate()
            .filter_map(|(i, p)| {
                let include = self
                    .gap_every
                    .is_none_or(|n| n == 0 || (i + 1) % n as usize != 0)
                    && self.blocks.is_none_or(|(size, gap)| {
                        let cycle = (size + gap) as usize;
                        cycle == 0 || i % cycle < size as usize
                    });
                include.then_some(p)
            })
            .filter(|p| !excluded.contains(p))
            .collect())
    }
}
pub struct FamilyBuilder<'a> {
    sheet: &'a mut SheetShape,
    index: usize,
}
impl FamilyBuilder<'_> {
    fn family_mut(&mut self) -> &mut Family {
        &mut self.sheet.families[self.index]
    }
    pub fn boundary(mut self, cell: impl AsRef<str>, formula: impl Into<String>) -> Self {
        let p = a1(cell.as_ref()).expect("valid cell");
        self.family_mut().boundaries.insert(p, formula.into());
        self
    }
    pub fn except(mut self, cells: Range) -> Self {
        self.family_mut().except.push(cells);
        self
    }
    pub fn override_cell(mut self, cell: impl AsRef<str>, value: Cell) -> Self {
        let p = a1(cell.as_ref()).expect("valid cell");
        self.family_mut().overrides.insert(p, value);
        self
    }
    pub fn gap_every(mut self, k: u32) -> Self {
        self.family_mut().gap_every = Some(k);
        self
    }
    pub fn blocks(mut self, size: u32, gap: u32) -> Self {
        self.family_mut().blocks = Some((size, gap));
        self
    }
}
#[derive(Clone)]
pub struct ValueRange {
    pub range: Range,
    pub generator: Generator,
}
#[derive(Clone)]
pub struct SheetShape {
    pub name: String,
    pub values: Vec<ValueRange>,
    pub families: Vec<Family>,
    pub roles: BTreeMap<String, Role>,
}
impl SheetShape {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            values: vec![],
            families: vec![],
            roles: BTreeMap::new(),
        }
    }
    pub fn values(&mut self, range: Range, generator: Generator) -> &mut Self {
        self.values.push(ValueRange { range, generator });
        self
    }
    pub fn family(
        &mut self,
        name: impl Into<String>,
        range: Range,
        formula: impl Into<String>,
    ) -> FamilyBuilder<'_> {
        self.families.push(Family {
            name: name.into(),
            range,
            formula: formula.into(),
            boundaries: BTreeMap::new(),
            except: vec![],
            overrides: BTreeMap::new(),
            gap_every: None,
            blocks: None,
        });
        let index = self.families.len() - 1;
        FamilyBuilder { sheet: self, index }
    }
    pub fn role(&mut self, name: impl Into<String>, role: Role) -> &mut Self {
        self.roles.insert(name.into(), role);
        self
    }
}
#[derive(Clone, Default)]
pub struct Shape {
    pub scale: Scale,
    pub sheets: Vec<SheetShape>,
}
impl Shape {
    pub fn new() -> Self {
        Self {
            scale: Scale::new(1, 1),
            sheets: vec![],
        }
    }
    pub fn scale(mut self, scale: Scale) -> Self {
        self.scale = scale;
        self
    }
    pub fn sheet(mut self, name: impl Into<String>, build: impl FnOnce(&mut SheetShape)) -> Self {
        let mut s = SheetShape::new(name);
        build(&mut s);
        self.sheets.push(s);
        self
    }
    pub fn render(&self) -> Result<Vec<RenderedCell>, ShapeError> {
        let mut out = vec![];
        for sheet in &self.sheets {
            for v in &sheet.values {
                for (r, c) in v.range.resolve(self.scale)? {
                    out.push(RenderedCell {
                        sheet: sheet.name.clone(),
                        row: r,
                        col: c,
                        cell: (v.generator)(r, c, &self.scale),
                    })
                }
            }
            for family in &sheet.families {
                let selected = family.selected(self.scale)?;
                let range = family.range.resolve(self.scale)?;
                let end = match &family.range.kind {
                    RangeKind::Row { .. } => range.iter().map(|(_, c)| *c).max().unwrap_or(0),
                    _ => range.iter().map(|(r, _)| *r).max().unwrap_or(0),
                };
                for (r, c) in selected {
                    let cell = if let Some(x) = family.overrides.get(&(r, c)) {
                        x.clone()
                    } else if let Some(f) = family.boundaries.get(&(r, c)) {
                        Cell::Formula(render_template_with_axis(
                            f,
                            r,
                            c,
                            self.scale,
                            end,
                            matches!(family.range.kind, RangeKind::Row { .. }),
                        )?)
                    } else {
                        Cell::Formula(render_template_with_axis(
                            &family.formula,
                            r,
                            c,
                            self.scale,
                            end,
                            matches!(family.range.kind, RangeKind::Row { .. }),
                        )?)
                    };
                    out.push(RenderedCell {
                        sheet: sheet.name.clone(),
                        row: r,
                        col: c,
                        cell,
                    })
                }
            }
        }
        Ok(out)
    }

    /// Resolve a semantic role to concrete cells at this shape's current scale.
    /// Role names may identify either an A1 cell or a named family.
    pub fn resolve_role(&self, role: Role) -> Result<Vec<(String, u32, u32)>, ShapeError> {
        let mut out = Vec::new();
        for sheet in &self.sheets {
            for (name, assigned) in &sheet.roles {
                if *assigned != role {
                    continue;
                }
                if let Some(family) = sheet.families.iter().find(|family| family.name == *name) {
                    out.extend(
                        family
                            .selected(self.scale)?
                            .into_iter()
                            .map(|(row, col)| (sheet.name.clone(), row, col)),
                    );
                } else {
                    let (row, col) = a1(name).map_err(|_| {
                        ShapeError(format!(
                            "role {role:?} refers to unknown cell or family {name:?}"
                        ))
                    })?;
                    out.push((sheet.name.clone(), row, col));
                }
            }
        }
        if out.is_empty() {
            return Err(ShapeError(format!("shape does not define role {role:?}")));
        }
        Ok(out)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct RenderedCell {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub cell: Cell,
}
pub fn render_template(
    template: &str,
    row: u32,
    col: u32,
    scale: Scale,
    end: u32,
) -> Result<String, ShapeError> {
    render_template_with_axis(template, row, col, scale, end, false)
}
fn render_template_with_axis(
    template: &str,
    row: u32,
    col: u32,
    scale: Scale,
    end: u32,
    end_is_col: bool,
) -> Result<String, ShapeError> {
    let mut result = String::new();
    let mut rest = template;
    while let Some(i) = rest.find('{') {
        result.push_str(&rest[..i]);
        let tail = &rest[i + 1..];
        let Some(j) = tail.find('}') else {
            return Err(ShapeError("unclosed placeholder".into()));
        };
        let token = &tail[..j];
        let val = match token {
            "r" => row.to_string(),
            "c" => column_letter(col)?,
            "n" => scale.rows.to_string(),
            "m" => scale.cols.to_string(),
            "end" => {
                if end_is_col {
                    column_letter(end)?
                } else {
                    end.to_string()
                }
            }
            _ => {
                let (axis, delta) = token.split_at(1);
                let amount: i64 = delta
                    .parse()
                    .map_err(|_| ShapeError(format!("invalid placeholder {{{token}}}")))?;
                let n = if axis == "r" {
                    row as i64 + amount
                } else if axis == "c" {
                    col as i64 + amount
                } else {
                    return Err(ShapeError(format!("invalid placeholder {{{token}}}")));
                };
                if n < 1 {
                    return Err(ShapeError(format!(
                        "placeholder {{{token}}} resolves outside grid"
                    )));
                }
                if axis == "r" {
                    n.to_string()
                } else {
                    column_letter(n as u32)?
                }
            }
        };
        result.push_str(&val);
        rest = &tail[j + 1..]
    }
    result.push_str(rest);
    Ok(result)
}
pub fn column_letter(mut col: u32) -> Result<String, ShapeError> {
    if col == 0 {
        return Err(ShapeError("column zero is outside grid".into()));
    }
    let mut s = String::new();
    while col > 0 {
        col -= 1;
        s.insert(0, (b'A' + (col % 26) as u8) as char);
        col /= 26
    }
    Ok(s)
}
fn column_index(s: &str) -> Result<u32, ShapeError> {
    let mut n = 0;
    for b in s.bytes() {
        if !b.is_ascii_alphabetic() {
            return Err(ShapeError("invalid column".into()));
        }
        n = n * 26 + (b.to_ascii_uppercase() - b'A' + 1) as u32
    }
    if n == 0 {
        Err(ShapeError("empty column".into()))
    } else {
        Ok(n)
    }
}
fn a1(s: &str) -> Result<(u32, u32), ShapeError> {
    let i = s
        .find(|c: char| c.is_ascii_digit())
        .ok_or_else(|| ShapeError("invalid A1 cell".into()))?;
    Ok((
        s[i..]
            .parse()
            .map_err(|_| ShapeError("invalid row".into()))?,
        column_index(&s[..i])?,
    ))
}
