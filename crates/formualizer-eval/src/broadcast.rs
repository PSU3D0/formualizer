use formualizer_common::{ExcelError, ExcelErrorKind};

pub type Shape2D = (usize, usize);

pub fn broadcast_shape(shapes: &[Shape2D]) -> Result<Shape2D, ExcelError> {
    if shapes.is_empty() {
        return Ok((1, 1));
    }
    let mut rows = 1usize;
    let mut cols = 1usize;
    for &(r, c) in shapes {
        rows = broadcast_dim(rows, r)?;
        cols = broadcast_dim(cols, c)?;
    }
    Ok((rows, cols))
}

fn broadcast_dim(a: usize, b: usize) -> Result<usize, ExcelError> {
    if a == b || a == 1 {
        return Ok(b.max(a));
    }
    if b == 1 {
        return Ok(a.max(b));
    }
    Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
        "Incompatible dimensions for broadcasting: {a} vs {b}"
    )))
}

#[inline]
pub fn project_index(idx: (usize, usize), shape: Shape2D) -> (usize, usize) {
    let (r, c) = shape;
    let rr = if r == 1 { 0 } else { idx.0 };
    let cc = if c == 1 { 0 } else { idx.1 };
    (rr, cc)
}
