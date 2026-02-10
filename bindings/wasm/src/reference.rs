use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct Reference {
    sheet: Option<String>,
    row_start: usize,
    col_start: usize,
    row_end: usize,
    col_end: usize,
    row_abs_start: bool,
    col_abs_start: bool,
    row_abs_end: bool,
    col_abs_end: bool,
}

#[wasm_bindgen]
impl Reference {
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sheet: Option<String>,
        row_start: usize,
        col_start: usize,
        row_end: usize,
        col_end: usize,
        row_abs_start: bool,
        col_abs_start: bool,
        row_abs_end: bool,
        col_abs_end: bool,
    ) -> Reference {
        Reference {
            sheet,
            row_start,
            col_start,
            row_end,
            col_end,
            row_abs_start,
            col_abs_start,
            row_abs_end,
            col_abs_end,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn sheet(&self) -> Option<String> {
        self.sheet.clone()
    }

    #[wasm_bindgen(getter, js_name = "rowStart")]
    pub fn row_start(&self) -> usize {
        self.row_start
    }

    #[wasm_bindgen(getter, js_name = "colStart")]
    pub fn col_start(&self) -> usize {
        self.col_start
    }

    #[wasm_bindgen(getter, js_name = "rowEnd")]
    pub fn row_end(&self) -> usize {
        self.row_end
    }

    #[wasm_bindgen(getter, js_name = "colEnd")]
    pub fn col_end(&self) -> usize {
        self.col_end
    }

    #[wasm_bindgen(getter, js_name = "rowAbsStart")]
    pub fn row_abs_start(&self) -> bool {
        self.row_abs_start
    }

    #[wasm_bindgen(getter, js_name = "colAbsStart")]
    pub fn col_abs_start(&self) -> bool {
        self.col_abs_start
    }

    #[wasm_bindgen(getter, js_name = "rowAbsEnd")]
    pub fn row_abs_end(&self) -> bool {
        self.row_abs_end
    }

    #[wasm_bindgen(getter, js_name = "colAbsEnd")]
    pub fn col_abs_end(&self) -> bool {
        self.col_abs_end
    }

    #[wasm_bindgen(js_name = "isSingleCell")]
    pub fn is_single_cell(&self) -> bool {
        self.row_start == self.row_end && self.col_start == self.col_end
    }

    #[wasm_bindgen(js_name = "isRange")]
    pub fn is_range(&self) -> bool {
        !self.is_single_cell()
    }

    #[wasm_bindgen(js_name = "toString")]
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        let sheet_prefix = self
            .sheet
            .as_ref()
            .map(|s| format!("{s}!"))
            .unwrap_or_default();

        let start_col = Self::col_to_letters(self.col_start);
        let end_col = Self::col_to_letters(self.col_end);

        let start_ref = format!(
            "{}{}{}{}",
            if self.col_abs_start { "$" } else { "" },
            start_col,
            if self.row_abs_start { "$" } else { "" },
            self.row_start
        );

        if self.is_single_cell() {
            format!("{sheet_prefix}{start_ref}")
        } else {
            let end_ref = format!(
                "{}{}{}{}",
                if self.col_abs_end { "$" } else { "" },
                end_col,
                if self.row_abs_end { "$" } else { "" },
                self.row_end
            );
            format!("{sheet_prefix}{start_ref}:{end_ref}")
        }
    }

    #[wasm_bindgen(js_name = "toJSON")]
    pub fn to_json(&self) -> Result<JsValue, JsValue> {
        let data = ReferenceData {
            sheet: self.sheet.clone(),
            row_start: self.row_start,
            col_start: self.col_start,
            row_end: self.row_end,
            col_end: self.col_end,
            row_abs_start: self.row_abs_start,
            col_abs_start: self.col_abs_start,
            row_abs_end: self.row_abs_end,
            col_abs_end: self.col_abs_end,
        };

        serde_wasm_bindgen::to_value(&data).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    fn col_to_letters(col: usize) -> String {
        let mut result = String::new();
        let mut n = col;

        loop {
            n -= 1;
            result.insert(0, ((n % 26) as u8 + b'A') as char);
            n /= 26;
            if n == 0 {
                break;
            }
        }

        result
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReferenceData {
    sheet: Option<String>,
    row_start: usize,
    col_start: usize,
    row_end: usize,
    col_end: usize,
    row_abs_start: bool,
    col_abs_start: bool,
    row_abs_end: bool,
    col_abs_end: bool,
}
