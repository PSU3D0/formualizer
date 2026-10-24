//! Typed worksheet attributes. Unknown names and noncanonical lexical forms
//! replay through the original binder/validator before any semantic effect.
use crate::xlsx::xml::known_attribute;
use std::ops::Range;

#[derive(Clone)]
pub(crate) struct RawValue<'a> {
    pub(crate) value: &'a str,
    pub(crate) span: Range<usize>,
}

macro_rules! attributes {
    ($name:ident { $($field:ident => $key:literal),* $(,)? }) => {
        #[derive(Default)]
        pub(crate) struct $name<'a> { $( $field: Option<RawValue<'a>>, )* }
        impl<'a> $name<'a> {
            fn get(&self, key: &str) -> Option<&RawValue<'a>> {
                match key { $( $key => self.$field.as_ref(), )* _ => None }
            }
            fn set(&mut self, key: &str, value: RawValue<'a>) {
                match key { $( $key => self.$field = Some(value), )* _ => {} }
            }
        }
    };
}
attributes!(RowAttrs {
    r => "r", height => "ht", hidden => "hidden", outline => "outlineLevel",
    collapsed => "collapsed", spans => "spans",
});
attributes!(CellAttrs { r => "r", style => "s", kind => "t", cm => "cm", vm => "vm" });
attributes!(FormulaAttrs { kind => "t", index => "si", extent => "ref" });

#[derive(Default)]
pub(crate) enum HotAttributes<'a> {
    #[default]
    Empty,
    Row(RowAttrs<'a>),
    Cell(CellAttrs<'a>),
    Formula(FormulaAttrs<'a>),
}
impl<'a> HotAttributes<'a> {
    pub(crate) fn get(&self, key: &str) -> Option<&RawValue<'a>> {
        match self {
            Self::Empty => None,
            Self::Row(a) => a.get(key),
            Self::Cell(a) => a.get(key),
            Self::Formula(a) => a.get(key),
        }
    }
    fn set(&mut self, key: &str, value: RawValue<'a>) {
        match self {
            Self::Empty => {}
            Self::Row(a) => a.set(key, value),
            Self::Cell(a) => a.set(key, value),
            Self::Formula(a) => a.set(key, value),
        }
    }
    pub(crate) fn parse(&mut self, content: &'a str, name: &str, start: usize) -> bool {
        *self = match name {
            "row" => Self::Row(RowAttrs::default()),
            "c" => Self::Cell(CellAttrs::default()),
            "f" => Self::Formula(FormulaAttrs::default()),
            _ => Self::Empty,
        };
        if content.len() == name.len() {
            return true;
        }
        let allowed = match name {
            "row" => 0b11 | (((1 << 15) - 1) & !((1 << 6) - 1)),
            "c" => (1 << 6) - 1,
            "f" => (1 << 2) | (((1 << 26) - 1) & !((1 << 15) - 1)),
            _ => 0,
        };
        let bytes = content.as_bytes();
        let mut at = name.len();
        let mut seen = 0u32;
        loop {
            while bytes
                .get(at)
                .is_some_and(|b| matches!(b, b' ' | b'\r' | b'\n' | b'\t'))
            {
                at += 1;
            }
            if at == bytes.len() {
                return true;
            }
            let key_start = at;
            while bytes
                .get(at)
                .is_some_and(|b| !matches!(b, b'=' | b' ' | b'\r' | b'\n' | b'\t'))
            {
                at += 1;
            }
            let Some(slot) = bytes.get(key_start..at).and_then(known_attribute) else {
                return false;
            };
            let bit = 1u32 << slot;
            if bit & allowed == 0 || bit & seen != 0 || bytes.get(at) != Some(&b'=') {
                return false;
            }
            let Some(&quote @ (b'\'' | b'"')) = bytes.get(at + 1) else {
                return false;
            };
            let value_start = at + 2;
            let key_end = at;
            at = value_start;
            while let Some(&b) = bytes.get(at) {
                if b == quote {
                    break;
                }
                if matches!(b, b'&' | b'<' | b'\t' | b'\r' | b'\n') {
                    return false;
                }
                at += 1;
            }
            if bytes.get(at) != Some(&quote) {
                return false;
            }
            let (Some(key), Some(value)) = (
                content.get(key_start..key_end),
                content.get(value_start..at),
            ) else {
                return false;
            };
            self.set(
                key,
                RawValue {
                    value,
                    span: start + 1 + key_start..start + 2 + at,
                },
            );
            seen |= bit;
            at += 1;
        }
    }
}
