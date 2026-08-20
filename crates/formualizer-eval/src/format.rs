use formualizer_common::numfmt::{FormatClass, NumberFormat};
use rustc_hash::FxHashMap;

/// Workbook-local interned number-format identifier.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormatId(pub u16);

impl FormatId {
    pub const GENERAL: Self = Self(0);
    pub const DATE: Self = Self(14);
    pub const TIME: Self = Self(21);
    pub const DATETIME: Self = Self(22);
    pub const DURATION: Self = Self(46);
}

/// Workbook-local registry of classified number-format codes.
#[derive(Clone, Debug)]
pub struct FormatRegistry {
    formats: Vec<Option<NumberFormat>>,
    by_code: FxHashMap<Box<str>, FormatId>,
}

impl Default for FormatRegistry {
    fn default() -> Self {
        let mut formats = Vec::with_capacity(50);
        let mut by_code = FxHashMap::default();
        for raw_id in 0..=49u16 {
            let format = NumberFormat::builtin(raw_id).cloned();
            if let Some(format) = &format {
                by_code.insert(format.code().into(), FormatId(raw_id));
            }
            formats.push(format);
        }
        Self { formats, by_code }
    }
}

impl FormatRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern(&mut self, code: &str) -> FormatId {
        let parsed = NumberFormat::parse(code);
        if let Some(id) = self.by_code.get(parsed.code()) {
            return *id;
        }
        let id = u16::try_from(self.formats.len())
            .ok()
            .map(FormatId)
            .unwrap_or(FormatId::GENERAL);
        if id == FormatId::GENERAL && !self.formats.is_empty() {
            return id;
        }
        self.by_code.insert(parsed.code().into(), id);
        self.formats.push(Some(parsed));
        id
    }

    pub fn get(&self, id: FormatId) -> Option<&NumberFormat> {
        self.formats.get(id.0 as usize).and_then(Option::as_ref)
    }

    pub fn class(&self, id: FormatId) -> Option<&FormatClass> {
        self.get(id).map(NumberFormat::class)
    }

    pub fn code(&self, id: FormatId) -> Option<&str> {
        self.get(id).map(NumberFormat::code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_keep_stable_ids_and_custom_codes_are_interned() {
        let mut registry = FormatRegistry::new();
        assert_eq!(registry.class(FormatId::DATE), Some(&FormatClass::Date));
        assert_eq!(registry.intern("m/d/yy"), FormatId::DATE);
        let custom = registry.intern("yyyy-mm-dd");
        assert_eq!(custom, registry.intern("yyyy-mm-dd"));
        assert_eq!(registry.class(custom), Some(&FormatClass::Date));
        assert!(custom.0 >= 50);
    }
}
