//! Tag-specific lexical attribute fast path. Uncommon syntax is replayed
//! through the generic validator on the same event, before semantic effects.
use super::*;

/// At most twelve formula attributes, eleven row attributes, or six cell
/// attributes. Entries retain source order; no 26-slot lookup table is cleared.
pub(super) struct HotAttributes<'a> {
    entries: [Attribute<'a>; 12],
    len: usize,
}
impl Default for HotAttributes<'_> {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| Attribute {
                ns: Ns::Static(""),
                local: "",
                qualified: "",
                value: Cow::Borrowed(""),
                span: 0..0,
            }),
            len: 0,
        }
    }
}
impl<'a> HotAttributes<'a> {
    pub(super) fn parse(&mut self, content: &'a str, name: &str, start: usize) -> bool {
        self.len = 0;
        // No attribute iterator, slot initialization, or byte scan for the
        // common attribute-free formula/value opening.
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
            let (Some(key), Some(value), Some(entry)) = (
                content.get(key_start..key_end),
                content.get(value_start..at),
                self.entries.get_mut(self.len),
            ) else {
                return false;
            };
            *entry = Attribute {
                ns: Ns::Static(""),
                local: key,
                qualified: key,
                value: Cow::Borrowed(value),
                span: start + 1 + key_start..start + 2 + at,
            };
            seen |= bit;
            self.len += 1;
            at += 1;
        }
    }
    pub(super) fn as_slice(&self) -> &[Attribute<'a>] {
        self.entries.get(..self.len).unwrap_or_default()
    }
}
