//! Preserve the pre-0.41 attribute decoding contract during XML consolidation.
//!
//! quick-xml 0.41's deprecated decode-and-unescape API also normalizes literal
//! attribute whitespace. Existing metadata readers intentionally preserve it.
use quick_xml::{encoding::Decoder, events::attributes::Attribute};
use std::borrow::Cow;

pub(crate) trait AttributeExt<'a> {
    fn decoded_without_normalization(&self, decoder: Decoder) -> quick_xml::Result<Cow<'a, str>>;
}

impl<'a> AttributeExt<'a> for Attribute<'a> {
    fn decoded_without_normalization(&self, decoder: Decoder) -> quick_xml::Result<Cow<'a, str>> {
        let decoded = match &self.value {
            Cow::Borrowed(bytes) => decoder.decode(bytes)?,
            Cow::Owned(bytes) => Cow::Owned(decoder.decode(bytes)?.into_owned()),
        };
        match quick_xml::escape::unescape(&decoded)? {
            Cow::Borrowed(_) => Ok(decoded),
            Cow::Owned(value) => Ok(Cow::Owned(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::name::QName;

    #[test]
    fn attributes_retain_literal_whitespace_and_unescape_once() {
        for value in [
            Cow::Borrowed(b"a\t\r\nb&#10;&amp;lt;".as_slice()),
            Cow::Owned(b"a\t\r\nb&#10;&amp;lt;".to_vec()),
        ] {
            let attr = Attribute {
                key: QName(b"name"),
                value,
            };
            assert_eq!(
                attr.decoded_without_normalization(quick_xml::Reader::from_str("").decoder())
                    .unwrap(),
                "a\t\r\nb\n&lt;"
            );
        }
    }
}
