//! Pull tokenizer for the worksheet machine. The generic visitor stays unchanged.
use super::*;

pub(crate) struct Tokens<'a> {
    text: &'a str,
    bytes: &'a [u8],
    reader: Reader<&'a [u8]>,
    scopes: Scopes<'a>,
    scratch: Scratch<'a>,
    path: Vec<Element<'a>>,
    roots: usize,
    events: u64,
    declaration: bool,
    pending_pop: bool,
    pending_path_pop: bool,
    scoped_depths: Vec<usize>,
    sheet_data_generation: Option<u64>,
}
impl<'a> Tokens<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, IoError> {
        let text =
            std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
        valid_text(text)?;
        let mut reader = Reader::from_str(text);
        reader.config_mut().check_end_names = true;
        reader.config_mut().check_comments = true;
        Ok(Self {
            text,
            bytes,
            reader,
            scopes: Scopes::new(),
            scratch: Scratch::default(),
            path: Vec::new(),
            roots: 0,
            events: 0,
            declaration: false,
            pending_pop: false,
            pending_path_pop: false,
            scoped_depths: Vec::new(),
            sheet_data_generation: None,
        })
    }
    pub(crate) fn next(
        &mut self,
        options: &CacheOptions,
    ) -> Result<Option<(&[Element<'a>], Node<'_, 'a>)>, IoError> {
        let Self {
            text,
            bytes,
            reader,
            scopes,
            scratch,
            path,
            roots,
            events,
            declaration,
            pending_pop,
            pending_path_pop,
            scoped_depths,
            sheet_data_generation,
        } = self;
        loop {
            if *events & 1023 == 0 {
                checkpoint(&options.cancel)?;
            }
            *events += 1;
            if *pending_path_pop {
                path.pop();
                *pending_path_pop = false;
            }
            if *pending_pop {
                scopes.pop();
                *pending_pop = false;
            }
            let decoder = reader.decoder();
            let start = reader.buffer_position() as usize;
            let event = reader.read_event().map_err(backend)?;
            let end = reader.buffer_position() as usize;
            match event {
                Event::Start(ref e) | Event::Empty(ref e) => {
                    let empty = matches!(event, Event::Empty(_));
                    let (_, content) = content(text, e)?;
                    let name_len = e.name().as_ref().len();
                    let qualified = &content[..name_len];
                    let in_sheet_data = path.len() >= 2
                        && path[0].ns.is_main()
                        && path[0].local == "worksheet"
                        && path[1].ns.is_main()
                        && path[1].local == "sheetData";
                    let fast = in_sheet_data
                        && *sheet_data_generation == Some(scopes.generation)
                        && matches!(qualified, "row" | "c" | "f" | "v")
                        && scratch.fast.parse(content, name_len, start)
                        && scopes.element(qualified)?.is_main();
                    let malformed = if fast {
                        false
                    } else {
                        bind(scopes, content, name_len, &mut scratch.parsed)?
                    };
                    *pending_pop = empty && !fast;
                    if path.len() >= options.limits.max_xml_depth {
                        return Err(unsupported("XML depth limit", "XLSX XML"));
                    }
                    if path.is_empty() {
                        *roots += 1;
                        if *roots != 1 {
                            return Err(unsupported("multiple XML roots", "XLSX XML"));
                        }
                    }
                    qualified_name(qualified.as_bytes())?;
                    let element = Element {
                        ns: scopes.element(qualified)?,
                        local: local_part(qualified),
                        qualified,
                    };
                    let attributes = &mut scratch.attributes;
                    attributes.clear();
                    let mut seen = None;
                    if fast {
                        // Fully validated stack attributes; no generic vector.
                    } else if malformed {
                        // Replay with quick-xml's own duplicate checks for its
                        // exact error.
                        for a in Attributes::new(content, name_len) {
                            let a = a.map_err(backend)?;
                            attribute(a, content, start, decoder, scopes, &mut seen, attributes)?;
                        }
                    } else {
                        for (i, p) in scratch.parsed.iter().enumerate() {
                            // quick-xml reports a repeated raw key before its value.
                            if let Some(prev) = scratch.parsed[..i].iter().find(|q| q.key == p.key)
                            {
                                let raw = content.as_bytes();
                                return Err(backend(AttrError::Duplicated(
                                    offset(raw, p.key)?,
                                    offset(raw, prev.key)?,
                                )));
                            }
                            attribute(
                                RawAttribute {
                                    key: QName(p.key),
                                    value: Cow::Borrowed(p.value),
                                },
                                content,
                                start,
                                decoder,
                                scopes,
                                &mut seen,
                                attributes,
                            )?;
                        }
                    }
                    path.push(element);
                    if !empty && !fast {
                        scoped_depths.push(path.len());
                    }
                    if path.len() == 2
                        && path[0].ns.is_main()
                        && path[0].local == "worksheet"
                        && path[1].ns.is_main()
                        && path[1].local == "sheetData"
                    {
                        *sheet_data_generation = Some(scopes.generation);
                    }
                    let node = Node {
                        kind: Kind::Open {
                            empty,
                            attributes: if fast {
                                scratch.fast.as_slice()
                            } else {
                                &scratch.attributes
                            },
                        },
                        span: start..end,
                        fast_attributes: fast.then_some(&scratch.fast),
                    };
                    *pending_path_pop = empty;
                    return Ok(Some((path.as_slice(), node)));
                }
                Event::End(_) => {
                    *pending_pop = scoped_depths.last().copied() == Some(path.len());
                    if *pending_pop {
                        scoped_depths.pop();
                    }
                    if path.is_empty() {
                        return Err(unsupported("unbalanced XML", "XLSX XML"));
                    }
                    let node = Node {
                        kind: Kind::Close,
                        span: start..end,
                        fast_attributes: None,
                    };
                    *pending_path_pop = true;
                    return Ok(Some((path.as_slice(), node)));
                }
                Event::Text(t) => {
                    let value = match plain_text(text, &t) {
                        Some(value) => Cow::Borrowed(value),
                        None => {
                            if cdata_end(&t) {
                                return Err(unsupported("CDATA terminator in text", "XLSX XML"));
                            }
                            let value = t.xml_content().map_err(backend)?;
                            // A borrowed value is a slice of the already validated input.
                            if let Cow::Owned(value) = &value {
                                valid_text(value)?;
                            }
                            value
                        }
                    };
                    if path.is_empty() && !value.trim().is_empty() {
                        return Err(unsupported("text outside XML root", "XLSX XML"));
                    }
                    let node = Node {
                        kind: Kind::Text(value),
                        span: start..end,
                        fast_attributes: None,
                    };
                    return Ok(Some((path.as_slice(), node)));
                }
                Event::CData(_) => {
                    // Calamine's formula/string readers ignore these events rather
                    // than concatenating their text. Never accept a divergent view.
                    return Err(unsupported(
                        "CDATA is not supported by the ingestion view",
                        "XLSX XML",
                    ));
                }
                Event::GeneralRef(reference) => {
                    let name = reference.decode().map_err(backend)?;
                    let value: Cow<'a, str> = match name.as_ref() {
                        "amp" => "&".into(),
                        "lt" => "<".into(),
                        "gt" => ">".into(),
                        "quot" => "\"".into(),
                        "apos" => "'".into(),
                        _ => reference
                            .resolve_char_ref()
                            .map_err(backend)?
                            .ok_or_else(|| unsupported("unknown XML entity", "XLSX XML"))?
                            .to_string()
                            .into(),
                    };
                    if path.is_empty() {
                        return Err(unsupported("entity outside XML root", "XLSX XML"));
                    }
                    valid_text(&value)?;
                    let node = Node {
                        kind: Kind::Text(value),
                        span: start..end,
                        fast_attributes: None,
                    };
                    return Ok(Some((path.as_slice(), node)));
                }
                Event::DocType(_) => {
                    return Err(unsupported("DTD/entity declarations", "XLSX XML"));
                }
                Event::Decl(d) => {
                    if *roots != 0 || *declaration {
                        return Err(unsupported("misplaced XML declaration", "XLSX XML"));
                    }
                    *declaration = true;
                    if d.version().map_err(backend)?.as_ref() != b"1.0" {
                        return Err(unsupported("unsupported XML version", "XLSX XML"));
                    }
                    if let Some(encoding) = d.encoding() {
                        let encoding = encoding.map_err(backend)?;
                        if !(encoding.eq_ignore_ascii_case(b"utf-8")
                            || encoding.eq_ignore_ascii_case(b"us-ascii") && bytes.is_ascii())
                        {
                            return Err(unsupported("unsupported XML encoding", "XLSX XML"));
                        }
                    }
                }
                Event::Eof => {
                    if !path.is_empty() || *roots != 1 {
                        return Err(unsupported("incomplete XML document", "XLSX XML"));
                    }
                    return Ok(None);
                }
                Event::Comment(_) | Event::PI(_) => {}
            }
        }
    }
}
