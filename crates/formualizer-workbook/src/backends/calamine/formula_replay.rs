use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom, Write},
};

use calamine::expand_shared_formula_into;
use formualizer_eval::engine::{
    DeferredFormulaReplay, DeferredReplayFormula, SourceCoord, SourceFamilyId, SourceRect,
};

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FormulaMetadataEnvelope {
    Ordinary,
    Shared {
        shared_index: usize,
        parsed_range: Option<SourceRect>,
    },
    Unknown,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(super) enum FormulaSourceKind {
    Ordinary {
        formula: Arc<str>,
        metadata: FormulaMetadataEnvelope,
    },
    SharedAnchor {
        family: SourceFamilyId,
        declared_range: Option<SourceRect>,
        formula: Arc<str>,
        metadata: FormulaMetadataEnvelope,
    },
    SharedDescendant {
        family: SourceFamilyId,
        metadata: FormulaMetadataEnvelope,
    },
    Unsupported {
        formula_if_available: Option<Arc<str>>,
        metadata: FormulaMetadataEnvelope,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(super) struct FormulaSourceEvent {
    pub(super) sheet_name: Arc<str>,
    pub(super) coord0: SourceCoord,
    pub(super) source_sequence: u64,
    pub(super) formula: FormulaSourceKind,
}

const MAGIC: &[u8; 4] = b"FZFR";
const VERSION: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SpoolOffset(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SpoolStorageKind {
    Memory,
    #[cfg(not(target_arch = "wasm32"))]
    NativeFile,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SpoolFormulaRecord<'a> {
    Ordinary {
        sequence: u64,
        coord0: SourceCoord,
        text: &'a str,
    },
    SharedAnchor {
        sequence: u64,
        coord0: SourceCoord,
        shared_index: usize,
        declared_range: Option<SourceRect>,
        text: &'a str,
    },
    SharedDescendant {
        sequence: u64,
        coord0: SourceCoord,
        shared_index: usize,
    },
    Unsupported {
        sequence: u64,
        coord0: SourceCoord,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnedSpoolFormulaRecord {
    Ordinary {
        sequence: u64,
        coord0: SourceCoord,
        text: String,
    },
    SharedAnchor {
        sequence: u64,
        coord0: SourceCoord,
        shared_index: usize,
        declared_range: Option<SourceRect>,
        text: String,
    },
    SharedDescendant {
        sequence: u64,
        coord0: SourceCoord,
        shared_index: usize,
    },
    Unsupported {
        sequence: u64,
        coord0: SourceCoord,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) enum SpoolError {
    SheetLimit {
        limit: u64,
        attempted: u64,
    },
    WorkbookLimit {
        limit: u64,
        attempted: u64,
    },
    MemoryLimit {
        limit: u64,
        attempted: u64,
    },
    FileLimit {
        limit: u32,
        attempted: u32,
    },
    #[cfg(target_arch = "wasm32")]
    DiskDisabled {
        memory_limit: u64,
    },
    Io(std::io::ErrorKind),
    InvalidHeader,
    UnsupportedVersion(u8),
    Truncated,
    Malformed(&'static str),
    OffsetOverflow,
    InjectedAppendFailure,
    InjectedReplayFailure,
}

impl std::fmt::Display for SpoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SheetLimit { limit, attempted } => write!(
                f,
                "formula replay spool per-sheet limit of {limit} bytes exceeded by {attempted} bytes"
            ),
            Self::WorkbookLimit { limit, attempted } => write!(
                f,
                "formula replay spool per-workbook limit of {limit} bytes exceeded by {attempted} bytes"
            ),
            Self::MemoryLimit { limit, attempted } => write!(
                f,
                "formula replay spool memory-only limit of {limit} bytes exceeded by {attempted} bytes"
            ),
            Self::FileLimit { limit, attempted } => write!(
                f,
                "formula replay spool file limit of {limit} exceeded by {attempted}"
            ),
            #[cfg(target_arch = "wasm32")]
            Self::DiskDisabled { memory_limit } => write!(
                f,
                "formula replay spool disk use is disabled and its memory limit is {memory_limit} bytes"
            ),
            Self::Io(kind) => write!(f, "formula replay spool I/O failure ({kind:?})"),
            Self::InvalidHeader => f.write_str("invalid formula replay spool header"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported formula replay spool version {version}")
            }
            Self::Truncated => f.write_str("truncated formula replay spool"),
            Self::Malformed(reason) => write!(f, "malformed formula replay spool: {reason}"),
            Self::OffsetOverflow => f.write_str("formula replay spool offset overflow"),
            Self::InjectedAppendFailure => f.write_str("injected formula replay append failure"),
            Self::InjectedReplayFailure => f.write_str("injected formula replay failure"),
        }
    }
}

impl std::error::Error for SpoolError {}

pub(super) trait FormulaReplaySpool {
    fn append(&mut self, record: SpoolFormulaRecord<'_>) -> Result<SpoolOffset, SpoolError>;
    fn replay(&mut self) -> Result<FormulaReplayIter<'_>, SpoolError>;
    fn encoded_bytes(&self) -> u64;
    fn storage_kind(&self) -> SpoolStorageKind;
}

#[cfg(test)]
pub(super) struct MemoryFormulaReplaySpool {
    bytes: Vec<u8>,
    max_bytes: u64,
    #[cfg(test)]
    fail_append: bool,
    #[cfg(test)]
    fail_replay: bool,
}

#[cfg(test)]
impl MemoryFormulaReplaySpool {
    fn with_max_bytes(max_bytes: u64) -> Self {
        Self {
            bytes: [MAGIC.as_slice(), &[VERSION]].concat(),
            max_bytes,
            #[cfg(test)]
            fail_append: false,
            #[cfg(test)]
            fail_replay: false,
        }
    }

    #[cfg(test)]
    fn from_bytes(bytes: Vec<u8>, max_bytes: u64) -> Self {
        Self {
            bytes,
            max_bytes,
            fail_append: false,
            fail_replay: false,
        }
    }

    pub(super) fn replay_events(
        &mut self,
        sheet_name: &str,
        sheet_instance: u32,
    ) -> Result<Vec<FormulaSourceEvent>, SpoolError> {
        self.replay()?
            .map(|record| {
                let record = record?;
                Ok(record.into_source_event(sheet_name, sheet_instance))
            })
            .collect()
    }
}

#[cfg(test)]
impl FormulaReplaySpool for MemoryFormulaReplaySpool {
    fn append(&mut self, record: SpoolFormulaRecord<'_>) -> Result<SpoolOffset, SpoolError> {
        #[cfg(test)]
        if self.fail_append {
            return Err(SpoolError::InjectedAppendFailure);
        }

        let mut frame = Vec::new();
        append_frame_to_vec(&mut frame, record)?;

        let offset = u64::try_from(self.bytes.len()).map_err(|_| SpoolError::OffsetOverflow)?;
        let attempted = checked_next_offset(offset, frame.len())?;
        if attempted > self.max_bytes {
            return Err(SpoolError::MemoryLimit {
                limit: self.max_bytes,
                attempted,
            });
        }
        self.bytes.extend_from_slice(&frame);
        Ok(SpoolOffset(offset))
    }

    fn replay(&mut self) -> Result<FormulaReplayIter<'_>, SpoolError> {
        #[cfg(test)]
        if self.fail_replay {
            return Err(SpoolError::InjectedReplayFailure);
        }
        if self.bytes.len() < HEADER_LEN || &self.bytes[..MAGIC.len()] != MAGIC {
            return Err(SpoolError::InvalidHeader);
        }
        if self.bytes[MAGIC.len()] != VERSION {
            return Err(SpoolError::UnsupportedVersion(self.bytes[MAGIC.len()]));
        }
        Ok(FormulaReplayIter::Memory {
            bytes: &self.bytes,
            cursor: HEADER_LEN,
            failed: false,
        })
    }

    fn encoded_bytes(&self) -> u64 {
        u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
    }

    fn storage_kind(&self) -> SpoolStorageKind {
        SpoolStorageKind::Memory
    }
}

pub(super) struct FormulaSpoolLimits {
    pub sheet_bytes: u64,
    pub workbook_bytes_remaining: u64,
    pub workbook_bytes_used: u64,
    pub memory_prefix_bytes: u64,
    pub memory_only_bytes: u64,
    pub allow_disk: bool,
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub spill_files_remaining: u32,
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub spill_files_limit: u32,
}

pub(super) struct HybridFormulaReplaySpool {
    memory: Vec<u8>,
    encoded_bytes: u64,
    peak_memory_bytes: u64,
    #[cfg(test)]
    append_scratch_heap_allocations: u64,
    limits: FormulaSpoolLimits,
    #[cfg(not(target_arch = "wasm32"))]
    file: Option<tempfile::NamedTempFile>,
    #[cfg(test)]
    fail_write: bool,
    #[cfg(test)]
    fail_replay_io: bool,
}

impl HybridFormulaReplaySpool {
    pub(super) fn new(limits: FormulaSpoolLimits) -> Self {
        Self {
            memory: [MAGIC.as_slice(), &[VERSION]].concat(),
            encoded_bytes: HEADER_LEN as u64,
            peak_memory_bytes: HEADER_LEN as u64,
            #[cfg(test)]
            append_scratch_heap_allocations: 0,
            limits,
            #[cfg(not(target_arch = "wasm32"))]
            file: None,
            #[cfg(test)]
            fail_write: false,
            #[cfg(test)]
            fail_replay_io: false,
        }
    }

    pub(super) fn peak_memory_bytes(&self) -> u64 {
        self.peak_memory_bytes
    }

    pub(super) fn spilled(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.file.is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    }

    #[cfg(test)]
    fn memory_capacity(&self) -> usize {
        self.memory.capacity()
    }

    #[cfg(test)]
    fn append_scratch_heap_allocations(&self) -> u64 {
        self.append_scratch_heap_allocations
    }
}

pub(super) struct CalamineDeferredFormulaReplay {
    spool: HybridFormulaReplaySpool,
    sheet_name: String,
    sheet_instance: u32,
}

impl CalamineDeferredFormulaReplay {
    pub(super) fn new(
        spool: HybridFormulaReplaySpool,
        sheet_name: String,
        sheet_instance: u32,
    ) -> Self {
        Self {
            spool,
            sheet_name,
            sheet_instance,
        }
    }
}

impl DeferredFormulaReplay for CalamineDeferredFormulaReplay {
    fn replay(
        &mut self,
        skip_families: &std::collections::BTreeSet<SourceFamilyId>,
        suppressed: &std::collections::BTreeSet<(u32, u32)>,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        let mut formulas = Vec::new();
        let sheet_instance = self.sheet_instance;
        replay_spool_with_family(
            &mut self.spool,
            &self.sheet_name,
            |shared_index| {
                skip_families.contains(&SourceFamilyId {
                    sheet_instance,
                    source_index: shared_index,
                })
            },
            |coord0, text, family| {
                let family = family.map(|shared_index| SourceFamilyId {
                    sheet_instance,
                    source_index: shared_index,
                });
                if suppressed.contains(&(coord0.row + 1, coord0.col + 1)) {
                    return Ok(());
                }
                formulas.push(DeferredReplayFormula {
                    row: coord0.row + 1,
                    col: coord0.col + 1,
                    text: text.to_string(),
                    family,
                });
                Ok(())
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(formulas)
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        let mut found = None;
        let sheet_instance = self.sheet_instance;
        replay_spool_with_family(
            &mut self.spool,
            &self.sheet_name,
            |_| false,
            |coord0, text, family| {
                if coord0.row + 1 == row && coord0.col + 1 == col {
                    found = Some(DeferredReplayFormula {
                        row,
                        col,
                        text: text.to_string(),
                        family: family.map(|shared_index| SourceFamilyId {
                            sheet_instance,
                            source_index: shared_index,
                        }),
                    });
                }
                Ok(())
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(found)
    }
}

impl FormulaReplaySpool for HybridFormulaReplaySpool {
    fn append(&mut self, record: SpoolFormulaRecord<'_>) -> Result<SpoolOffset, SpoolError> {
        let frame_len = encoded_frame_len(record)?;
        let offset = self.encoded_bytes;
        let attempted = checked_next_offset(offset, frame_len)?;
        if attempted > self.limits.sheet_bytes {
            return Err(SpoolError::SheetLimit {
                limit: self.limits.sheet_bytes,
                attempted,
            });
        }
        if attempted > self.limits.workbook_bytes_remaining {
            return Err(SpoolError::WorkbookLimit {
                limit: self
                    .limits
                    .workbook_bytes_used
                    .saturating_add(self.limits.workbook_bytes_remaining),
                attempted: self.limits.workbook_bytes_used.saturating_add(attempted),
            });
        }
        if !self.limits.allow_disk && attempted > self.limits.memory_only_bytes {
            return Err(SpoolError::MemoryLimit {
                limit: self.limits.memory_only_bytes,
                attempted,
            });
        }

        #[cfg(not(target_arch = "wasm32"))]
        if self.file.is_none()
            && self.limits.allow_disk
            && attempted > self.limits.memory_prefix_bytes
        {
            if self.limits.spill_files_remaining == 0 {
                return Err(SpoolError::FileLimit {
                    limit: self.limits.spill_files_limit,
                    attempted: self.limits.spill_files_limit.saturating_add(1),
                });
            }
            let mut file = tempfile::NamedTempFile::new().map_err(|e| SpoolError::Io(e.kind()))?;
            file.write_all(&self.memory)
                .map_err(|e| SpoolError::Io(e.kind()))?;
            self.memory = Vec::new();
            self.file = Some(file);
        }

        #[cfg(target_arch = "wasm32")]
        if attempted > self.limits.memory_prefix_bytes && self.limits.allow_disk {
            return Err(SpoolError::DiskDisabled {
                memory_limit: self.limits.memory_only_bytes,
            });
        }

        #[cfg(test)]
        if self.fail_write {
            return Err(SpoolError::Io(std::io::ErrorKind::WriteZero));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(file) = self.file.as_mut() {
            write_frame(file, record)?;
        } else {
            append_frame_to_vec(&mut self.memory, record)?;
        }
        #[cfg(target_arch = "wasm32")]
        append_frame_to_vec(&mut self.memory, record)?;
        self.encoded_bytes = attempted;
        self.peak_memory_bytes = self
            .peak_memory_bytes
            .max(u64::try_from(self.memory.len()).unwrap_or(u64::MAX));
        Ok(SpoolOffset(offset))
    }

    fn replay(&mut self) -> Result<FormulaReplayIter<'_>, SpoolError> {
        #[cfg(test)]
        if self.fail_replay_io {
            return Err(SpoolError::Io(std::io::ErrorKind::Other));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(file) = self.file.as_mut() {
            file.flush().map_err(|e| SpoolError::Io(e.kind()))?;
            let mut replay_file = file
                .as_file()
                .try_clone()
                .map_err(|e| SpoolError::Io(e.kind()))?;
            replay_file
                .seek(SeekFrom::Start(0))
                .map_err(|e| SpoolError::Io(e.kind()))?;
            let mut header = [0u8; HEADER_LEN];
            replay_file
                .read_exact(&mut header)
                .map_err(|e| SpoolError::Io(e.kind()))?;
            validate_header(&header)?;
            return Ok(FormulaReplayIter::Native {
                reader: BufReader::new(replay_file),
                remaining: self.encoded_bytes - HEADER_LEN as u64,
                failed: false,
            });
        }
        validate_header(&self.memory)?;
        Ok(FormulaReplayIter::Memory {
            bytes: &self.memory,
            cursor: HEADER_LEN,
            failed: false,
        })
    }

    fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    fn storage_kind(&self) -> SpoolStorageKind {
        #[cfg(not(target_arch = "wasm32"))]
        if self.file.is_some() {
            return SpoolStorageKind::NativeFile;
        }
        SpoolStorageKind::Memory
    }
}

fn validate_header(bytes: &[u8]) -> Result<(), SpoolError> {
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Err(SpoolError::InvalidHeader);
    }
    if bytes[MAGIC.len()] != VERSION {
        return Err(SpoolError::UnsupportedVersion(bytes[MAGIC.len()]));
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_frame_from_reader(
    reader: &mut BufReader<File>,
    remaining: &mut u64,
) -> Result<OwnedSpoolFormulaRecord, SpoolError> {
    let (len, varint_bytes) = read_varint(reader)?;
    let frame_bytes = varint_bytes
        .checked_add(len)
        .ok_or(SpoolError::OffsetOverflow)?;
    if frame_bytes > *remaining {
        return Err(SpoolError::Truncated);
    }
    let len = usize::try_from(len)
        .map_err(|_| SpoolError::Malformed("record length does not fit usize"))?;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            SpoolError::Truncated
        } else {
            SpoolError::Io(e.kind())
        }
    })?;
    *remaining -= frame_bytes;
    decode_record(&payload)
}

#[cfg(not(target_arch = "wasm32"))]
fn read_varint(reader: &mut BufReader<File>) -> Result<(u64, u64), SpoolError> {
    let mut value = 0u64;
    for index in 0..10 {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                SpoolError::Truncated
            } else {
                SpoolError::Io(e.kind())
            }
        })?;
        if index == 9 && byte[0] > 1 {
            return Err(SpoolError::Malformed("varint overflow"));
        }
        value |= u64::from(byte[0] & 0x7f) << (index * 7);
        if byte[0] & 0x80 == 0 {
            return Ok((value, index as u64 + 1));
        }
    }
    Err(SpoolError::Malformed("varint overflow"))
}

fn encoded_frame_len(record: SpoolFormulaRecord<'_>) -> Result<usize, SpoolError> {
    fn common(sequence: u64, coord: SourceCoord) -> Option<usize> {
        varint_len(sequence)
            .checked_add(varint_len(u64::from(coord.row)))?
            .checked_add(varint_len(u64::from(coord.col)))
    }
    let payload = match record {
        SpoolFormulaRecord::Ordinary {
            sequence,
            coord0,
            text,
        } => common(sequence, coord0)
            .and_then(|n| n.checked_add(1 + varint_len(text.len() as u64)))
            .and_then(|n| n.checked_add(text.len())),
        SpoolFormulaRecord::SharedAnchor {
            sequence,
            coord0,
            shared_index,
            declared_range,
            text,
        } => {
            let mut n = common(sequence, coord0)
                .and_then(|n| n.checked_add(2 + varint_len(shared_index as u64)));
            if let Some(range) = declared_range {
                n = n.and_then(|n| {
                    n.checked_add(
                        varint_len(u64::from(range.start.row))
                            + varint_len(u64::from(range.start.col))
                            + varint_len(u64::from(range.end.row))
                            + varint_len(u64::from(range.end.col)),
                    )
                });
            }
            n.and_then(|n| n.checked_add(varint_len(text.len() as u64)))
                .and_then(|n| n.checked_add(text.len()))
        }
        SpoolFormulaRecord::SharedDescendant {
            sequence,
            coord0,
            shared_index,
        } => common(sequence, coord0)
            .and_then(|n| n.checked_add(1 + varint_len(shared_index as u64))),
        SpoolFormulaRecord::Unsupported { sequence, coord0 } => {
            common(sequence, coord0).and_then(|n| n.checked_add(1))
        }
    }
    .ok_or(SpoolError::OffsetOverflow)?;
    payload
        .checked_add(varint_len(payload as u64))
        .ok_or(SpoolError::OffsetOverflow)
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

#[cfg(not(target_arch = "wasm32"))]
fn write_frame(
    file: &mut tempfile::NamedTempFile,
    record: SpoolFormulaRecord<'_>,
) -> Result<(), SpoolError> {
    let mut prefix = StackEncoder::new();
    let text = encode_frame_prefix(record, &mut prefix)?;
    file.write_all(prefix.as_slice())
        .and_then(|_| file.write_all(text))
        .map_err(|e| SpoolError::Io(e.kind()))
}

struct StackEncoder {
    bytes: [u8; 64],
    len: usize,
}

impl StackEncoder {
    fn new() -> Self {
        Self {
            bytes: [0; 64],
            len: 0,
        }
    }
    fn push(&mut self, byte: u8) -> Result<(), SpoolError> {
        let slot = self
            .bytes
            .get_mut(self.len)
            .ok_or(SpoolError::OffsetOverflow)?;
        *slot = byte;
        self.len += 1;
        Ok(())
    }
    fn varint(&mut self, mut value: u64) -> Result<(), SpoolError> {
        while value >= 0x80 {
            self.push((value as u8) | 0x80)?;
            value >>= 7;
        }
        self.push(value as u8)
    }
    fn coord(&mut self, coord: SourceCoord) -> Result<(), SpoolError> {
        self.varint(u64::from(coord.row))?;
        self.varint(u64::from(coord.col))
    }
    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn encode_frame_prefix<'a>(
    record: SpoolFormulaRecord<'a>,
    out: &mut StackEncoder,
) -> Result<&'a [u8], SpoolError> {
    let frame_len = encoded_frame_len(record)?;
    let mut payload_len = frame_len;
    loop {
        let next = frame_len
            .checked_sub(varint_len(payload_len as u64))
            .ok_or(SpoolError::OffsetOverflow)?;
        if next == payload_len {
            break;
        }
        payload_len = next;
    }
    out.varint(payload_len as u64)?;
    let (tag, sequence, coord0, text): (u8, u64, SourceCoord, &[u8]) = match record {
        SpoolFormulaRecord::Ordinary {
            sequence,
            coord0,
            text,
        } => (0, sequence, coord0, text.as_bytes()),
        SpoolFormulaRecord::SharedAnchor {
            sequence,
            coord0,
            text,
            ..
        } => (1, sequence, coord0, text.as_bytes()),
        SpoolFormulaRecord::SharedDescendant {
            sequence, coord0, ..
        } => (2, sequence, coord0, &[]),
        SpoolFormulaRecord::Unsupported { sequence, coord0 } => (3, sequence, coord0, &[]),
    };
    out.push(tag)?;
    out.varint(sequence)?;
    out.coord(coord0)?;
    match record {
        SpoolFormulaRecord::Ordinary { .. } => out.varint(text.len() as u64)?,
        SpoolFormulaRecord::SharedAnchor {
            shared_index,
            declared_range,
            ..
        } => {
            out.varint(shared_index as u64)?;
            if let Some(range) = declared_range {
                out.push(1)?;
                out.coord(range.start)?;
                out.coord(range.end)?;
            } else {
                out.push(0)?;
            }
            out.varint(text.len() as u64)?;
        }
        SpoolFormulaRecord::SharedDescendant { shared_index, .. } => {
            out.varint(shared_index as u64)?
        }
        SpoolFormulaRecord::Unsupported { .. } => {}
    }
    Ok(text)
}

fn append_frame_to_vec(
    out: &mut Vec<u8>,
    record: SpoolFormulaRecord<'_>,
) -> Result<(), SpoolError> {
    let mut prefix = StackEncoder::new();
    let text = encode_frame_prefix(record, &mut prefix)?;
    out.reserve(prefix.len.saturating_add(text.len()));
    out.extend_from_slice(prefix.as_slice());
    out.extend_from_slice(text);
    Ok(())
}

#[cfg(test)]
fn encode_frame(record: SpoolFormulaRecord<'_>) -> Result<Vec<u8>, SpoolError> {
    let mut frame = Vec::new();
    append_frame_to_vec(&mut frame, record)?;
    Ok(frame)
}

#[derive(Debug)]
pub(super) enum FormulaReplayIter<'a> {
    Memory {
        bytes: &'a [u8],
        cursor: usize,
        failed: bool,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        reader: BufReader<File>,
        remaining: u64,
        failed: bool,
    },
}

impl Iterator for FormulaReplayIter<'_> {
    type Item = Result<OwnedSpoolFormulaRecord, SpoolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory {
                bytes,
                cursor,
                failed,
            } => {
                if *failed || *cursor == bytes.len() {
                    return None;
                }
                let result = decode_frame(bytes, cursor);
                if result.is_err() {
                    *failed = true;
                }
                Some(result)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native {
                reader,
                remaining,
                failed,
            } => {
                if *failed || *remaining == 0 {
                    return None;
                }
                let result = decode_frame_from_reader(reader, remaining);
                if result.is_err() {
                    *failed = true;
                }
                Some(result)
            }
        }
    }
}

impl OwnedSpoolFormulaRecord {
    #[cfg(test)]
    fn into_source_event(self, sheet_name: &str, sheet_instance: u32) -> FormulaSourceEvent {
        let (source_sequence, coord0, formula) = match self {
            Self::Ordinary {
                sequence,
                coord0,
                text,
            } => (
                sequence,
                coord0,
                FormulaSourceKind::Ordinary {
                    formula: Arc::from(text),
                    metadata: FormulaMetadataEnvelope::Ordinary,
                },
            ),
            Self::SharedAnchor {
                sequence,
                coord0,
                shared_index,
                declared_range,
                text,
            } => (
                sequence,
                coord0,
                FormulaSourceKind::SharedAnchor {
                    family: SourceFamilyId {
                        sheet_instance,
                        source_index: shared_index,
                    },
                    declared_range,
                    formula: Arc::from(text),
                    metadata: FormulaMetadataEnvelope::Shared {
                        shared_index,
                        parsed_range: declared_range,
                    },
                },
            ),
            Self::SharedDescendant {
                sequence,
                coord0,
                shared_index,
            } => (
                sequence,
                coord0,
                FormulaSourceKind::SharedDescendant {
                    family: SourceFamilyId {
                        sheet_instance,
                        source_index: shared_index,
                    },
                    metadata: FormulaMetadataEnvelope::Shared {
                        shared_index,
                        parsed_range: None,
                    },
                },
            ),
            Self::Unsupported { sequence, coord0 } => (
                sequence,
                coord0,
                FormulaSourceKind::Unsupported {
                    formula_if_available: None,
                    metadata: FormulaMetadataEnvelope::Unknown,
                },
            ),
        };
        FormulaSourceEvent {
            sheet_name: Arc::from(sheet_name),
            coord0,
            source_sequence,
            formula,
        }
    }
}

fn decode_frame(bytes: &[u8], cursor: &mut usize) -> Result<OwnedSpoolFormulaRecord, SpoolError> {
    let payload_len = get_varint(bytes, cursor)?;
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| SpoolError::Malformed("record length does not fit usize"))?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or(SpoolError::OffsetOverflow)?;
    if end > bytes.len() {
        return Err(SpoolError::Truncated);
    }
    let payload = &bytes[*cursor..end];
    *cursor = end;
    decode_record(payload)
}

fn decode_record(payload: &[u8]) -> Result<OwnedSpoolFormulaRecord, SpoolError> {
    let mut cursor = 0;
    let tag = take_byte(payload, &mut cursor)?;
    let sequence = get_varint(payload, &mut cursor)?;
    let coord0 = get_coord(payload, &mut cursor)?;
    let record = match tag {
        0 => OwnedSpoolFormulaRecord::Ordinary {
            sequence,
            coord0,
            text: get_text(payload, &mut cursor)?,
        },
        1 => {
            let shared_index = usize::try_from(get_varint(payload, &mut cursor)?)
                .map_err(|_| SpoolError::Malformed("shared index does not fit usize"))?;
            let declared_range = match take_byte(payload, &mut cursor)? {
                0 => None,
                1 => Some(SourceRect {
                    start: get_coord(payload, &mut cursor)?,
                    end: get_coord(payload, &mut cursor)?,
                }),
                _ => return Err(SpoolError::Malformed("invalid range presence tag")),
            };
            OwnedSpoolFormulaRecord::SharedAnchor {
                sequence,
                coord0,
                shared_index,
                declared_range,
                text: get_text(payload, &mut cursor)?,
            }
        }
        2 => OwnedSpoolFormulaRecord::SharedDescendant {
            sequence,
            coord0,
            shared_index: usize::try_from(get_varint(payload, &mut cursor)?)
                .map_err(|_| SpoolError::Malformed("shared index does not fit usize"))?,
        },
        3 => OwnedSpoolFormulaRecord::Unsupported { sequence, coord0 },
        _ => return Err(SpoolError::Malformed("unknown record tag")),
    };
    if cursor != payload.len() {
        return Err(SpoolError::Malformed("trailing record bytes"));
    }
    Ok(record)
}

fn get_coord(bytes: &[u8], cursor: &mut usize) -> Result<SourceCoord, SpoolError> {
    Ok(SourceCoord {
        row: u32::try_from(get_varint(bytes, cursor)?)
            .map_err(|_| SpoolError::Malformed("row does not fit u32"))?,
        col: u32::try_from(get_varint(bytes, cursor)?)
            .map_err(|_| SpoolError::Malformed("column does not fit u32"))?,
    })
}

fn get_text(bytes: &[u8], cursor: &mut usize) -> Result<String, SpoolError> {
    let len = usize::try_from(get_varint(bytes, cursor)?)
        .map_err(|_| SpoolError::Malformed("text length does not fit usize"))?;
    let end = cursor.checked_add(len).ok_or(SpoolError::OffsetOverflow)?;
    let text = bytes.get(*cursor..end).ok_or(SpoolError::Truncated)?;
    *cursor = end;
    String::from_utf8(text.to_vec()).map_err(|_| SpoolError::Malformed("formula is not UTF-8"))
}

fn get_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, SpoolError> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = take_byte(bytes, cursor)?;
        if shift == 63 && byte > 1 {
            return Err(SpoolError::Malformed("varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(SpoolError::Malformed("varint overflow"))
}

fn take_byte(bytes: &[u8], cursor: &mut usize) -> Result<u8, SpoolError> {
    let byte = bytes.get(*cursor).copied().ok_or(SpoolError::Truncated)?;
    *cursor = cursor.checked_add(1).ok_or(SpoolError::OffsetOverflow)?;
    Ok(byte)
}

fn checked_next_offset(offset: u64, appended_len: usize) -> Result<u64, SpoolError> {
    offset
        .checked_add(u64::try_from(appended_len).map_err(|_| SpoolError::OffsetOverflow)?)
        .ok_or(SpoolError::OffsetOverflow)
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct ExpandedFormulaCell {
    pub(super) coord0: SourceCoord,
    pub(super) formula: Arc<str>,
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) enum SourceFormulaError {
    Expansion(calamine::XlsxError),
    UnsupportedMetadata {
        sheet_name: Arc<str>,
        coord0: SourceCoord,
    },
}

impl SourceFormulaError {
    pub(super) fn into_calamine(self) -> calamine::Error {
        match self {
            Self::Expansion(error) => calamine::Error::Xlsx(error),
            error @ Self::UnsupportedMetadata { .. } => {
                calamine::Error::Io(std::io::Error::other(error.to_string()))
            }
        }
    }
}

impl std::fmt::Display for SourceFormulaError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expansion(error) => error.fmt(formatter),
            Self::UnsupportedMetadata { sheet_name, coord0 } => write!(
                formatter,
                "unsupported Calamine formula metadata at {sheet_name}!R{}C{}",
                coord0.row + 1,
                coord0.col + 1
            ),
        }
    }
}

impl std::error::Error for SourceFormulaError {}

fn replay_spool_with_family<S: FormulaReplaySpool>(
    spool: &mut S,
    sheet_name: &str,
    mut skip_family: impl FnMut(usize) -> bool,
    mut emit: impl FnMut(SourceCoord, &str, Option<usize>) -> Result<(), calamine::Error>,
) -> Result<(), calamine::Error> {
    let mut shared: rustc_hash::FxHashMap<usize, (SourceCoord, String)> =
        rustc_hash::FxHashMap::default();
    let mut pending: rustc_hash::FxHashMap<usize, Vec<SourceCoord>> =
        rustc_hash::FxHashMap::default();
    let mut expansion = String::with_capacity(128);
    let records = spool
        .replay()
        .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;

    for record in records {
        let record = record
            .map_err(|error| calamine::Error::Io(std::io::Error::other(error.to_string())))?;
        match record {
            OwnedSpoolFormulaRecord::Ordinary { coord0, text, .. } => emit(coord0, &text, None)?,
            OwnedSpoolFormulaRecord::SharedAnchor {
                coord0,
                shared_index,
                text,
                ..
            } => {
                if skip_family(shared_index) {
                    continue;
                }
                emit(coord0, &text, Some(shared_index))?;
                shared.insert(shared_index, (coord0, text));
                if let Some(waiting) = pending.remove(&shared_index) {
                    let (anchor, template) = shared.get(&shared_index).expect("anchor inserted");
                    for target in waiting {
                        expand_shared_formula_into(
                            template,
                            (anchor.row, anchor.col),
                            (target.row, target.col),
                            &mut expansion,
                        )
                        .map_err(calamine::Error::Xlsx)?;
                        emit(target, &expansion, Some(shared_index))?;
                    }
                }
            }
            OwnedSpoolFormulaRecord::SharedDescendant {
                coord0,
                shared_index,
                ..
            } => {
                if skip_family(shared_index) {
                    continue;
                }
                if let Some((anchor, template)) = shared.get(&shared_index) {
                    expand_shared_formula_into(
                        template,
                        (anchor.row, anchor.col),
                        (coord0.row, coord0.col),
                        &mut expansion,
                    )
                    .map_err(calamine::Error::Xlsx)?;
                    emit(coord0, &expansion, Some(shared_index))?;
                } else {
                    pending.entry(shared_index).or_default().push(coord0);
                }
            }
            OwnedSpoolFormulaRecord::Unsupported { coord0, .. } => {
                return Err(SourceFormulaError::UnsupportedMetadata {
                    sheet_name: Arc::from(sheet_name),
                    coord0,
                }
                .into_calamine());
            }
        }
    }
    Ok(())
}

pub(super) fn replay_spool_per_cell_filtered<S: FormulaReplaySpool>(
    spool: &mut S,
    sheet_name: &str,
    mut skip_family: impl FnMut(usize) -> bool,
    mut emit: impl FnMut(SourceCoord, &str) -> Result<(), calamine::Error>,
) -> Result<(), calamine::Error> {
    replay_spool_with_family(spool, sheet_name, &mut skip_family, |coord, text, _| {
        emit(coord, text)
    })
}

#[cfg(test)]
pub(super) fn expand_source_events_per_cell(
    events: &[FormulaSourceEvent],
) -> Result<Vec<ExpandedFormulaCell>, SourceFormulaError> {
    let mut shared: rustc_hash::FxHashMap<usize, (SourceCoord, Arc<str>)> =
        rustc_hash::FxHashMap::default();
    let mut pending: rustc_hash::FxHashMap<usize, Vec<SourceCoord>> =
        rustc_hash::FxHashMap::default();
    let mut expanded = Vec::with_capacity(events.len());
    let mut expansion = String::with_capacity(128);

    for event in events {
        match &event.formula {
            FormulaSourceKind::Ordinary { formula, .. } => expanded.push(ExpandedFormulaCell {
                coord0: event.coord0,
                formula: Arc::clone(formula),
            }),
            FormulaSourceKind::SharedAnchor {
                family, formula, ..
            } => {
                expanded.push(ExpandedFormulaCell {
                    coord0: event.coord0,
                    formula: Arc::clone(formula),
                });
                shared.insert(family.source_index, (event.coord0, Arc::clone(formula)));
                if let Some(waiting) = pending.remove(&family.source_index) {
                    let (anchor, template) = shared
                        .get(&family.source_index)
                        .expect("shared anchor inserted");
                    for target in waiting {
                        expand_shared_formula_into(
                            template,
                            (anchor.row, anchor.col),
                            (target.row, target.col),
                            &mut expansion,
                        )
                        .map_err(SourceFormulaError::Expansion)?;
                        expanded.push(ExpandedFormulaCell {
                            coord0: target,
                            formula: Arc::from(expansion.as_str()),
                        });
                    }
                }
            }
            FormulaSourceKind::SharedDescendant { family, .. } => {
                if let Some((anchor, template)) = shared.get(&family.source_index) {
                    expand_shared_formula_into(
                        template,
                        (anchor.row, anchor.col),
                        (event.coord0.row, event.coord0.col),
                        &mut expansion,
                    )
                    .map_err(SourceFormulaError::Expansion)?;
                    expanded.push(ExpandedFormulaCell {
                        coord0: event.coord0,
                        formula: Arc::from(expansion.as_str()),
                    });
                } else {
                    pending
                        .entry(family.source_index)
                        .or_default()
                        .push(event.coord0);
                }
            }
            FormulaSourceKind::Unsupported { .. } => {
                return Err(SourceFormulaError::UnsupportedMetadata {
                    sheet_name: Arc::clone(&event.sheet_name),
                    coord0: event.coord0,
                });
            }
        }
    }

    // Missing anchors are intentionally omitted to preserve the existing oracle.
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(row: u32, col: u32) -> SourceCoord {
        SourceCoord { row, col }
    }

    #[test]
    fn versioned_round_trip_covers_all_records_and_max_coordinates() {
        let range = SourceRect {
            start: coord(0, 1),
            end: coord(u32::MAX, u32::MAX),
        };
        let mut spool = MemoryFormulaReplaySpool::with_max_bytes(4096);
        spool
            .append(SpoolFormulaRecord::Ordinary {
                sequence: 0,
                coord0: coord(u32::MAX, u32::MAX),
                text: "SUM(A1:B2)",
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::SharedAnchor {
                sequence: u64::MAX,
                coord0: coord(0, 1),
                shared_index: usize::MAX,
                declared_range: Some(range),
                text: "A1",
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::SharedDescendant {
                sequence: 2,
                coord0: coord(3, 4),
                shared_index: usize::MAX,
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::Unsupported {
                sequence: 3,
                coord0: coord(5, 6),
            })
            .unwrap();

        let records = spool
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 4);
        assert!(matches!(
            &records[0],
            OwnedSpoolFormulaRecord::Ordinary { sequence: 0, coord0, text }
                if *coord0 == coord(u32::MAX, u32::MAX) && text == "SUM(A1:B2)"
        ));
        assert!(matches!(
            &records[1],
            OwnedSpoolFormulaRecord::SharedAnchor {
                sequence: u64::MAX,
                shared_index: usize::MAX,
                declared_range: Some(found),
                ..
            } if *found == range
        ));
        assert_eq!(spool.storage_kind(), SpoolStorageKind::Memory);
        assert_eq!(spool.encoded_bytes(), spool.bytes.len() as u64);
    }

    #[test]
    fn rejects_bad_versions_malformed_varints_lengths_and_truncation() {
        let mut bad_header = MemoryFormulaReplaySpool::from_bytes(b"nope!".to_vec(), 64);
        assert_eq!(bad_header.replay().unwrap_err(), SpoolError::InvalidHeader);

        let mut bad_version =
            MemoryFormulaReplaySpool::from_bytes([MAGIC.as_slice(), &[VERSION + 1]].concat(), 64);
        assert_eq!(
            bad_version.replay().unwrap_err(),
            SpoolError::UnsupportedVersion(VERSION + 1)
        );

        for tail in [vec![0x80], vec![5, 0], vec![1, 99]] {
            let mut spool = MemoryFormulaReplaySpool::from_bytes(
                [MAGIC.as_slice(), &[VERSION], tail.as_slice()].concat(),
                64,
            );
            assert!(spool.replay().unwrap().next().unwrap().is_err());
        }

        let overflow_varint = vec![0xff; 11];
        let mut spool = MemoryFormulaReplaySpool::from_bytes(
            [MAGIC.as_slice(), &[VERSION], overflow_varint.as_slice()].concat(),
            64,
        );
        assert!(matches!(
            spool.replay().unwrap().next().unwrap(),
            Err(SpoolError::Malformed("varint overflow"))
        ));
    }

    #[test]
    fn checked_offsets_memory_cap_and_failure_injection_are_typed() {
        assert_eq!(
            checked_next_offset(u64::MAX, 1),
            Err(SpoolError::OffsetOverflow)
        );

        let mut limited = MemoryFormulaReplaySpool::with_max_bytes(HEADER_LEN as u64 + 2);
        assert!(matches!(
            limited.append(SpoolFormulaRecord::Ordinary {
                sequence: 0,
                coord0: coord(0, 0),
                text: "too large",
            }),
            Err(SpoolError::MemoryLimit { .. })
        ));

        let mut append_failure = MemoryFormulaReplaySpool::with_max_bytes(64);
        append_failure.fail_append = true;
        assert_eq!(
            append_failure.append(SpoolFormulaRecord::Unsupported {
                sequence: 0,
                coord0: coord(0, 0),
            }),
            Err(SpoolError::InjectedAppendFailure)
        );

        let mut replay_failure = MemoryFormulaReplaySpool::with_max_bytes(64);
        replay_failure.fail_replay = true;
        assert_eq!(
            replay_failure.replay().unwrap_err(),
            SpoolError::InjectedReplayFailure
        );
    }

    fn hybrid_limits(
        sheet_bytes: u64,
        workbook_bytes_remaining: u64,
        memory_prefix_bytes: u64,
        memory_only_bytes: u64,
        allow_disk: bool,
    ) -> FormulaSpoolLimits {
        FormulaSpoolLimits {
            sheet_bytes,
            workbook_bytes_remaining,
            workbook_bytes_used: 100,
            memory_prefix_bytes,
            memory_only_bytes,
            allow_disk,
            spill_files_remaining: 1,
            spill_files_limit: 1,
        }
    }

    fn ordinary(text: &str) -> SpoolFormulaRecord<'_> {
        SpoolFormulaRecord::Ordinary {
            sequence: 0,
            coord0: coord(0, 0),
            text,
        }
    }

    #[test]
    fn hybrid_stack_encoder_round_trips_every_record_kind() {
        let mut spool =
            HybridFormulaReplaySpool::new(hybrid_limits(4096, 4096, u64::MAX, 4096, false));
        let range = SourceRect {
            start: coord(1, 2),
            end: coord(4, 5),
        };
        for record in [
            SpoolFormulaRecord::Ordinary {
                sequence: 1,
                coord0: coord(1, 2),
                text: "A1+1",
            },
            SpoolFormulaRecord::SharedAnchor {
                sequence: 2,
                coord0: coord(3, 4),
                shared_index: 99,
                declared_range: Some(range),
                text: "$A$1+7",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 3,
                coord0: coord(4, 4),
                shared_index: 99,
            },
            SpoolFormulaRecord::Unsupported {
                sequence: 4,
                coord0: coord(u32::MAX, u32::MAX),
            },
        ] {
            spool.append(record).unwrap();
        }
        let records = spool
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 4);
        assert!(matches!(
            records[0],
            OwnedSpoolFormulaRecord::Ordinary { .. }
        ));
        assert!(matches!(
            records[1],
            OwnedSpoolFormulaRecord::SharedAnchor { .. }
        ));
        assert!(matches!(
            records[2],
            OwnedSpoolFormulaRecord::SharedDescendant { .. }
        ));
        assert!(matches!(
            records[3],
            OwnedSpoolFormulaRecord::Unsupported { .. }
        ));
    }

    #[test]
    fn hybrid_limits_are_checked_at_exact_encoded_boundaries() {
        let frame_len = encode_frame(ordinary("x")).unwrap().len() as u64;
        let exact = HEADER_LEN as u64 + frame_len;
        let mut spool =
            HybridFormulaReplaySpool::new(hybrid_limits(exact, exact, u64::MAX, exact, false));
        spool.append(ordinary("x")).unwrap();

        let mut sheet_limited =
            HybridFormulaReplaySpool::new(hybrid_limits(exact - 1, exact, u64::MAX, exact, false));
        assert!(matches!(
            sheet_limited.append(ordinary("x")),
            Err(SpoolError::SheetLimit { .. })
        ));

        let mut workbook_limited =
            HybridFormulaReplaySpool::new(hybrid_limits(exact, exact - 1, u64::MAX, exact, false));
        assert!(matches!(
            workbook_limited.append(ordinary("x")),
            Err(SpoolError::WorkbookLimit { .. })
        ));
    }

    #[test]
    fn million_descendant_appends_use_no_per_record_heap_scratch() {
        let mut spool = HybridFormulaReplaySpool::new(hybrid_limits(
            64 * 1024 * 1024,
            64 * 1024 * 1024,
            u64::MAX,
            64 * 1024 * 1024,
            false,
        ));
        for row in 0..1_000_000 {
            spool
                .append(SpoolFormulaRecord::SharedDescendant {
                    sequence: u64::from(row),
                    coord0: coord(row, 0),
                    shared_index: 7,
                })
                .unwrap();
        }
        assert_eq!(spool.append_scratch_heap_allocations(), 0);
        assert!(spool.encoded_bytes() < 32 * 1024 * 1024);
    }

    #[test]
    fn memory_only_policy_is_bounded_without_silent_spill() {
        let mut spool =
            HybridFormulaReplaySpool::new(hybrid_limits(4096, 4096, HEADER_LEN as u64, 24, false));
        assert!(matches!(
            spool.append(ordinary("a formula longer than the cap")),
            Err(SpoolError::MemoryLimit { .. })
        ));
        assert_eq!(spool.storage_kind(), SpoolStorageKind::Memory);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_spill_releases_prefix_replays_and_uses_owner_only_cleanup() {
        let mut spool = HybridFormulaReplaySpool::new(hybrid_limits(4096, 4096, 6, 64, true));
        spool.append(ordinary("A1+1")).unwrap();
        assert_eq!(spool.storage_kind(), SpoolStorageKind::NativeFile);
        assert_eq!(spool.memory.len(), 0);
        assert_eq!(spool.memory_capacity(), 0);
        let path = spool.file.as_ref().unwrap().path().to_path_buf();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        let records = spool
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            matches!(&records[..], [OwnedSpoolFormulaRecord::Ordinary { text, .. }] if text == "A1+1")
        );
        drop(spool);
        assert!(!path.exists());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_spill_surfaces_injected_write_and_replay_io() {
        let mut spool = HybridFormulaReplaySpool::new(hybrid_limits(4096, 4096, 5, 64, true));
        spool.fail_write = true;
        assert_eq!(
            spool.append(ordinary("x")),
            Err(SpoolError::Io(std::io::ErrorKind::WriteZero))
        );
        spool.fail_write = false;
        spool.append(ordinary("x")).unwrap();
        spool.fail_replay_io = true;
        assert_eq!(
            spool.replay().unwrap_err(),
            SpoolError::Io(std::io::ErrorKind::Other)
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_spill_cleans_up_during_unwind_and_enforces_file_count() {
        let mut no_files = hybrid_limits(4096, 4096, 5, 64, true);
        no_files.spill_files_remaining = 0;
        let mut spool = HybridFormulaReplaySpool::new(no_files);
        assert!(matches!(
            spool.append(ordinary("x")),
            Err(SpoolError::FileLimit { .. })
        ));

        let path = std::sync::Mutex::new(None);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut spool = HybridFormulaReplaySpool::new(hybrid_limits(4096, 4096, 5, 64, true));
            spool.append(ordinary("x")).unwrap();
            *path.lock().unwrap() = Some(spool.file.as_ref().unwrap().path().to_path_buf());
            panic!("injected unwind");
        }));
        assert!(result.is_err());
        assert!(!path.lock().unwrap().as_ref().unwrap().exists());
    }

    type ReplayedFormula = ((u32, u32), String);

    fn replay_production(
        records: Vec<SpoolFormulaRecord<'static>>,
    ) -> Result<Vec<ReplayedFormula>, calamine::Error> {
        let mut spool =
            HybridFormulaReplaySpool::new(hybrid_limits(16_384, 16_384, u64::MAX, 16_384, false));
        for record in records {
            spool.append(record).unwrap();
        }
        let mut output = Vec::new();
        replay_spool_per_cell_filtered(
            &mut spool,
            "Sheet1",
            |_| false,
            |coord, text| {
                output.push(((coord.row, coord.col), text.to_string()));
                Ok(())
            },
        )?;
        Ok(output)
    }

    #[test]
    fn replayed_events_preserve_oracle_order_and_missing_anchor_omission() {
        let mut spool = MemoryFormulaReplaySpool::with_max_bytes(1024);
        spool
            .append(SpoolFormulaRecord::SharedDescendant {
                sequence: 0,
                coord0: coord(2, 1),
                shared_index: 7,
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::Ordinary {
                sequence: 1,
                coord0: coord(0, 4),
                text: "1+1",
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::SharedAnchor {
                sequence: 2,
                coord0: coord(1, 1),
                shared_index: 7,
                declared_range: None,
                text: "A2",
            })
            .unwrap();
        spool
            .append(SpoolFormulaRecord::SharedDescendant {
                sequence: 3,
                coord0: coord(9, 9),
                shared_index: 99,
            })
            .unwrap();

        let events = spool.replay_events("Sheet1", 42).unwrap();
        let expanded = expand_source_events_per_cell(&events).unwrap();
        let oracle: Vec<_> = expanded
            .into_iter()
            .map(|cell| ((cell.coord0.row, cell.coord0.col), cell.formula.to_string()))
            .collect();
        let mut production = Vec::new();
        replay_spool_per_cell_filtered(
            &mut spool,
            "Sheet1",
            |_| false,
            |coord, text| {
                production.push(((coord.row, coord.col), text.to_string()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(production, oracle);
        assert_eq!(
            production,
            vec![
                ((0, 4), "1+1".to_string()),
                ((1, 1), "A2".to_string()),
                ((2, 1), "A3".to_string()),
            ]
        );
    }

    #[test]
    fn production_replay_preserves_duplicate_anchor_member_and_ordinary_order() {
        let output = replay_production(vec![
            SpoolFormulaRecord::Ordinary {
                sequence: 0,
                coord0: coord(2, 3),
                text: "1+1",
            },
            SpoolFormulaRecord::Ordinary {
                sequence: 1,
                coord0: coord(2, 3),
                text: "2+2",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 2,
                coord0: coord(1, 1),
                shared_index: 4,
            },
            SpoolFormulaRecord::SharedAnchor {
                sequence: 3,
                coord0: coord(0, 1),
                shared_index: 4,
                declared_range: None,
                text: "A1",
            },
            SpoolFormulaRecord::SharedAnchor {
                sequence: 4,
                coord0: coord(4, 1),
                shared_index: 4,
                declared_range: None,
                text: "A5*10",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 5,
                coord0: coord(5, 1),
                shared_index: 4,
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 6,
                coord0: coord(5, 1),
                shared_index: 4,
            },
        ])
        .unwrap();
        assert_eq!(
            output,
            vec![
                ((2, 3), "1+1".to_string()),
                ((2, 3), "2+2".to_string()),
                ((0, 1), "A1".to_string()),
                ((1, 1), "A2".to_string()),
                ((4, 1), "A5*10".to_string()),
                ((5, 1), "A6*10".to_string()),
                ((5, 1), "A6*10".to_string()),
            ]
        );
    }

    #[test]
    fn production_replay_never_synthesizes_declared_holes_or_clips_members() {
        let range = SourceRect {
            start: coord(0, 1),
            end: coord(3, 1),
        };
        let output = replay_production(vec![
            SpoolFormulaRecord::SharedAnchor {
                sequence: 0,
                coord0: coord(0, 1),
                shared_index: 1,
                declared_range: Some(range),
                text: "A1",
            },
            SpoolFormulaRecord::Ordinary {
                sequence: 1,
                coord0: coord(1, 1),
                text: "99",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 2,
                coord0: coord(3, 1),
                shared_index: 1,
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 3,
                coord0: coord(5, 1),
                shared_index: 1,
            },
        ])
        .unwrap();
        assert_eq!(
            output,
            vec![
                ((0, 1), "A1".to_string()),
                ((1, 1), "99".to_string()),
                ((3, 1), "A4".to_string()),
                ((5, 1), "A6".to_string()),
            ]
        );
    }

    #[test]
    fn production_replay_preserves_empty_anchor_boundaries_and_unsupported_errors() {
        let max = coord(1_048_575, 16_383);
        let output = replay_production(vec![
            SpoolFormulaRecord::SharedAnchor {
                sequence: 0,
                coord0: coord(0, 0),
                shared_index: 1,
                declared_range: None,
                text: "",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 1,
                coord0: coord(1, 0),
                shared_index: 1,
            },
            SpoolFormulaRecord::SharedAnchor {
                sequence: 2,
                coord0: max,
                shared_index: 8,
                declared_range: None,
                text: "$XFD$1048576+XFD1048576",
            },
            SpoolFormulaRecord::SharedDescendant {
                sequence: 3,
                coord0: max,
                shared_index: 8,
            },
        ])
        .unwrap();
        assert_eq!(output[0], ((0, 0), String::new()));
        assert_eq!(output[1], ((1, 0), String::new()));
        assert_eq!(output[2].1, "$XFD$1048576+XFD1048576");
        assert_eq!(output[3].1, "$XFD$1048576+XFD1048576");

        let error = replay_production(vec![SpoolFormulaRecord::Unsupported {
            sequence: 0,
            coord0: coord(4, 6),
        }])
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported Calamine formula metadata at Sheet1!R5C7")
        );
    }
}
