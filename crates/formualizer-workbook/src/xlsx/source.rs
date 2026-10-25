use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{Arc, Mutex},
};
struct SharedFile {
    file: Mutex<File>,
    len: u64,
}

/// Per-cursor readahead window for the shared-file policy.
///
/// ZIP central-directory and part reads are small and mostly forward, so an
/// unbuffered cursor turns each into its own lock/seek/read syscall. The window
/// is keyed by absolute offset, so a seek that lands inside it is still served
/// from memory.
///
/// This widens the window in which a cursor can serve bytes a concurrent writer
/// has since changed. `SharedFile` was never a snapshot, so no guarantee moves:
/// a concurrently mutated file still yields ordinary EOF/I/O/ZIP errors or
/// ordinary stale bytes, never a mapped-memory fault.
const SHARED_FILE_READAHEAD: usize = 64 * 1024;

#[derive(Clone)]
struct SharedFileCursor {
    source: Arc<SharedFile>,
    position: u64,
    /// Bytes covering `[buffer_start, buffer_start + buffer.len())`.
    buffer: Vec<u8>,
    buffer_start: u64,
}

impl SharedFileCursor {
    fn new(source: Arc<SharedFile>) -> Self {
        Self {
            source,
            position: 0,
            buffer: Vec::new(),
            buffer_start: 0,
        }
    }

    /// Serves `buf` from the readahead window when it covers `self.position`.
    fn read_buffered(&mut self, buf: &mut [u8]) -> Option<usize> {
        let offset = usize::try_from(self.position.checked_sub(self.buffer_start)?).ok()?;
        let available = self.buffer.len().checked_sub(offset)?;
        if available == 0 {
            return None;
        }
        let read = buf.len().min(available);
        buf[..read].copy_from_slice(&self.buffer[offset..offset + read]);
        self.position += read as u64;
        Some(read)
    }

    /// One locked seek+read. Fills the readahead window unless `direct` is set,
    /// in which case the caller's larger buffer is filled straight through.
    fn read_physical(&mut self, buf: &mut [u8], limit: usize) -> std::io::Result<usize> {
        let mut file = self
            .source
            .file
            .lock()
            .map_err(|_| std::io::Error::other("shared XLSX file lock poisoned"))?;
        file.seek(SeekFrom::Start(self.position))?;
        if limit >= SHARED_FILE_READAHEAD {
            let read = file.read(&mut buf[..limit])?;
            drop(file);
            self.position += read as u64;
            self.buffer.clear();
            return Ok(read);
        }
        let window = limit.max(
            SHARED_FILE_READAHEAD
                .min(usize::try_from(self.source.len - self.position).unwrap_or(usize::MAX)),
        );
        self.buffer.resize(window, 0);
        let filled = file.read(&mut self.buffer[..window])?;
        drop(file);
        self.buffer.truncate(filled);
        self.buffer_start = self.position;
        Ok(self.read_buffered(buf).unwrap_or(0))
    }
}

#[cfg(any(unix, windows))]
struct MappedXlsxSource {
    // The handle pins the opened file rather than its path. It does not prevent
    // another process from truncating the file on Unix; see XlsxPathSource::DirectMmap.
    _file: File,
    map: memmap2::Mmap,
}

#[derive(Clone)]
enum SharedXlsxReader {
    Bytes {
        bytes: Arc<[u8]>,
        position: u64,
    },
    File(SharedFileCursor),
    #[cfg(any(unix, windows))]
    Mapped {
        source: Arc<MappedXlsxSource>,
        position: u64,
    },
}

/// Selects the retained backing source used for an XLSX filesystem path.
///
/// This policy applies only to path-based XLSX loads. Byte and reader
/// inputs always retain one shared immutable byte allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum XlsxPathSource {
    /// Retain one opened file and serialize physical seeks and reads while
    /// giving each XLSX consumer an independent logical cursor.
    ///
    /// This mutation-safe option is the default. Renaming or replacing the
    /// pathname is safe because the opened handle is retained.
    #[default]
    SharedFile,
    /// Retain the opened file and map it read-only.
    ///
    /// Mapping is attempted explicitly and errors are returned without falling
    /// back to [`SharedFile`](Self::SharedFile). The caller guarantees that the
    /// opened underlying file/inode is not destructively modified or truncated
    /// for the adapter's lifetime. Violating this contract can terminate the
    /// process; in particular, Unix may deliver `SIGBUS` when a mapped page past
    /// a new end-of-file is accessed. Renaming or replacing the pathname is safe
    /// because the original opened handle remains retained.
    DirectMmap,
}

fn seek_position(position: u64, len: u64, from: SeekFrom) -> std::io::Result<u64> {
    let next = match from {
        SeekFrom::Start(next) => Some(next),
        SeekFrom::Current(offset) => position.checked_add_signed(offset),
        SeekFrom::End(offset) => len.checked_add_signed(offset),
    };
    next.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek"))
}

fn read_slice(source: &[u8], position: &mut u64, buf: &mut [u8]) -> std::io::Result<usize> {
    let start = usize::try_from(*position).unwrap_or(usize::MAX);
    if start >= source.len() {
        return Ok(0);
    }
    let read = buf.len().min(source.len() - start);
    buf[..read].copy_from_slice(&source[start..start + read]);
    *position = position
        .checked_add(read as u64)
        .ok_or_else(|| std::io::Error::other("reader position overflow"))?;
    Ok(read)
}

/// Retained source access shared by XLSX readers without reopening paths.
#[cfg(feature = "xlsx-recalc")]
pub(crate) struct XlsxSource(SharedXlsxReader);
#[cfg(feature = "xlsx-recalc")]
impl XlsxSource {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(SharedXlsxReader::from_bytes(bytes))
    }
    pub(crate) fn open_path(
        path: &Path,
        policy: XlsxPathSource,
        limit: usize,
    ) -> std::io::Result<Self> {
        let file = File::open(path)?;
        if file.metadata()?.len() > limit as u64 {
            return Err(std::io::Error::other("XLSX input byte limit exceeded"));
        }
        SharedXlsxReader::from_file(file, policy).map(Self)
    }
    pub(crate) fn bytes(
        &self,
        options: &super::options::ReadOptions,
    ) -> Result<std::borrow::Cow<'_, [u8]>, crate::IoError> {
        let limit = options.limits.max_input_bytes;
        super::options::checkpoint(&options.cancel)?;
        let borrowed = match &self.0 {
            SharedXlsxReader::Bytes { bytes, .. } => Some(bytes.as_ref()),
            #[cfg(any(unix, windows))]
            SharedXlsxReader::Mapped { source, .. } => Some(source.map.as_ref()),
            SharedXlsxReader::File(_) => None,
        };
        if let Some(bytes) = borrowed {
            if bytes.len() > limit {
                return Err(std::io::Error::other("XLSX input byte limit exceeded").into());
            }
            return Ok(std::borrow::Cow::Borrowed(bytes));
        }
        Ok(std::borrow::Cow::Owned(read_input(
            self.0.reader(),
            options,
        )?))
    }
}

// Compute requested growth without trusting archive size hints. The caller's
// byte allowance bounds reservation targets; allocator rounding is not an RSS cap.
fn bounded_capacity(current: usize, required: usize, limit: usize) -> Option<usize> {
    if required > limit {
        return None;
    }
    if required <= current {
        return Some(current.min(limit));
    }
    Some(
        current
            .checked_mul(2)
            .unwrap_or(limit)
            .max(required)
            .min(limit),
    )
}

pub(crate) fn reserve_bounded(
    bytes: &mut Vec<u8>,
    additional: usize,
    limit: usize,
) -> Result<(), crate::IoError> {
    let required = bytes
        .len()
        .checked_add(additional)
        .ok_or_else(|| super::options::unsupported("buffer length overflow", "XLSX read"))?;
    let target = bounded_capacity(bytes.capacity(), required, limit)
        .ok_or_else(|| super::options::unsupported("buffer allocation limit", "XLSX read"))?;
    if target > bytes.capacity() {
        bytes
            .try_reserve_exact(target - bytes.len())
            .map_err(|error| crate::IoError::from_backend("xlsx allocation", error))?;
    }
    Ok(())
}

pub(crate) fn read_input(
    mut reader: impl Read,
    options: &super::options::ReadOptions,
) -> Result<Vec<u8>, crate::IoError> {
    let mut bytes = Vec::new();
    let mut scratch = [0; 64 * 1024];
    loop {
        super::options::checkpoint(&options.cancel)?;
        let remaining = options.limits.max_input_bytes - bytes.len();
        let window = remaining.saturating_add(1).min(scratch.len());
        let n = reader.read(&mut scratch[..window])?;
        if n > remaining {
            return Err(super::options::unsupported(
                "input byte limit",
                "XLSX source",
            ));
        }
        if n == 0 {
            super::options::checkpoint(&options.cancel)?;
            return Ok(bytes);
        }
        reserve_bounded(&mut bytes, n, options.limits.max_input_bytes)?;
        bytes.extend_from_slice(&scratch[..n]);
    }
}

impl SharedXlsxReader {
    fn from_bytes(data: Vec<u8>) -> Self {
        Self::Bytes {
            bytes: Arc::from(data),
            position: 0,
        }
    }

    fn from_file(file: File, policy: XlsxPathSource) -> Result<Self, std::io::Error> {
        let len = file.metadata()?.len();
        match policy {
            XlsxPathSource::SharedFile => {
                Ok(Self::File(SharedFileCursor::new(Arc::new(SharedFile {
                    file: Mutex::new(file),
                    len,
                }))))
            }
            XlsxPathSource::DirectMmap => {
                #[cfg(any(unix, windows))]
                {
                    if len == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "cannot memory-map an empty XLSX file",
                        ));
                    }
                    // SAFETY: the map is read-only and its source handle remains
                    // alive. The caller accepts the mutation/truncation contract
                    // documented on `XlsxPathSource::DirectMmap`.
                    let map = unsafe { memmap2::MmapOptions::new().map(&file) }?;
                    Ok(Self::Mapped {
                        source: Arc::new(MappedXlsxSource { _file: file, map }),
                        position: 0,
                    })
                }
                #[cfg(not(any(unix, windows)))]
                {
                    let _ = (file, len);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "DirectMmap XLSX path loading is unsupported on this target",
                    ))
                }
            }
        }
    }

    fn reader(&self) -> Self {
        match self {
            Self::Bytes { bytes, .. } => Self::Bytes {
                bytes: Arc::clone(bytes),
                position: 0,
            },
            Self::File(cursor) => Self::File(SharedFileCursor::new(Arc::clone(&cursor.source))),
            #[cfg(any(unix, windows))]
            Self::Mapped { source, .. } => Self::Mapped {
                source: Arc::clone(source),
                position: 0,
            },
        }
    }

    #[cfg(test)]
    fn bytes_backing_ptr(&self) -> Option<*const u8> {
        match self {
            Self::Bytes { bytes, .. } => Some(bytes.as_ptr()),
            _ => None,
        }
    }

    #[cfg(test)]
    fn strong_count(&self) -> usize {
        match self {
            Self::Bytes { bytes, .. } => Arc::strong_count(bytes),
            Self::File(cursor) => Arc::strong_count(&cursor.source),
            #[cfg(any(unix, windows))]
            Self::Mapped { source, .. } => Arc::strong_count(source),
        }
    }
}

impl Read for SharedXlsxReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Bytes { bytes, position } => read_slice(bytes, position, buf),
            Self::File(cursor) => {
                if buf.is_empty() || cursor.position >= cursor.source.len {
                    return Ok(0);
                }
                if let Some(read) = cursor.read_buffered(buf) {
                    return Ok(read);
                }
                let remaining = cursor.source.len - cursor.position;
                let limit = buf
                    .len()
                    .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                cursor.read_physical(buf, limit)
            }
            #[cfg(any(unix, windows))]
            Self::Mapped { source, position } => read_slice(&source.map, position, buf),
        }
    }
}

impl Seek for SharedXlsxReader {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        let position = match self {
            Self::Bytes { bytes, position } => {
                *position = seek_position(*position, bytes.len() as u64, from)?;
                *position
            }
            Self::File(cursor) => {
                cursor.position = seek_position(cursor.position, cursor.source.len, from)?;
                cursor.position
            }
            #[cfg(any(unix, windows))]
            Self::Mapped { source, position } => {
                *position = seek_position(*position, source.map.len() as u64, from)?;
                *position
            }
        };
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_capacity_handles_zero_tiny_exact_and_host_bounds() {
        assert_eq!(bounded_capacity(0, 0, 0), Some(0));
        assert_eq!(bounded_capacity(0, 1, 0), None);
        assert_eq!(bounded_capacity(0, 1, 1), Some(1));
        assert_eq!(bounded_capacity(1, 2, 3), Some(2));
        assert_eq!(bounded_capacity(2, 3, 3), Some(3));
        assert_eq!(bounded_capacity(3, 3, 3), Some(3));
        assert_eq!(bounded_capacity(3, 4, 3), None);
        let current = usize::MAX / 2 + 1;
        assert_eq!(
            bounded_capacity(current, current + 1, usize::MAX),
            Some(usize::MAX)
        );
        assert_eq!(
            bounded_capacity(0, isize::MAX as usize, isize::MAX as usize),
            Some(isize::MAX as usize)
        );
    }

    #[test]
    fn bounded_capacity_growth_is_logarithmic_and_never_exceeds_allowance() {
        let limit = 128 * 1024 * 1024;
        let chunk = 64 * 1024;
        let mut capacity = 0;
        let mut growths = 0;
        for required in (chunk..=limit).step_by(chunk) {
            let next = bounded_capacity(capacity, required, limit).unwrap();
            assert!(next >= required && next <= limit);
            growths += usize::from(next != capacity);
            capacity = next;
        }
        assert_eq!(capacity, limit);
        assert_eq!(
            growths, 12,
            "64 KiB to 128 MiB requires only doubling steps"
        );
    }

    #[test]
    fn shared_file_cursors_keep_independent_positions_under_concurrency() {
        use std::io::Write;

        let mut file = tempfile::tempfile().unwrap();
        let data: Vec<u8> = (0..=255).cycle().take(16 * 1024).collect();
        file.write_all(&data).unwrap();
        let source = SharedXlsxReader::File(SharedFileCursor::new(Arc::new(SharedFile {
            file: Mutex::new(file),
            len: data.len() as u64,
        })));

        std::thread::scope(|scope| {
            for thread_index in 0..8_u64 {
                let mut reader = source.reader();
                let data = &data;
                scope.spawn(move || {
                    for round in 0..200_u64 {
                        let offset = (thread_index * 997 + round * 43) % 16_000;
                        reader.seek(SeekFrom::Start(offset)).unwrap();
                        let mut actual = [0_u8; 31];
                        reader.read_exact(&mut actual).unwrap();
                        assert_eq!(&actual, &data[offset as usize..offset as usize + 31]);
                        assert_eq!(reader.stream_position().unwrap(), offset + 31);
                    }
                });
            }
        });
    }
    #[test]
    fn shared_file_readahead_serves_seeks_within_the_window() {
        use std::io::Write;

        let mut file = tempfile::tempfile().unwrap();
        let data: Vec<u8> = (0..=255).cycle().take(4 * SHARED_FILE_READAHEAD).collect();
        file.write_all(&data).unwrap();
        let source = SharedXlsxReader::File(SharedFileCursor::new(Arc::new(SharedFile {
            file: Mutex::new(file),
            len: data.len() as u64,
        })));

        let mut reader = source.reader();
        for offset in [0_u64, 4096, 1, 64, 4095, 8192, 200_000, 199_999] {
            reader.seek(SeekFrom::Start(offset)).unwrap();
            let mut actual = [0_u8; 17];
            reader.read_exact(&mut actual).unwrap();
            assert_eq!(&actual, &data[offset as usize..offset as usize + 17]);
            assert_eq!(reader.stream_position().unwrap(), offset + 17);
        }

        // A request larger than the window bypasses it and still reads exactly.
        reader.seek(SeekFrom::Start(3)).unwrap();
        let mut large = vec![0_u8; 2 * SHARED_FILE_READAHEAD];
        reader.read_exact(&mut large).unwrap();
        assert_eq!(&large[..], &data[3..3 + 2 * SHARED_FILE_READAHEAD]);
    }
    #[test]
    fn bounded_source_reads_poll_cancellation_between_chunks() {
        struct Cancelling {
            cancel: crate::CancelToken,
            reads: usize,
        }
        impl Read for Cancelling {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                self.reads += 1;
                assert_eq!(self.reads, 1, "must stop before another input chunk");
                out.fill(b'x');
                self.cancel.cancel();
                Ok(out.len())
            }
        }
        let cancel = crate::CancelToken::new();
        let options = super::super::options::ReadOptions {
            cancel: Some(cancel.clone()),
            ..Default::default()
        };
        assert!(read_input(Cancelling { cancel, reads: 0 }, &options).is_err());
        for (size, limit, success) in [(0, 0, true), (1, 0, false), (3, 3, true), (4, 3, false)] {
            let mut options = options.clone();
            options.cancel = None;
            options.limits.max_input_bytes = limit;
            assert_eq!(
                read_input(std::io::Cursor::new(vec![0; size]), &options).is_ok(),
                success
            );
        }
    }
    #[test]
    fn immutable_byte_cursors_share_one_allocation() {
        let source = SharedXlsxReader::from_bytes(vec![1, 2, 3]);
        let mut reader = source.reader();
        assert_eq!(source.strong_count(), 2);
        assert_eq!(source.bytes_backing_ptr(), reader.bytes_backing_ptr());
        let mut byte = [0];
        reader.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [1]);
        let mut independent = source.reader();
        independent.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [1]);
    }
}
