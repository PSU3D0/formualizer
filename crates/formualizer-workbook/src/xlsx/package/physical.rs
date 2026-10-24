//! Audit raw physical identities before the permissive archive resolves names.
//! A bounded synthetic directory selects one original member without copying
//! its payload or allowing a duplicate name to hide it from the ZIP decoder.
use super::*;
use std::io::{self, Seek, SeekFrom};

struct MemberView<'a> {
    body: &'a [u8],
    tail: Vec<u8>,
    position: u64,
}
impl<'a> MemberView<'a> {
    fn new(bytes: &'a [u8], member: &Member) -> Result<Self, IoError> {
        let body = &bytes[member.local..member.end];
        let length = u32::try_from(body.len())
            .map_err(|_| unsupported("ZIP32 member extent", "XLSX package"))?;
        let mut tail = bytes[member.central..member.central + member.central_len].to_vec();
        // The borrowed local record starts at zero in this one-member view.
        // CRC, method, flags, sizes, names and extra fields remain unchanged.
        tail[42..46].copy_from_slice(&0u32.to_le_bytes());
        tail.extend_from_slice(b"PK\x05\x06");
        for value in [0u16, 0, 1, 1] {
            tail.extend_from_slice(&value.to_le_bytes());
        }
        tail.extend_from_slice(&(member.central_len as u32).to_le_bytes());
        tail.extend_from_slice(&length.to_le_bytes());
        tail.extend_from_slice(&0u16.to_le_bytes());
        Ok(Self {
            body,
            tail,
            position: 0,
        })
    }
    fn len(&self) -> u64 {
        self.body.len() as u64 + self.tail.len() as u64
    }
}
impl Read for MemberView<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.len() {
            return Ok(0);
        }
        let source = if self.position < self.body.len() as u64 {
            &self.body[self.position as usize..]
        } else {
            &self.tail[(self.position - self.body.len() as u64) as usize..]
        };
        let count = output.len().min(source.len());
        output[..count].copy_from_slice(&source[..count]);
        self.position += count as u64;
        Ok(count)
    }
}
impl Seek for MemberView<'_> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::Current(delta) => i128::from(self.position) + i128::from(delta),
            SeekFrom::End(delta) => i128::from(self.len()) + i128::from(delta),
        };
        self.position = u64::try_from(position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid ZIP member seek"))?;
        Ok(self.position)
    }
}

pub(super) fn audit(
    bytes: &[u8],
    directory: &Directory,
    options: &ReadOptions,
) -> Result<(), IoError> {
    let mut total = 0usize;
    let mut buffer = [0u8; 64 * 1024];
    for member in &directory.members {
        checkpoint(&options.cancel)?;
        let view = MemberView::new(bytes, member)?;
        let mut archive =
            ZipArchive::new(view).map_err(|error| IoError::from_backend("zip", error))?;
        let mut file = archive
            .by_index(0)
            .map_err(|error| IoError::from_backend("zip", error))?;
        #[cfg(test)]
        record_inflate(file.name());
        let mut actual = 0usize;
        loop {
            checkpoint(&options.cancel)?;
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            actual = actual
                .checked_add(count)
                .ok_or_else(|| unsupported("expanded byte overflow", "XLSX package"))?;
            total = total
                .checked_add(count)
                .ok_or_else(|| unsupported("expanded byte overflow", "XLSX package"))?;
            if actual > options.limits.max_expanded_bytes
                || total > options.limits.max_expanded_bytes
            {
                return Err(unsupported("actual ZIP expansion limit", "XLSX package"));
            }
        }
        if actual as u64 != file.size() {
            return Err(unsupported("ZIP expanded-size mismatch", file.name()));
        }
    }
    Ok(())
}
