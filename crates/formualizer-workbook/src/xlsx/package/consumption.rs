//! Ordinary selected-output accounting. A budget moves from the opening
//! archive to its adapter; lazy consumers borrow it and cannot refresh it.
use crate::{
    IoError,
    xlsx::options::{ReadOptions, checkpoint, unsupported},
};
use std::{
    io::{Read, Seek},
    sync::OnceLock,
};

#[derive(Default)]
pub(crate) struct ConsumptionBudget {
    pub(crate) emitted: usize,
    failed: OnceLock<()>,
}
impl ConsumptionBudget {
    pub(crate) fn check(&self) -> Result<(), IoError> {
        if self.failed.get().is_some() {
            Err(unsupported("terminal XLSX read failure", "ordinary load"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn fail(&self) {
        let _ = self.failed.set(());
    }
}

pub(crate) fn read_selected<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    name: &str,
    options: &ReadOptions,
    budget: &mut ConsumptionBudget,
) -> Result<Vec<u8>, IoError> {
    budget.check()?;
    let result = (|| {
        checkpoint(&options.cancel)?;
        let mut file = archive
            .by_name(name)
            .map_err(|e| IoError::from_backend("zip", e))?;
        #[cfg(test)]
        super::record_inflate(file.name());
        let declared = file.size();
        let mut bytes = Vec::new();
        let buffer_limit = options.limits.max_worksheet_bytes.min(
            options
                .limits
                .max_expanded_bytes
                .saturating_sub(budget.emitted),
        );
        let mut scratch = [0u8; 64 * 1024];
        loop {
            checkpoint(&options.cancel)?;
            let aggregate = options
                .limits
                .max_expanded_bytes
                .checked_sub(budget.emitted)
                .ok_or_else(|| unsupported("expanded byte limit", name))?;
            let part = options
                .limits
                .max_worksheet_bytes
                .checked_sub(bytes.len())
                .ok_or_else(|| unsupported("XML part byte limit", name))?;
            // A nonempty probe is essential even at zero/exact allowance:
            // only the decoder's true EOF validates CRC and declared size.
            let window = aggregate.min(part).saturating_add(1).min(scratch.len());
            let n = file.read(&mut scratch[..window])?;
            budget.emitted = budget
                .emitted
                .checked_add(n)
                .ok_or_else(|| unsupported("expanded byte count overflow", name))?;
            if n > aggregate {
                return Err(unsupported("expanded byte limit", name));
            }
            if n > part {
                return Err(unsupported("XML part byte limit", name));
            }
            if n == 0 {
                if u64::try_from(bytes.len()).ok() != Some(declared) {
                    return Err(unsupported(
                        "ZIP actual size differs from declaration",
                        name,
                    ));
                }
                checkpoint(&options.cancel)?;
                return Ok(bytes);
            }
            crate::xlsx::source::reserve_bounded(&mut bytes, n, buffer_limit)?;
            bytes.extend_from_slice(&scratch[..n]);
        }
    })();
    if result.is_err() {
        budget.fail();
    }
    result
}
