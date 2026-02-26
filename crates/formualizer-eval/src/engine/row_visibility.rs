#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowVisibilitySource {
    Manual,
    Filter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VisibilityMaskMode {
    IncludeAll,
    ExcludeManualHidden,
    ExcludeFilterHidden,
    ExcludeManualOrFilterHidden,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RowBitSet {
    words: Vec<u64>,
}

impl RowBitSet {
    fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    fn get(&self, row0: u32) -> bool {
        let word_idx = (row0 / 64) as usize;
        let bit_idx = row0 % 64;
        self.words
            .get(word_idx)
            .map(|word| (word & (1u64 << bit_idx)) != 0)
            .unwrap_or(false)
    }

    fn set(&mut self, row0: u32, hidden: bool) -> bool {
        let word_idx = (row0 / 64) as usize;
        let bit_idx = row0 % 64;

        if word_idx >= self.words.len() {
            if !hidden {
                return false;
            }
            self.words.resize(word_idx + 1, 0);
        }

        let mask = 1u64 << bit_idx;
        let word = &mut self.words[word_idx];
        let old = (*word & mask) != 0;
        if old == hidden {
            return false;
        }

        if hidden {
            *word |= mask;
        } else {
            *word &= !mask;
            self.trim_trailing_zeros();
        }
        true
    }

    fn set_range(&mut self, start_row0: u32, end_row0: u32, hidden: bool) -> bool {
        if start_row0 > end_row0 {
            return false;
        }

        let mut changed = false;
        for row0 in start_row0..=end_row0 {
            changed |= self.set(row0, hidden);
        }
        changed
    }

    fn insert_rows(&mut self, before0: u32, count: u32) -> bool {
        if count == 0 {
            return false;
        }

        let old_rows = self.collect_set_rows();
        if old_rows.is_empty() {
            return false;
        }

        let mut changed = false;
        let mut new_rows = Vec::with_capacity(old_rows.len());
        for row0 in old_rows {
            let shifted = if row0 >= before0 {
                changed = true;
                row0.saturating_add(count)
            } else {
                row0
            };
            new_rows.push(shifted);
        }

        if changed {
            self.rebuild_from_set_rows(&new_rows);
        }
        changed
    }

    fn delete_rows(&mut self, start0: u32, count: u32) -> bool {
        if count == 0 {
            return false;
        }

        let old_rows = self.collect_set_rows();
        if old_rows.is_empty() {
            return false;
        }

        let end0 = start0.saturating_add(count.saturating_sub(1));
        let mut changed = false;
        let mut new_rows = Vec::with_capacity(old_rows.len());

        for row0 in old_rows {
            if row0 < start0 {
                new_rows.push(row0);
                continue;
            }
            if row0 <= end0 {
                changed = true;
                continue;
            }

            changed = true;
            new_rows.push(row0 - count);
        }

        if changed {
            self.rebuild_from_set_rows(&new_rows);
        }
        changed
    }

    fn collect_set_rows(&self) -> Vec<u32> {
        let mut rows = Vec::new();
        for (word_idx, word) in self.words.iter().copied().enumerate() {
            let mut bits = word;
            while bits != 0 {
                let tz = bits.trailing_zeros();
                rows.push((word_idx as u32) * 64 + tz);
                bits &= bits - 1;
            }
        }
        rows
    }

    fn rebuild_from_set_rows(&mut self, rows: &[u32]) {
        self.words.clear();
        for row0 in rows {
            let _ = self.set(*row0, true);
        }
    }

    fn trim_trailing_zeros(&mut self) {
        while self.words.last().copied() == Some(0) {
            self.words.pop();
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowVisibilityState {
    manual_hidden: RowBitSet,
    filter_hidden: RowBitSet,
    version: u64,
}

impl RowVisibilityState {
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn is_empty(&self) -> bool {
        self.manual_hidden.is_empty() && self.filter_hidden.is_empty()
    }

    pub fn set_row_hidden(&mut self, row0: u32, hidden: bool, source: RowVisibilitySource) -> bool {
        let changed = match source {
            RowVisibilitySource::Manual => self.manual_hidden.set(row0, hidden),
            RowVisibilitySource::Filter => self.filter_hidden.set(row0, hidden),
        };
        if changed {
            self.version = self.version.saturating_add(1);
        }
        changed
    }

    pub fn set_rows_hidden(
        &mut self,
        start_row0: u32,
        end_row0: u32,
        hidden: bool,
        source: RowVisibilitySource,
    ) -> bool {
        let changed = match source {
            RowVisibilitySource::Manual => {
                self.manual_hidden.set_range(start_row0, end_row0, hidden)
            }
            RowVisibilitySource::Filter => {
                self.filter_hidden.set_range(start_row0, end_row0, hidden)
            }
        };
        if changed {
            self.version = self.version.saturating_add(1);
        }
        changed
    }

    pub fn is_row_hidden(&self, row0: u32, source: Option<RowVisibilitySource>) -> bool {
        match source {
            Some(RowVisibilitySource::Manual) => self.manual_hidden.get(row0),
            Some(RowVisibilitySource::Filter) => self.filter_hidden.get(row0),
            None => self.manual_hidden.get(row0) || self.filter_hidden.get(row0),
        }
    }

    pub fn rows_hidden(
        &self,
        start_row0: u32,
        end_row0: u32,
        source: Option<RowVisibilitySource>,
    ) -> Vec<bool> {
        if start_row0 > end_row0 {
            return Vec::new();
        }
        (start_row0..=end_row0)
            .map(|row0| self.is_row_hidden(row0, source))
            .collect()
    }

    pub fn insert_rows(&mut self, before0: u32, count: u32) -> bool {
        let changed = self.manual_hidden.insert_rows(before0, count)
            | self.filter_hidden.insert_rows(before0, count);
        if changed {
            self.version = self.version.saturating_add(1);
        }
        changed
    }

    pub fn delete_rows(&mut self, start0: u32, count: u32) -> bool {
        let changed = self.manual_hidden.delete_rows(start0, count)
            | self.filter_hidden.delete_rows(start0, count);
        if changed {
            self.version = self.version.saturating_add(1);
        }
        changed
    }
}
