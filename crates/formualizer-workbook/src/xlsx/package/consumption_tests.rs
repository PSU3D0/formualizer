use super::*;
use crate::xlsx::options::ReadOptions;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
fn bytes(data: &[u8], method: zip::CompressionMethod) -> Vec<u8> {
    let mut w = zip::ZipWriter::new(Cursor::new(Vec::new()));
    w.start_file(
        "part",
        zip::write::SimpleFileOptions::default().compression_method(method),
    )
    .unwrap();
    w.write_all(data).unwrap();
    w.finish().unwrap().into_inner()
}
#[test]
fn selected_zero_exact_and_plus_one_require_true_eof() {
    for method in [
        zip::CompressionMethod::Stored,
        zip::CompressionMethod::Deflated,
    ] {
        for size in [0, 1, 65536, 65537] {
            let data = vec![b'x'; size];
            for limit in [0, size.saturating_sub(1), size, size + 1] {
                let source = bytes(&data, method);
                let mut z = zip::ZipArchive::new(Cursor::new(source)).unwrap();
                let mut options = ReadOptions::default();
                options.limits.max_worksheet_bytes = limit;
                options.limits.max_expanded_bytes = limit;
                let mut budget = ConsumptionBudget::default();
                let result = read_selected(&mut z, "part", &options, &mut budget);
                assert_eq!(
                    result.is_ok(),
                    size <= limit,
                    "{method:?} size={size} limit={limit}"
                );
                assert_eq!(budget.emitted, size.min(limit + 1));
                if size > limit {
                    assert!(budget.check().is_err());
                }
            }
        }
    }
}
#[test]
fn partial_crc_failure_is_charged_and_terminal() {
    let mut source = bytes(&vec![b'x'; 70000], zip::CompressionMethod::Stored);
    let offset = zip::ZipArchive::new(Cursor::new(&source))
        .unwrap()
        .by_name("part")
        .unwrap()
        .data_start() as usize;
    source[offset] ^= 1;
    let mut z = zip::ZipArchive::new(Cursor::new(source)).unwrap();
    let mut budget = ConsumptionBudget::default();
    let mut options = ReadOptions::default();
    options.limits.max_expanded_bytes = 70000;
    options.limits.max_worksheet_bytes = 70000;
    assert!(read_selected(&mut z, "part", &options, &mut budget).is_err());
    assert_eq!(budget.emitted, 70000);
    let before = budget.emitted;
    assert!(read_selected(&mut z, "part", &ReadOptions::default(), &mut budget).is_err());
    assert_eq!(budget.emitted, before);
}
#[test]
fn partial_io_failure_is_charged_and_terminal() {
    struct Failing {
        inner: Cursor<Vec<u8>>,
        fail_at: u64,
    }
    impl Read for Failing {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.inner.position() == self.fail_at {
                return Err(std::io::Error::other("injected selected source failure"));
            }
            self.inner.read(out)
        }
    }
    impl Seek for Failing {
        fn seek(&mut self, at: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(at)
        }
    }
    let source = bytes(&vec![b'x'; 200000], zip::CompressionMethod::Stored);
    let data = zip::ZipArchive::new(Cursor::new(&source))
        .unwrap()
        .by_name("part")
        .unwrap()
        .data_start();
    let mut z = zip::ZipArchive::new(Failing {
        inner: Cursor::new(source),
        fail_at: data + 65536,
    })
    .unwrap();
    let mut budget = ConsumptionBudget::default();
    assert!(read_selected(&mut z, "part", &ReadOptions::default(), &mut budget).is_err());
    assert_eq!(budget.emitted, 65536);
    assert!(budget.check().is_err());
}
#[test]
fn real_rereads_are_charged_again() {
    let mut z =
        zip::ZipArchive::new(Cursor::new(bytes(b"123", zip::CompressionMethod::Deflated))).unwrap();
    let mut options = ReadOptions::default();
    options.limits.max_expanded_bytes = 5;
    let mut budget = ConsumptionBudget::default();
    assert_eq!(
        read_selected(&mut z, "part", &options, &mut budget).unwrap(),
        b"123"
    );
    assert!(read_selected(&mut z, "part", &options, &mut budget).is_err());
    assert_eq!(budget.emitted, 6);
}
#[test]
fn selected_cancellation_during_output_is_terminal_and_charged() {
    struct Cancelling {
        inner: Cursor<Vec<u8>>,
        cancel: crate::CancelToken,
        data: u64,
    }
    impl Read for Cancelling {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let at = self.inner.position();
            let n = self.inner.read(out)?;
            if at >= self.data && out.len() == 65536 {
                self.cancel.cancel();
            }
            Ok(n)
        }
    }
    impl Seek for Cancelling {
        fn seek(&mut self, at: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(at)
        }
    }
    let source = bytes(&vec![b'x'; 200000], zip::CompressionMethod::Stored);
    let data = zip::ZipArchive::new(Cursor::new(&source))
        .unwrap()
        .by_name("part")
        .unwrap()
        .data_start();
    let cancel = crate::CancelToken::new();
    let mut z = zip::ZipArchive::new(Cancelling {
        inner: Cursor::new(source),
        cancel: cancel.clone(),
        data,
    })
    .unwrap();
    let options = ReadOptions {
        cancel: Some(cancel),
        ..Default::default()
    };
    let mut budget = ConsumptionBudget::default();
    assert!(read_selected(&mut z, "part", &options, &mut budget).is_err());
    assert_eq!(budget.emitted, 65536);
    assert!(budget.check().is_err());
}
