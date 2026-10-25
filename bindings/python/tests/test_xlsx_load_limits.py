"""Ordinary native package limits are checked before admission, not engine limits."""

import io
import struct
import sys
import zipfile

import pytest

import formualizer as fz

PATH_LOADERS = [fz.load_workbook, fz.Workbook.load_path, fz.Workbook.from_path]
BYTE_LOADERS = [fz.load_workbook_bytes, fz.Workbook.from_bytes]
FIELDS = [
    "max_input_bytes",
    "max_entries",
    "max_expanded_bytes",
    "max_part_bytes",
    "max_xml_depth",
]


@pytest.fixture
def package(xlsx_builder):
    def populate(wb):
        wb.active["A1"] = 21
        wb.active["B1"] = "=A1*2"
        # Make the largest part a consumed worksheet, not the unread theme.
        wb.active["C1"] = "v" * 15_000

    return xlsx_builder(populate)


def assert_loaded(wb):
    assert wb.evaluate_cell("Sheet1", 1, 2) == 42.0


@pytest.mark.parametrize("backend", [None, "native", "calamine"])
@pytest.mark.parametrize("limits", [None, {}, {"max_entries": 100}])
def test_all_entrypoints(package, backend, limits):
    for loader in PATH_LOADERS:
        for source in (fz.XlsxPathSource.SHARED_FILE, fz.XlsxPathSource.DIRECT_MMAP):
            assert_loaded(
                loader(
                    str(package),
                    backend=backend,
                    path_source=source,
                    load_limits=limits,
                )
            )
    for loader in BYTE_LOADERS:
        assert_loaded(loader(package.read_bytes(), backend=backend, load_limits=limits))


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        1.5,
        1.0,
        float("inf"),
        float("nan"),
        "100",
        None,
        2**128,
        sys.maxsize + 1,
    ],
)
def test_invalid_values_before_source_access(field, value, tmp_path):
    for loader in PATH_LOADERS:
        with pytest.raises((ValueError, TypeError, OverflowError), match="load_limits"):
            loader(str(tmp_path / "missing.xlsx"), load_limits={field: value})
    for loader in BYTE_LOADERS:
        with pytest.raises((ValueError, TypeError, OverflowError), match="load_limits"):
            loader(b"not a package", load_limits={field: value})


@pytest.mark.parametrize("limits", [{"max_part_byte": 1}, {1: 2}, [], "limits"])
def test_invalid_dictionary(limits, tmp_path):
    for loader in PATH_LOADERS:
        with pytest.raises((ValueError, TypeError), match="load_limits"):
            loader(str(tmp_path / "missing.xlsx"), load_limits=limits)


@pytest.mark.parametrize("field", FIELDS)
def test_host_representable_maximum(package, field):
    for loader in BYTE_LOADERS:
        assert_loaded(loader(package.read_bytes(), load_limits={field: sys.maxsize}))


@pytest.mark.parametrize("field", FIELDS)
def test_zero_is_a_limit(package, field):
    for loader in BYTE_LOADERS:
        with pytest.raises(OSError):
            loader(package.read_bytes(), load_limits={field: 0})


@pytest.mark.parametrize("backend", ["umya", "json", "unknown"])
def test_unsupported_backend_before_source_access(tmp_path, backend):
    for loader in PATH_LOADERS:
        with pytest.raises(ValueError, match="load_limits"):
            loader(str(tmp_path / "missing.xlsx"), backend=backend, load_limits={})
    for loader in BYTE_LOADERS:
        with pytest.raises(ValueError, match="load_limits"):
            loader(b"not a package", backend=backend, load_limits={})


@pytest.mark.parametrize("suffix", [".json", ".csv", ".xls", ""])
def test_non_xlsx_content_is_refused_by_native_admission(tmp_path, suffix):
    path = tmp_path / f"not-a-workbook{suffix}"
    path.write_bytes(b'{"not": "an XLSX package"}')
    for loader in PATH_LOADERS:
        with pytest.raises(OSError):
            loader(str(path), load_limits={})


@pytest.mark.parametrize("suffix", [".bin", ""])
@pytest.mark.parametrize("backend", [None, "native", "calamine"])
def test_content_based_shared_file_limits(package, tmp_path, suffix, backend):
    path = tmp_path / f"renamed{suffix}"
    data = package.read_bytes()
    path.write_bytes(data)
    for loader in PATH_LOADERS:
        kwargs = {"backend": backend, "path_source": fz.XlsxPathSource.SHARED_FILE}
        assert_loaded(loader(str(path), **kwargs))
        assert_loaded(loader(str(path), load_limits={}, **kwargs))
        with pytest.raises(OSError):
            loader(str(path), load_limits={"max_input_bytes": len(data) - 1}, **kwargs)
        assert_loaded(
            loader(str(path), load_limits={"max_input_bytes": len(data)}, **kwargs)
        )
        with pytest.raises(ValueError, match="DIRECT_MMAP"):
            loader(
                str(path),
                backend=backend,
                path_source=fz.XlsxPathSource.DIRECT_MMAP,
                load_limits={},
            )


@pytest.mark.parametrize("backend", [None, "native", "calamine"])
@pytest.mark.parametrize(
    "source", [fz.XlsxPathSource.SHARED_FILE, fz.XlsxPathSource.DIRECT_MMAP]
)
@pytest.mark.parametrize(
    "field", ["max_input_bytes", "max_part_bytes", "max_expanded_bytes"]
)
def test_lowered_and_raised_actual_limits(package, backend, source, field):
    data = package.read_bytes()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if field == "max_input_bytes":
            size = len(data)
        elif field == "max_part_bytes":
            size = max(info.file_size for info in archive.infolist())
        else:
            # Metadata can be read again by the external-cache consumer. Use a
            # conservative successful allowance, not an assumed one-read sum.
            size = sum(info.file_size for info in archive.infolist()) * 4
        lower = (
            max(info.file_size for info in archive.infolist())
            if field == "max_expanded_bytes"
            else 1
        )
    for loader, arg, options in [
        (f, str(package), {"path_source": source}) for f in PATH_LOADERS
    ] + [(f, data, {}) for f in BYTE_LOADERS]:
        with pytest.raises(OSError):
            loader(arg, backend=backend, load_limits={field: lower}, **options)
        assert_loaded(
            loader(arg, backend=backend, load_limits={field: size}, **options)
        )
        if field != "max_expanded_bytes":
            with pytest.raises(OSError):
                loader(arg, backend=backend, load_limits={field: size - 1}, **options)


def rewrite(data, extra=False):
    out = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(data)) as src,
        zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as dst,
    ):
        for item in src.infolist():
            dst.writestr(item.filename, src.read(item))
        if extra:
            dst.writestr("unused/payload.bin", b"x" * 100_000)
    return out.getvalue()


def corrupt_crc(data, name):
    data = bytearray(data)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entry = archive.getinfo(name)
        # Set the same wrong CRC in both headers, preserving ZIP structure.
        wrong = entry.CRC ^ 1
        struct.pack_into("<I", data, entry.header_offset + 14, wrong)
        pos = archive.start_dir
        while data[pos : pos + 4] == b"PK\x01\x02":
            n, e, c = struct.unpack_from("<HHH", data, pos + 28)
            if data[pos + 46 : pos + 46 + n].decode() == name:
                struct.pack_into("<I", data, pos + 16, wrong)
                break
            pos += 46 + n + e + c
        else:
            raise AssertionError("central entry not found")
    return bytes(data)


@pytest.mark.parametrize("selected", [False, True])
def test_selected_corruption_vs_unread_large_payload(package, selected):
    data = rewrite(package.read_bytes(), extra=True)
    name = "xl/worksheets/sheet1.xml" if selected else "unused/payload.bin"
    data = corrupt_crc(data, name)
    package.write_bytes(data)
    for loader, arg in [(f, str(package)) for f in PATH_LOADERS] + [
        (f, data) for f in BYTE_LOADERS
    ]:
        kwargs = {
            "load_limits": {"max_part_bytes": 20_000, "max_expanded_bytes": 40_000}
        }
        if selected:
            with pytest.raises(OSError):
                loader(arg, **kwargs)
        else:
            assert_loaded(loader(arg, **kwargs))


@pytest.mark.parametrize("suffix", [".xlsx", ".bin", ""])
@pytest.mark.parametrize("backend", ["native", "calamine"])
def test_sheetport_forwarding(package, tmp_path, suffix, backend):
    path = tmp_path / f"sheetport-source{suffix}"
    path.write_bytes(package.read_bytes())
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("""spec: fio
spec_version: "0.3.0"
manifest:
  id: limits-test
  name: Limits test
  workbook:
    uri: memory://limits.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: result
    dir: out
    shape: scalar
    location:
      a1: Sheet1!B1
    schema:
      type: number
""")
    fz.SheetPortSession.from_manifest_path(str(manifest), str(path), backend=backend)
    fz.SheetPortSession.from_manifest_path(
        str(manifest), str(path), backend=backend, load_limits={}
    )
    with pytest.raises(OSError):
        fz.SheetPortSession.from_manifest_path(
            str(manifest),
            str(path),
            backend=backend,
            load_limits={"max_input_bytes": 0},
        )
    fz.SheetPortSession.from_manifest_path(
        str(manifest),
        str(path),
        backend=backend,
        load_limits={"max_input_bytes": path.stat().st_size},
    )


def test_load_limits_keyword_only(package):
    # Existing positional backend/strategy arguments retain their meaning.
    assert_loaded(fz.load_workbook_bytes(package.read_bytes(), None, "native"))
    assert_loaded(fz.Workbook.load_path(str(package), None, "native"))
    assert_loaded(fz.Workbook.from_path(str(package), "native"))
    assert_loaded(fz.Workbook.from_bytes(package.read_bytes(), "native"))
    for loader in BYTE_LOADERS:
        with pytest.raises(TypeError):
            loader(package.read_bytes(), None, None, {})
