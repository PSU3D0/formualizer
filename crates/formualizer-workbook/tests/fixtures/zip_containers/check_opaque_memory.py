"""Linux process-RSS regression; run after building the native CLI.

uv run --no-project python check_opaque_memory.py /path/to/formualizer

Opaque members deliberately precede all discovery metadata. The 16 MiB
allowance is a test tolerance, NOT a package admission limit. No dependencies.
"""
import hashlib
import json
import pathlib
import statistics
import struct
import subprocess
import sys
import tempfile
import zipfile

binary = str(pathlib.Path(sys.argv[1]).resolve())
fixture = pathlib.Path(__file__).with_name("openpyxl.xlsx")
with zipfile.ZipFile(fixture) as archive:
    original = [(name, archive.read(name)) for name in archive.namelist()]


def build(path, opaque_mib, bad_root=False, corrupt=False):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        block = b"x" * (64 * 1024)
        for i in range(opaque_mib // 8):
            with archive.open(f"opaque/{i}.bin", "w") as member:
                for _ in range(128):
                    member.write(block)
        for name, data in original:
            if name == "[Content_Types].xml":
                data = data.replace(b"</Types>", b'<Default Extension="bin" ContentType="application/octet-stream"/></Types>')
            if bad_root and name == "xl/worksheets/sheet1.xml":
                data = b"<bad/>"
            archive.writestr(name, data)
        if corrupt:
            archive.writestr("unused.bin", b"unused payload", compress_type=zipfile.ZIP_STORED)
    if corrupt:
        with zipfile.ZipFile(path) as archive:
            offset = archive.getinfo("unused.bin").header_offset
        data = bytearray(path.read_bytes())
        name_len, extra_len = struct.unpack_from("<HH", data, offset + 26)
        data[offset + 30 + name_len + extra_len] ^= 1
        path.write_bytes(data)


with tempfile.TemporaryDirectory(prefix="formualizer-opaque-rss-") as directory:
    root = pathlib.Path(directory)
    results = []
    for name, mib, bad, corrupt in [("control", 0, False, False), ("admitted-opaque-first", 240, False, False), ("semantic-refusal", 240, True, False), ("late-crc-before-semantic", 240, True, True), ("expansion-refusal", 264, False, False)]:
        path = root / "input.xlsx"
        build(path, mib, bad, corrupt)
        rss = []
        fingerprints = []
        for _ in range(3):
            out = root / "output.xlsx"
            out.unlink(missing_ok=True)
            run = subprocess.run(["/usr/bin/time", "-f", "%M", "-o", str(root / "rss"), binary, "recalc", str(path), "-o", str(out), "--json", "--now", "2020-01-02T00:00:00Z", "--tz", "UTC"], capture_output=True, timeout=120)
            rss.append(int((root / "rss").read_text().splitlines()[-1]))
            fingerprints.append((run.returncode, run.stdout.hex(), run.stderr.hex(), hashlib.sha256(out.read_bytes()).hexdigest() if out.exists() else None))
        assert len(set(fingerprints)) == 1, f"unstable output: {name}"
        assert (run.returncode == 0) == (not bad and mib < 256), f"unexpected outcome: {name}"
        if name == "semantic-refusal":
            assert json.loads(run.stdout)["refusal"]["feature"] == "worksheet root/namespace"
        if name == "expansion-refusal":
            assert json.loads(run.stdout)["refusal"]["feature"] == "actual ZIP expansion limit"
        if name == "late-crc-before-semantic":
            assert b"checksum" in (run.stdout + run.stderr).lower(), (run.stdout, run.stderr)
        result = {"case": name, "rss_kib": rss, "median_rss_kib": statistics.median(rss), "exit": run.returncode, "stdout": run.stdout.decode(), "stderr": run.stderr.decode(), "xlsx": fingerprints[0][-1]}
        results.append(result)
        print(json.dumps(result), flush=True)
    control = results[0]["median_rss_kib"]
    assert all(result["median_rss_kib"] <= control + 16 * 1024 for result in results[1:]), "opaque payload bytes retained beyond bounded streaming working memory"
