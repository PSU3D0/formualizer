'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

const assembleLib = require('../scripts/assemble.js');
const { CONFIG, platformPackageName } = require('../bin/formualizer.js');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'assemble.js');
const byTarget = Object.fromEntries(CONFIG.platforms.map((p) => [p.target, p]));

// Minimal synthetic headers good enough for the format/arch/static checks.
function elf(machine, { interp = false, glibc = null } = {}) {
  const buf = Buffer.alloc(256);
  buf.writeUInt32BE(0x7f454c46, 0);
  buf[4] = 2; // 64-bit
  buf[5] = 1; // little endian
  buf.writeUInt16LE(machine, 18);
  buf.writeBigUInt64LE(64n, 32); // e_phoff
  buf.writeUInt16LE(56, 54); // e_phentsize
  buf.writeUInt16LE(1, 56); // e_phnum
  buf.writeUInt32LE(interp ? 3 : 1, 64); // PT_INTERP or PT_LOAD
  if (glibc) buf.write(`GLIBC_2.2.5\0${glibc}\0`, 160, 'latin1');
  return buf;
}

function macho(cpu) {
  const buf = Buffer.alloc(64);
  buf.writeUInt32LE(0xfeedfacf, 0);
  buf.writeUInt32LE(cpu, 4);
  return buf;
}

function pe(machine) {
  const buf = Buffer.alloc(256);
  buf.write('MZ', 0, 'latin1');
  buf.writeUInt32LE(128, 0x3c);
  buf.write('PE\0\0', 128, 'latin1');
  buf.writeUInt16LE(machine, 132);
  return buf;
}

const GOOD = {
  'x86_64-unknown-linux-gnu': () => elf(0x3e, { interp: true, glibc: 'GLIBC_2.17' }),
  'aarch64-unknown-linux-gnu': () => elf(0xb7, { interp: true, glibc: 'GLIBC_2.17' }),
  'x86_64-unknown-linux-musl': () => elf(0x3e),
  'aarch64-unknown-linux-musl': () => elf(0xb7),
  'x86_64-apple-darwin': () => macho(0x01000007),
  'aarch64-apple-darwin': () => macho(0x0100000c),
  'x86_64-pc-windows-msvc': () => pe(0x8664),
};

function binDir(overrides = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fz-assemble-'));
  for (const [target, make] of Object.entries({ ...GOOD, ...overrides })) {
    if (!make) continue;
    const p = byTarget[target];
    fs.mkdirSync(path.join(dir, target));
    fs.writeFileSync(path.join(dir, target, `formualizer${p.exe || ''}`), make());
  }
  return dir;
}

function run(args) {
  return spawnSync(process.execPath, [SCRIPT, ...args], { encoding: 'utf8' });
}

test('config is valid and the README names every platform package', () => {
  assert.deepEqual(assembleLib.validateConfig(), []);
  const readme = fs.readFileSync(path.join(__dirname, '..', 'README.md'), 'utf8');
  for (const p of CONFIG.platforms) assert.ok(readme.includes(platformPackageName(p)), p.target);
});

test('the release workflow builds exactly the configured targets', () => {
  const workflow = fs.readFileSync(path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'cli-build.yml'), 'utf8');
  const targets = [...workflow.matchAll(/^\s+- target: (\S+)$/gm)].map((m) => m[1]);
  assert.deepEqual(targets, CONFIG.platforms.map((p) => p.target));
});

test('full assembly writes synced platform and meta packages', () => {
  const bins = binDir();
  const out = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'fz-out-')), 'npm');
  const r = run(['--binaries', bins, '--out', out, '--max-glibc', '2.17']);
  assert.equal(r.status, 0, r.stderr);

  const version = assembleLib.cargoVersion();
  const manifest = JSON.parse(fs.readFileSync(path.join(out, 'manifest.json'), 'utf8'));
  assert.equal(manifest.version, version);
  assert.equal(manifest.packages.at(-1).kind, 'meta');
  assert.equal(manifest.packages.length, 8);

  const meta = JSON.parse(fs.readFileSync(path.join(out, 'formualizer-cli', 'package.json'), 'utf8'));
  assert.equal(meta.name, 'formualizer-cli');
  assert.equal(meta.version, version);
  assert.deepEqual(meta.bin, { formualizer: 'bin/formualizer.js' });
  assert.equal(meta.scripts, undefined);
  assert.deepEqual(
    meta.optionalDependencies,
    Object.fromEntries(CONFIG.platforms.map((p) => [platformPackageName(p), version]))
  );
  for (const f of ['bin/formualizer.js', 'platforms.json', 'README.md', 'LICENSE-MIT', 'LICENSE-APACHE']) {
    assert.ok(fs.existsSync(path.join(out, 'formualizer-cli', f)), f);
  }

  for (const p of CONFIG.platforms) {
    const dir = path.join(out, assembleLib.packageDirName(platformPackageName(p)));
    const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
    assert.equal(pkg.name, platformPackageName(p));
    assert.equal(pkg.version, version);
    assert.deepEqual(pkg.os, [p.os]);
    assert.deepEqual(pkg.cpu, [p.cpu]);
    assert.deepEqual(pkg.libc, p.libc ? [p.libc] : undefined);
    assert.equal(pkg.bin, undefined);
    assert.equal(pkg.scripts, undefined);
    const binary = path.join(dir, 'bin', `formualizer${p.exe || ''}`);
    if (process.platform !== 'win32') assert.equal(fs.statSync(binary).mode & 0o777, 0o755);
    for (const f of ['README.md', 'LICENSE-MIT', 'LICENSE-APACHE']) assert.ok(fs.existsSync(path.join(dir, f)));
  }
});

test('subset assembly only references the assembled targets', () => {
  const bins = binDir();
  const out = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'fz-out-')), 'npm');
  const r = run(['--binaries', bins, '--out', out, '--targets', 'x86_64-unknown-linux-gnu']);
  assert.equal(r.status, 0, r.stderr);
  const meta = JSON.parse(fs.readFileSync(path.join(out, 'formualizer-cli', 'package.json'), 'utf8'));
  assert.deepEqual(Object.keys(meta.optionalDependencies), ['@formualizer/cli-linux-x64-gnu']);
  assert.deepEqual(fs.readdirSync(out).sort(), ['formualizer-cli', 'formualizer-cli-linux-x64-gnu', 'manifest.json']);
});

test('--check validates without writing', () => {
  const bins = binDir();
  const before = fs.readdirSync(bins).sort();
  const r = run(['--binaries', bins, '--check']);
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /^checked: /);
  assert.deepEqual(fs.readdirSync(bins).sort(), before);
});

test('validation rejects wrong, missing or dynamically linked binaries', () => {
  const cases = [
    [{ 'x86_64-unknown-linux-musl': () => elf(0x3e, { interp: true }) }, /musl build is dynamically linked/],
    [{ 'aarch64-unknown-linux-gnu': () => elf(0x3e, { interp: true }) }, /ELF machine 0x3e is not arm64/],
    [{ 'x86_64-apple-darwin': () => macho(0x0100000c) }, /cputype .* is not x64/],
    [{ 'x86_64-pc-windows-msvc': () => Buffer.from('not a binary') }, /not a PE file/],
    [{ 'aarch64-apple-darwin': null }, /missing binary/],
    [{ 'x86_64-unknown-linux-gnu': () => elf(0x3e, { interp: true, glibc: 'GLIBC_2.34' }) }, /requires GLIBC_2.34 > 2.17/],
  ];
  for (const [overrides, pattern] of cases) {
    const r = run(['--binaries', binDir(overrides), '--check', '--max-glibc', '2.17']);
    assert.equal(r.status, 1, String(pattern));
    assert.match(r.stderr, pattern);
  }
});

test('usage errors exit 64; version must match the crate', () => {
  assert.equal(run(['--check']).status, 64);
  assert.equal(run(['--binaries', 'x', '--targets', 'sparc-sun-solaris', '--check']).status, 64);
  const r = run(['--binaries', binDir(), '--check', '--version', '0.0.1']);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /does not match crates\/formualizer-cli\/Cargo.toml/);
});

test('refuses to assemble into a non-empty directory', () => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'fz-out-'));
  fs.writeFileSync(path.join(out, 'stale'), '');
  const r = run(['--binaries', binDir(), '--out', out]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /is not empty/);
});
