'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const test = require('node:test');

const launcher = require('../bin/formualizer.js');

const LAUNCHER = path.join(__dirname, '..', 'bin', 'formualizer.js');
const posix = process.platform !== 'win32';

test('platform map covers the seven release targets with exact npm names', () => {
  const names = launcher.CONFIG.platforms.map((p) => [p.target, launcher.platformPackageName(p)]);
  assert.deepEqual(names, [
    ['x86_64-unknown-linux-gnu', '@formualizer/cli-linux-x64-gnu'],
    ['aarch64-unknown-linux-gnu', '@formualizer/cli-linux-arm64-gnu'],
    ['x86_64-unknown-linux-musl', '@formualizer/cli-linux-x64-musl'],
    ['aarch64-unknown-linux-musl', '@formualizer/cli-linux-arm64-musl'],
    ['x86_64-apple-darwin', '@formualizer/cli-darwin-x64'],
    ['aarch64-apple-darwin', '@formualizer/cli-darwin-arm64'],
    ['x86_64-pc-windows-msvc', '@formualizer/cli-win32-x64-msvc'],
  ]);
  const win = launcher.CONFIG.platforms.find((p) => p.os === 'win32');
  assert.equal(launcher.binaryFileName(win), 'formualizer.exe');
});

test('candidate platforms per host', () => {
  const names = (osName, cpu, libc) =>
    launcher.candidatePlatforms(osName, cpu, libc).map((p) => p.platform);
  assert.deepEqual(names('linux', 'x64', 'glibc'), ['linux-x64-gnu', 'linux-x64-musl']);
  assert.deepEqual(names('linux', 'arm64', 'glibc'), ['linux-arm64-gnu', 'linux-arm64-musl']);
  assert.deepEqual(names('linux', 'x64', 'musl'), ['linux-x64-musl']);
  assert.deepEqual(names('linux', 'arm64', 'musl'), ['linux-arm64-musl']);
  assert.deepEqual(names('darwin', 'x64', null), ['darwin-x64']);
  assert.deepEqual(names('darwin', 'arm64', null), ['darwin-arm64']);
  assert.deepEqual(names('win32', 'x64', null), ['win32-x64-msvc']);
  assert.deepEqual(names('win32', 'arm64', null), []);
  assert.deepEqual(names('linux', 'ia32', 'glibc'), []);
  assert.deepEqual(names('freebsd', 'x64', null), []);
});

test('platform package names follow the single config pattern (unscoped fallback)', () => {
  const config = { ...launcher.CONFIG, platformPackage: 'formualizer-cli-{platform}' };
  const p = config.platforms[0];
  assert.equal(launcher.platformPackageName(p, config), 'formualizer-cli-linux-x64-gnu');
});

test('libc detection from the diagnostic report', () => {
  const report = (header) => ({ getReport: () => ({ header }) });
  assert.equal(launcher.detectLibc({ platform: 'linux', report: report({ glibcVersionRuntime: '2.39' }) }), 'glibc');
  assert.equal(
    launcher.detectLibc({ platform: 'linux', report: report({}), readdirSync: () => [], readFileSync: () => '' }),
    'musl'
  );
  assert.equal(launcher.detectLibc({ platform: 'darwin' }), null);
  assert.equal(launcher.detectLibc({ platform: 'win32' }), null);
});

test('libc detection falls back to the filesystem without a usable report', () => {
  const throwing = { getReport: () => { throw new Error('no report'); } };
  const noLib = () => { throw new Error('ENOENT'); };
  assert.equal(
    launcher.detectLibc({ platform: 'linux', report: throwing, readdirSync: () => ['ld-musl-x86_64.so.1'], readFileSync: noLib }),
    'musl'
  );
  assert.equal(
    launcher.detectLibc({ platform: 'linux', report: undefined, readdirSync: noLib, readFileSync: () => '#!/bin/sh\n# musl libc ldd\n' }),
    'musl'
  );
  assert.equal(
    launcher.detectLibc({ platform: 'linux', report: undefined, readdirSync: () => ['ld-linux-x86-64.so.2'], readFileSync: () => 'GNU libc' }),
    'glibc'
  );
  assert.equal(launcher.detectLibc({ platform: 'linux', report: null, readdirSync: noLib, readFileSync: noLib }), 'glibc');
});

test('signal and exit code mapping', () => {
  const signals = os.constants.signals;
  assert.equal(launcher.exitCodeFor(0, null), 0);
  assert.equal(launcher.exitCodeFor(2, null), 2);
  assert.equal(launcher.exitCodeFor(64, null), 64);
  assert.equal(launcher.exitCodeFor(130, null), 130);
  assert.equal(launcher.exitCodeFor(null, 'SIGINT', signals), 128 + signals.SIGINT);
  assert.equal(launcher.exitCodeFor(null, 'SIGINT', { SIGINT: 2 }), 130);
  assert.equal(launcher.exitCodeFor(null, 'SIGTERM', { SIGTERM: 15 }), 143);
  assert.equal(launcher.exitCodeFor(null, 'SIGKILL', { SIGKILL: 9 }), 137);
  assert.equal(launcher.exitCodeFor(null, 'SIGWHAT', {}), 1);
  assert.equal(launcher.exitCodeFor(null, null), 1);
});

test('resolution prefers the override, then installed packages in order', () => {
  const root = '/nm';
  const installed = new Set(['@formualizer/cli-linux-x64-musl']);
  const resolve = (request) => {
    const name = request.replace(/\/package\.json$/, '');
    if (!installed.has(name)) throw new Error('MODULE_NOT_FOUND');
    return path.join(root, name, 'package.json');
  };
  const existsSync = () => true;

  const override = launcher.resolveBinary({ env: { FORMUALIZER_CLI_BINARY: '/opt/formualizer' }, resolve: () => { throw new Error('unused'); } });
  assert.deepEqual(override, { path: '/opt/formualizer', source: 'FORMUALIZER_CLI_BINARY' });

  const fallback = launcher.resolveBinary({ env: {}, platform: 'linux', arch: 'x64', libc: 'glibc', resolve, existsSync });
  assert.equal(fallback.path, path.join(root, '@formualizer/cli-linux-x64-musl', 'bin', 'formualizer'));
  assert.equal(fallback.source, '@formualizer/cli-linux-x64-musl');

  const muslOnly = launcher.resolveBinary({ env: {}, platform: 'linux', arch: 'arm64', libc: 'musl', resolve, existsSync });
  assert.equal(muslOnly.path, null);
  assert.equal(muslOnly.supported, true);
  assert.deepEqual(muslOnly.missing, ['@formualizer/cli-linux-arm64-musl']);

  const noBinary = launcher.resolveBinary({ env: {}, platform: 'linux', arch: 'x64', libc: 'musl', resolve, existsSync: () => false });
  assert.equal(noBinary.path, null);

  const unsupported = launcher.resolveBinary({ env: {}, platform: 'aix', arch: 'ppc64', libc: null, resolve, existsSync });
  assert.equal(unsupported.supported, false);
  const message = launcher.missingBinaryMessage(unsupported);
  assert.match(message, /no native binary is published for aix-ppc64/);
  assert.match(message, /uvx formualizer/);
  assert.match(message, /cargo install formualizer-cli/);
  assert.match(message, /FORMUALIZER_CLI_BINARY/);
  for (const p of launcher.CONFIG.platforms) assert.ok(message.includes(launcher.platformPackageName(p)));

  assert.match(launcher.missingBinaryMessage(muslOnly), /--omit=optional/);
});

// --- process-level behaviour through a stand-in binary ----------------------

function standIn(dir, body) {
  const file = path.join(dir, 'stand-in.sh');
  fs.writeFileSync(file, `#!/bin/sh\n${body}\n`, { mode: 0o755 });
  return file;
}

function runLauncher(binary, args = []) {
  return spawnSync(process.execPath, [LAUNCHER, ...args], {
    env: { ...process.env, FORMUALIZER_CLI_BINARY: binary },
    encoding: 'utf8',
  });
}

test('argv and exit codes pass through unchanged', { skip: !posix }, () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fz-launcher-'));
  const bin = standIn(dir, 'for a in "$@"; do printf "<%s>" "$a"; done; exit "${CODE:-0}"');
  const out = runLauncher(bin, ['recalc', 'a b.xlsx', '--json', '']);
  assert.equal(out.status, 0);
  assert.equal(out.stdout, '<recalc><a b.xlsx><--json><>');
  for (const code of [1, 2, 3, 64, 130]) {
    const r = spawnSync(process.execPath, [LAUNCHER], {
      env: { ...process.env, FORMUALIZER_CLI_BINARY: bin, CODE: String(code) },
    });
    assert.equal(r.status, code);
  }
});

test('a child killed by a signal maps to 128 + signo', { skip: !posix }, () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fz-launcher-'));
  const bin = standIn(dir, 'kill -TERM $$');
  assert.equal(runLauncher(bin).status, 143);
});

test('SIGINT to the launcher is forwarded and yields 130', { skip: !posix }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fz-launcher-'));
  const marker = path.join(dir, 'ready');
  // Default SIGINT disposition: the stand-in dies from the forwarded signal.
  const bin = standIn(dir, `touch "${marker}"; exec sleep 30`);
  const child = spawn(process.execPath, [LAUNCHER], {
    env: { ...process.env, FORMUALIZER_CLI_BINARY: bin },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 10000;
  while (!fs.existsSync(marker)) {
    if (Date.now() > deadline) throw new Error('stand-in never started');
    await new Promise((r) => setTimeout(r, 20));
  }
  await new Promise((r) => setTimeout(r, 50));
  const status = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })));
  child.kill('SIGINT');
  assert.deepEqual(await status, { code: 130, signal: null });
});

test('missing binary prints the platform list and exits 1', () => {
  const r = spawnSync(process.execPath, ['-e', `
    const l = require(${JSON.stringify(LAUNCHER)});
    const res = l.resolveBinary({ env: {}, platform: 'linux', arch: 'x64', libc: 'glibc', resolve: () => { throw new Error('nope'); } });
    process.stderr.write(l.missingBinaryMessage(res));
  `], { encoding: 'utf8' });
  assert.equal(r.status, 0);
  assert.match(r.stderr, /no native binary installed for linux-x64 \(glibc\)/);
  assert.match(r.stderr, /@formualizer\/cli-linux-x64-gnu or @formualizer\/cli-linux-x64-musl/);
});

test('an unspawnable override reports the failure and exits 1', () => {
  const r = runLauncher(path.join(os.tmpdir(), 'definitely-not-a-formualizer-binary'));
  assert.equal(r.status, 1);
  assert.match(r.stderr, /failed to run/);
});
