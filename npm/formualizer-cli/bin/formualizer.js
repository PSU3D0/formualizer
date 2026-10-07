#!/usr/bin/env node
// Launcher for the native `formualizer` binary shipped in a per-platform
// optional dependency. No dependencies, no install scripts.
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const CONFIG = require('../platforms.json');

const OVERRIDE_ENV = 'FORMUALIZER_CLI_BINARY';
const RELEASES_URL = 'https://github.com/psu3d0/formualizer/releases';

function platformPackageName(platform, config = CONFIG) {
  return config.platformPackage.replace('{platform}', platform.platform);
}

function binaryFileName(platform, config = CONFIG) {
  return config.binName + (platform.exe || '');
}

// glibc vs musl. Node's diagnostic report exposes the runtime glibc version on
// glibc systems and omits it on musl. Without a usable report, look for the
// musl dynamic loader; default to glibc (the musl build is a fallback anyway).
function detectLibc(deps = {}) {
  const platform = deps.platform || process.platform;
  if (platform !== 'linux') return null;

  const report = 'report' in deps ? deps.report : process.report;
  if (report && typeof report.getReport === 'function') {
    try {
      const previous = report.excludeNetwork;
      report.excludeNetwork = true;
      let header;
      try {
        header = report.getReport().header;
      } finally {
        report.excludeNetwork = previous;
      }
      if (header) return header.glibcVersionRuntime ? 'glibc' : 'musl';
    } catch (_) {
      // Fall through to the filesystem probe.
    }
  }

  const readdir = deps.readdirSync || fs.readdirSync;
  const readFile = deps.readFileSync || fs.readFileSync;
  try {
    if (readdir('/lib').some((name) => name.startsWith('ld-musl-'))) return 'musl';
  } catch (_) {
    // No /lib listing available.
  }
  try {
    if (readFile('/usr/bin/ldd', 'latin1').includes('musl')) return 'musl';
  } catch (_) {
    // No ldd script available.
  }
  return 'glibc';
}

// Platform packages to try, in order. A static musl binary also runs on glibc
// systems, so glibc hosts fall back to it; the reverse is not true.
function candidatePlatforms(osName, cpu, libc, config = CONFIG) {
  const matches = config.platforms.filter((p) => p.os === osName && p.cpu === cpu);
  if (osName !== 'linux') return matches;
  const preferred = matches.filter((p) => p.libc === libc);
  if (libc === 'musl') return preferred;
  return preferred.concat(matches.filter((p) => p.libc === 'musl'));
}

function describePlatform(p) {
  return p.libc ? `${p.os}-${p.cpu} (${p.libc})` : `${p.os}-${p.cpu}`;
}

function resolveBinary(opts = {}) {
  const env = opts.env || process.env;
  const config = opts.config || CONFIG;
  const override = env[OVERRIDE_ENV];
  if (override) return { path: override, source: OVERRIDE_ENV };

  const osName = opts.platform || process.platform;
  const cpu = opts.arch || process.arch;
  const libc = 'libc' in opts ? opts.libc : detectLibc({ platform: osName });
  const resolve = opts.resolve || require.resolve;
  const exists = opts.existsSync || fs.existsSync;

  const candidates = candidatePlatforms(osName, cpu, libc, config);
  const missing = [];
  for (const platform of candidates) {
    const name = platformPackageName(platform, config);
    let manifest;
    try {
      manifest = resolve(`${name}/package.json`);
    } catch (_) {
      missing.push(name);
      continue;
    }
    const binary = path.join(path.dirname(manifest), 'bin', binaryFileName(platform, config));
    if (exists(binary)) return { path: binary, source: name };
    missing.push(name);
  }
  return {
    path: null,
    host: libc ? `${osName}-${cpu} (${libc})` : `${osName}-${cpu}`,
    supported: candidates.length > 0,
    missing,
  };
}

function missingBinaryMessage(result, config = CONFIG) {
  const lines = [];
  if (result.supported) {
    lines.push(
      `${config.metaPackage}: no native binary installed for ${result.host}.`,
      `Expected optional package ${result.missing.join(' or ')}.`,
      'It may have been skipped by --omit=optional / --no-optional, or by a lockfile',
      'created on another platform. Reinstall with optional dependencies enabled.'
    );
  } else {
    lines.push(`${config.metaPackage}: no native binary is published for ${result.host}.`);
  }
  lines.push(
    '',
    'Supported platforms:',
    ...config.platforms.map((p) => `  ${describePlatform(p).padEnd(20)} ${platformPackageName(p, config)}`),
    '',
    'Alternatives:',
    '  uvx formualizer recalc book.xlsx    (Python wheel, same engine)',
    '  cargo install formualizer-cli       (build from source)',
    `  prebuilt archives: ${RELEASES_URL}`,
    '',
    `Or set ${OVERRIDE_ENV} to the path of a formualizer binary.`
  );
  return lines.join('\n') + '\n';
}

// Conventional shell status for a child terminated by a signal: 128 + signo.
function exitCodeFor(code, signal, signals = os.constants.signals) {
  if (signal) {
    const number = signals[signal];
    return typeof number === 'number' ? 128 + number : 1;
  }
  return typeof code === 'number' ? code : 1;
}

function main() {
  const resolved = resolveBinary();
  if (!resolved.path) {
    process.stderr.write(missingBinaryMessage(resolved));
    process.exit(1);
  }

  const child = spawn(resolved.path, process.argv.slice(2), { stdio: 'inherit' });

  // Terminal Ctrl-C already reaches the child through the process group; the
  // handler keeps the launcher alive until the child reports its status and
  // forwards signals sent to the launcher alone. Windows delivers console
  // Ctrl-C to the child directly and child.kill() there would terminate it.
  const forward = (signal) => {
    if (process.platform === 'win32' || child.exitCode !== null || child.signalCode !== null) return;
    try {
      child.kill(signal);
    } catch (_) {
      // The child already exited.
    }
  };
  const signals = ['SIGINT', 'SIGTERM'];
  for (const signal of signals) process.on(signal, forward);

  child.on('error', (err) => {
    process.stderr.write(`${CONFIG.metaPackage}: failed to run ${resolved.path}: ${err.message}\n`);
    process.exit(1);
  });
  child.on('exit', (code, signal) => {
    for (const s of signals) process.removeListener(s, forward);
    process.exit(exitCodeFor(code, signal));
  });
}

module.exports = {
  CONFIG,
  OVERRIDE_ENV,
  binaryFileName,
  candidatePlatforms,
  detectLibc,
  exitCodeFor,
  missingBinaryMessage,
  platformPackageName,
  resolveBinary,
};

if (require.main === module) main();
