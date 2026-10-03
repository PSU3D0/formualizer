#!/usr/bin/env node
// Assemble the npm packages for the native `formualizer` CLI from release
// binaries. No dependencies.
//
//   node npm/formualizer-cli/scripts/assemble.js \
//     --binaries DIR --out DIR [--version X.Y.Z] [--targets t1,t2] \
//     [--max-glibc 2.17] [--check]
//
// DIR/<rust-target-triple>/formualizer[.exe] holds each binary. The output
// gets one directory per platform package plus `formualizer-cli/` for the
// `@formualizer/cli` meta package, and a manifest.json listing package
// directories in publish order
// (platform packages first). --check validates everything without writing.
'use strict';

const fs = require('fs');
const path = require('path');

const PKG_ROOT = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(PKG_ROOT, '..', '..');
const CARGO_MANIFEST = path.join(REPO_ROOT, 'crates', 'formualizer-cli', 'Cargo.toml');
const LICENSES = ['LICENSE-MIT', 'LICENSE-APACHE'];
const DEFAULT_SCOPE_PATTERN = '@formualizer/cli-{platform}';

const CONFIG = require(path.join(PKG_ROOT, 'platforms.json'));
const { binaryFileName, platformPackageName } = require(path.join(PKG_ROOT, 'bin', 'formualizer.js'));

const SEMVER = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

class UsageError extends Error {}

function parseArgs(argv) {
  const opts = { check: false, targets: null, version: null, binaries: null, out: null, maxGlibc: null };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const value = () => {
      if (i + 1 >= argv.length) throw new UsageError(`${arg} requires a value`);
      return argv[++i];
    };
    switch (arg) {
      case '--check': opts.check = true; break;
      case '--binaries': opts.binaries = value(); break;
      case '--out': opts.out = value(); break;
      case '--version': opts.version = value(); break;
      case '--targets': opts.targets = value().split(',').map((t) => t.trim()).filter(Boolean); break;
      case '--max-glibc': opts.maxGlibc = value(); break;
      case '-h': case '--help': opts.help = true; break;
      default: throw new UsageError(`unknown argument: ${arg}`);
    }
  }
  if (opts.help) return opts;
  if (!opts.binaries) throw new UsageError('--binaries is required');
  if (!opts.check && !opts.out) throw new UsageError('--out is required unless --check');
  return opts;
}

function cargoVersion(manifest = CARGO_MANIFEST) {
  const text = fs.readFileSync(manifest, 'utf8');
  const section = text.split(/^\[/m).find((s) => s.startsWith('package]'));
  const match = section && section.match(/^version\s*=\s*"([^"]+)"/m);
  if (!match) throw new Error(`no [package] version in ${manifest}`);
  return match[1];
}

function selectPlatforms(targets, config = CONFIG) {
  if (!targets) return config.platforms.slice();
  const known = new Map(config.platforms.map((p) => [p.target, p]));
  const unknown = targets.filter((t) => !known.has(t));
  if (unknown.length) {
    throw new UsageError(`unknown target(s): ${unknown.join(', ')}; known: ${[...known.keys()].join(', ')}`);
  }
  // Keep config order so output is deterministic.
  return config.platforms.filter((p) => targets.includes(p.target));
}

function validateConfig(config = CONFIG) {
  const errors = [];
  if (!/^(@[a-z0-9-~][a-z0-9-._~]*\/)?[a-z0-9-~][a-z0-9-._~]*$/.test(config.metaPackage)) {
    errors.push(`invalid meta package name ${config.metaPackage}`);
  }
  if (!config.platformPackage.includes('{platform}')) errors.push('platformPackage must contain {platform}');
  const seen = new Set();
  for (const p of config.platforms) {
    const name = platformPackageName(p, config);
    if (seen.has(name)) errors.push(`duplicate platform package ${name}`);
    seen.add(name);
    if (p.os === 'linux' && !['glibc', 'musl'].includes(p.libc)) errors.push(`${p.target}: linux needs libc`);
    if (p.os !== 'linux' && p.libc) errors.push(`${p.target}: libc only applies to linux`);
    if ((p.os === 'win32') !== (p.exe === '.exe')) errors.push(`${p.target}: exe suffix mismatch`);
  }
  return errors;
}

// --- binary inspection -----------------------------------------------------

const ELF_MACHINE = { x64: 0x3e, arm64: 0xb7 };
const MACHO_CPU = { x64: 0x01000007, arm64: 0x0100000c };
const PE_MACHINE = { x64: 0x8664, arm64: 0xaa64 };
const PT_INTERP = 3;

function elfHasInterpreter(buf) {
  const phoff = Number(buf.readBigUInt64LE(32));
  const phentsize = buf.readUInt16LE(54);
  const phnum = buf.readUInt16LE(56);
  for (let i = 0; i < phnum; i++) {
    const off = phoff + i * phentsize;
    if (off + 4 > buf.length) break;
    if (buf.readUInt32LE(off) === PT_INTERP) return true;
  }
  return false;
}

function maxGlibcVersion(buf) {
  const text = buf.toString('latin1');
  const re = /GLIBC_(\d+)\.(\d+)(?:\.(\d+))?/g;
  let best = null;
  let m;
  while ((m = re.exec(text))) {
    const v = [Number(m[1]), Number(m[2]), Number(m[3] || 0)];
    if (!best || compareVersions(v, best.parts) > 0) best = { parts: v, text: m[0].slice('GLIBC_'.length) };
  }
  return best;
}

function compareVersions(a, b) {
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const d = (a[i] || 0) - (b[i] || 0);
    if (d) return d;
  }
  return 0;
}

function inspectBinary(file, platform, opts = {}) {
  const errors = [];
  let stat;
  try {
    stat = fs.statSync(file);
  } catch (_) {
    return [`${platform.target}: missing binary ${file}`];
  }
  if (!stat.isFile() || stat.size === 0) return [`${platform.target}: ${file} is not a non-empty file`];
  const buf = fs.readFileSync(file);
  const where = `${platform.target}: ${file}`;

  if (platform.os === 'linux') {
    if (buf.length < 64 || buf.readUInt32BE(0) !== 0x7f454c46) return [`${where}: not an ELF file`];
    if (buf[4] !== 2 || buf[5] !== 1) return [`${where}: not a 64-bit little-endian ELF`];
    const machine = buf.readUInt16LE(18);
    if (machine !== ELF_MACHINE[platform.cpu]) errors.push(`${where}: ELF machine 0x${machine.toString(16)} is not ${platform.cpu}`);
    const interp = elfHasInterpreter(buf);
    if (platform.libc === 'musl' && interp) errors.push(`${where}: musl build is dynamically linked (PT_INTERP present)`);
    if (platform.libc === 'glibc' && opts.maxGlibc) {
      const limit = opts.maxGlibc.split('.').map(Number);
      const found = maxGlibcVersion(buf);
      if (found && compareVersions(found.parts, limit) > 0) {
        errors.push(`${where}: requires GLIBC_${found.text} > ${opts.maxGlibc}`);
      }
    }
  } else if (platform.os === 'darwin') {
    if (buf.length < 8 || buf.readUInt32LE(0) !== 0xfeedfacf) return [`${where}: not a 64-bit Mach-O file`];
    const cpu = buf.readUInt32LE(4);
    if (cpu !== MACHO_CPU[platform.cpu]) errors.push(`${where}: Mach-O cputype 0x${cpu.toString(16)} is not ${platform.cpu}`);
  } else if (platform.os === 'win32') {
    if (buf.length < 64 || buf.toString('latin1', 0, 2) !== 'MZ') return [`${where}: not a PE file`];
    const pe = buf.readUInt32LE(0x3c);
    if (pe + 6 > buf.length || buf.toString('latin1', pe, pe + 4) !== 'PE\0\0') return [`${where}: missing PE header`];
    const machine = buf.readUInt16LE(pe + 4);
    if (machine !== PE_MACHINE[platform.cpu]) errors.push(`${where}: PE machine 0x${machine.toString(16)} is not ${platform.cpu}`);
  }
  return errors;
}

// --- package generation ----------------------------------------------------

function readTemplate() {
  return JSON.parse(fs.readFileSync(path.join(PKG_ROOT, 'package.template.json'), 'utf8'));
}

function packageDirName(name) {
  return name.replace(/^@/, '').replace('/', '-');
}

function platformManifest(platform, version, template, config = CONFIG) {
  const manifest = {
    name: platformPackageName(platform, config),
    version,
    description: `The formualizer CLI binary for ${platform.platform}. Install ${config.metaPackage} instead.`,
    homepage: template.homepage,
    repository: template.repository,
    license: template.license,
    os: [platform.os],
    cpu: [platform.cpu],
  };
  if (platform.libc) manifest.libc = [platform.libc];
  manifest.files = [`bin/${binaryFileName(platform, config)}`, 'README.md', ...LICENSES];
  manifest.preferUnplugged = true;
  return manifest;
}

function metaManifest(platforms, version, template, config = CONFIG) {
  const manifest = { ...template, name: config.metaPackage, version };
  manifest.optionalDependencies = Object.fromEntries(
    platforms.map((p) => [platformPackageName(p, config), version])
  );
  return manifest;
}

function metaReadme(config = CONFIG) {
  let text = fs.readFileSync(path.join(PKG_ROOT, 'README.md'), 'utf8');
  for (const p of config.platforms) {
    const fromDefault = DEFAULT_SCOPE_PATTERN.replace('{platform}', p.platform);
    text = text.split(fromDefault).join(platformPackageName(p, config));
  }
  return text;
}

function platformReadme(platform, config = CONFIG) {
  return [
    `# ${platformPackageName(platform, config)}`,
    '',
    `The native \`${config.binName}\` binary for ${platform.platform} (Rust target \`${platform.target}\`).`,
    '',
    `Do not depend on this package directly; install [\`${config.metaPackage}\`](https://www.npmjs.com/package/${config.metaPackage}), which selects the right binary for the platform.`,
    '',
    'License: MIT OR Apache-2.0.',
    '',
  ].join('\n');
}

function writeJson(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n');
}

function copyLicenses(dir) {
  for (const license of LICENSES) fs.copyFileSync(path.join(REPO_ROOT, license), path.join(dir, license));
}

function assemble(opts) {
  const errors = validateConfig();
  const platforms = selectPlatforms(opts.targets);
  const version = opts.version || cargoVersion();
  const crateVersion = cargoVersion();
  if (!SEMVER.test(version)) errors.push(`invalid version ${version}`);
  if (version !== crateVersion) {
    errors.push(`version ${version} does not match crates/formualizer-cli/Cargo.toml ${crateVersion}`);
  }
  for (const license of LICENSES) {
    if (!fs.existsSync(path.join(REPO_ROOT, license))) errors.push(`missing ${license} at repository root`);
  }

  const binaries = path.resolve(opts.binaries);
  const sources = new Map();
  for (const platform of platforms) {
    const file = path.join(binaries, platform.target, binaryFileName(platform));
    sources.set(platform.target, file);
    errors.push(...inspectBinary(file, platform, { maxGlibc: opts.maxGlibc }));
  }

  let out = null;
  if (!opts.check) {
    out = path.resolve(opts.out);
    if (fs.existsSync(out) && fs.readdirSync(out).length) errors.push(`output directory ${out} is not empty`);
  }
  if (errors.length) {
    const err = new Error(errors.join('\n'));
    err.errors = errors;
    throw err;
  }

  const template = readTemplate();
  const packages = platforms.map((p) => ({
    kind: 'platform',
    target: p.target,
    name: platformPackageName(p),
    dir: packageDirName(platformPackageName(p)),
  }));
  packages.push({ kind: 'meta', name: CONFIG.metaPackage, dir: packageDirName(CONFIG.metaPackage) });
  const manifest = { version, packages };
  if (opts.check) return manifest;

  fs.mkdirSync(out, { recursive: true });
  for (const platform of platforms) {
    const dir = path.join(out, packageDirName(platformPackageName(platform)));
    fs.mkdirSync(path.join(dir, 'bin'), { recursive: true });
    const dest = path.join(dir, 'bin', binaryFileName(platform));
    fs.copyFileSync(sources.get(platform.target), dest);
    fs.chmodSync(dest, 0o755);
    writeJson(path.join(dir, 'package.json'), platformManifest(platform, version, template));
    fs.writeFileSync(path.join(dir, 'README.md'), platformReadme(platform));
    copyLicenses(dir);
  }

  const metaDir = path.join(out, packageDirName(CONFIG.metaPackage));
  fs.mkdirSync(path.join(metaDir, 'bin'), { recursive: true });
  const launcher = path.join(metaDir, 'bin', 'formualizer.js');
  fs.copyFileSync(path.join(PKG_ROOT, 'bin', 'formualizer.js'), launcher);
  fs.chmodSync(launcher, 0o755);
  fs.copyFileSync(path.join(PKG_ROOT, 'platforms.json'), path.join(metaDir, 'platforms.json'));
  writeJson(path.join(metaDir, 'package.json'), metaManifest(platforms, version, template));
  fs.writeFileSync(path.join(metaDir, 'README.md'), metaReadme());
  copyLicenses(metaDir);

  writeJson(path.join(out, 'manifest.json'), manifest);
  return manifest;
}

function main(argv) {
  let opts;
  try {
    opts = parseArgs(argv);
  } catch (err) {
    if (!(err instanceof UsageError)) throw err;
    process.stderr.write(`assemble: ${err.message}\n`);
    return 64;
  }
  if (opts.help) {
    process.stdout.write(fs.readFileSync(__filename, 'utf8').split('\n').slice(1, 12).map((l) => l.replace(/^\/\/ ?/, '')).join('\n') + '\n');
    return 0;
  }
  try {
    const manifest = assemble(opts);
    const verb = opts.check ? 'checked' : `assembled into ${path.resolve(opts.out)}`;
    process.stdout.write(`${verb}: ${manifest.packages.map((p) => p.name).join(', ')} @ ${manifest.version}\n`);
    return 0;
  } catch (err) {
    if (err instanceof UsageError) {
      process.stderr.write(`assemble: ${err.message}\n`);
      return 64;
    }
    process.stderr.write(`assemble: ${err.message}\n`);
    return 1;
  }
}

module.exports = {
  assemble,
  cargoVersion,
  inspectBinary,
  maxGlibcVersion,
  metaManifest,
  packageDirName,
  parseArgs,
  platformManifest,
  selectPlatforms,
  validateConfig,
};

if (require.main === module) process.exitCode = main(process.argv.slice(2));
