import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// This whole staging directory is under the existing ignored pkg-node tree.
// wasm-pack must first generate --target web --out-dir pkg-node/recalc-browser/raw.
const stage = path.join(root, 'pkg-node/recalc-browser');
await fs.access(path.join(stage, 'raw/formualizer_wasm.js'));
await fs.mkdir(path.join(stage, 'examples'), { recursive: true });
for (const file of ['index.js', 'recalc.js', 'manifest.js']) {
  const built = await fs.readFile(path.join(root, 'dist', file), 'utf8');
  await fs.writeFile(path.join(stage, file), built.replaceAll('../pkg/formualizer_wasm.js', './raw-entry.js'));
}
await fs.writeFile(path.join(stage, 'raw-entry.js'), `
import init from './raw/formualizer_wasm.js';
export * from './raw/formualizer_wasm.js';
await init();
`);
for (const file of ['recalc-viewport.mjs', 'recalc-async.mjs', 'recalc-pause.mjs']) {
  await fs.copyFile(path.join(root, 'examples', file), path.join(stage, 'examples', file));
}
await fs.copyFile(path.join(root, 'examples/recalc-browser.html'), path.join(stage, 'index.html'));
console.log('Browser entry: pkg-node/recalc-browser/index.html');
console.log('Success selector: #result[data-status="passed"]; text starts RECALC_BROWSER_OK');
