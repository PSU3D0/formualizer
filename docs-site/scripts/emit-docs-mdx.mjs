// Post-build step for the static export (`output: 'export'`).
//
// The `/llms.mdx/docs/[[...slug]]` route handler emits per-page Markdown as
// `out/llms.mdx/docs/**/<name>.mdx`. On Vercel these were exposed at the public
// `/docs/**.mdx` URLs via a Next.js rewrite, which is unavailable in a static
// export. This script mirrors each emitted file to `out/docs/**.mdx` so the
// original public Markdown URLs (used by the "Copy Markdown" / "Open in ..."
// page actions) keep working as plain static assets — no runtime rewrite needed.

import { cp, mkdir, readdir, stat } from 'node:fs/promises';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const srcRoot = join(root, 'out', 'llms.mdx', 'docs');
const destRoot = join(root, 'out', 'docs');

async function walk(dir) {
  const out = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await walk(full)));
    else if (entry.name.endsWith('.mdx')) out.push(full);
  }
  return out;
}

async function main() {
  try {
    await stat(srcRoot);
  } catch {
    console.error(`[emit-docs-mdx] source dir missing: ${srcRoot}`);
    process.exit(1);
  }

  const files = await walk(srcRoot);
  let copied = 0;
  for (const file of files) {
    const rel = relative(srcRoot, file); // e.g. reference/functions/database.mdx
    const dest = join(destRoot, rel); // out/docs/reference/functions/database.mdx
    await mkdir(dirname(dest), { recursive: true });
    await cp(file, dest);
    copied++;
  }
  console.log(`[emit-docs-mdx] mirrored ${copied} Markdown files to out/docs/**.mdx`);

  // Next.js excludes `_`-prefixed files from the public/ copy, so `_headers` is
  // placed into the export output here (Cloudflare reads it from the assets root).
  await cp(join(root, 'public', '_headers'), join(root, 'out', '_headers'));
  console.log('[emit-docs-mdx] copied _headers into out/');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
