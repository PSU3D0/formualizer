// Post-build check for the static export, run in docs CI after `pnpm build`.
//
// The Recalc CLI pages pull docs/cli.md, docs/agents.md, docs/cache-only-xlsx.md
// and skills/formualizer-recalc/SKILL.md in at build time with <include>, so
// they cannot drift from those files: a missing section or an unknown
// repository link already fails the build. This script checks what the build
// cannot: that every include was expanded, that the curated llms.txt lists the
// Recalc CLI pages and stays curated, and that the copied skill, redirects and
// headers reached out/.

import { readdir, readFile, stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const out = join(root, "out");
const failures = [];

function fail(message) {
  failures.push(message);
}

async function exists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

async function walk(dir, suffix) {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(full, suffix)));
    else if (entry.name.endsWith(suffix)) files.push(full);
  }
  return files;
}

async function main() {
  const site = (await readFile(join(root, ".env.production"), "utf8")).match(
    /^NEXT_PUBLIC_SITE_URL=(.+)$/m,
  )?.[1];
  const meta = JSON.parse(
    await readFile(join(root, "content/docs/recalc-cli/meta.json"), "utf8"),
  );
  const pages = meta.pages.map((page) =>
    page === "index" ? "/docs/recalc-cli" : `/docs/recalc-cli/${page}`,
  );

  for (const page of pages) {
    for (const ext of [".html", ".mdx"]) {
      if (!(await exists(join(out, `${page}${ext}`)))) {
        fail(`missing ${page}${ext} in out/`);
      }
    }
  }

  for (const file of [
    ...(await walk(join(out, "docs"), ".mdx")),
    join(out, "llms-full.txt"),
  ]) {
    const content = await readFile(file, "utf8");
    if (content.includes("<include")) {
      fail(`unexpanded <include> in ${file.slice(root.length + 1)}`);
    }
    if (content.includes("<FunctionExample ")) {
      fail(`unexpanded <FunctionExample> in ${file.slice(root.length + 1)}`);
    }
  }

  const sitemap = await readFile(join(out, "sitemap.xml"), "utf8");
  const routes = [...sitemap.matchAll(/<url>([\s\S]*?)<\/url>/g)];
  if (routes.length === 0) fail("sitemap contains no URLs");
  for (const [, route] of routes) {
    if (!/<lastmod>\d{4}-\d{2}-\d{2}<\/lastmod>/.test(route)) {
      fail(
        `sitemap missing a source date for ${route.match(/<loc>(.*?)<\/loc>/)?.[1]}`,
      );
    }
  }

  const functionPages = JSON.parse(
    await readFile(join(root, "src/generated/functions-meta.json"), "utf8"),
  );
  const dateManifest = JSON.parse(
    await readFile(join(root, "src/generated/content-dates.json"), "utf8"),
  );
  const sitemapDates = new Map(
    routes.map(([, route]) => [
      route.match(/<loc>(.*?)<\/loc>/)?.[1],
      route.match(/<lastmod>(.*?)<\/lastmod>/)?.[1],
    ]),
  );
  const { version, unreleasedChanges } = JSON.parse(
    await readFile(join(root, "src/generated/reference-engine.json"), "utf8"),
  );
  const compatibility = JSON.parse(
    await readFile(
      join(root, "src/generated/example-editor-compatibility.json"),
      "utf8",
    ),
  );
  const engineLabel =
    version + (unreleasedChanges ? " plus unreleased changes" : "");
  for (const [id, meta] of Object.entries(functionPages)) {
    const path = `/docs/reference/functions/${id}`;
    const html = await readFile(join(out, `${path}.html`), "utf8");
    const date = dateManifest[`docs-site/content${path}.mdx`]?.date;
    if (!date || !html.includes(`"dateModified":"${date}"`)) {
      fail(`function page has the wrong dateModified for ${path}`);
    }
    const robots = html.match(/<meta name="robots" content="([^"]*)"\/?>/)?.[1];
    const sitemapDate = sitemapDates.get(`${site}${path}`);
    if (
      meta.noindex
        ? robots !== "noindex, follow" || sitemapDate
        : robots?.includes("noindex") || sitemapDate !== date
    ) {
      fail(`unexpected robots or sitemap entry for ${path}`);
    }
    if (
      !html.includes(
        `The Formualizer engine in this repository at ${engineLabel} computes the example results.`,
      )
    ) {
      fail(`missing reference engine version for ${path}`);
    }
    const examples = html.split("<section data-function-example=").slice(1);
    const editors = examples.filter((example) =>
      example.split("</section>")[0].includes("data-example-editor"),
    );
    const excluded = compatibility.mismatches.filter(
      ({ name }) => name === meta.name,
    );
    if (
      editors.length !== examples.length - excluded.length ||
      editors.some(
        (editor) =>
          !editor.includes(
            `published Formualizer ${compatibility.version} package`,
          ),
      )
    ) {
      fail(`unexpected browser editor availability or version for ${path}`);
    }
  }

  const llms = await readFile(join(out, "llms.txt"), "utf8");
  if (!/^# Formualizer\n\n> \S/.test(llms)) {
    fail("llms.txt must start with '# Formualizer' and a blockquote summary");
  }
  const sections = [...llms.matchAll(/^## (.+)$/gm)].map((m) => m[1]);
  if (sections[0] !== "Recalc CLI") {
    fail(`llms.txt must list Recalc CLI first, found ${sections[0]}`);
  }
  for (const page of pages) {
    if (!llms.includes(`](${site}${page})`)) {
      fail(`llms.txt does not link ${page}`);
    }
  }
  if (llms.includes("/docs/reference/functions/")) {
    fail("llms.txt lists individual function pages; link the index only");
  }

  const skill = "skills/formualizer-recalc/SKILL.md";
  const [source, copied] = await Promise.all([
    readFile(join(dirname(root), skill), "utf8"),
    readFile(join(out, skill), "utf8").catch(() => null),
  ]);
  if (copied !== source)
    fail(`out/${skill} is missing or differs from ${skill}`);

  for (const file of ["_headers", "_redirects"]) {
    const [want, got] = await Promise.all([
      readFile(join(root, "public", file), "utf8"),
      readFile(join(out, file), "utf8").catch(() => null),
    ]);
    if (got !== want)
      fail(`out/${file} is missing or differs from public/${file}`);
  }

  if (failures.length > 0) {
    console.error(`[check-export] ${failures.length} problem(s):`);
    for (const message of failures) console.error(`  - ${message}`);
    process.exit(1);
  }
  console.log(
    `[check-export] ok: ${pages.length} Recalc CLI pages, Markdown expanded, ${routes.length} sitemap dates, ${Object.keys(functionPages).length} function robots and dates, llms.txt curated, skill and edge files copied`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
