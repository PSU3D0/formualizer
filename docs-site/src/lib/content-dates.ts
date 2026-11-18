import { readFileSync } from "node:fs";
import { posix, resolve } from "node:path";
import contentDates from "@/generated/content-dates.json";

const docs = "docs-site/content/docs";
const app = "docs-site/src/app";
const home = `${app}/(home)`;
const repo = resolve(process.cwd(), "..");

export const staticRouteSources = {
  "/": [
    `${home}/page.tsx`,
    "docs-site/src/components/home",
    "docs-site/public/home",
  ],
  "/formula-parser": [
    `${app}/formula-parser/page.tsx`,
    `${app}/formula-parser/page.client.tsx`,
    "docs-site/src/components/playground/formula-parser-tool.tsx",
  ],
  "/evaluate-excel-formulas-python": [
    `${home}/evaluate-excel-formulas-python/page.tsx`,
  ],
  "/spreadsheet-engine-for-ai-agents": [
    `${home}/spreadsheet-engine-for-ai-agents/page.tsx`,
  ],
};

export function sourceLastModified(paths: string[]): string {
  let latest = "";
  for (const [file, { date }] of Object.entries(contentDates)) {
    if (
      date > latest &&
      paths.some((path) => file === path || file.startsWith(`${path}/`))
    ) {
      latest = date;
    }
  }
  if (!latest)
    throw new Error(`No committed content source for ${paths.join(", ")}`);
  return latest;
}

export function docsLastModified(path: string): string {
  const file = `${docs}/${path}`;
  const content = readFileSync(resolve(repo, file), "utf8");
  const includes = [
    ...content.matchAll(
      /<include(?:\s+[^>]*)?>([^<#]+)(?:#[^<]*)?<\/include>/g,
    ),
  ].map(([, dependency]) =>
    posix.normalize(posix.join(posix.dirname(file), dependency.trim())),
  );
  return sourceLastModified([file, ...includes]);
}
