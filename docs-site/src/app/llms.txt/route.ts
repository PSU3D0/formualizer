import type { Folder, Node } from "fumadocs-core/page-tree";
import { siteUrl } from "@/lib/env";
import { source } from "@/lib/source";

export const revalidate = false;

// Curated llms.txt (https://llmstxt.org): H1, blockquote summary, then H2
// sections of links. The Recalc CLI pages come first. The ~400 generated
// function pages are represented by a single link to the function index;
// /llms-full.txt still carries every page in full.

const SUMMARY =
  "Formualizer is an open-source spreadsheet engine written in Rust, with Python, JavaScript/WASM and Rust APIs. It parses Excel formulas, tracks dependencies and evaluates 400+ functions in-process. Its `formualizer recalc` command fills in the cached formula values of an .xlsx after openpyxl or another tool has edited it.";

const INTRO = [
  "Agents that edit .xlsx files: keep your editor, then run `formualizer recalc file.xlsx --json` as the last writing step and branch on the exit code (0 done, 2 refused, 3 stale with --check). Start with the Recalc CLI pages.",
  "Every page is also available as Markdown by appending `.mdx` to its URL. /llms-full.txt contains the full text of all pages.",
];

const SECTIONS: Array<{ folder: string; title: string }> = [
  { folder: "recalc-cli", title: "Recalc CLI" },
  { folder: "introduction", title: "Introduction" },
  { folder: "quickstarts", title: "Quickstarts" },
  { folder: "core-concepts", title: "Core concepts" },
  { folder: "guides", title: "Guides" },
  { folder: "sheetport", title: "SheetPort" },
  { folder: "reference", title: "Reference" },
];
const OPTIONAL_FOLDERS = ["playground"];
// Folders listed only by their index page.
const INDEX_ONLY = new Set(["/docs/reference/functions"]);

const base = siteUrl.replace(/\/$/, "");

function line(url: string): string {
  const page = source.getPages().find((p) => p.url === url);
  if (!page) throw new Error(`llms.txt: no page for ${url}`);
  const description = page.data.description ? `: ${page.data.description}` : "";
  return `- [${page.data.title}](${base}${url})${description}`;
}

// A folder's own page is `folder.index`, or a direct child page when its
// meta.json lists "index" explicitly.
function ownPages(folder: Folder): string[] {
  const urls = folder.children.flatMap((c) =>
    c.type === "page" ? [c.url] : [],
  );
  return folder.index ? [folder.index.url, ...urls] : urls;
}

function folderLines(folder: Folder): string[] {
  const indexOnly = ownPages(folder).find((url) => INDEX_ONLY.has(url));
  if (indexOnly) return [line(indexOnly)];
  const lines: string[] = [];
  if (folder.index) lines.push(line(folder.index.url));
  for (const child of folder.children) lines.push(...nodeLines(child));
  return lines;
}

function nodeLines(node: Node): string[] {
  if (node.type === "page") return [line(node.url)];
  if (node.type === "folder") return folderLines(node);
  return [];
}

function findFolder(nodes: Node[], url: string): Folder | undefined {
  for (const node of nodes) {
    if (node.type !== "folder") continue;
    if (ownPages(node).includes(url)) return node;
    const found = findFolder(node.children, url);
    if (found) return found;
  }
  return undefined;
}

function topFolder(name: string): Folder {
  const folder = findFolder(source.getPageTree().children, `/docs/${name}`);
  if (!folder) throw new Error(`llms.txt: no docs folder /docs/${name}`);
  return folder;
}

export async function GET() {
  const out: string[] = ["# Formualizer", "", `> ${SUMMARY}`, ""];
  out.push(...INTRO.flatMap((p) => [p, ""]));
  for (const { folder, title } of SECTIONS) {
    out.push(`## ${title}`, "", ...folderLines(topFolder(folder)), "");
  }
  out.push("## Optional", "");
  for (const folder of OPTIONAL_FOLDERS)
    out.push(...folderLines(topFolder(folder)));
  out.push(
    `- [Full documentation text](${base}/llms-full.txt): every page, including all function references, in one file`,
  );
  return new Response(`${out.join("\n")}\n`);
}
