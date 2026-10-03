// Rewrites repository-relative Markdown links in docs that the site pulls in
// with <include> (docs/cli.md, docs/agents.md, docs/cache-only-xlsx.md, the
// agent skill). Those files are written for GitHub, where `cli.md` or
// `cache-only-xlsx.md#refusal-messages` resolve next to the file; on the site
// they must point at the matching page. An unknown relative `.md` link fails
// the build, so a new cross-reference cannot silently ship as a dead link.

const REPO_BLOB = "https://github.com/psu3d0/formualizer/blob/main";

/** Repository Markdown file (as linked from docs/) -> site page. */
export const REPO_DOC_PAGES: Record<string, string> = {
  "cli.md": "/docs/recalc-cli/cli-reference",
  "agents.md": "/docs/recalc-cli/agent-workflow",
  "cache-only-xlsx.md": "/docs/recalc-cli/supported-and-refused",
  "../skills/formualizer-recalc/SKILL.md": "/docs/recalc-cli/agent-skill",
};

/** Repository files linked from included docs that have no site page. */
const REPO_FILES: Record<string, string> = {
  "packaging-and-releases.md": `${REPO_BLOB}/docs/packaging-and-releases.md`,
};

type MdNode = { type: string; url?: string; children?: MdNode[] };

function isRelativeMarkdown(url: string): boolean {
  return (
    !/^[a-z][a-z0-9+.-]*:/i.test(url) &&
    !url.startsWith("/") &&
    !url.startsWith("#") &&
    /\.md(#|$)/.test(url)
  );
}

export function rewriteRepoLink(url: string): string | undefined {
  if (!isRelativeMarkdown(url)) return undefined;
  const hash = url.indexOf("#");
  const path = (hash === -1 ? url : url.slice(0, hash)).replace(/^\.\//, "");
  const fragment = hash === -1 ? "" : url.slice(hash);
  const page = REPO_DOC_PAGES[path];
  if (page) return page + fragment;
  const file = REPO_FILES[path];
  if (file) return file + fragment;
  throw new Error(
    `remark-repo-links: no site page for repository link "${url}". Add it to REPO_DOC_PAGES or REPO_FILES in docs-site/src/lib/remark-repo-links.ts.`,
  );
}

function walk(node: MdNode): void {
  if ((node.type === "link" || node.type === "definition") && node.url) {
    const rewritten = rewriteRepoLink(node.url);
    if (rewritten) node.url = rewritten;
  }
  for (const child of node.children ?? []) walk(child);
}

/** Remark plugin. Runs after fumadocs-mdx's <include> expansion. */
export function remarkRepoLinks() {
  return (tree: MdNode) => walk(tree);
}
