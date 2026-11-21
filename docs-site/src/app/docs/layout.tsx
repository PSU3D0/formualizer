import type { Node } from "fumadocs-core/page-tree";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import Link from "next/link";
import { baseOptions } from "@/lib/layout.shared";
import { source } from "@/lib/source";

const functionDetail = /^\/docs\/reference\/functions\/[^/]+\/[^/]+$/;

function sidebarNodes(nodes: Node[]): Node[] {
  // Category pages and the A–Z index link to every function. Embedding the
  // complete function tree here duplicates those links in every HTML export.
  return nodes.flatMap((node): Node[] => {
    if (node.type === "page" && functionDetail.test(node.url)) return [];
    if (node.type !== "folder") return [node];

    const children = sidebarNodes(node.children);
    if (children.length === 0 && node.index) return [node.index];
    return [{ ...node, children }];
  });
}

export default function Layout({ children }: LayoutProps<"/docs">) {
  const tree = source.getPageTree();

  return (
    <DocsLayout
      tree={{ ...tree, children: sidebarNodes(tree.children) }}
      {...baseOptions()}
      sidebar={{
        banner: (
          <nav aria-label="Function reference" className="grid gap-1.5 text-sm">
            <Link href="/docs/reference/functions" className="hover:underline">
              Excel Function Reference
            </Link>
            <Link
              href="/docs/reference/functions/a-z"
              className="hover:underline"
            >
              Functions A–Z
            </Link>
          </nav>
        ),
      }}
    >
      {children}
    </DocsLayout>
  );
}
