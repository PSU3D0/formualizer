import { getLLMText, source } from '@/lib/source';
import { notFound } from 'next/navigation';

export const revalidate = false;

// The last slug segment carries a literal `.mdx` suffix so the static export
// emits real `foo.mdx` files rather than extensionless `foo` files. Extensionless
// files collide with sibling directories of the same name (e.g. a `database` leaf
// page next to a `database/` category dir) and crash the export copy step (EISDIR).
// A post-build step (scripts/emit-docs-mdx.mjs) mirrors these to `/docs/**.mdx`
// to preserve the original public Markdown URLs without a runtime rewrite.
function stripMdx(slug: string[] | undefined): string[] {
  if (!slug || slug.length === 0) return [];
  const last = slug[slug.length - 1].replace(/\.mdx$/, '');
  return [...slug.slice(0, -1), last];
}

export async function GET(_req: Request, { params }: RouteContext<'/llms.mdx/docs/[[...slug]]'>) {
  const { slug } = await params;
  const page = source.getPage(stripMdx(slug));
  if (!page) notFound();

  return new Response(await getLLMText(page), {
    headers: {
      'Content-Type': 'text/markdown',
    },
  });
}

export function generateStaticParams() {
  // Drop the empty-slug (docs index) entry — it would emit `out/llms.mdx/docs`,
  // colliding with the `out/llms.mdx/docs/` directory. Append `.mdx` to the final
  // segment so emitted filenames never collide with sibling directories.
  return source
    .generateParams()
    .filter((p) => (p.slug?.length ?? 0) > 0)
    .map((p) => {
      const slug = p.slug as string[];
      return { ...p, slug: [...slug.slice(0, -1), `${slug[slug.length - 1]}.mdx`] };
    });
}
