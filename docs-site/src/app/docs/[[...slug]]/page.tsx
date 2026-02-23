import { getPageImage, source } from '@/lib/source';
import { DocsBody, DocsDescription, DocsPage, DocsTitle } from 'fumadocs-ui/layouts/docs/page';
import { notFound } from 'next/navigation';
import { getMDXComponents } from '@/mdx-components';
import type { Metadata } from 'next';
import { createRelativeLink } from 'fumadocs-ui/mdx';
import { LLMCopyButton, ViewOptions } from '@/components/ai/page-actions';
import { gitConfig } from '@/lib/layout.shared';
import { siteUrl } from '@/lib/env';

function buildBreadcrumbJsonLd(slugs: string[], title: string) {
  const base = siteUrl.replace(/\/$/, '');
  const items = [
    { name: 'Formualizer', url: `${base}/` },
    { name: 'Docs', url: `${base}/docs` },
  ];

  // Build intermediate breadcrumbs from slug segments
  let path = '/docs';
  for (let i = 0; i < slugs.length - 1; i++) {
    path += `/${slugs[i]}`;
    // Capitalize segment for display (e.g., "core-concepts" → "Core Concepts")
    const name = slugs[i]
      .split('-')
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(' ');
    items.push({ name, url: `${base}${path}` });
  }

  // Final breadcrumb is the current page
  items.push({ name: title, url: `${base}/docs/${slugs.join('/')}` });

  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: items.map((item, index) => ({
      '@type': 'ListItem',
      position: index + 1,
      name: item.name,
      item: item.url,
    })),
  };
}

export default async function Page(props: PageProps<'/docs/[[...slug]]'>) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const MDX = page.data.body;
  const slugs = params.slug ?? [];
  const breadcrumbJsonLd = buildBreadcrumbJsonLd(slugs, page.data.title);

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumbJsonLd) }}
      />
      <DocsPage toc={page.data.toc} full={page.data.full}>
        <DocsTitle>{page.data.title}</DocsTitle>
        <DocsDescription className="mb-0">{page.data.description}</DocsDescription>
        <div className="flex flex-row gap-2 items-center border-b pb-6">
          <LLMCopyButton markdownUrl={`${page.url}.mdx`} />
          <ViewOptions
            markdownUrl={`${page.url}.mdx`}
            githubUrl={`https://github.com/${gitConfig.user}/${gitConfig.repo}/blob/${gitConfig.branch}/content/docs/${page.path}`}
          />
        </div>
        <DocsBody>
          <MDX
            components={getMDXComponents({
              // this allows you to link to other pages with relative file paths
              a: createRelativeLink(source, page),
            })}
          />
        </DocsBody>
      </DocsPage>
    </>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

function getPageKeywords(slugs: string[], title: string): string[] | undefined {
  const section = slugs[0];

  // Function reference pages: excel-specific keywords
  if (slugs.length >= 4 && section === 'reference' && slugs[1] === 'functions') {
    return [
      `${title} function`,
      `${title} formula`,
      `excel ${title.toLowerCase()} function`,
      'spreadsheet function reference',
      'formualizer',
    ];
  }

  // SheetPort pages
  if (section === 'sheetport') {
    return [
      'sheetport',
      'spreadsheet as API',
      'spreadsheet typed interface',
      'spreadsheet function interface',
      'FIO manifest',
      'workbook I/O',
      'formualizer sheetport',
      'deterministic spreadsheet evaluation',
    ];
  }

  // Reference pages (non-function)
  if (section === 'reference') {
    return [
      'formualizer API',
      'spreadsheet engine API',
      title.toLowerCase(),
      'excel formula engine',
      'wasm spreadsheet',
    ];
  }

  // Core concepts
  if (section === 'core-concepts') {
    return [
      'spreadsheet engine',
      'formula evaluation',
      title.toLowerCase(),
      'excel engine internals',
      'formualizer',
    ];
  }

  // Guides
  if (section === 'guides') {
    return [
      'formualizer guide',
      'spreadsheet engine tutorial',
      title.toLowerCase(),
      'excel formula engine',
    ];
  }

  // Quickstarts
  if (section === 'quickstarts') {
    return [
      'formualizer quickstart',
      'spreadsheet engine setup',
      title.toLowerCase(),
      'formula engine getting started',
    ];
  }

  return undefined;
}

export async function generateMetadata(props: PageProps<'/docs/[[...slug]]'>): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const slugs = params.slug ?? [];
  const keywords = getPageKeywords(slugs, page.data.title);

  return {
    title: page.data.title,
    description: page.data.description,
    keywords,
    alternates: {
      canonical: page.url,
    },
    openGraph: {
      title: page.data.title,
      description: page.data.description,
      url: page.url,
      images: getPageImage(page).url,
    },
    twitter: {
      card: 'summary_large_image',
      title: page.data.title,
      description: page.data.description,
      images: [getPageImage(page).url],
    },
  };
}
