import functionsMeta from '@/generated/functions-meta.json';
import { siteUrl } from '@/lib/env';

type FunctionMetaRecord = {
  name: string;
  category: string;
  shortSummary?: string;
};

const META = functionsMeta as Record<string, FunctionMetaRecord>;

function displayCategory(category: string): string {
  switch (category) {
    case 'datetime':
      return 'Date & Time';
    case 'stats':
      return 'Statistics';
    case 'logical-ext':
      return 'Logical (Extended)';
    case 'reference-fns':
      return 'Reference';
    case 'lambda':
      return 'LET / LAMBDA';
    case 'info':
      return 'Information';
    default:
      return category
        .split(/[-_]/)
        .filter(Boolean)
        .map((part) => part[0].toUpperCase() + part.slice(1))
        .join(' ');
  }
}

export function FunctionPageSchema({ id }: { id: string }) {
  const meta = META[id];
  if (!meta) return null;

  const [categorySlug, functionSlug] = id.split('/');
  if (!categorySlug || !functionSlug) return null;

  const base = siteUrl.replace(/\/$/, '');
  const pageUrl = `${siteUrl}/docs/reference/functions/${categorySlug}/${functionSlug}`;
  const description = meta.shortSummary ?? `${meta.name} function reference and examples.`;

  const breadcrumb = {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: [
      {
        '@type': 'ListItem',
        position: 1,
        name: 'Docs',
        item: `${siteUrl}/docs`,
      },
      {
        '@type': 'ListItem',
        position: 2,
        name: 'Reference',
        item: `${siteUrl}/docs/reference`,
      },
      {
        '@type': 'ListItem',
        position: 3,
        name: 'Functions',
        item: `${siteUrl}/docs/reference/functions`,
      },
      {
        '@type': 'ListItem',
        position: 4,
        name: displayCategory(meta.category),
        item: `${siteUrl}/docs/reference/functions/${categorySlug}`,
      },
      {
        '@type': 'ListItem',
        position: 5,
        name: meta.name,
        item: pageUrl,
      },
    ],
  };

  const article = {
    '@context': 'https://schema.org',
    '@type': 'TechArticle',
    headline: `${meta.name} function`,
    description,
    url: pageUrl,
    mainEntityOfPage: pageUrl,
    inLanguage: 'en',
    author: {
      '@type': 'Organization',
      name: 'Formualizer',
    },
    publisher: {
      '@type': 'Organization',
      name: 'Formualizer',
    },
    about: [
      'Spreadsheet functions',
      'Excel formula compatibility',
      `${meta.name} formula`,
    ],
  };

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }}
      />
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(article) }}
      />
    </>
  );
}
