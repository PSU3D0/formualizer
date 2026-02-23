import type { MetadataRoute } from 'next';
import { source } from '@/lib/source';

const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000';

export default function sitemap(): MetadataRoute.Sitemap {
  const base = siteUrl.replace(/\/$/, '');

  const staticRoutes: MetadataRoute.Sitemap = [
    {
      url: `${base}/`,
      changeFrequency: 'weekly',
      priority: 1,
    },
    {
      url: `${base}/docs`,
      changeFrequency: 'daily',
      priority: 0.95,
    },
    {
      url: `${base}/formula-parser`,
      changeFrequency: 'weekly',
      priority: 0.9,
    },
  ];

  const docsRoutes: MetadataRoute.Sitemap = source.getPages().map((page) => ({
    url: `${base}${page.url}`,
    changeFrequency: 'weekly',
    priority: page.url.includes('/reference/functions/') ? 0.75 : 0.8,
  }));

  return [...staticRoutes, ...docsRoutes];
}
