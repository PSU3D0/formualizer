import type { MetadataRoute } from 'next';
import { siteUrl } from '@/lib/env';

export default function robots(): MetadataRoute.Robots {
  const base = siteUrl.replace(/\/$/, '');

  return {
    rules: {
      userAgent: '*',
      allow: ['/', '/docs', '/formula-parser'],
      disallow: ['/llms.txt', '/llms-full.txt', '/llms.mdx/'],
    },
    sitemap: `${base}/sitemap.xml`,
  };
}
