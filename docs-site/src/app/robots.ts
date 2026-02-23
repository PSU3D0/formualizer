import type { MetadataRoute } from 'next';

const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000';

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
