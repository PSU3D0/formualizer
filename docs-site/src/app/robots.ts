import type { MetadataRoute } from 'next';
import { siteUrl } from '@/lib/env';

export default function robots(): MetadataRoute.Robots {
  const base = siteUrl.replace(/\/$/, '');

  return {
    rules: [
      {
        // OpenAI search — must be allowed to appear in ChatGPT search results
        userAgent: 'OAI-SearchBot',
        allow: '/',
      },
      {
        // OpenAI training crawler — allow so content is represented in models
        userAgent: 'GPTBot',
        allow: '/',
      },
      {
        // ChatGPT user-initiated browsing (robots.txt advisory per OAI docs)
        userAgent: 'ChatGPT-User',
        allow: '/',
      },
      {
        // Anthropic
        userAgent: ['ClaudeBot', 'anthropic-ai', 'Claude-Web'],
        allow: '/',
      },
      {
        // Other AI crawlers
        userAgent: ['PerplexityBot', 'Google-Extended', 'Bytespider', 'cohere-ai'],
        allow: '/',
      },
      {
        // All other bots — index HTML pages, skip raw LLM text (duplicate content)
        userAgent: '*',
        allow: ['/', '/docs', '/formula-parser'],
        disallow: ['/llms.txt', '/llms-full.txt', '/llms.mdx/'],
      },
    ],
    sitemap: `${base}/sitemap.xml`,
  };
}
