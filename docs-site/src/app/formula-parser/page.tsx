import type { Metadata } from 'next';
import FormulaParserPageClient from './page.client';
import { siteUrl } from '@/lib/env';

export const metadata: Metadata = {
  title: 'Formula Parser',
  description:
    'Free browser-based Excel formula parser. Inspect AST, token stream, dependencies, and evaluation flow with actionable diagnostics.',
  keywords: [
    'excel formula parser',
    'spreadsheet formula parser',
    'formula syntax checker',
    'formula AST viewer',
    'excel formula debugger',
    'formula tokenizer',
    'parse excel formula online',
    'formula dependency viewer',
  ],
  alternates: {
    canonical: '/formula-parser',
  },
  openGraph: {
    title: 'Formula Parser | Formualizer',
    description:
      'Parse and inspect Excel-style formulas in-browser with AST, references, and evaluation flow.',
    url: '/formula-parser',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Formula Parser | Formualizer',
    description:
      'Parse and inspect Excel-style formulas with AST, token stream, and diagnostics.',
  },
};

export default function FormulaParserPage() {
  const base = siteUrl.replace(/\/$/, '');

  const jsonLd = [
    {
      '@context': 'https://schema.org',
      '@type': 'WebApplication',
      name: 'Formualizer Formula Parser',
      applicationCategory: 'DeveloperApplication',
      operatingSystem: 'Web Browser',
      url: `${base}/formula-parser`,
      description:
        'Browser-based tool to parse Excel-style formulas, inspect AST/tokens, and debug evaluation flow.',
      offers: {
        '@type': 'Offer',
        price: '0',
        priceCurrency: 'USD',
      },
    },
    {
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: [
        {
          '@type': 'Question',
          name: 'How do I parse an Excel formula online?',
          acceptedAnswer: {
            '@type': 'Answer',
            text: 'Paste your formula into the editor and pause typing. The parser auto-runs in your browser and shows diagnostics, references, AST structure, and evaluation steps.',
          },
        },
        {
          '@type': 'Question',
          name: 'Can this tool debug nested IF and complex formulas?',
          acceptedAnswer: {
            '@type': 'Answer',
            text: 'Yes. It handles deeply nested formulas, then breaks them into intermediate evaluation steps so you can understand operation order and identify fragile segments.',
          },
        },
        {
          '@type': 'Question',
          name: 'Does this parser show formula dependencies?',
          acceptedAnswer: {
            '@type': 'Answer',
            text: 'Yes. The references panel extracts cell and range dependencies, and the function panel lists all called functions found in your formula.',
          },
        },
        {
          '@type': 'Question',
          name: 'Can it fix formula syntax errors automatically?',
          acceptedAnswer: {
            '@type': 'Answer',
            text: 'It suggests safe fixes for common syntax issues such as unmatched parentheses, trailing separators, and unclosed quotes. Suggested fixes are validated before being shown.',
          },
        },
      ],
    },
  ];

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <FormulaParserPageClient />
    </>
  );
}
