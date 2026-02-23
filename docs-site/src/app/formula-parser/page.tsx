import type { Metadata } from 'next';
import FormulaParserPageClient from './page.client';

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
  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'WebApplication',
    name: 'Formualizer Formula Parser',
    applicationCategory: 'DeveloperApplication',
    operatingSystem: 'Web Browser',
    url: '/formula-parser',
    description:
      'Browser-based tool to parse Excel-style formulas, inspect AST/tokens, and debug evaluation flow.',
    offers: {
      '@type': 'Offer',
      price: '0',
      priceCurrency: 'USD',
    },
  };

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
