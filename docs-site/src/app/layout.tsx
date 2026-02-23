import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';
import { Inter } from 'next/font/google';
import type { Metadata } from 'next';

const inter = Inter({
  subsets: ['latin'],
});

const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000';

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: {
    default: 'Formualizer Docs',
    template: '%s | Formualizer Docs',
  },
  description:
    'Formualizer documentation and utilities for Excel-style formula parsing, AST inspection, workbook evaluation, and bindings.',
  keywords: [
    'excel formula parser',
    'formula AST',
    'spreadsheet engine',
    'workbook evaluator',
    'formualizer',
    'xlsx formulas',
    'formula debugging',
    'wasm spreadsheet',
    'formula tokenizer',
  ],
  alternates: {
    canonical: '/',
  },
  icons: {
    icon: [
      { url: '/favicon.ico' },
      { url: '/icon.svg', type: 'image/svg+xml' },
      { url: '/icon.png', sizes: '512x512', type: 'image/png' },
    ],
    apple: [{ url: '/apple-icon.png', sizes: '180x180', type: 'image/png' }],
  },
  manifest: '/site.webmanifest',
  openGraph: {
    type: 'website',
    url: siteUrl,
    title: 'Formualizer Docs',
    description:
      'Documentation and interactive tools for parsing and evaluating Excel-style formulas.',
    siteName: 'Formualizer',
    images: [{ url: '/opengraph-image.png', width: 1200, height: 630 }],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Formualizer Docs',
    description:
      'Interactive formula parser and docs for Formualizer workbook and evaluation engine.',
    images: ['/twitter-image.png'],
  },
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <body className="flex min-h-screen flex-col overflow-x-clip">
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
