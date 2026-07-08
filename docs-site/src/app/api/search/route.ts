import { source } from '@/lib/source';
import { createFromSource } from 'fumadocs-core/search/server';

// Static export: emit a pre-built search index as a static asset at /api/search,
// consumed client-side by the Orama static search client (see RootProvider config).
export const revalidate = false;

export const { staticGET: GET } = createFromSource(source, {
  // https://docs.orama.com/docs/orama-js/supported-languages
  language: 'english',
});
