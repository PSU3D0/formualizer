import { docs } from 'fumadocs-mdx:collections/server';
import { type InferPageType, loader } from 'fumadocs-core/source';
import { lucideIconsPlugin } from 'fumadocs-core/source/lucide-icons';
import functionsMeta from '@/generated/functions-meta.json';

// See https://fumadocs.dev/docs/headless/source-api for more info
export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
  plugins: [lucideIconsPlugin()],
});

type FunctionArg = {
  name: string;
  kinds: string[];
  required: boolean;
  shape: string;
  by_ref: boolean;
  coercion: string;
  max: string | null;
  repeating: string | null;
  has_default: boolean;
};

type FunctionMetaRecord = {
  name: string;
  category: string;
  minArgs: number | null;
  maxArgs: number | null;
  variadic: boolean | null;
  signature: string | null;
  args?: FunctionArg[];
  caps: string[];
  implementationSource: string;
  registrationSource: string;
};

const META = functionsMeta as Record<string, FunctionMetaRecord>;

export function getPageImage(page: InferPageType<typeof source>) {
  const segments = [...page.slugs, 'image.webp'];

  return {
    segments,
    url: `/og/docs/${segments.join('/')}`,
  };
}

function llmSignature(meta: FunctionMetaRecord): string {
  const args = meta.args ?? [];
  if (args.length === 0) return meta.signature ?? `${meta.name}(...)`;

  const minArgs = meta.minArgs ?? 0;
  if (args.length === 1 && minArgs > 1) {
    return `${meta.name}(arg1, arg2, ... argN)`;
  }

  const rendered = args.map((arg, index) => {
    const optional = arg.required ? '?' : '';
    const variadicTail = meta.variadic && index === args.length - 1 ? '…' : '';
    const shape = arg.shape === 'scalar' ? '' : ` (${arg.shape})`;
    return `${arg.name}${optional}${variadicTail}: ${arg.kinds.join('|')}${shape}`;
  });

  return `${meta.name}(${rendered.join(', ')})`;
}

function markdownRuntimeMeta(id: string): string {
  const meta = META[id];
  if (!meta) return `Runtime metadata unavailable for ${id}.`;

  const args = (meta.args ?? [])
    .map((arg) => {
      const optional = arg.required ? '' : '?';
      const shape = arg.shape === 'scalar' ? '' : ` (${arg.shape})`;
      const extra = [] as string[];
      if (arg.by_ref) extra.push('by-ref');
      if (arg.has_default) extra.push('default');
      if (arg.coercion && arg.coercion !== 'None') extra.push(`coercion=${arg.coercion}`);
      return `- ${arg.name}${optional}: ${arg.kinds.join('|')}${shape}${extra.length ? ` [${extra.join(', ')}]` : ''}`;
    })
    .join('\n');

  const caps = meta.caps.length > 0 ? meta.caps.join(', ') : 'none';
  const maxArgs = meta.variadic ? 'variadic' : (meta.maxArgs ?? 'unknown').toString();

  return [
    `- Name: ${meta.name}`,
    `- Category: ${meta.category}`,
    `- Arity: min ${meta.minArgs ?? 'unknown'}, max ${maxArgs}`,
    `- Signature: ${llmSignature(meta)}`,
    `- Caps: ${caps}`,
    `- Source: ${meta.implementationSource}`,
    args ? '- Arguments:\n' + args : '',
  ]
    .filter(Boolean)
    .join('\n');
}

function renderLlmFriendlyMarkdown(processed: string): string {
  const withRuntime = processed.replace(
    /<FunctionMeta\s+id="([^"]+)"\s*\/>/g,
    (_full, id: string) => markdownRuntimeMeta(id),
  );

  return withRuntime.replace(/\{\/\*\s*\[formualizer-docgen:function-meta:(start|end)\]\s*\*\/\}/g, '');
}

export async function getLLMText(page: InferPageType<typeof source>) {
  const processed = await page.data.getText('processed');
  const llmSafe = renderLlmFriendlyMarkdown(processed);

  return `# ${page.data.title}

${llmSafe}`;
}
