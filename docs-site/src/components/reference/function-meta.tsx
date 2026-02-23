import Link from 'next/link';
import functionsMeta from '@/generated/functions-meta.json';

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
  shortSummary?: string;
  typeName: string;
  minArgs: number | null;
  maxArgs: number | null;
  variadic: boolean | null;
  signature: string | null;
  argSchema: string | null;
  args?: FunctionArg[];
  caps: string[];
  registrationSource: string;
  implementationSource: string;
};

const META = functionsMeta as Record<string, FunctionMetaRecord>;

const CAP_DESCRIPTIONS: Record<string, string> = {
  PURE: 'No side effects for identical inputs.',
  VOLATILE: 'May change between recalculations without input changes.',
  REDUCTION: 'Aggregates many values into one result.',
  ELEMENTWISE: 'Evaluates value-by-value across inputs.',
  WINDOWED: 'Depends on positional context or windows.',
  LOOKUP: 'Performs key/index lookups across ranges.',
  NUMERIC_ONLY: 'Expects numeric arguments.',
  BOOL_ONLY: 'Expects logical/boolean arguments.',
  SIMD_OK: 'Suitable for vectorized execution paths.',
  STREAM_OK: 'Suitable for streaming/chunked execution.',
  GPU_OK: 'Potentially suitable for GPU execution paths.',
  RETURNS_REFERENCE: 'Returns references/ranges instead of only scalar values.',
  SHORT_CIRCUIT: 'May skip evaluation of some arguments.',
  PARALLEL_ARGS: 'Independent args can evaluate in parallel.',
  PARALLEL_CHUNKS: 'Chunked data can evaluate in parallel.',
  DYNAMIC_DEPENDENCY: 'Dependency shape may change at runtime.',
};

function titleCase(input: string): string {
  return input
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

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
      return titleCase(category);
  }
}

function prettyKinds(kinds: string[]): string {
  return kinds
    .map((kind) => {
      switch (kind) {
        case 'any':
          return 'Any';
        case 'logical':
          return 'Logical';
        case 'number':
          return 'Number';
        case 'text':
          return 'Text';
        case 'range':
          return 'Range';
        case 'array':
          return 'Array';
        case 'reference':
          return 'Reference';
        default:
          return titleCase(kind);
      }
    })
    .join(' | ');
}

function prettyShape(shape: string): string {
  switch (shape) {
    case 'scalar':
      return 'Scalar';
    case 'range':
      return 'Range';
    case 'array':
      return 'Array';
    default:
      return titleCase(shape);
  }
}

function unwrapOptional(raw: string | null | undefined): string | null {
  if (!raw || raw === 'None') return null;
  const some = raw.match(/^Some\((.*)\)$/);
  return some ? some[1] : raw;
}

function refinedSignature(meta: FunctionMetaRecord): string {
  const args = meta.args ?? [];
  if (args.length === 0) return meta.signature ?? `${meta.name}(...)`;

  const minArgs = meta.minArgs ?? 0;
  if (args.length === 1 && minArgs > 1) {
    const arg = args[0];
    const kinds = prettyKinds(arg.kinds);
    const shape = arg.shape === 'scalar' ? '' : ` (${prettyShape(arg.shape)})`;
    return `${meta.name}(arg1, arg2, ... argN: ${kinds}${shape})`;
  }

  const rendered = args.map((arg, index) => {
    const optional = arg.required ? '' : '?';
    const variadicTail = meta.variadic && index === args.length - 1 ? '…' : '';
    const kinds = prettyKinds(arg.kinds);
    const shape = arg.shape === 'scalar' ? '' : ` (${prettyShape(arg.shape)})`;
    return `${arg.name}${optional}${variadicTail}: ${kinds}${shape}`;
  });

  return `${meta.name}(${rendered.join(', ')})`;
}

function sourceLink(path: string): string {
  return `https://github.com/psu3d0/formualizer/blob/main/${path}`;
}

export function FunctionMeta({ id }: { id: string }) {
  const meta = META[id];

  if (!meta) {
    return (
      <p className="text-sm text-fd-muted-foreground">
        Runtime metadata is unavailable for <code>{id}</code>.
      </p>
    );
  }

  const args = meta.args ?? [];
  const minArgs = meta.minArgs ?? 'unknown';
  const maxArgs = meta.variadic ? 'variadic' : (meta.maxArgs ?? 'unknown');
  const sameSource = meta.registrationSource === meta.implementationSource;

  const repeatedPatternHint =
    args.length === 1 && (meta.variadic || (meta.minArgs ?? 0) > 1)
      ? `This function accepts a repeating argument pattern (min args: ${minArgs}).`
      : null;

  return (
    <div className="rounded-xl border bg-fd-card p-4 text-sm">
      <p className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">Category</p>
      <p className="text-base font-semibold">{displayCategory(meta.category)}</p>

      <p className="pt-3 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">Signature</p>
      <code className="mt-1 block overflow-x-auto rounded-md border bg-fd-background/70 px-3 py-2 text-xs">
        {refinedSignature(meta)}
      </code>
      {repeatedPatternHint ? (
        <p className="mt-2 text-xs text-fd-muted-foreground">{repeatedPatternHint}</p>
      ) : null}

      <p className="pt-3 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">Arity</p>
      <p>
        <code>
          min {minArgs}, max {maxArgs}
        </code>
      </p>

      {args.length > 0 ? (
        <>
          <p className="pt-3 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
            Arguments
          </p>
          <div className="mt-1 space-y-2">
            {args.map((arg) => {
              const max = unwrapOptional(arg.max);
              const repeating = unwrapOptional(arg.repeating);
              const coercion = arg.coercion === 'None' ? null : arg.coercion;

              return (
                <div key={arg.name} className="rounded-md border bg-fd-background/40 px-3 py-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <code className="font-medium">{arg.name}</code>
                    {!arg.required ? (
                      <span className="rounded border px-1.5 py-0.5 text-[11px]">optional</span>
                    ) : null}
                    {arg.has_default ? (
                      <span className="rounded border px-1.5 py-0.5 text-[11px]">default</span>
                    ) : null}
                    {arg.by_ref ? (
                      <span className="rounded border px-1.5 py-0.5 text-[11px]">by-ref</span>
                    ) : null}
                    {repeating ? (
                      <span className="rounded border px-1.5 py-0.5 text-[11px]">repeating {repeating}</span>
                    ) : null}
                    {max ? <span className="rounded border px-1.5 py-0.5 text-[11px]">max {max}</span> : null}
                  </div>
                  <p className="mt-1 text-xs text-fd-muted-foreground">
                    <span className="font-medium text-fd-foreground">{prettyKinds(arg.kinds)}</span> ·{' '}
                    {prettyShape(arg.shape)}
                    {coercion ? <> · coercion {coercion}</> : null}
                  </p>
                </div>
              );
            })}
          </div>
        </>
      ) : null}

      <p className="pt-3 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">Caps</p>
      <div className="mt-1 flex flex-wrap gap-1.5">
        {meta.caps.length === 0 ? (
          <code>none</code>
        ) : (
          meta.caps.map((cap) => (
            <span
              key={cap}
              className="rounded-md border px-2 py-0.5 text-xs"
              title={CAP_DESCRIPTIONS[cap] ?? 'Function capability flag'}
            >
              {cap}
            </span>
          ))
        )}
      </div>

      <p className="pt-3 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">Source</p>
      <div className="mt-1 space-y-1 text-xs">
        <p>
          <Link
            href={sourceLink(meta.implementationSource)}
            className="underline"
            target="_blank"
            rel="noreferrer"
          >
            {meta.implementationSource}
          </Link>
        </p>
        {!sameSource ? (
          <p className="text-fd-muted-foreground">
            Registration entry at{' '}
            <Link
              href={sourceLink(meta.registrationSource)}
              className="underline"
              target="_blank"
              rel="noreferrer"
            >
              {meta.registrationSource}
            </Link>
            .
          </p>
        ) : null}
      </div>
    </div>
  );
}
