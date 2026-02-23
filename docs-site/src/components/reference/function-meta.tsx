import functionsMeta from '@/generated/functions-meta.json';

type FunctionMetaRecord = {
  name: string;
  category: string;
  typeName: string;
  minArgs: number | null;
  maxArgs: number | null;
  variadic: boolean | null;
  signature: string | null;
  argSchema: string | null;
  caps: string[];
  registrationSource: string;
  implementationSource: string;
};

const META = functionsMeta as Record<string, FunctionMetaRecord>;

export function FunctionMeta({ id }: { id: string }) {
  const meta = META[id];

  if (!meta) {
    return (
      <p className="text-sm text-fd-muted-foreground">
        Runtime metadata is unavailable for <code>{id}</code>.
      </p>
    );
  }

  const maxArgs = meta.variadic ? 'variadic' : (meta.maxArgs ?? 'unknown').toString();
  const minArgs = (meta.minArgs ?? 'unknown').toString();
  const signature = meta.signature ?? 'unknown';
  const argSchema = meta.argSchema ?? 'unknown';

  return (
    <div className="rounded-xl border bg-fd-card p-4 text-sm">
      <dl className="grid gap-x-4 gap-y-2 sm:grid-cols-[180px_1fr]">
        <dt className="text-fd-muted-foreground">Name</dt>
        <dd>
          <code>{meta.name}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Category</dt>
        <dd>
          <code>{meta.category}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Type</dt>
        <dd>
          <code>{meta.typeName}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Min / Max args</dt>
        <dd>
          <code>
            {minArgs} / {maxArgs}
          </code>
        </dd>

        <dt className="text-fd-muted-foreground">Variadic</dt>
        <dd>
          <code>{meta.variadic == null ? 'unknown' : String(meta.variadic)}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Signature</dt>
        <dd className="break-words">
          <code>{signature}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Arg schema</dt>
        <dd className="break-words">
          <code>{argSchema}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Caps</dt>
        <dd className="flex flex-wrap gap-1.5">
          {meta.caps.length === 0 ? (
            <code>none</code>
          ) : (
            meta.caps.map((cap) => (
              <span key={cap} className="rounded-md border px-2 py-0.5 text-xs">
                {cap}
              </span>
            ))
          )}
        </dd>

        <dt className="text-fd-muted-foreground">Registration source</dt>
        <dd className="break-all">
          <code>{meta.registrationSource}</code>
        </dd>

        <dt className="text-fd-muted-foreground">Implementation source</dt>
        <dd className="break-all">
          <code>{meta.implementationSource}</code>
        </dd>
      </dl>
    </div>
  );
}
