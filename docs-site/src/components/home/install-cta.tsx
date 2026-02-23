'use client';

import { useMemo, useState } from 'react';
import Link from 'next/link';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';

type RuntimeTab = 'rust' | 'python' | 'wasm';

const tabs: Array<{ key: RuntimeTab; label: string }> = [
  { key: 'rust', label: 'Rust' },
  { key: 'python', label: 'Python' },
  { key: 'wasm', label: 'JS / WASM' },
];

const installByRuntime: Record<RuntimeTab, { lang: string; code: string; title: string }> = {
  rust: {
    lang: 'bash',
    title: 'Install (Rust)',
    code: `cargo add formualizer-workbook --features umya`,
  },
  python: {
    lang: 'bash',
    title: 'Install (Python)',
    code: `pip install formualizer`,
  },
  wasm: {
    lang: 'bash',
    title: 'Install (JS/WASM)',
    code: `npm install formualizer`,
  },
};

const workflowByRuntime: Record<RuntimeTab, { lang: string; code: string; title: string }> = {
  rust: {
    lang: 'rust',
    title: 'Load → edit → recalc (Rust)',
    code: `use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use formualizer_common::LiteralValue;

let backend = UmyaAdapter::open_path("model.xlsx")?;
let mut wb = Workbook::from_reader(
    backend,
    LoadStrategy::EagerAll,
    WorkbookConfig::interactive(),
)?;

let before = wb.evaluate_cell("Inputs", 2, 2)?;

// Edit input value
wb.set_value("Inputs", 2, 2, LiteralValue::Number(1250.0))?;

// Recalculate dependent output cell
let after = wb.evaluate_cell("Outputs", 2, 4)?;

println!("before={before:?} after={after:?}");`,
  },
  python: {
    lang: 'python',
    title: 'Load → edit → recalc (Python)',
    code: `import formualizer as fz

wb = fz.load_workbook("model.xlsx")

# B2
before = wb.evaluate_cell("Inputs", 2, 2)

# Edit input value
wb.set_value("Inputs", 2, 2, 1250.0)

# Recalculate workbook and pull updated output
wb.evaluate_all()
after = wb.sheet("Outputs").get_value(2, 4)

print({"before": before, "after": after})`,
  },
  wasm: {
    lang: 'ts',
    title: 'Load → edit → recalc (JS/WASM)',
    code: `import init, { Workbook } from "formualizer";

await init();

const xlsxBytes = new Uint8Array(
  await fetch("/model.xlsx").then((r) => r.arrayBuffer()),
);

const wb = Workbook.fromXlsxBytes(xlsxBytes);

const before = wb.evaluateCell("Inputs", 2, 2);

// Edit input value
wb.sheet("Inputs").setValue(2, 2, 1250);

// Recalculate and read output
wb.evaluateAll();
const after = wb.sheet("Outputs").getValue(2, 4);

console.log({ before, after });`,
  },
};

export function InstallCta() {
  const [active, setActive] = useState<RuntimeTab>('rust');
  const install = useMemo(() => installByRuntime[active], [active]);
  const workflow = useMemo(() => workflowByRuntime[active], [active]);

  return (
    <section className="rounded-2xl border bg-fd-card p-6 md:p-8 max-[430px]:-mx-6 max-[430px]:rounded-none max-[430px]:border-x-0 max-[430px]:p-4">
      <div className="mx-auto max-w-3xl text-center">
        <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">
          Minimal surface, maximum extensibility.
        </h2>
        <p className="mt-3 text-fd-muted-foreground">
          Pick a runtime and follow the same flow: import workbook, edit an input, recalculate, and
          read updated output.
        </p>
      </div>

      <div className="mt-6 flex flex-wrap items-center justify-center gap-3 text-sm">
        {tabs.map((tab, idx) => (
          <div key={tab.key} className="inline-flex items-center gap-3">
            {idx > 0 ? <span className="text-fd-muted-foreground">→</span> : null}
            <button
              type="button"
              onClick={() => setActive(tab.key)}
              className={`font-medium transition-colors ${
                active === tab.key ? 'text-fd-primary' : 'text-fd-muted-foreground hover:text-fd-foreground'
              }`}
            >
              {tab.label}
            </button>
          </div>
        ))}
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <div
          key={`install-${active}`}
          className="animate-fd-fade-in min-w-0 rounded-xl border bg-fd-background/40 p-3"
        >
          <p className="mb-2 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
            Install
          </p>
          <DynamicCodeBlock
            lang={install.lang}
            code={install.code}
            codeblock={{
              title: install.title,
              className: 'my-0 w-full',
              viewportProps: { className: 'w-full' },
            }}
          />
        </div>

        <div
          key={`workflow-${active}`}
          className="animate-fd-fade-in min-w-0 rounded-xl border bg-fd-background/40 p-3"
        >
          <p className="mb-2 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
            First flow
          </p>
          <DynamicCodeBlock
            lang={workflow.lang}
            code={workflow.code}
            codeblock={{
              title: workflow.title,
              className: 'my-0 w-full',
              viewportProps: { className: 'w-full' },
            }}
          />
        </div>
      </div>

      <div className="mt-5 flex flex-wrap items-center justify-center gap-3">
        <Link href="/docs/quickstarts" className="rounded-full border px-4 py-2 text-sm font-medium">
          Open quickstarts
        </Link>
        <Link
          href="/docs/guides/workbook-edits-and-batching"
          className="rounded-full border px-4 py-2 text-sm font-medium"
        >
          Workbook editing guide
        </Link>
      </div>
    </section>
  );
}
