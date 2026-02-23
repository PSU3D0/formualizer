'use client';

import { useMemo, useState } from 'react';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';

type TabKey = 'engineer' | 'agents' | 'automation';

const tabs: Array<{ key: TabKey; label: string }> = [
  { key: 'engineer', label: 'Engineer' },
  { key: 'agents', label: 'Agents' },
  { key: 'automation', label: 'Automation' },
];

const content: Record<
  TabKey,
  {
    title: string;
    summary: string;
    bullets: string[];
    lang: string;
    code: string;
  }
> = {
  engineer: {
    title: 'The familiar workbook model.',
    summary:
      'Use Workbook APIs directly to set values, author formulas, and evaluate cells with explicit control.',
    bullets: [
      'Ergonomic workbook API in Rust with sheet/cell operations',
      'Incremental recalc through dependency graph',
      'Custom function registration for domain logic',
      'Deterministic controls for reproducible results',
    ],
    lang: 'rust',
    code: `use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

let mut wb = Workbook::new();
wb.add_sheet("Sheet1")?;
wb.set_value("Sheet1", 1, 1, LiteralValue::Number(100.0))?;
wb.set_value("Sheet1", 2, 1, LiteralValue::Number(20.0))?;
wb.set_formula("Sheet1", 1, 2, "=A1-A2")?;

assert_eq!(wb.evaluate_cell("Sheet1", 1, 2)?, LiteralValue::Number(80.0));`,
  },
  agents: {
    title: 'Agent-safe deterministic execution.',
    summary:
      'Freeze volatile behavior and evaluate workbook logic predictably for tool-use, planning, and auditability.',
    bullets: [
      'Deterministic mode for clock/timezone/RNG stability',
      'Structured errors and typed values for reliable parsing',
      'SheetPort sessions for typed input/output contracts',
      'Reproducible evaluation in CI and agent orchestration',
    ],
    lang: 'python',
    code: `from formualizer import SheetPortSession

session = SheetPortSession.from_manifest_yaml(manifest_yaml, workbook)
session.write_inputs({"loan_amount": 250000, "rate": 0.045, "term_months": 360})

result = session.evaluate_once(freeze_volatile=True)
print(result["monthly_payment"])`,
  },
  automation: {
    title: 'Cross-runtime automation flows.',
    summary:
      'Run equivalent workbook logic in Python scripts and JS/WASM apps while keeping behavior aligned.',
    bullets: [
      'Python and JS bindings share core engine semantics',
      'Workbook-local custom callbacks for integration points',
      'Batch write APIs for high-volume updates',
      'Portable function behavior across backend and frontend',
    ],
    lang: 'ts',
    code: `import init, { Workbook } from "formualizer";

await init();
const wb = new Workbook();
wb.addSheet("Sheet1");
wb.setValue("Sheet1", 1, 1, 100);
wb.setValue("Sheet1", 2, 1, 20);
wb.setFormula("Sheet1", 1, 2, "=A1-A2");

console.log(wb.evaluateCell("Sheet1", 1, 2)); // 80`,
  },
};

export function WorkflowTabs() {
  const [active, setActive] = useState<TabKey>('engineer');
  const current = useMemo(() => content[active], [active]);

  return (
    <section className="rounded-2xl border bg-fd-card p-6 md:p-8 max-[430px]:-mx-6 max-[430px]:rounded-none max-[430px]:border-x-0 max-[430px]:p-4">
      <div className="mx-auto max-w-3xl text-center">
        <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">Anybody can build.</h2>
        <p className="mt-3 text-fd-muted-foreground">
          Formualizer supports hands-on engineering, agent-native workflows, and production
          automation with one consistent engine.
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

      <div className="mt-6 grid gap-5 lg:grid-cols-2">
        <div className="min-w-0 rounded-xl border bg-fd-background/40 p-3">
          <DynamicCodeBlock
            lang={current.lang}
            code={current.code}
            codeblock={{
              title: `${tabs.find((t) => t.key === active)?.label} flow`,
              className: 'my-0 w-full',
              viewportProps: { className: 'w-full' },
            }}
          />
        </div>

        <div className="min-w-0">
          <h3 className="text-3xl font-medium tracking-tight">{current.title}</h3>
          <p className="mt-4 text-fd-muted-foreground">{current.summary}</p>
          <ul className="mt-6 list-disc space-y-1 pl-5 text-sm text-fd-muted-foreground">
            {current.bullets.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
