'use client';

import { useState } from 'react';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import { RunnableCode } from './runnable-code';

type TabDef = {
  label: string;
  lang: string;
  code: string;
  /** Whether this tab should be executable via WASM. */
  run?: boolean;
};

export interface CodeTabsProps {
  /** Optional heading shown above the tabs. */
  title?: string;
  rust?: string;
  python?: string;
  ts?: string;
  /** Override JS tab label. Default: "JS / WASM ▶" when executable. */
  tsLabel?: string;
  /** Extra tabs beyond the standard three (advanced use). */
  extra?: TabDef[];
  /**
   * Set to false to disable the run button on the JS tab.
   * Default: true when `ts` is provided.
   */
  runnable?: boolean;
}

/**
 * Multi-language code tabs with optional WASM execution on the JS tab.
 *
 * Usage in MDX:
 * ```mdx
 * <CodeTabs
 *   rust={`let wb = Workbook::new();`}
 *   python={`wb = Workbook()`}
 *   ts={`const wb = new Workbook();`}
 * />
 * ```
 *
 * The JS tab automatically gets a "▶ Run in Browser" button unless
 * `runnable={false}` is set.
 */
export function CodeTabs({
  title,
  rust,
  python,
  ts,
  tsLabel,
  extra,
  runnable = true,
}: CodeTabsProps) {
  const tabs: TabDef[] = [];

  if (rust) tabs.push({ label: 'Rust', lang: 'rust', code: rust });
  if (python) tabs.push({ label: 'Python', lang: 'python', code: python });
  if (ts) {
    tabs.push({
      label: tsLabel ?? (runnable ? 'JS / WASM ▶' : 'JS / WASM'),
      lang: 'ts',
      code: ts,
      run: runnable,
    });
  }
  if (extra) tabs.push(...extra);

  const [activeIdx, setActiveIdx] = useState(0);
  const active = tabs[activeIdx] ?? tabs[0];

  if (tabs.length === 0) return null;

  return (
    <div className="my-6">
      {title && (
        <div className="mb-2 text-sm font-semibold text-fd-foreground">{title}</div>
      )}
      <div className="rounded-xl border bg-fd-card overflow-hidden">
        {/* Tab bar */}
        <div className="flex border-b bg-fd-muted/30">
          {tabs.map((tab, idx) => (
            <button
              key={tab.label}
              type="button"
              onClick={() => setActiveIdx(idx)}
              className={`px-4 py-2 text-xs font-medium transition-colors border-b-2 -mb-px ${
                idx === activeIdx
                  ? 'border-fd-primary text-fd-foreground bg-fd-background/50'
                  : 'border-transparent text-fd-muted-foreground hover:text-fd-foreground hover:bg-fd-background/30'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="p-0">
          {active.run ? (
            <RunnableCode code={active.code} lang={active.lang} />
          ) : (
            <DynamicCodeBlock
              lang={active.lang}
              code={active.code}
              codeblock={{ className: 'my-0 rounded-none border-0' }}
            />
          )}
        </div>
      </div>
    </div>
  );
}
