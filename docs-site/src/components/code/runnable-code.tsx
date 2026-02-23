'use client';

import { useCallback, useRef, useState } from 'react';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import { loadFormualizer } from '@/lib/wasm/formualizer-loader';

export interface RunnableCodeProps {
  /** The JS/TS code to display and execute. */
  code: string;
  /** Language hint for syntax highlighting. */
  lang?: string;
  /** Title shown in the code block header. */
  title?: string;
}

/**
 * A code block with a "▶ Run" button. Executes the displayed JS code
 * against the formualizer WASM module in-browser.
 *
 * The runner provides `formualizer` as an implicit global binding so
 * the displayed code can stay clean — users see `new Workbook()` and
 * the harness wires it to the WASM module automatically.
 */
export function RunnableCode({ code, lang = 'ts', title }: RunnableCodeProps) {
  const [output, setOutput] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const outputRef = useRef<HTMLDivElement>(null);

  const run = useCallback(async () => {
    setIsRunning(true);
    setError(null);
    setOutput([]);

    try {
      const mod = await loadFormualizer();

      // Collect console.log output
      const logs: string[] = [];
      const capture = (...args: unknown[]) => {
        logs.push(args.map((a) => (typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a))).join(' '));
      };

      // Build a sandbox scope with formualizer exports + console capture.
      // We expose top-level names so user code can write `new Workbook()` etc.
      const scope: Record<string, unknown> = {
        console: { ...console, log: capture, info: capture, warn: capture },
        Workbook: mod.Workbook,
        SheetPortSession: mod.SheetPortSession,
        Tokenizer: mod.Tokenizer,
        Parser: mod.Parser,
        Reference: mod.Reference,
        FormulaDialect: mod.FormulaDialect,
        tokenize: mod.tokenize,
        parse: mod.parse,
        formualizer: mod,
      };

      // Strip import/export statements — the sandbox provides everything
      const cleaned = code
        .replace(/^\s*import\s+.*?from\s+['"].*?['"];?\s*$/gm, '')
        .replace(/^\s*export\s+/gm, '')
        .replace(/^\s*await\s+init\(\);?\s*$/gm, '');

      // Wrap in async function so top-level await works
      const keys = Object.keys(scope);
      const values = keys.map((k) => scope[k]);
      const fn = new Function(...keys, `return (async () => {\n${cleaned}\n})();`);

      await fn(...values);

      setOutput(logs);
      setHasRun(true);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      setHasRun(true);
    } finally {
      setIsRunning(false);
      // Scroll output into view
      requestAnimationFrame(() => {
        outputRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    }
  }, [code]);

  return (
    <div className="space-y-0">
      <DynamicCodeBlock
        lang={lang}
        code={code}
        codeblock={{
          title: title ?? 'JS / WASM',
          className: 'my-0 rounded-b-none border-b-0',
        }}
      />

      <div className="flex items-center gap-2 rounded-b-lg border border-t-0 bg-fd-muted/20 px-3 py-2">
        <button
          type="button"
          onClick={run}
          disabled={isRunning}
          className={`inline-flex items-center gap-1.5 rounded-md px-3 py-1 text-xs font-medium transition-colors ${
            isRunning
              ? 'bg-fd-muted text-fd-muted-foreground cursor-wait'
              : 'bg-fd-primary text-fd-primary-foreground hover:bg-fd-primary/90 shadow-sm'
          }`}
        >
          <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor">
            <path d="M4 2v12l10-6L4 2z" />
          </svg>
          {isRunning ? 'Running…' : 'Run in Browser'}
        </button>

        {hasRun && !error && (
          <span className="text-[11px] text-emerald-600 dark:text-emerald-400">✓ Executed</span>
        )}
        {error && (
          <span className="text-[11px] text-rose-600 dark:text-rose-400">✗ Error</span>
        )}
      </div>

      {(output.length > 0 || error) && (
        <div
          ref={outputRef}
          className="mt-2 rounded-lg border bg-fd-background p-3 font-mono text-xs"
        >
          {output.length > 0 && (
            <div className="space-y-0.5">
              {output.map((line, i) => (
                <div key={`${i}-${line.slice(0, 20)}`} className="text-fd-foreground whitespace-pre-wrap">{line}</div>
              ))}
            </div>
          )}
          {error && (
            <div className="text-rose-600 dark:text-rose-400 whitespace-pre-wrap">{error}</div>
          )}
        </div>
      )}
    </div>
  );
}
