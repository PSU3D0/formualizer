'use client';

import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { ASTNodeData, Token } from 'formualizer';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import {
  astTreeLines,
  extractFunctionNames,
  extractReferences,
  formatReference,
} from '@/lib/formula/ast-utils';
import { loadFormualizer } from '@/lib/wasm/formualizer-loader';

type Dialect = 'excel' | 'openFormula';

type EvalStep = {
  id: string;
  expr: string;
  explanation: string;
  sourceStart: number | null;
  sourceEnd: number | null;
};

type ExprRenderNode =
  | { kind: 'leaf'; text: string }
  | { kind: 'binary'; stepId: string; left: ExprRenderNode; right: ExprRenderNode; op: string }
  | { kind: 'unary'; stepId: string; operand: ExprRenderNode; op: string }
  | { kind: 'function'; stepId: string; name: string; args: ExprRenderNode[] }
  | { kind: 'array'; stepId: string };

type FixSuggestion = {
  label: string;
  formula: string;
};

function normalizeFormula(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return '';
  return trimmed.startsWith('=') ? trimmed : `=${trimmed}`;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function dialectLabel(dialect: Dialect): string {
  return dialect === 'openFormula' ? 'OpenFormula' : 'Excel';
}

function normalizedTokenType(token: Token): string {
  const value = token.value.trim();
  const lowerType = token.tokenType.toLowerCase();
  const lowerSubtype = (token.subtype ?? '').toLowerCase();

  if (lowerType === 'func' && lowerSubtype === 'close') return 'Paren';
  if (lowerType === 'func' && value.endsWith('(')) return 'FunctionCall';
  if ((value === '(' || value === ')') && lowerType === 'func') return 'Paren';
  return token.tokenType;
}

function tokenRole(token: Token): string {
  const value = token.value.trim();
  const lowerType = token.tokenType.toLowerCase();
  const lowerSubtype = (token.subtype ?? '').toLowerCase();

  if (lowerType === 'func' && value.endsWith('(')) return 'function-open';
  if (lowerType === 'func' && lowerSubtype === 'close') return 'function-close';
  if (value === '(') return 'group-open';
  if (value === ')') return 'group-close';
  if (lowerType === 'opin') return 'binary-operator';
  if (lowerType === 'oppre') return 'unary-prefix';
  if (lowerType === 'oppost') return 'unary-postfix';
  if (lowerType === 'sep') return 'separator';
  if (lowerType === 'operand') return 'operand';
  return 'token';
}

function extractErrorPosition(message: string | null): number | null {
  if (!message) return null;
  const m = message.match(/position\s+(?:Some\()?([0-9]+)/i);
  if (!m) return null;
  const parsed = Number(m[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildErrorPointer(formula: string, pos: number | null): string {
  if (pos == null || formula.length === 0) return formula || '(empty formula)';

  const safePos = Math.max(0, Math.min(formula.length - 1, pos));
  const start = Math.max(0, safePos - 28);
  const end = Math.min(formula.length, safePos + 28);
  const prefix = start > 0 ? '…' : '';
  const suffix = end < formula.length ? '…' : '';
  const snippet = `${prefix}${formula.slice(start, end)}${suffix}`;
  const caretOffset = (start > 0 ? 1 : 0) + (safePos - start);
  const caretLine = `${' '.repeat(Math.max(0, caretOffset))}^`;

  return `${snippet}\n${caretLine}`;
}

function leafToExpr(node: ASTNodeData): string {
  switch (node.type) {
    case 'number':
      return String(node.value ?? 0);
    case 'text':
      return `"${String(node.value ?? '')}"`;
    case 'boolean':
      return String(node.value);
    case 'reference':
      return node.reference ? formatReference(node.reference) : '(reference)';
    case 'error':
      return node.message ? `#ERROR(${node.message})` : '#ERROR';
    default:
      return node.type;
  }
}

function renderExpression(node: ASTNodeData, depth = 0): string {
  if (depth > 6) return '…';

  switch (node.type) {
    case 'number':
    case 'text':
    case 'boolean':
    case 'reference':
    case 'error':
      return leafToExpr(node);
    case 'unaryOp': {
      const operand = node.operand ? renderExpression(node.operand, depth + 1) : '…';
      return `${node.op ?? ''}${operand}`;
    }
    case 'binaryOp': {
      const left = node.left ? renderExpression(node.left, depth + 1) : '…';
      const right = node.right ? renderExpression(node.right, depth + 1) : '…';
      return `(${left} ${node.op ?? '?'} ${right})`;
    }
    case 'function': {
      const args = (node.args ?? []).map((arg) => renderExpression(arg, depth + 1)).join(', ');
      return `${node.name ?? 'FN'}(${args})`;
    }
    case 'array':
      return '{…array…}';
    default:
      return node.type;
  }
}

function opExplanation(op: string): string {
  switch (op) {
    case '+':
      return 'adds left and right values';
    case '-':
      return 'subtracts the right value from the left';
    case '*':
      return 'multiplies the two values';
    case '/':
      return 'divides left by right';
    case '^':
      return 'raises left to the power of right';
    case '&':
      return 'concatenates text values';
    case ':':
      return 'creates a range between two references';
    case '=':
    case '<>':
    case '<':
    case '>':
    case '<=':
    case '>=':
      return 'compares values';
    default:
      return 'applies an operator';
  }
}

function collectMetrics(ast: ASTNodeData | null): {
  nodeCount: number;
  maxDepth: number;
  operatorCount: number;
} {
  if (!ast) return { nodeCount: 0, maxDepth: 0, operatorCount: 0 };

  let nodeCount = 0;
  let maxDepth = 0;
  let operatorCount = 0;

  const visit = (node: ASTNodeData, depth: number) => {
    nodeCount += 1;
    maxDepth = Math.max(maxDepth, depth);

    if (node.type === 'binaryOp' || node.type === 'unaryOp') operatorCount += 1;

    if (node.left) visit(node.left, depth + 1);
    if (node.right) visit(node.right, depth + 1);
    if (node.operand) visit(node.operand, depth + 1);
    if (node.args) for (const arg of node.args) visit(arg, depth + 1);
    if (node.elements) {
      for (const row of node.elements) for (const item of row) visit(item, depth + 1);
    }
  };

  visit(ast, 1);
  return { nodeCount, maxDepth, operatorCount };
}

function buildEvaluationPlan(ast: ASTNodeData | null): {
  steps: EvalStep[];
  finalExpr: string;
  root: ExprRenderNode | null;
} {
  if (!ast) return { steps: [], finalExpr: '', root: null };

  const steps: EvalStep[] = [];
  let counter = 1;

  type Span = { start: number; end: number } | null;

  const nodeOwnSpan = (node: ASTNodeData): Span => {
    if (typeof node.sourceStart === 'number' && typeof node.sourceEnd === 'number') {
      return { start: node.sourceStart, end: node.sourceEnd };
    }
    return null;
  };

  const mergeSpan = (a: Span, b: Span): Span => {
    if (!a) return b;
    if (!b) return a;
    return {
      start: Math.min(a.start, b.start),
      end: Math.max(a.end, b.end),
    };
  };

  const missingResult = (): { placeholder: string; render: ExprRenderNode; span: Span } => ({
    placeholder: '…',
    render: { kind: 'leaf', text: '…' },
    span: null,
  });

  const evalNode = (node: ASTNodeData): { placeholder: string; render: ExprRenderNode; span: Span } => {
    if (
      node.type === 'number' ||
      node.type === 'text' ||
      node.type === 'boolean' ||
      node.type === 'reference' ||
      node.type === 'error'
    ) {
      return {
        placeholder: leafToExpr(node),
        render: { kind: 'leaf', text: leafToExpr(node) },
        span: nodeOwnSpan(node),
      };
    }

    if (node.type === 'unaryOp') {
      const operand = node.operand ? evalNode(node.operand) : missingResult();
      const expr = `${node.op ?? ''}${operand.placeholder}`;
      const id = `S${counter++}`;
      const span = mergeSpan(nodeOwnSpan(node), operand.span);
      steps.push({
        id,
        expr,
        explanation: `Apply unary operator "${node.op ?? '?'}" to ${operand.placeholder}.`,
        sourceStart: span?.start ?? null,
        sourceEnd: span?.end ?? null,
      });
      return {
        placeholder: `[${id}]`,
        render: {
          kind: 'unary',
          stepId: id,
          op: node.op ?? '',
          operand: operand.render,
        },
        span,
      };
    }

    if (node.type === 'binaryOp') {
      const left = node.left ? evalNode(node.left) : missingResult();
      const right = node.right ? evalNode(node.right) : missingResult();
      const expr = `${left.placeholder} ${node.op ?? '?'} ${right.placeholder}`;
      const id = `S${counter++}`;
      const span = mergeSpan(nodeOwnSpan(node), mergeSpan(left.span, right.span));
      steps.push({
        id,
        expr,
        explanation: `Compute ${expr} (${opExplanation(node.op ?? '?')}).`,
        sourceStart: span?.start ?? null,
        sourceEnd: span?.end ?? null,
      });
      return {
        placeholder: `[${id}]`,
        render: {
          kind: 'binary',
          stepId: id,
          left: left.render,
          right: right.render,
          op: node.op ?? '?',
        },
        span,
      };
    }

    if (node.type === 'function') {
      const fn = (node.name ?? 'FN').toUpperCase();
      const args = (node.args ?? []).map((arg) => evalNode(arg));
      const expr = `${fn}(${args.map((a) => a.placeholder).join(', ')})`;
      const id = `S${counter++}`;
      const argSpan = args.reduce<Span>((acc, curr) => mergeSpan(acc, curr.span), null);
      const span = mergeSpan(nodeOwnSpan(node), argSpan);
      steps.push({
        id,
        expr,
        explanation: `Evaluate ${fn} using ${args.length} argument(s).`,
        sourceStart: span?.start ?? null,
        sourceEnd: span?.end ?? null,
      });
      return {
        placeholder: `[${id}]`,
        render: {
          kind: 'function',
          stepId: id,
          name: fn,
          args: args.map((a) => a.render),
        },
        span,
      };
    }

    const id = `S${counter++}`;
    const span = nodeOwnSpan(node);
    steps.push({
      id,
      expr: '{…array…}',
      explanation: 'Construct array literal values.',
      sourceStart: span?.start ?? null,
      sourceEnd: span?.end ?? null,
    });
    return {
      placeholder: `[${id}]`,
      render: { kind: 'array', stepId: id },
      span,
    };
  };

  const finalExpr = renderExpression(ast);
  const root = evalNode(ast).render;

  return { steps, finalExpr, root };
}

function stepHighlightClass(index: number): string {
  const palette = [
    'bg-sky-400/25 ring-sky-500/40 dark:bg-sky-500/25',
    'bg-emerald-400/25 ring-emerald-500/40 dark:bg-emerald-500/25',
    'bg-violet-400/25 ring-violet-500/40 dark:bg-violet-500/25',
    'bg-amber-400/25 ring-amber-500/40 dark:bg-amber-500/25',
    'bg-rose-400/25 ring-rose-500/40 dark:bg-rose-500/25',
    'bg-cyan-400/25 ring-cyan-500/40 dark:bg-cyan-500/25',
  ];
  return palette[index % palette.length];
}

function renderExpressionNode(
  node: ExprRenderNode,
  hoveredStepId: string | null,
  stepClassById: Map<string, string>,
): ReactNode {
  const wrap = (stepId: string, content: ReactNode) => {
    const active = hoveredStepId === stepId;
    return (
      <span
        className={`rounded px-0.5 transition-colors ${active ? `${stepClassById.get(stepId) ?? ''} ring-1` : ''}`}
      >
        {content}
      </span>
    );
  };

  switch (node.kind) {
    case 'leaf':
      return <span>{node.text}</span>;
    case 'unary':
      return wrap(
        node.stepId,
        <>
          {node.op}
          {renderExpressionNode(node.operand, hoveredStepId, stepClassById)}
        </>,
      );
    case 'binary':
      return wrap(
        node.stepId,
        <>
          (
          {renderExpressionNode(node.left, hoveredStepId, stepClassById)} {node.op}{' '}
          {renderExpressionNode(node.right, hoveredStepId, stepClassById)})
        </>,
      );
    case 'function':
      return wrap(
        node.stepId,
        <>
          {node.name}(
          {node.args.map((arg, i) => (
            <span key={`${node.stepId}-${i}`}>
              {i > 0 ? ', ' : ''}
              {renderExpressionNode(arg, hoveredStepId, stepClassById)}
            </span>
          ))}
          )
        </>,
      );
    case 'array':
      return wrap(node.stepId, <>{'{…array…}'}</>);
    default:
      return null;
  }
}

function renderFormulaSourceWithHighlight(
  formula: string,
  start: number | null,
  end: number | null,
  highlightClass: string,
): ReactNode {
  if (!formula) return '(empty formula)';
  if (start == null || end == null || start > end) return formula;

  const safeStart = Math.max(0, Math.min(formula.length, start));
  const safeEnd = Math.max(safeStart, Math.min(formula.length, end));

  const before = formula.slice(0, safeStart);
  const mid = formula.slice(safeStart, safeEnd);
  const after = formula.slice(safeEnd);

  return (
    <>
      <span>{before}</span>
      <span className={`rounded px-0.5 ring-1 ${highlightClass}`}>{mid || ' '}</span>
      <span>{after}</span>
    </>
  );
}

function dedupeFixes(items: FixSuggestion[]): FixSuggestion[] {
  const map = new Map<string, FixSuggestion>();
  for (const item of items) {
    if (!map.has(item.formula)) map.set(item.formula, item);
  }
  return Array.from(map.values());
}

function buildFixCandidates(normalized: string, errorMessage: string | null, pos: number | null): FixSuggestion[] {
  if (!normalized) return [];

  const msg = (errorMessage ?? '').toLowerCase();
  const out: FixSuggestion[] = [];

  const openParenCount = (normalized.match(/\(/g) ?? []).length;
  const closeParenCount = (normalized.match(/\)/g) ?? []).length;
  const openBracketCount = (normalized.match(/\[/g) ?? []).length;
  const closeBracketCount = (normalized.match(/\]/g) ?? []).length;
  const quoteCount = (normalized.match(/"/g) ?? []).length;

  if (openParenCount > closeParenCount || msg.includes('unmatched opening parenthesis')) {
    out.push({ label: 'Close missing parenthesis', formula: `${normalized})` });
  }

  if (openBracketCount > closeBracketCount || msg.includes('unmatched opening') && msg.includes('bracket')) {
    out.push({ label: 'Close missing bracket', formula: `${normalized}]` });
  }

  if ((msg.includes('unmatched closing') || msg.includes('unexpected closing')) && pos != null) {
    const i = Math.max(0, Math.min(normalized.length - 1, pos));
    out.push({ label: 'Remove unexpected closing character', formula: `${normalized.slice(0, i)}${normalized.slice(i + 1)}` });
  }

  if (quoteCount % 2 === 1 || msg.includes('unterminated string') || msg.includes('unclosed string')) {
    out.push({ label: 'Close missing quote', formula: `${normalized}"` });
  }

  if (/[,;]\s*\)/.test(normalized)) {
    out.push({ label: 'Remove trailing separator before )', formula: normalized.replace(/([,;])\s*\)/g, ')') });
  }

  if (/[,;]\s*$/.test(normalized)) {
    out.push({ label: 'Remove trailing separator at end', formula: normalized.replace(/([,;])\s*$/, '') });
  }

  return dedupeFixes(out);
}

export function FormulaParserTool() {
  const [formula, setFormula] = useState('=SUM(A1:A3)');
  const [dialect, setDialect] = useState<Dialect>('excel');

  const [isParsing, setIsParsing] = useState(false);
  const [parseMs, setParseMs] = useState<number | null>(null);
  const [normalizedFormula, setNormalizedFormula] = useState('=SUM(A1:A3)');

  const [tokens, setTokens] = useState<Token[]>([]);
  const [ast, setAst] = useState<ASTNodeData | null>(null);
  const [tokenError, setTokenError] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [errorPos, setErrorPos] = useState<number | null>(null);
  const [fixes, setFixes] = useState<FixSuggestion[]>([]);
  const [hoveredStepId, setHoveredStepId] = useState<string | null>(null);
  const [lockedStepId, setLockedStepId] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const params = new URLSearchParams(window.location.search);
    const initialFormula = params.get('f');
    const initialDialect = params.get('dialect');

    if (initialFormula) setFormula(initialFormula);
    if (initialDialect === 'excel' || initialDialect === 'openFormula') {
      setDialect(initialDialect);
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const params = new URLSearchParams(window.location.search);
    params.set('f', formula);
    params.set('dialect', dialect);
    window.history.replaceState(null, '', `/formula-parser?${params.toString()}`);
  }, [formula, dialect]);

  const runParse = useCallback(async () => {
    const normalized = normalizeFormula(formula);
    setNormalizedFormula(normalized);

    if (!normalized) {
      setTokens([]);
      setAst(null);
      setTokenError(null);
      setParseError('Enter a formula to parse.');
      setParseMs(null);
      setErrorPos(null);
      setFixes([]);
      return;
    }

    setIsParsing(true);
    setTokenError(null);
    setParseError(null);

    const started = performance.now();

    try {
      const formualizer = await loadFormualizer();
      const wasmDialect =
        dialect === 'openFormula'
          ? formualizer.FormulaDialect.OpenFormula
          : formualizer.FormulaDialect.Excel;

      let nextTokens: Token[] = [];
      let nextAst: ASTNodeData | null = null;
      let nextTokenError: string | null = null;
      let nextParseError: string | null = null;

      try {
        const tokenized = await formualizer.tokenize(normalized, wasmDialect);
        nextTokens = tokenized.tokens;
      } catch (error) {
        nextTokenError = toErrorMessage(error);
      }

      try {
        nextAst = await formualizer.parse(normalized, wasmDialect);
      } catch (error) {
        nextParseError = toErrorMessage(error);
      }

      const activeError = nextParseError ?? nextTokenError;
      const pos = extractErrorPosition(activeError);

      let verifiedFixes: FixSuggestion[] = [];
      if (activeError) {
        const candidates = buildFixCandidates(normalized, activeError, pos);
        for (const candidate of candidates) {
          try {
            await formualizer.parse(candidate.formula, wasmDialect);
            verifiedFixes.push(candidate);
          } catch {
            // keep only verified fixes
          }
        }
      }

      setTokens(nextTokens);
      setAst(nextAst);
      setTokenError(nextTokenError);
      setParseError(nextParseError);
      setErrorPos(pos);
      setFixes(verifiedFixes);
      setParseMs(performance.now() - started);
    } catch (error) {
      setTokens([]);
      setAst(null);
      setTokenError(null);
      setParseError(toErrorMessage(error));
      setParseMs(performance.now() - started);
      setErrorPos(extractErrorPosition(toErrorMessage(error)));
      setFixes([]);
    } finally {
      setIsParsing(false);
    }
  }, [formula, dialect]);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      void runParse();
    }, 260);
    return () => window.clearTimeout(handle);
  }, [runParse]);

  const references = useMemo(() => (ast ? extractReferences(ast) : []), [ast]);
  const functions = useMemo(() => (ast ? extractFunctionNames(ast) : []), [ast]);
  const treeLines = useMemo(() => (ast ? astTreeLines(ast) : []), [ast]);
  const astJson = useMemo(() => (ast ? JSON.stringify(ast, null, 2) : ''), [ast]);
  const metrics = useMemo(() => collectMetrics(ast), [ast]);
  const plan = useMemo(() => buildEvaluationPlan(ast), [ast]);
  const stepClassById = useMemo(() => {
    const map = new Map<string, string>();
    plan.steps.forEach((step, idx) => {
      map.set(step.id, stepHighlightClass(idx));
    });
    return map;
  }, [plan.steps]);

  const stepById = useMemo(() => {
    const map = new Map<string, EvalStep>();
    plan.steps.forEach((step) => map.set(step.id, step));
    return map;
  }, [plan.steps]);

  useEffect(() => {
    setHoveredStepId(null);
    setLockedStepId(null);
  }, [plan.finalExpr]);

  const activeStepId = lockedStepId ?? hoveredStepId;
  const activeStep = activeStepId ? stepById.get(activeStepId) ?? null : null;

  const status = parseError
    ? 'error'
    : ast
      ? 'ok'
      : tokens.length > 0
        ? 'partial'
        : 'idle';

  return (
    <div className="mx-auto w-full max-w-6xl px-4 pb-10 pt-8 md:px-6">
      <div className="mb-6 space-y-2">
        <div className="flex flex-wrap items-center gap-2 text-xs text-fd-muted-foreground">
          <a href="/docs" className="rounded-md border px-2 py-1 hover:bg-fd-muted">
            ← Docs Home
          </a>
          <a href="/docs/reference/functions" className="rounded-md border px-2 py-1 hover:bg-fd-muted">
            Function Reference
          </a>
        </div>

        <h1 className="text-2xl font-semibold tracking-tight md:text-3xl">Formula Parser</h1>
        <p className="text-sm text-fd-muted-foreground md:text-base">
          Parse formulas in-browser using Formualizer WASM, then inspect a practical explanation,
          dependencies, and advanced parser output.
        </p>
      </div>

      <div className="rounded-xl border bg-fd-card p-4 shadow-sm md:p-5 dark:border-fd-border/70">
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <label className="text-xs font-medium text-fd-muted-foreground">Dialect</label>
          <select
            className="rounded-md border bg-fd-background px-2 py-1 text-xs"
            value={dialect}
            onChange={(event) => setDialect(event.target.value as Dialect)}
          >
            <option value="excel">Excel</option>
            <option value="openFormula">OpenFormula</option>
          </select>

          <span className="ml-auto text-xs text-fd-muted-foreground">
            {isParsing ? 'Recomputing…' : 'Auto-recomputes after typing pauses'}
          </span>
        </div>

        <textarea
          value={formula}
          onChange={(event) => setFormula(event.target.value)}
          placeholder="=SUM(A1:A3)"
          className="min-h-24 w-full resize-y rounded-lg border bg-fd-background px-3 py-2 font-mono text-sm outline-none ring-fd-primary/40 focus:ring"
        />

        <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
          <span
            className={`rounded-full px-2 py-0.5 font-medium ${
              isParsing
                ? 'bg-sky-500/10 text-sky-700 dark:text-sky-300'
                : status === 'ok'
                  ? 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                  : status === 'error'
                    ? 'bg-rose-500/10 text-rose-700 dark:text-rose-300'
                    : 'bg-fd-muted text-fd-muted-foreground'
            }`}
          >
            {isParsing
              ? 'Parsing…'
              : status === 'ok'
                ? 'Parse successful'
                : status === 'error'
                  ? 'Parse error'
                  : status === 'partial'
                    ? 'Partial output'
                    : 'Idle'}
          </span>

          <span className="rounded-full bg-fd-muted px-2 py-0.5 text-fd-muted-foreground">
            {dialectLabel(dialect)}
          </span>

          {parseMs !== null && (
            <span className="rounded-full bg-amber-500/10 px-2 py-0.5 font-mono text-amber-700 dark:text-amber-300">
              ⚡ {parseMs.toFixed(2)}ms
            </span>
          )}

          <span className="text-fd-muted-foreground">Normalized: {normalizedFormula || '—'}</span>
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-lg border bg-fd-card p-3 text-sm">
          <div className="text-xs text-fd-muted-foreground">Tokens</div>
          <div className="mt-1 font-mono text-lg">{tokens.length}</div>
        </div>
        <div className="rounded-lg border bg-fd-card p-3 text-sm">
          <div className="text-xs text-fd-muted-foreground">AST Nodes</div>
          <div className="mt-1 font-mono text-lg">{metrics.nodeCount}</div>
        </div>
        <div className="rounded-lg border bg-fd-card p-3 text-sm">
          <div className="text-xs text-fd-muted-foreground">Tree Depth</div>
          <div className="mt-1 font-mono text-lg">{metrics.maxDepth}</div>
        </div>
        <div className="rounded-lg border bg-fd-card p-3 text-sm">
          <div className="text-xs text-fd-muted-foreground">Operators</div>
          <div className="mt-1 font-mono text-lg">{metrics.operatorCount}</div>
        </div>
      </div>

      <details open className="mt-5 rounded-xl border bg-fd-card">
        <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">Explain</summary>
        <div className="space-y-4 p-4 text-sm">
          {plan.finalExpr ? (
            <>
              <div className="rounded-md border bg-fd-muted/10 p-3">
                <div className="mb-1 text-xs uppercase tracking-wide text-fd-muted-foreground">Final expression</div>
                <div className="font-mono text-xs leading-relaxed md:text-sm">
                  {plan.root
                    ? renderExpressionNode(plan.root, activeStepId, stepClassById)
                    : plan.finalExpr}
                </div>

                <div className="mt-3 border-t pt-3">
                  <div className="mb-1 text-xs uppercase tracking-wide text-fd-muted-foreground">
                    Source formula span
                  </div>
                  <div className="font-mono text-xs leading-relaxed">
                    {activeStep
                      ? renderFormulaSourceWithHighlight(
                          normalizedFormula,
                          activeStep.sourceStart,
                          activeStep.sourceEnd,
                          stepClassById.get(activeStep.id) ?? '',
                        )
                      : normalizedFormula || '—'}
                  </div>
                </div>

                <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] text-fd-muted-foreground">
                  <span>Hover step: temporary highlight</span>
                  <span>•</span>
                  <span>Click step: lock highlight</span>
                  {lockedStepId && (
                    <button
                      type="button"
                      onClick={() => setLockedStepId(null)}
                      className="rounded border px-1.5 py-0.5 hover:bg-fd-muted"
                    >
                      Clear lock
                    </button>
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-xs uppercase tracking-wide text-fd-muted-foreground">Evaluation sequence</div>
                {plan.steps.length === 0 ? (
                  <p className="text-fd-muted-foreground">No intermediate steps.</p>
                ) : (
                  <ol className="space-y-2">
                    {plan.steps.map((step) => {
                      const highlightClass = stepClassById.get(step.id) ?? '';
                      const isHovered = hoveredStepId === step.id;
                      const isLocked = lockedStepId === step.id;
                      const isActive = activeStepId === step.id;

                      return (
                        <li
                          key={step.id}
                          className={`rounded-md border p-2 transition-colors ${
                            isActive ? 'border-fd-primary/40 bg-fd-muted/20' : ''
                          } ${isLocked ? 'ring-1 ring-fd-primary/40' : ''}`}
                          onMouseEnter={() => setHoveredStepId(step.id)}
                          onMouseLeave={() => setHoveredStepId(null)}
                          onClick={() =>
                            setLockedStepId((prev) => (prev === step.id ? null : step.id))
                          }
                          role="button"
                          tabIndex={0}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault();
                              setLockedStepId((prev) => (prev === step.id ? null : step.id));
                            }
                          }}
                        >
                          <div className="mb-1 flex items-center gap-2 text-xs font-semibold text-fd-muted-foreground">
                            <span className={`inline-block h-2.5 w-2.5 rounded-full ${highlightClass}`} />
                            {step.id}
                            {isLocked && <span className="text-[10px]">(locked)</span>}
                            {!isLocked && isHovered && <span className="text-[10px]">(hover)</span>}
                          </div>
                          <div className="font-mono text-xs">{step.expr}</div>
                          <div className="mt-1 text-xs text-fd-muted-foreground">{step.explanation}</div>
                        </li>
                      );
                    })}
                  </ol>
                )}
              </div>
            </>
          ) : (
            <p className="text-fd-muted-foreground">No parsed expression to explain yet.</p>
          )}
        </div>
      </details>

      <details open className="mt-4 rounded-xl border bg-fd-card">
        <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">
          References and function calls
        </summary>
        <div className="grid gap-4 p-4 md:grid-cols-2">
          <div className="rounded-md border p-3">
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-fd-muted-foreground">
              References
            </h3>
            {references.length === 0 ? (
              <p className="text-sm text-fd-muted-foreground">No references found.</p>
            ) : (
              <ul className="space-y-1 font-mono text-xs">
                {references.map((reference) => (
                  <li key={reference}>{reference}</li>
                ))}
              </ul>
            )}
          </div>

          <div className="rounded-md border p-3">
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-fd-muted-foreground">
              Function calls
            </h3>
            {functions.length === 0 ? (
              <p className="text-sm text-fd-muted-foreground">No function calls found.</p>
            ) : (
              <ul className="space-y-1 font-mono text-xs">
                {functions.map((name) => (
                  <li key={name}>{name}</li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </details>

      {(parseError || tokenError) && (
        <details open className="mt-4 rounded-xl border bg-fd-card">
          <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">
            Error diagnostics + fixes
          </summary>
          <div className="space-y-3 p-4 text-sm">
            {parseError && (
              <div className="rounded-md border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-rose-700 dark:text-rose-300">
                {parseError}
              </div>
            )}
            {tokenError && (
              <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-amber-700 dark:text-amber-300">
                {tokenError}
              </div>
            )}

            <DynamicCodeBlock
              lang="text"
              code={buildErrorPointer(normalizedFormula, errorPos)}
              codeblock={{
                className: 'my-0 rounded-md border',
              }}
            />

            {fixes.length > 0 && (
              <div className="space-y-2">
                <div className="text-xs uppercase tracking-wide text-fd-muted-foreground">Auto-fix suggestions</div>
                <div className="flex flex-wrap gap-2">
                  {fixes.map((fix) => (
                    <button
                      key={`${fix.label}-${fix.formula}`}
                      type="button"
                      onClick={() => setFormula(fix.formula)}
                      className="rounded-md border px-2 py-1 text-xs hover:bg-fd-muted"
                    >
                      Apply: {fix.label}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </details>
      )}

      <details className="mt-4 rounded-xl border bg-fd-card">
        <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">AST Tree</summary>
        <div className="p-4">
          <DynamicCodeBlock
            lang="text"
            code={treeLines.length > 0 ? treeLines.join('\n') : 'No AST output.'}
            codeblock={{
              className: 'my-0 max-h-[420px] overflow-auto rounded-md border',
              viewportProps: { className: 'max-h-[420px]' },
            }}
          />
        </div>
      </details>

      <details className="mt-4 rounded-xl border bg-fd-card">
        <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">
          Raw token stream
        </summary>
        <div className="p-4">
          <div className="overflow-auto rounded-md border">
            <table className="min-w-full border-collapse text-xs">
              <thead className="bg-fd-muted/50">
                <tr>
                  <th className="border-b px-3 py-2 text-left">#</th>
                  <th className="border-b px-3 py-2 text-left">Type</th>
                  <th className="border-b px-3 py-2 text-left">Role</th>
                  <th className="border-b px-3 py-2 text-left">Value</th>
                  <th className="border-b px-3 py-2 text-left">Position</th>
                </tr>
              </thead>
              <tbody>
                {tokens.map((token, index) => (
                  <tr key={`${token.tokenType}-${token.pos}-${index}`} className="border-b last:border-b-0">
                    <td className="px-3 py-2 font-mono text-fd-muted-foreground">{index}</td>
                    <td className="px-3 py-2 font-mono">{normalizedTokenType(token)}</td>
                    <td className="px-3 py-2 font-mono text-fd-muted-foreground">{tokenRole(token)}</td>
                    <td className="px-3 py-2 font-mono">{token.value}</td>
                    <td className="px-3 py-2 font-mono text-fd-muted-foreground">{token.pos}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {tokens.length === 0 && (
              <div className="px-3 py-4 text-sm text-fd-muted-foreground">No tokens to display.</div>
            )}
          </div>
        </div>
      </details>

      <details className="mt-4 rounded-xl border bg-fd-card">
        <summary className="cursor-pointer list-none border-b px-4 py-3 text-sm font-semibold">
          Raw AST JSON
        </summary>
        <div className="space-y-3 p-4">
          <DynamicCodeBlock
            lang="json"
            code={astJson || '{\n  "message": "No AST JSON available."\n}'}
            codeblock={{
              className: 'my-0 max-h-[360px] overflow-auto rounded-md border',
              viewportProps: { className: 'max-h-[360px]' },
            }}
          />
        </div>
      </details>

      <section className="mt-8 space-y-5 rounded-xl border bg-fd-card p-5">
        <h2 className="text-lg font-semibold">Why use this Excel formula parser?</h2>
        <p className="text-sm text-fd-muted-foreground">
          This tool helps spreadsheet users and developers debug complex formulas by showing
          syntax diagnostics, AST structure, reference dependencies, and evaluation flow in one view.
        </p>

        <div className="grid gap-4 md:grid-cols-2">
          <div>
            <h3 className="mb-1 text-sm font-semibold">Common use cases</h3>
            <ul className="list-disc space-y-1 pl-5 text-sm text-fd-muted-foreground">
              <li>Check formula syntax before sharing across teams.</li>
              <li>Understand nested formulas with readable evaluation steps.</li>
              <li>Audit references and function calls for model reviews.</li>
              <li>Prototype formulas before moving into production workbooks.</li>
            </ul>
          </div>

          <div>
            <h3 className="mb-1 text-sm font-semibold">Related tools</h3>
            <ul className="space-y-1 text-sm text-fd-muted-foreground">
              <li>
                <a className="underline underline-offset-4" href="/docs/reference/functions">
                  Function reference
                </a>
              </li>
              <li>
                <a className="underline underline-offset-4" href="/docs/playground/formula-sandbox">
                  Formula sandbox docs
                </a>
              </li>
              <li>
                <a className="underline underline-offset-4" href="/docs/playground/parser-ast-inspector">
                  Parser + AST inspector docs
                </a>
              </li>
            </ul>
          </div>
        </div>

        <div className="space-y-3 border-t pt-4">
          <h2 className="text-lg font-semibold">FAQ: Excel formula parser</h2>

          <div>
            <h3 className="text-sm font-semibold">How do I parse an Excel formula online?</h3>
            <p className="text-sm text-fd-muted-foreground">
              Paste your formula into the editor and pause typing. The parser auto-runs in your
              browser and shows diagnostics, references, AST structure, and evaluation steps.
            </p>
          </div>

          <div>
            <h3 className="text-sm font-semibold">Can this tool debug nested IF and complex formulas?</h3>
            <p className="text-sm text-fd-muted-foreground">
              Yes. It handles deeply nested formulas, then breaks them into intermediate evaluation
              steps so you can understand operation order and identify fragile segments.
            </p>
          </div>

          <div>
            <h3 className="text-sm font-semibold">Does this parser show formula dependencies?</h3>
            <p className="text-sm text-fd-muted-foreground">
              Yes. The references panel extracts cell and range dependencies, and the function panel
              lists all called functions found in your formula.
            </p>
          </div>

          <div>
            <h3 className="text-sm font-semibold">Can it fix formula syntax errors automatically?</h3>
            <p className="text-sm text-fd-muted-foreground">
              It suggests safe fixes for common syntax issues (for example unmatched parentheses,
              trailing separators, and unclosed quotes). Suggested fixes are validated before shown.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
