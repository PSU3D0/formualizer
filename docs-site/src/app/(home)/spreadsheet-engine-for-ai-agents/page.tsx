import type { Metadata } from 'next';
import Link from 'next/link';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import { siteUrl } from '@/lib/env';

export const metadata: Metadata = {
  title: 'A Spreadsheet Engine Built for AI Agents',
  description:
    'Deterministic, native spreadsheet evaluation for AI agents. Freeze the clock and RNG, treat workbooks as typed functions with SheetPort, and interrogate a dependency graph — no headless LibreOffice.',
  keywords: [
    'spreadsheet engine for AI agents',
    'deterministic spreadsheet evaluation',
    'excel MCP server recalculate',
    'LLM agent excel formulas',
    'agent spreadsheet tool',
    'model context protocol spreadsheet',
  ],
  alternates: {
    canonical: '/spreadsheet-engine-for-ai-agents',
  },
  openGraph: {
    title: 'A Spreadsheet Engine Built for AI Agents | Formualizer',
    description:
      'Deterministic evaluation, typed workbook contracts, and an inspectable dependency graph — a spreadsheet engine agents can verify against.',
    url: '/spreadsheet-engine-for-ai-agents',
    type: 'website',
    images: ['/opengraph-image.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'A Spreadsheet Engine Built for AI Agents | Formualizer',
    description:
      'Deterministic evaluation, typed SheetPort contracts, and an inspectable dependency graph for AI agents.',
    images: ['/twitter-image.png'],
  },
};

const faqs = [
  {
    question: 'Why does determinism matter for an agent?',
    answer:
      "Because verification requires reproducibility. If NOW() or RAND() shifts between runs, the agent can't distinguish \"my edit changed the output\" from \"the clock did.\" Frozen-clock, seeded evaluation makes model runs pure functions.",
  },
  {
    question: "Can't I just use a headless LibreOffice recalc?",
    answer:
      "You can — that's what most agent tooling does today. You pay process-spawn latency per recalculation, add a system dependency, and get no dependency tracing or typed I/O. A library call is faster and gives the agent structure to reason over.",
  },
  {
    question: 'What makes spreadsheet-mcp different from other Excel MCP servers?',
    answer:
      'Most Excel MCP servers manipulate cells but cannot compute a formula. spreadsheet-mcp evaluates natively with this engine and adds dependency tracing and fork/diff/verify workflows.',
  },
  {
    question: 'Is the license safe for commercial agents?',
    answer:
      'Yes — MIT. Embed it in commercial products, SaaS, or internal tools without copyleft obligations.',
  },
];

const deterministicCode = `import datetime
from formualizer import SheetPortSession, Workbook

manifest_yaml = """
spec: fio
spec_version: "0.3.0"
manifest:
  id: pricing-model
  name: Pricing Model
  workbook:
    uri: memory://pricing.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: base_price
    dir: in
    shape: scalar
    location: { a1: Inputs!A1 }
    schema: { type: number }
  - id: final_price
    dir: out
    shape: scalar
    location: { a1: Outputs!A1 }
    schema: { type: number }
"""

wb = Workbook()
wb.add_sheet("Inputs")
wb.add_sheet("Outputs")
wb.set_formula("Outputs", 1, 1, "=Inputs!A1*1.2")

session = SheetPortSession.from_manifest_yaml(manifest_yaml, wb)
session.write_inputs({"base_price": 100.0})

out = session.evaluate_once(
    freeze_volatile=True,
    rng_seed=42,
    deterministic_timestamp_utc=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    deterministic_timezone="utc",
)
# same inputs + same seed + same frozen clock => identical outputs, every run`;

const portsYaml = `ports:
  - id: base_price
    dir: in
    shape: scalar
    location: { a1: Inputs!A1 }
    schema: { type: number }
  - id: final_price
    dir: out
    shape: scalar
    location: { a1: Outputs!A1 }
    schema: { type: number }`;

const astCode = `import formualizer as fz

ast = fz.parse("=SUM(A1:B2) + VLOOKUP(C1, D:E, 2, FALSE)")
# typed AST — walk references, inspect structure, explain the calculation`;

export default function SpreadsheetEngineForAiAgentsPage() {
  const base = siteUrl.replace(/\/$/, '');

  const jsonLd = [
    {
      '@context': 'https://schema.org',
      '@type': 'SoftwareApplication',
      name: 'Formualizer',
      applicationCategory: 'DeveloperApplication',
      operatingSystem: 'Cross-platform',
      description:
        'A spreadsheet engine built for AI agents: deterministic evaluation, typed SheetPort contracts over workbooks, and an inspectable incremental dependency graph. Native recalculation without headless LibreOffice.',
      url: `${base}/spreadsheet-engine-for-ai-agents`,
      offers: {
        '@type': 'Offer',
        price: '0',
        priceCurrency: 'USD',
      },
      softwareRequirements: 'Rust 1.85+, Python 3.10+, or Node.js 18+ (WASM)',
      programmingLanguage: ['Rust', 'Python', 'JavaScript', 'TypeScript'],
      license: 'https://opensource.org/licenses/MIT',
      codeRepository: 'https://github.com/psu3d0/formualizer',
      featureList: [
        'Deterministic evaluation with frozen clock, timezone, and RNG',
        'SheetPort: typed input/output contracts over workbooks',
        'Incremental dependency graph with cycle detection',
        'Formula parsing to a typed, inspectable AST',
        'Native recalculation MCP server (no LibreOffice)',
      ],
    },
    {
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: faqs.map((faq) => ({
        '@type': 'Question',
        name: faq.question,
        acceptedAnswer: {
          '@type': 'Answer',
          text: faq.answer,
        },
      })),
    },
    {
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: [
        {
          '@type': 'ListItem',
          position: 1,
          name: 'Formualizer',
          item: base,
        },
        {
          '@type': 'ListItem',
          position: 2,
          name: 'A spreadsheet engine built for AI agents',
          item: `${base}/spreadsheet-engine-for-ai-agents`,
        },
      ],
    },
  ];

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <div className="mx-auto flex w-full max-w-4xl flex-1 flex-col gap-10 px-6 py-10 md:py-14">
        <section className="rounded-2xl border bg-fd-card p-6 md:p-10">
          <span className="inline-flex rounded-full border bg-fd-background/90 px-3 py-1 text-xs font-medium text-fd-muted-foreground">
            For AI agents
          </span>
          <h1 className="mt-4 text-4xl font-bold tracking-tight md:text-5xl">
            A spreadsheet engine built for AI agents
          </h1>
          <div className="mt-5 space-y-4 text-base text-fd-muted-foreground md:text-lg">
            <p>
              Agents that work with spreadsheets today mostly don&apos;t compute. The popular Excel
              MCP servers read and write cells but can&apos;t evaluate a formula. The workaround —
              shelling out to headless LibreOffice for a recalc — is what even first-party agent
              tooling ships. That means slow round-trips, a process to babysit, and results that can
              change under the agent&apos;s feet.
            </p>
            <p>
              An agent needs three things from a spreadsheet engine, and they&apos;re the three
              things Formualizer was built around.
            </p>
          </div>
          <div className="mt-6 flex flex-wrap gap-3">
            <Link
              href="/docs/quickstarts"
              className="rounded-full bg-fd-foreground px-4 py-2 text-sm font-medium text-fd-background transition-opacity hover:opacity-90"
            >
              Get started
            </Link>
            <a
              href="https://github.com/PSU3D0/spreadsheet-mcp"
              className="rounded-full border bg-fd-background/85 px-4 py-2 text-sm font-medium hover:bg-fd-background"
            >
              spreadsheet-mcp on GitHub
            </a>
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">1. Deterministic evaluation</h2>
          <p className="mt-4 text-fd-muted-foreground">
            An agent that can&apos;t reproduce a result can&apos;t verify its own work. Formualizer
            lets you freeze every source of nondeterminism — volatile functions, the clock, the
            timezone, the RNG:
          </p>
          <div className="mt-4">
            <DynamicCodeBlock lang="python" code={deterministicCode} />
          </div>
          <p className="mt-4 text-fd-muted-foreground">
            Same inputs, same outputs — NOW(), TODAY(), RAND() included. That turns &quot;run the
            model&quot; into a pure function an agent can retry, cache, and test against.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">
            2. Typed contracts over workbooks: SheetPort
          </h2>
          <p className="mt-4 text-fd-muted-foreground">
            A workbook is an API with no schema. SheetPort adds one: a manifest declares named input
            and output ports (cell locations + JSON-schema types), and the session validates inputs,
            writes them, evaluates once, and reads typed outputs back:
          </p>
          <div className="mt-4">
            <DynamicCodeBlock lang="yaml" code={portsYaml} />
          </div>
          <p className="mt-4 text-fd-muted-foreground">
            The agent stops guessing which cells matter. The spreadsheet becomes a typed,
            deterministic function — callable from a tool definition like any other.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">
            3. A dependency graph the agent can interrogate
          </h2>
          <p className="mt-4 text-fd-muted-foreground">
            Formualizer maintains an incremental dependency graph: edits recompute only the dirty
            subgraph, cycles are detected and reported, and the formula structure is inspectable —
            parse any formula to a typed AST, walk its references:
          </p>
          <div className="mt-4">
            <DynamicCodeBlock lang="python" code={astCode} />
          </div>
          <p className="mt-4 text-fd-muted-foreground">
            Errors are values (<code>{`{'type': 'Error', 'kind': 'Div'}`}</code>), not exceptions —
            an agent inspects failures cell-by-cell instead of losing the whole evaluation to one bad
            divide.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">The MCP server</h2>
          <p className="mt-4 text-fd-muted-foreground">
            <a
              href="https://github.com/PSU3D0/spreadsheet-mcp"
              className="font-medium text-fd-foreground underline underline-offset-4"
            >
              spreadsheet-mcp
            </a>{' '}
            puts this engine behind the Model Context Protocol: native recalculation (no
            LibreOffice), dependency tracing, and fork/diff/verify loops — an agent forks the
            workbook, makes a change, recalculates, and diffs the result before committing. It is, as
            far as we know, the only MCP server that computes rather than just reads.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Runs where agents run</h2>
          <p className="mt-4 text-fd-muted-foreground">
            Rust core, permissively licensed (MIT). Python wheels for CPython 3.10–3.13, a JS/WASM
            package for browser and Node, and a Pyodide wheel for in-browser Python. No GPL wall, no
            office-suite dependency, no license fee to embed it in your product.
          </p>
          <p className="mt-4 text-fd-muted-foreground">
            The engine also powers the SheetPort workflows documented across the{' '}
            <Link
              href="/docs"
              className="font-medium text-fd-foreground underline underline-offset-4"
            >
              docs
            </Link>{' '}
            and the broader{' '}
            <a
              href="https://formualizer.com/open-source"
              className="font-medium text-fd-foreground underline underline-offset-4"
            >
              open-source project
            </a>
            .
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">FAQ</h2>
          <div className="mt-6 space-y-6">
            <div>
              <h3 className="font-semibold">Why does determinism matter for an agent?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                Because verification requires reproducibility. If NOW() or RAND() shifts between
                runs, the agent can&apos;t distinguish &quot;my edit changed the output&quot; from
                &quot;the clock did.&quot; Frozen-clock, seeded evaluation makes model runs pure
                functions.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">Can&apos;t I just use a headless LibreOffice recalc?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                You can — that&apos;s what most agent tooling does today. You pay process-spawn
                latency per recalculation, add a system dependency, and get no dependency tracing or
                typed I/O. A library call is faster and gives the agent structure to reason over.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">
                What makes spreadsheet-mcp different from other Excel MCP servers?
              </h3>
              <p className="mt-2 text-fd-muted-foreground">
                Most Excel MCP servers manipulate cells but cannot compute a formula. spreadsheet-mcp
                evaluates natively with this engine and adds dependency tracing and fork/diff/verify
                workflows.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">Is the license safe for commercial agents?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                Yes — MIT. Embed it in commercial products, SaaS, or internal tools without copyleft
                obligations.
              </p>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 text-center md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Give your agent a real engine</h2>
          <p className="mx-auto mt-2 max-w-2xl text-fd-muted-foreground">
            Start with a quickstart, wire up the MCP server, and inspect the dependency graph your
            agent reasons over.
          </p>
          <div className="mt-5 flex flex-wrap items-center justify-center gap-3">
            <Link
              href="/docs/quickstarts"
              className="rounded-full bg-fd-foreground px-4 py-2 text-sm font-medium text-fd-background transition-opacity hover:opacity-90"
            >
              Get started
            </Link>
            <Link
              href="/docs/core-concepts/dependency-graph-and-recalc"
              className="rounded-full border px-4 py-2 text-sm font-medium"
            >
              Dependency graph
            </Link>
            <Link href="/formula-parser" className="rounded-full border px-4 py-2 text-sm font-medium">
              Formula parser
            </Link>
            <a
              href="https://github.com/PSU3D0/spreadsheet-mcp"
              className="rounded-full border px-4 py-2 text-sm font-medium"
            >
              spreadsheet-mcp
            </a>
          </div>
        </section>
      </div>
    </>
  );
}
