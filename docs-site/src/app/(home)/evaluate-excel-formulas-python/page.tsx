import type { Metadata } from 'next';
import Link from 'next/link';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import { siteUrl } from '@/lib/env';

export const metadata: Metadata = {
  title: 'Evaluate Excel Formulas in Python',
  description:
    'Calculate Excel formulas in Python without Excel installed. Formualizer is a Rust engine with Python bindings — 400+ Excel-compatible functions, incremental recalc, and Arrow-backed storage.',
  keywords: [
    'evaluate excel formulas python',
    'excel formula evaluation library',
    'python excel formula engine',
    'calculate excel formulas without excel',
    'openpyxl calculate formulas',
    'python spreadsheet engine',
  ],
  alternates: {
    canonical: '/evaluate-excel-formulas-python',
  },
  openGraph: {
    title: 'Evaluate Excel Formulas in Python | Formualizer',
    description:
      'Calculate Excel formulas in Python — no Excel, LibreOffice, or JVM. 400+ Excel-compatible functions, incremental recalculation, Arrow-backed storage.',
    url: '/evaluate-excel-formulas-python',
    type: 'website',
    images: ['/opengraph-image.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Evaluate Excel Formulas in Python | Formualizer',
    description:
      'A Rust spreadsheet engine with Python bindings — evaluate Excel formulas without Excel installed.',
    images: ['/twitter-image.png'],
  },
};

const faqs = [
  {
    question: 'Can Python evaluate Excel formulas without Excel installed?',
    answer:
      'Yes. Formualizer is a self-contained Rust engine with Python bindings — pip install formualizer, load the workbook, call evaluate_cell. No Excel, LibreOffice, or COM automation involved.',
  },
  {
    question: 'How is this different from openpyxl?',
    answer:
      'openpyxl reads and writes the xlsx file format, including formula text, but has no calculation engine. Formualizer evaluates the formulas. They compose: author with openpyxl if you like, calculate with formualizer — see the full recipe.',
  },
  {
    question: 'What happens on a formula error like #DIV/0!?',
    answer:
      "You get an error value ({'type': 'Error', 'kind': 'Div'}), not an exception. Check the return value to branch on errors; the rest of the workbook keeps evaluating.",
  },
  {
    question: 'Which Excel functions are supported?',
    answer:
      '400+ built-ins across math, lookup (VLOOKUP/XLOOKUP/INDEX/MATCH), text, date/time, financial, statistical, and dynamic arrays. See the function reference for the full list.',
  },
  {
    question: 'Is it fast enough for large workbooks?',
    answer:
      'The engine stores data in Arrow columns and vectorizes aggregate-heavy calculations (SUM/SUMIFS over large ranges); recalculation is incremental, so edits recompute only the dirty subgraph.',
  },
];

const comparison = {
  columns: ['formualizer', 'formulas', 'pycel', 'xlcalculator', 'openpyxl'],
  rows: [
    {
      label: 'Evaluates formulas',
      cells: ['yes — 400+ functions', 'partial', 'partial', 'partial', 'no'],
    },
    {
      label: 'Core',
      cells: [
        'Rust (native wheels)',
        'pure Python',
        'pure Python',
        'pure Python',
        'pure Python',
      ],
    },
    {
      label: 'Incremental recalc (dependency graph)',
      cells: ['yes', 'graph, full recompute focus', 'per-target compile', 'model compile', '—'],
    },
    {
      label: 'Deterministic evaluation (seed/clock)',
      cells: ['yes', 'no', 'no', 'no', '—'],
    },
    {
      label: 'Errors as values',
      cells: ['yes', 'varies', 'varies', 'varies', '—'],
    },
  ],
};

const evaluateExisting = `import formualizer as fz

wb = fz.load_workbook("model.xlsx")
value = wb.evaluate_cell("Sheet1", 1, 2)   # row 1, col 2 (B1), 1-based
print(value)                                # native float/str/bool — not a formula string`;

const buildFromScratch = `import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Sheet1")
s.set_value(1, 1, 1000.0)
s.set_value(2, 1, 2000.0)
s.set_value(3, 1, 1500.0)
s.set_formula(1, 2, "=SUM(A1:A3)")
print(wb.evaluate_cell("Sheet1", 1, 2))   # 4500.0`;

const writeBack = `import formualizer as fz

summary = fz.recalculate_file("model.xlsx", output="model.recalc.xlsx")
print(summary["status"], summary["evaluated"], summary["errors"])`;

export default function EvaluateExcelFormulasPythonPage() {
  const base = siteUrl.replace(/\/$/, '');

  const jsonLd = [
    {
      '@context': 'https://schema.org',
      '@type': 'SoftwareApplication',
      name: 'Formualizer',
      applicationCategory: 'DeveloperApplication',
      operatingSystem: 'Cross-platform',
      description:
        'Evaluate Excel formulas in Python with a Rust engine: 400+ Excel-compatible functions, incremental dependency graph, and Arrow-backed columnar storage. No Excel, LibreOffice, or JVM.',
      url: `${base}/evaluate-excel-formulas-python`,
      offers: {
        '@type': 'Offer',
        price: '0',
        priceCurrency: 'USD',
      },
      softwareRequirements: 'Python 3.10+',
      programmingLanguage: ['Python', 'Rust'],
      license: 'https://opensource.org/licenses/MIT',
      codeRepository: 'https://github.com/psu3d0/formualizer',
      featureList: [
        '400+ Excel-compatible built-in functions',
        'Incremental dependency graph with cycle detection',
        'Arrow-powered columnar storage',
        'Deterministic evaluation controls',
        'Errors returned as values, not exceptions',
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
          name: 'Evaluate Excel formulas in Python',
          item: `${base}/evaluate-excel-formulas-python`,
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
            Python library
          </span>
          <h1 className="mt-4 text-4xl font-bold tracking-tight md:text-5xl">
            Evaluate Excel formulas in Python
          </h1>
          <div className="mt-5 space-y-4 text-base text-fd-muted-foreground md:text-lg">
            <p>
              Python can read xlsx files a dozen ways — and calculate them almost none. openpyxl
              stores formula text without evaluating it. <code>data_only=True</code> replays
              whatever Excel last cached. The pure-Python evaluators cover a fraction of Excel&apos;s
              function surface and slow down on real models.
            </p>
            <p>
              Formualizer is a spreadsheet engine written in Rust with first-class Python bindings:
              400+ Excel-compatible functions, an incremental dependency graph, and Arrow-backed
              columnar storage. <code>pip install formualizer</code> — no Excel, no LibreOffice, no
              JVM.
            </p>
          </div>
          <div className="mt-6">
            <DynamicCodeBlock lang="bash" code="pip install formualizer" />
          </div>
          <div className="mt-6 flex flex-wrap gap-3">
            <Link
              href="/docs/quickstarts/python-quickstart"
              className="rounded-full bg-fd-foreground px-4 py-2 text-sm font-medium text-fd-background transition-opacity hover:opacity-90"
            >
              Python quickstart
            </Link>
            <a
              href="https://github.com/psu3d0/formualizer"
              className="rounded-full border bg-fd-background/85 px-4 py-2 text-sm font-medium hover:bg-fd-background"
            >
              GitHub
            </a>
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Evaluate an existing workbook</h2>
          <div className="mt-4">
            <DynamicCodeBlock lang="python" code={evaluateExisting} />
          </div>
          <p className="mt-4 text-fd-muted-foreground">
            Results come back as native Python values: <code>float</code>, <code>str</code>,{' '}
            <code>bool</code>, <code>None</code>, or nested lists for spilled arrays. Errors are
            values too — a <code>#DIV/0!</code> returns{' '}
            <code>{`{'type': 'Error', 'kind': 'Div'}`}</code> instead of raising, so a single bad
            cell doesn&apos;t abort a batch evaluation.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">
            Build and calculate a workbook from scratch
          </h2>
          <div className="mt-4">
            <DynamicCodeBlock lang="python" code={buildFromScratch} />
          </div>
          <p className="mt-4 text-fd-muted-foreground">
            Batch APIs (<code>set_values_batch</code>, <code>set_formulas_batch</code>,{' '}
            <code>evaluate_cells</code>, <code>evaluate_all</code>) cover the write-many/read-many
            shapes, and <code>to_xlsx_bytes()</code> round-trips the result.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">
            Write calculated values back into the file
          </h2>
          <p className="mt-4 text-fd-muted-foreground">
            Downstream tools that read Excel&apos;s cached values — including openpyxl&apos;s own{' '}
            <code>data_only=True</code> — see real numbers after a recalculation pass:
          </p>
          <div className="mt-4">
            <DynamicCodeBlock lang="python" code={writeBack} />
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">How it compares</h2>
          <div className="mt-4 overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b">
                  <th className="px-3 py-2 text-left font-medium text-fd-muted-foreground" />
                  {comparison.columns.map((col) => (
                    <th
                      key={col}
                      className={`px-3 py-2 text-left font-semibold ${
                        col === 'formualizer' ? 'text-fd-foreground' : 'text-fd-muted-foreground'
                      }`}
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {comparison.rows.map((row) => (
                  <tr key={row.label} className="border-b last:border-0 align-top">
                    <th className="px-3 py-2 text-left font-medium">{row.label}</th>
                    {row.cells.map((cell, i) => (
                      <td
                        key={`${row.label}-${comparison.columns[i]}`}
                        className={`px-3 py-2 ${
                          i === 0 ? 'font-medium text-fd-foreground' : 'text-fd-muted-foreground'
                        }`}
                      >
                        {cell === 'no' ? <strong>no</strong> : cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Where it runs</h2>
          <p className="mt-4 text-fd-muted-foreground">
            Prebuilt wheels for CPython 3.10–3.13 on Linux, macOS, and Windows — plus a
            Pyodide-tagged wheel, so the same API runs in the browser. CI, containers, serverless:
            no office suite to install or babysit.
          </p>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">FAQ</h2>
          <div className="mt-6 space-y-6">
            <div>
              <h3 className="font-semibold">
                Can Python evaluate Excel formulas without Excel installed?
              </h3>
              <p className="mt-2 text-fd-muted-foreground">
                Yes. Formualizer is a self-contained Rust engine with Python bindings —{' '}
                <code>pip install formualizer</code>, load the workbook, call <code>evaluate_cell</code>.
                No Excel, LibreOffice, or COM automation involved.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">How is this different from openpyxl?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                openpyxl reads and writes the xlsx file format, including formula <em>text</em>, but
                has no calculation engine. Formualizer evaluates the formulas. They compose: author
                with openpyxl if you like, calculate with formualizer — see{' '}
                <a
                  href="https://formualizer.com/articles/openpyxl-calculate-formulas"
                  className="font-medium text-fd-foreground underline underline-offset-4"
                >
                  the full recipe
                </a>
                .
              </p>
            </div>
            <div>
              <h3 className="font-semibold">What happens on a formula error like #DIV/0!?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                You get an error value (<code>{`{'type': 'Error', 'kind': 'Div'}`}</code>), not an
                exception. Check the return value to branch on errors; the rest of the workbook keeps
                evaluating.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">Which Excel functions are supported?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                400+ built-ins across math, lookup (VLOOKUP/XLOOKUP/INDEX/MATCH), text, date/time,
                financial, statistical, and dynamic arrays. See the{' '}
                <Link
                  href="/docs/reference/functions"
                  className="font-medium text-fd-foreground underline underline-offset-4"
                >
                  function reference
                </Link>{' '}
                for the full list.
              </p>
            </div>
            <div>
              <h3 className="font-semibold">Is it fast enough for large workbooks?</h3>
              <p className="mt-2 text-fd-muted-foreground">
                The engine stores data in Arrow columns and vectorizes aggregate-heavy calculations
                (SUM/SUMIFS over large ranges); recalculation is incremental, so edits recompute only
                the dirty subgraph.
              </p>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border bg-fd-card p-6 text-center md:p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Start calculating</h2>
          <p className="mx-auto mt-2 max-w-2xl text-fd-muted-foreground">
            Install the wheel and evaluate your first workbook in minutes, or explore how the parser
            and function registry work.
          </p>
          <div className="mt-5 flex flex-wrap items-center justify-center gap-3">
            <Link
              href="/docs/quickstarts/python-quickstart"
              className="rounded-full bg-fd-foreground px-4 py-2 text-sm font-medium text-fd-background transition-opacity hover:opacity-90"
            >
              Python quickstart
            </Link>
            <Link href="/formula-parser" className="rounded-full border px-4 py-2 text-sm font-medium">
              Formula parser
            </Link>
            <Link
              href="/docs/reference/functions"
              className="rounded-full border px-4 py-2 text-sm font-medium"
            >
              Function reference
            </Link>
            <a
              href="https://github.com/psu3d0/formualizer"
              className="rounded-full border px-4 py-2 text-sm font-medium"
            >
              GitHub
            </a>
          </div>
        </section>
      </div>
    </>
  );
}
