import Image from 'next/image';
import Link from 'next/link';
import { InstallCta } from '@/components/home/install-cta';
import { WorkflowTabs } from '@/components/home/workflow-tabs';
import { siteUrl } from '@/lib/env';

const runtimeCards = [
  {
    title: 'Rust',
    href: '/docs/quickstarts/rust-quickstart',
    image: '/home/path-rust-v1.png',
    badge: 'Systems runtime',
    description: 'Compile-time safety and high-throughput workbook execution from native Rust.',
  },
  {
    title: 'Python',
    href: '/docs/quickstarts/python-quickstart',
    image: '/home/path-python-v1.png',
    badge: 'Automation runtime',
    description: 'Drop spreadsheet logic into data pipelines and notebooks with a Python-first API.',
  },
  {
    title: 'JS / WASM',
    href: '/docs/quickstarts/js-wasm-quickstart',
    image: '/home/path-js-wasm-v1.png',
    badge: 'Web runtime',
    description: 'Run the same engine in browser and Node with a portable WASM package.',
  },
];

const featureCards = [
  {
    title: '320+ Excel-compatible built-ins',
    href: '/docs/reference/functions',
    image: '/home/feature-builtins-v1.png',
    badge: 'Function depth',
    description: 'Lookup, math, text, date/time, financial, statistical, and dynamic-array coverage.',
  },
  {
    title: 'Incremental dependency graph',
    href: '/docs/core-concepts/dependency-graph-and-recalc',
    image: '/home/feature-recalc-v1.png',
    badge: 'Recalculation engine',
    description:
      'Topological scheduling, cycle detection, and selective recompute designed for large models.',
  },
  {
    title: 'Extensible by design',
    href: '/docs/guides/custom-functions-rust-python-js',
    image: '/home/feature-extensibility-v1.png',
    badge: 'Plugins and UDFs',
    description:
      'Workbook-local custom functions today, with WASM plugin and provider expansion paths underway.',
  },
];

export default function HomePage() {
  const base = siteUrl.replace(/\/$/, '');

  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'SoftwareApplication',
    name: 'Formualizer',
    applicationCategory: 'DeveloperApplication',
    operatingSystem: 'Cross-platform',
    description:
      'Embeddable spreadsheet formula engine with 320+ Excel-compatible functions, Arrow-powered storage, and deterministic evaluation. Available for Rust, Python, and JavaScript/WASM.',
    url: base,
    offers: {
      '@type': 'Offer',
      price: '0',
      priceCurrency: 'USD',
    },
    softwareRequirements: 'Rust 1.85+, Python 3.9+, or Node.js 18+ (WASM)',
    programmingLanguage: ['Rust', 'Python', 'JavaScript', 'TypeScript'],
    license: 'https://opensource.org/licenses/MIT',
    codeRepository: 'https://github.com/psu3d0/formualizer',
    featureList: [
      '320+ Excel-compatible built-in functions',
      'Incremental dependency graph with cycle detection',
      'SheetPort: treat spreadsheets as typed deterministic functions',
      'Arrow-powered columnar storage',
      'Custom function registration (Rust, Python, JS)',
      'WASM plugin support',
      'Deterministic evaluation controls',
    ],
  };

  return (
    <>
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
    />
    <div className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-10 px-6 py-10 md:py-14 max-[430px]:gap-8 max-[430px]:pt-0">
      <section className="relative overflow-hidden rounded-3xl border bg-fd-card max-[430px]:-mx-6 max-[430px]:rounded-none max-[430px]:border-x-0">
        <Image
          src="/home/hero-engine-core-v1.png"
          alt="Abstract compute flow background"
          width={1536}
          height={1024}
          priority
          className="pointer-events-none absolute inset-y-0 right-[-22%] h-full w-auto max-w-none object-cover object-right opacity-50 md:right-[-8%] md:opacity-60"
        />

        <div className="pointer-events-none absolute inset-0 bg-gradient-to-r from-fd-card via-fd-card/95 to-fd-card/35" />

        <div className="relative z-10 p-6 md:min-h-[500px] md:p-10 max-[430px]:p-4">
          <div className="max-w-2xl space-y-5">
            <span className="inline-flex rounded-full border bg-fd-background/90 px-3 py-1 text-xs font-medium text-fd-muted-foreground">
              The Arrow-native spreadsheet engine
            </span>

            <h1 className="text-4xl font-bold tracking-tight md:text-5xl">
              Build reliable spreadsheet workflows in Rust, Python, and WASM
            </h1>

            <p className="text-base text-fd-muted-foreground md:text-lg">
              Arrow-powered storage, incremental recalculation, and deterministic controls built
              for high-trust agent workflows.
            </p>

            <div className="flex flex-wrap gap-3 pt-1">
              <Link
                href="/docs/quickstarts"
                className="rounded-full bg-fd-foreground px-4 py-2 text-sm font-medium text-fd-background transition-opacity hover:opacity-90"
              >
                Get Started
              </Link>
              <Link
                href="/docs/reference/functions"
                className="rounded-full border bg-fd-background/85 px-4 py-2 text-sm font-medium hover:bg-fd-background"
              >
                Browse Functions
              </Link>
              <span className="relative inline-flex">
                <span className="pointer-events-none absolute inset-0 rounded-full border border-amber-400/70 animate-pulse" />
                <span className="pointer-events-none absolute -inset-1 rounded-full border border-amber-300/30 animate-ping" />
                <Link
                  href="/docs/playground"
                  className="relative rounded-full border bg-fd-background/90 px-4 py-2 text-sm font-medium hover:bg-fd-background"
                >
                  Open Playground
                </Link>
              </span>
            </div>

            <div className="pt-2">
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
                Pick your runtime
              </p>
              <div className="flex flex-wrap gap-2">
                <Link
                  href="/docs/quickstarts/rust-quickstart"
                  className="inline-flex items-center gap-2 rounded-md border border-orange-500/40 bg-orange-500/10 px-3 py-1.5 text-xs font-medium text-orange-800 hover:bg-orange-500/15 dark:text-orange-200"
                >
                  <span className="h-2 w-2 rounded-full bg-orange-500 dark:bg-orange-300" />
                  Rust
                </Link>
                <Link
                  href="/docs/quickstarts/python-quickstart"
                  className="inline-flex items-center gap-2 rounded-md border border-blue-500/40 bg-blue-500/10 px-3 py-1.5 text-xs font-medium text-blue-800 hover:bg-blue-500/15 dark:text-blue-200"
                >
                  <span className="h-2 w-2 rounded-full bg-blue-500 dark:bg-blue-300" />
                  Python
                </Link>
                <Link
                  href="/docs/quickstarts/js-wasm-quickstart"
                  className="inline-flex items-center gap-2 rounded-md border border-teal-500/40 bg-teal-500/10 px-3 py-1.5 text-xs font-medium text-teal-800 hover:bg-teal-500/15 dark:text-teal-200"
                >
                  <span className="h-2 w-2 rounded-full bg-teal-500 dark:bg-teal-300" />
                  JS / WASM
                </Link>
              </div>
            </div>
          </div>

          <div className="mt-8 grid gap-3 sm:grid-cols-3">
            <Link
              href="/docs/reference/functions"
              className="rounded-xl border border-violet-400/30 bg-gradient-to-b from-violet-500/10 to-fd-background/75 p-4 transition hover:-translate-y-0.5 hover:border-violet-300/60"
            >
              <p className="text-xs font-medium uppercase tracking-wide text-violet-800 dark:text-violet-300">
                Function depth
              </p>
              <p className="mt-1 text-lg font-semibold">320+ Excel-compatible built-ins</p>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                Math, lookup, text, financial, stats, and dynamic arrays in one registry.
              </p>
            </Link>
            <Link
              href="/docs/core-concepts/dependency-graph-and-recalc"
              className="rounded-xl border border-blue-400/30 bg-gradient-to-b from-blue-500/10 to-fd-background/75 p-4 transition hover:-translate-y-0.5 hover:border-blue-300/60"
            >
              <p className="text-xs font-medium uppercase tracking-wide text-blue-800 dark:text-blue-300">
                Recalculation engine
              </p>
              <p className="mt-1 text-lg font-semibold">Incremental dependency graph</p>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                Topological scheduling, cycle detection, and selective recompute for scale.
              </p>
            </Link>
            <Link
              href="/docs/guides/custom-functions-rust-python-js"
              className="rounded-xl border border-emerald-400/30 bg-gradient-to-b from-emerald-500/10 to-fd-background/75 p-4 transition hover:-translate-y-0.5 hover:border-emerald-300/60"
            >
              <p className="text-xs font-medium uppercase tracking-wide text-emerald-800 dark:text-emerald-300">
                Extensibility
              </p>
              <p className="mt-1 text-lg font-semibold">Custom functions + plugins</p>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                Workbook-local callbacks, WASM plugin binding, and source/provider growth path.
              </p>
            </Link>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-5xl px-1">
        <p className="text-2xl leading-relaxed text-fd-muted-foreground md:text-4xl md:leading-snug">
          Formualizer is a permissively licensed spreadsheet engine with{' '}
          <span className="font-semibold text-fd-foreground">Arrow-powered performance</span>,{' '}
          <span className="font-semibold text-fd-foreground">deterministic evaluation for agents</span>, and{' '}
          <span className="font-semibold text-fd-foreground">consistent Rust, Python, and WASM APIs</span>.
        </p>
      </section>

      <InstallCta />

      <section className="grid gap-6 md:grid-cols-3">
        <div className="md:col-span-3 text-center">
          <h2 className="text-3xl font-semibold tracking-tight">Pick your path</h2>
          <p className="mx-auto mt-2 max-w-3xl text-fd-muted-foreground">
            Start from the runtime you ship today, then expand to shared function coverage and
            cross-runtime parity as your models grow.
          </p>
        </div>

        {runtimeCards.map((card) => (
          <Link
            key={card.title}
            href={card.href}
            className="group overflow-hidden rounded-2xl border bg-fd-card transition hover:-translate-y-0.5 hover:shadow-lg"
          >
            <div className="relative aspect-[16/10] overflow-hidden border-b">
              <Image
                src={card.image}
                alt={`${card.title} runtime illustration`}
                fill
                className="object-cover transition-transform duration-300 group-hover:scale-[1.02]"
              />
            </div>
            <div className="space-y-2 p-4">
              <p className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
                {card.badge}
              </p>
              <h3 className="text-xl font-semibold">{card.title}</h3>
              <p className="text-sm text-fd-muted-foreground">{card.description}</p>
            </div>
          </Link>
        ))}
      </section>

      <section className="grid gap-6 md:grid-cols-3">
        <div className="md:col-span-3 text-center">
          <h2 className="text-3xl font-semibold tracking-tight">Engine capabilities</h2>
          <p className="mx-auto mt-2 max-w-3xl text-fd-muted-foreground">
            Formualizer is built for real workbook systems: deep function coverage, performant
            recalculation, and extensibility for custom business logic.
          </p>
        </div>

        {featureCards.map((card) => (
          <Link
            key={card.title}
            href={card.href}
            className="group overflow-hidden rounded-2xl border bg-fd-card transition hover:-translate-y-0.5 hover:shadow-lg"
          >
            <div className="relative aspect-[16/10] overflow-hidden border-b">
              <Image
                src={card.image}
                alt={`${card.title} illustration`}
                fill
                className="object-cover transition-transform duration-300 group-hover:scale-[1.02]"
              />
            </div>
            <div className="space-y-2 p-4">
              <p className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
                {card.badge}
              </p>
              <h3 className="text-lg font-semibold">{card.title}</h3>
              <p className="text-sm text-fd-muted-foreground">{card.description}</p>
            </div>
          </Link>
        ))}
      </section>

      <WorkflowTabs />
    </div>
    </>
  );
}
