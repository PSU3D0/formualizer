import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="mx-auto flex w-full max-w-3xl flex-1 flex-col items-center justify-center gap-6 px-6 text-center">
      <h1 className="text-3xl font-bold tracking-tight">Formualizer Documentation</h1>
      <p className="text-fd-muted-foreground">
        Parse, evaluate, and mutate Excel-compatible workbooks from Rust, Python, and WASM.
      </p>
      <div className="flex flex-wrap items-center justify-center gap-3">
        <Link href="/docs" className="font-medium underline">
          Open docs
        </Link>
        <Link href="/docs/quickstarts" className="font-medium underline">
          Quickstarts
        </Link>
        <Link href="/docs/reference/functions" className="font-medium underline">
          Function reference
        </Link>
      </div>
    </div>
  );
}
