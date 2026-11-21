"use client";

import dynamic from "next/dynamic";
import { useState } from "react";

const FunctionSandbox = dynamic(
  () => import("./function-sandbox").then((module) => module.FunctionSandbox),
  { ssr: false, loading: () => <p role="status">Loading formula editor…</p> },
);

export function ExampleEditor({
  formula,
  grid,
  version,
}: {
  formula: string;
  grid: Record<string, string>;
  version: string;
}) {
  const [opened, setOpened] = useState(false);

  return (
    <details
      className="my-4"
      data-example-editor
      onToggle={(event) => {
        if (event.currentTarget.open) setOpened(true);
      }}
    >
      <summary className="cursor-pointer text-sm font-medium">
        Try this formula
      </summary>
      {opened && <FunctionSandbox formula={formula} grid={grid} />}
      <p>
        {`This editor runs the published Formualizer ${version} package, so the last digits can differ and functions newer than that release return `}
        <code>#NAME?</code>.
      </p>
    </details>
  );
}
