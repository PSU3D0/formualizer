import { Tab, Tabs } from "fumadocs-ui/components/tabs";
import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { CodeTabs } from "@/components/code/code-tabs";
import { RunnableCode } from "@/components/code/runnable-code";
import {
  AccumulatorSandbox,
  ConvergentCycleSandbox,
  GuardedPairSandbox,
} from "@/components/playground/cycle-sandbox";
import { FunctionMeta } from "@/components/reference/function-meta";
import { FunctionPageSchema } from "@/components/reference/function-page-schema";
import { FunctionSandbox } from "@/components/reference/function-sandbox";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    FunctionMeta,
    FunctionPageSchema,
    FunctionSandbox,
    CodeTabs,
    RunnableCode,
    GuardedPairSandbox,
    ConvergentCycleSandbox,
    AccumulatorSandbox,
    Tab,
    Tabs,
    ...components,
  };
}
