import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
import { FunctionMeta } from '@/components/reference/function-meta';
import { FunctionPageSchema } from '@/components/reference/function-page-schema';
import { FunctionSandbox } from '@/components/reference/function-sandbox';
import { CodeTabs } from '@/components/code/code-tabs';
import { RunnableCode } from '@/components/code/runnable-code';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    FunctionMeta,
    FunctionPageSchema,
    FunctionSandbox,
    CodeTabs,
    RunnableCode,
    Tab,
    Tabs,
    ...components,
  };
}
