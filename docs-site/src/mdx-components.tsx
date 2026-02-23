import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
import { FunctionMeta } from '@/components/reference/function-meta';
import { FunctionSandbox } from '@/components/reference/function-sandbox';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    FunctionMeta,
    FunctionSandbox,
    Tab,
    Tabs,
    ...components,
  };
}
