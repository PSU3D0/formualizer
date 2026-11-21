import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
} from "fumadocs-ui/layouts/docs/page";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { LLMCopyButton, ViewOptions } from "@/components/ai/page-actions";
import { docsLastModified } from "@/lib/content-dates";
import { siteUrl } from "@/lib/env";
import { gitConfig } from "@/lib/layout.shared";
import { getPageImage, isNoindex, source } from "@/lib/source";
import { getMDXComponents } from "@/mdx-components";

function JsonLd({ data }: { data: object }) {
  return (
    <script type="application/ld+json">
      {JSON.stringify(data).replace(/</g, "\\u003c")}
    </script>
  );
}

function buildBreadcrumbJsonLd(slugs: string[], title: string) {
  const base = siteUrl.replace(/\/$/, "");
  const items = [
    { name: "Formualizer", url: `${base}/` },
    { name: "Docs", url: `${base}/docs` },
  ];

  // Build intermediate breadcrumbs from slug segments
  let path = "/docs";
  for (let i = 0; i < slugs.length - 1; i++) {
    path += `/${slugs[i]}`;
    const name = source.getPage(slugs.slice(0, i + 1))?.data.title ?? slugs[i];
    items.push({ name, url: `${base}${path}` });
  }

  // Final breadcrumb is the current page
  if (slugs.length > 0)
    items.push({ name: title, url: `${base}/docs/${slugs.join("/")}` });

  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: items.map((item, index) => ({
      "@type": "ListItem",
      position: index + 1,
      name: item.name,
      item: item.url,
    })),
  };
}

export default async function Page(props: PageProps<"/docs/[[...slug]]">) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const MDX = page.data.body;
  const slugs = params.slug ?? [];
  const breadcrumb = buildBreadcrumbJsonLd(slugs, page.data.title);
  const { seoTitle, title, description } = page.data;
  const category = seoTitle ? source.getPage(slugs.slice(0, -1)) : undefined;
  const article = seoTitle
    ? {
        "@context": "https://schema.org",
        "@type": "TechArticle",
        headline: seoTitle,
        description,
        dateModified: docsLastModified(page.path),
        mainEntityOfPage: new URL(page.url, siteUrl).href,
      }
    : undefined;

  return (
    <>
      <JsonLd data={breadcrumb} />
      {article && <JsonLd data={article} />}
      <DocsPage
        toc={page.data.toc}
        full={page.data.full}
        breadcrumb={{ enabled: !category }}
        footer={{ enabled: !category }}
      >
        {category && (
          <nav
            aria-label="Function breadcrumb"
            className="text-sm text-fd-muted-foreground"
          >
            <Link href="/docs/reference/functions" className="hover:underline">
              Functions
            </Link>
            <span aria-hidden="true"> / </span>
            <Link href={category.url} className="hover:underline">
              {category.data.title}
            </Link>
          </nav>
        )}
        <DocsTitle>{title}</DocsTitle>
        {!seoTitle && (
          <DocsDescription className="mb-0">{description}</DocsDescription>
        )}
        <div className="flex flex-row gap-2 items-center border-b pb-6">
          <LLMCopyButton markdownUrl={`${page.url}.mdx`} />
          <ViewOptions
            markdownUrl={`${page.url}.mdx`}
            githubUrl={`https://github.com/${gitConfig.user}/${gitConfig.repo}/blob/${gitConfig.branch}/docs-site/content/docs/${page.path}`}
          />
        </div>
        <DocsBody data-function-content={seoTitle ? "" : undefined}>
          <MDX
            components={getMDXComponents({
              // this allows you to link to other pages with relative file paths
              a: createRelativeLink(source, page),
            })}
          />
        </DocsBody>
      </DocsPage>
    </>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

function getPageKeywords(slugs: string[], title: string): string[] | undefined {
  const section = slugs[0];

  // Function reference pages: excel-specific keywords
  if (
    slugs.length >= 4 &&
    section === "reference" &&
    slugs[1] === "functions"
  ) {
    return [
      `${title} function`,
      `${title} formula`,
      `excel ${title.toLowerCase()} function`,
      "spreadsheet function reference",
      "formualizer",
    ];
  }

  // SheetPort pages
  if (section === "sheetport") {
    return [
      "sheetport",
      "spreadsheet as API",
      "spreadsheet typed interface",
      "spreadsheet function interface",
      "FIO manifest",
      "workbook I/O",
      "formualizer sheetport",
      "deterministic spreadsheet evaluation",
    ];
  }

  // Reference pages (non-function)
  if (section === "reference") {
    return [
      "formualizer API",
      "spreadsheet engine API",
      title.toLowerCase(),
      "excel formula engine",
      "wasm spreadsheet",
    ];
  }

  // Core concepts
  if (section === "core-concepts") {
    return [
      "spreadsheet engine",
      "formula evaluation",
      title.toLowerCase(),
      "excel engine internals",
      "formualizer",
    ];
  }

  // Guides
  if (section === "guides") {
    return [
      "formualizer guide",
      "spreadsheet engine tutorial",
      title.toLowerCase(),
      "excel formula engine",
    ];
  }

  // Quickstarts
  if (section === "quickstarts") {
    return [
      "formualizer quickstart",
      "spreadsheet engine setup",
      title.toLowerCase(),
      "formula engine getting started",
    ];
  }

  return undefined;
}

export async function generateMetadata(
  props: PageProps<"/docs/[[...slug]]">,
): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const slugs = params.slug ?? [];
  const { seoTitle, title } = page.data;
  const keywords = getPageKeywords(slugs, title);

  return {
    title: seoTitle ? { absolute: seoTitle } : title,
    description: page.data.description,
    keywords,
    robots: isNoindex(page) ? { index: false, follow: true } : undefined,
    alternates: {
      canonical: page.url,
    },
    openGraph: {
      title: seoTitle ?? title,
      description: page.data.description,
      url: page.url,
      images: getPageImage(page).url,
    },
    twitter: {
      card: "summary_large_image",
      title: seoTitle ?? title,
      description: page.data.description,
      images: [getPageImage(page).url],
    },
  };
}
