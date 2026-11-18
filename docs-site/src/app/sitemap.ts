import type { MetadataRoute } from "next";
import {
  docsLastModified,
  sourceLastModified,
  staticRouteSources,
} from "@/lib/content-dates";
import { siteUrl } from "@/lib/env";
import { isNoindex, source } from "@/lib/source";

export const dynamic = "force-static";

export default function sitemap(): MetadataRoute.Sitemap {
  const base = siteUrl.replace(/\/$/, "");

  const staticRoutes: MetadataRoute.Sitemap = Object.entries(
    staticRouteSources,
  ).map(([path, files]) => ({
    url: `${base}${path}`,
    lastModified: sourceLastModified(files),
    changeFrequency: "weekly",
    priority: path === "/" ? 1 : 0.9,
  }));

  const docsRoutes: MetadataRoute.Sitemap = source
    .getPages()
    .filter((page) => !isNoindex(page))
    .map((page) => ({
      url: `${base}${page.url}`,
      lastModified: docsLastModified(page.path),
      changeFrequency: page.url === "/docs" ? "daily" : "weekly",
      priority:
        page.url === "/docs"
          ? 0.95
          : page.url.includes("/reference/functions/")
            ? 0.75
            : 0.8,
    }));

  return [...staticRoutes, ...docsRoutes].sort((a, b) =>
    a.url.localeCompare(b.url, "en"),
  );
}
