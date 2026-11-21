import { docs } from "fumadocs-mdx:collections/server";
import { type InferPageType, loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import functionsMeta from "@/generated/functions-meta.json";

// See https://fumadocs.dev/docs/headless/source-api for more info
export const source = loader({
  baseUrl: "/docs",
  source: docs.toFumadocsSource(),
  plugins: [lucideIconsPlugin()],
});

export function isNoindex(page: InferPageType<typeof source>) {
  const [reference, functions, ...slugs] = page.slugs;
  if (reference !== "reference" || functions !== "functions") return false;
  const id = slugs.join("/") as keyof typeof functionsMeta;
  return functionsMeta[id]?.noindex ?? false;
}

export function getPageImage(page: InferPageType<typeof source>) {
  const segments = [...page.slugs, "image.webp"];

  return {
    segments,
    url: `/og/docs/${segments.join("/")}`,
  };
}

// Fumadocs serializes MDX expressions as XML-escaped attribute values.
function readExampleAttribute(value: string) {
  const decoded = value.replace(
    /&#(?:x([0-9a-f]+)|(\d+));|&amp;/gi,
    (_, hex, decimal) =>
      hex
        ? String.fromCodePoint(parseInt(hex, 16))
        : decimal
          ? String.fromCodePoint(Number(decimal))
          : "&",
  );
  return JSON.parse(decoded);
}

function renderLlmFriendlyMarkdown(processed: string): string {
  let llmSafe = processed.replace(
    /<FunctionExample\s+title="([^"]*)"\s+formula="([^"]*)"\s+grid="([^"]*)"\s+expected="([^"]*)"\s*\/>/g,
    (_match, titleJson, formulaJson, gridJson, expectedJson) => {
      const title: string = readExampleAttribute(titleJson);
      const formula: string = readExampleAttribute(formulaJson);
      const grid: Record<string, string> = readExampleAttribute(gridJson);
      const expected: string | null = readExampleAttribute(expectedJson);

      const lines = [
        ...(title ? [`### ${title}`, ""] : []),
        "```excel",
        formula,
        "```",
        "",
      ];
      if (Object.keys(grid).length > 0) {
        lines.push(
          "Input cells:",
          "```json",
          JSON.stringify(grid, null, 2),
          "```",
          "",
        );
      }
      if (expected !== null)
        lines.push(`Expected result: ${expected || '""'}`, "");
      return lines.join("\n");
    },
  );

  // Convert CodeTabs to multi-language code blocks
  llmSafe = llmSafe.replace(/<CodeTabs[\s\S]*?\/>/g, (match) => {
    const titleMatch = match.match(/title="([^"]*)"/);
    const title = titleMatch ? titleMatch[1] : "";

    const langMap: Array<[string, string, string]> = [
      ["rust", "rust", "Rust"],
      ["python", "python", "Python"],
      ["ts", "ts", "JS/WASM"],
    ];

    let out = title ? `**${title}**\n\n` : "";

    for (const [prop, lang, label] of langMap) {
      const codeMatch = match.match(
        new RegExp(`${prop}=\\{` + "`([\\s\\S]*?)`" + "\\}"),
      );
      if (codeMatch) {
        out += `**${label}:**\n\`\`\`${lang}\n${codeMatch[1]}\n\`\`\`\n\n`;
      }
    }

    return out;
  });

  // Convert RunnableCode to a standard code block
  llmSafe = llmSafe.replace(/<RunnableCode[\s\S]*?\/>/g, (match) => {
    const titleMatch = match.match(/title="([^"]*)"/);
    const title = titleMatch ? titleMatch[1] : "";
    const codeMatch = match.match(/code=\{`([\s\S]*?)`\}/);
    const code = codeMatch ? codeMatch[1] : "";
    const langMatch = match.match(/lang="([^"]*)"/);
    const lang = langMatch ? langMatch[1] : "ts";

    let out = title ? `**${title}:**\n` : "";
    out += `\`\`\`${lang}\n${code}\n\`\`\`\n`;
    return out;
  });

  return llmSafe;
}

export async function getLLMText(page: InferPageType<typeof source>) {
  const processed = await page.data.getText("processed");
  const llmSafe = renderLlmFriendlyMarkdown(processed);

  return `# ${page.data.title}

${llmSafe}`;
}
