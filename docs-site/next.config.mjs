import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();
const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const config = {
  // Static export for Cloudflare Workers static-assets hosting.
  output: 'export',
  // next/image optimization is unavailable in a static export; serve images as-is.
  images: { unoptimized: true },
  reactStrictMode: true,
  turbopack: {
    root: path.join(__dirname, '..'),
  },
  serverExternalPackages: ['formualizer'],
  webpack: (config, { isServer }) => {
    config.experiments = {
      ...config.experiments,
      asyncWebAssembly: true,
      layers: true,
    };

    // For WASM support
    if (!isServer) {
       config.output.webassemblyModuleFilename = 'static/wasm/[modulehash].wasm';
    }

    return config;
  },
  // NOTE: The `/docs/*.mdx` -> `/llms.mdx/docs/*` rewrite is unavailable under
  // `output: 'export'`. It is reproduced at the edge via docs-site/public/_redirects
  // (a Cloudflare static-assets rewrite).
};

export default withMDX(config);
