/**
 * Canonical site URL, resolved once at build time.
 *
 * Priority:
 *  1. Explicit NEXT_PUBLIC_SITE_URL env var
 *  2. Vercel's auto-injected VERCEL_PROJECT_PRODUCTION_URL (production builds)
 *  3. Vercel's VERCEL_URL (preview/branch deploys)
 *  4. Localhost fallback for local dev
 */
function resolveSiteUrl(): string {
  if (process.env.NEXT_PUBLIC_SITE_URL) {
    return process.env.NEXT_PUBLIC_SITE_URL.replace(/\/$/, '');
  }

  // Vercel injects these automatically — no dashboard config needed
  if (process.env.VERCEL_PROJECT_PRODUCTION_URL) {
    return `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`;
  }

  if (process.env.VERCEL_URL) {
    return `https://${process.env.VERCEL_URL}`;
  }

  return 'http://localhost:3000';
}

export const siteUrl = resolveSiteUrl();
