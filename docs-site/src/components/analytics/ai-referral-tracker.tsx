'use client';

import { useEffect } from 'react';
import { track } from '@vercel/analytics';

/**
 * Known AI platforms — maps utm_source values and referrer hostnames
 * to a normalized source name for analytics.
 */
const AI_SOURCES: Record<string, string> = {
  // OpenAI — ChatGPT search, browsing, custom GPTs
  'chatgpt.com': 'chatgpt',
  'chat.openai.com': 'chatgpt',
  'search.chatgpt.com': 'chatgpt',
  // Anthropic
  'claude.ai': 'claude',
  // Others
  'perplexity.ai': 'perplexity',
  'copilot.microsoft.com': 'copilot',
  'gemini.google.com': 'gemini',
  'you.com': 'you',
  'phind.com': 'phind',
  'kagi.com': 'kagi',
};

function detectAiSource(): { source: string; method: 'utm' | 'referrer' } | null {
  // 1. Check utm_source query param (highest signal — explicitly tagged)
  const params = new URLSearchParams(window.location.search);
  const utmSource = params.get('utm_source');
  if (utmSource) {
    const normalized = AI_SOURCES[utmSource.toLowerCase()];
    if (normalized) {
      return { source: normalized, method: 'utm' };
    }
    // Even if not in our known list, any utm_source is worth tracking
    // (new AI platforms we haven't mapped yet)
    if (utmSource.includes('ai') || utmSource.includes('chat') || utmSource.includes('llm')) {
      return { source: utmSource.toLowerCase(), method: 'utm' };
    }
  }

  // 2. Check referrer header (catches clicks from AI web UIs)
  try {
    const referrer = document.referrer;
    if (referrer) {
      const hostname = new URL(referrer).hostname.replace(/^www\./, '');
      const normalized = AI_SOURCES[hostname];
      if (normalized) {
        return { source: normalized, method: 'referrer' };
      }
    }
  } catch {
    // Invalid referrer URL, ignore
  }

  return null;
}

/**
 * Tracks AI-referred visits using Vercel Analytics custom events.
 * Fires once per page load — deduped by sessionStorage flag.
 */
export function AiReferralTracker() {
  useEffect(() => {
    // Only fire once per session to avoid double-counting on navigation
    const key = 'ai_referral_tracked';
    if (sessionStorage.getItem(key)) return;

    const detection = detectAiSource();
    if (detection) {
      track('ai_referral', {
        source: detection.source,
        method: detection.method,
        page: window.location.pathname,
      });
      sessionStorage.setItem(key, detection.source);
    }
  }, []);

  return null;
}
