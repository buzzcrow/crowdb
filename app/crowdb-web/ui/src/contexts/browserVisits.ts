// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Domain } from '../types';

export interface BrowserVisit { session: string; key: number }
export function writeVisit(visit: BrowserVisit, domain: Domain, push: boolean) {
  const url = new URL(window.location.href);
  url.searchParams.set('domain', domain);
  const state = { ...window.history.state, crowdbVisit: visit };
  if (push) window.history.pushState(state, '', url);
  else window.history.replaceState(state, '', url);
}
export function readVisit(state: unknown): BrowserVisit | undefined {
  const value = (state as { crowdbVisit?: BrowserVisit } | null)?.crowdbVisit;
  return value && typeof value.session === 'string' && Number.isSafeInteger(value.key) ? value : undefined;
}
