// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Domain } from '../types';

export interface BrowserVisit { session: string; key: number }
export function writeVisit(visit: BrowserVisit, domain: Domain, push: boolean, clusterView: 'topology' | 'capacity' = 'topology') {
  const url = new URL(window.location.href);
  url.searchParams.set('domain', domain === Domain.Capacity ? Domain.Cluster : domain);
  if (domain === Domain.Cluster || domain === Domain.Capacity) url.searchParams.set('view', domain === Domain.Capacity ? 'capacity' : clusterView);
  else url.searchParams.delete('view');
  const state = { ...window.history.state, crowdbVisit: visit };
  if (push) window.history.pushState(state, '', url);
  else window.history.replaceState(state, '', url);
}
export function readVisit(state: unknown): BrowserVisit | undefined {
  const value = (state as { crowdbVisit?: BrowserVisit } | null)?.crowdbVisit;
  return value && typeof value.session === 'string' && Number.isSafeInteger(value.key) ? value : undefined;
}
