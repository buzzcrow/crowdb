// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase } from '../../api';
import type { TableLoad } from '../types';
import type { IceSchema } from './schema';
import type { ImportFile } from './storage';
export interface TableDraft { version: 1; identity: string; namespace: string[]; table?: string; name: string; mode: string; schema: IceSchema; files: ImportFile[]; base?: TableLoad;
  revision?: number; source?: string | null; requestedSource?: string; removed?: Array<{ manifest: string; location: string }>;
}
const key = (namespace: string[], table?: string, source?: string) => `crowdb.iceberg.edit:${getApiBase()}:${JSON.stringify([namespace, table ?? null])}${source ? `:${source}` : ''}`;
export function loadDraft(namespace: string[], table?: string, source?: string): TableDraft | undefined {
  const text = localStorage.getItem(key(namespace, table, source)); if (!text) return;
  const draft = JSON.parse(text) as TableDraft;
  if (draft.version !== 1 || JSON.stringify(namespace) !== JSON.stringify(draft.namespace) || table !== draft.table || !Array.isArray(draft.files)) throw new Error('Invalid saved Table editing input.');
  return draft;
}
export function saveDraft(draft: TableDraft) {
  const existing = loadDraft(draft.namespace, draft.table, draft.requestedSource);
  if (existing && existing.identity !== draft.identity) throw new Error('Another Table edit is saved in this scope. Resume it before replacing it.');
  if ((existing?.revision ?? 0) !== (draft.revision ?? 0)) throw new Error('This Draft changed in another tab. Reopen it before editing.');
  const base = draft.base && { ...draft.base, config: Object.fromEntries(Object.entries(draft.base.config ?? {}).filter(([name]) => ['client.refresh-credentials-endpoint', 'crowdb.native-injection'].includes(name))) };
  const revision = (draft.revision ?? 0) + 1;
  const text = JSON.stringify({ ...draft, base, revision });
  if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) throw new Error('Table editing input exceeds 4 MiB.');
  localStorage.setItem(key(draft.namespace, draft.table, draft.requestedSource), text);
  draft.revision = revision;
}
export function forgetDraft(namespace: string[], table: string | undefined, identity: string, source?: string, revision?: number) {
  const existing = loadDraft(namespace, table, source);
  if (existing?.identity !== identity) return;
  if (revision !== undefined && (existing.revision ?? 0) !== revision) throw new Error('This Draft changed in another tab. Reopen it before discarding.');
  localStorage.removeItem(key(namespace, table, source));
}
