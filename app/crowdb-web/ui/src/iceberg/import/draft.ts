// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase } from '../../api';
import type { TableLoad } from '../types';
import type { IceSchema } from './schema';
import type { ImportFile } from './storage';
export interface TableDraft { version: 1; identity: string; namespace: string[]; table?: string; name: string; mode: string; schema: IceSchema; files: ImportFile[]; base?: TableLoad }
const key = (namespace: string[], table?: string) => `crowdb.iceberg.edit:${getApiBase()}:${JSON.stringify([namespace, table ?? null])}`;
export function loadDraft(namespace: string[], table?: string): TableDraft | undefined {
  const text = localStorage.getItem(key(namespace, table)); if (!text) return;
  const draft = JSON.parse(text) as TableDraft;
  if (draft.version !== 1 || JSON.stringify(namespace) !== JSON.stringify(draft.namespace) || table !== draft.table || !Array.isArray(draft.files)) throw new Error('Invalid saved Table editing input.');
  return draft;
}
export function saveDraft(draft: TableDraft) {
  const existing = loadDraft(draft.namespace, draft.table);
  if (existing && existing.identity !== draft.identity) throw new Error('Another Table edit is saved in this scope. Resume it before replacing it.');
  const base = draft.base && { ...draft.base, config: Object.fromEntries(Object.entries(draft.base.config ?? {}).filter(([name]) => ['client.refresh-credentials-endpoint', 'crowdb.native-injection'].includes(name))) };
  const text = JSON.stringify({ ...draft, base });
  if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) throw new Error('Table editing input exceeds 4 MiB.');
  localStorage.setItem(key(draft.namespace, draft.table), text);
}
export function forgetDraft(namespace: string[], table: string | undefined, identity: string) {
  if (loadDraft(namespace, table)?.identity === identity) localStorage.removeItem(key(namespace, table));
}
