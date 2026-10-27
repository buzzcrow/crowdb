// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase } from '../../api';
import type { TableLoad } from '../types';
import type { IceSchema } from './schema';
export interface ImportFile { name: string; size: number; md5: string; rows: string; identity: string; location?: string; multipart?: { id: string; parts: Array<{ number: number; etag: string }> } }
export interface ImportRequest {
  version: 1; namespace: string[]; name: string; create: boolean; schema: IceSchema; files: ImportFile[];
  identity: string; snapshot: string; table?: TableLoad; manifest?: string; list?: string; commit?: string; uncertain?: boolean;
  totals?: { rows: string; files: string; bytes?: string };
}
const key = (namespace: string[], name: string) => `crowdb.iceberg.import:${getApiBase()}:${JSON.stringify(namespace)}:${name}`;
export function load(namespace: string[], name: string): ImportRequest | undefined {
  const text = localStorage.getItem(key(namespace, name)); if (!text) return;
  const value = JSON.parse(text) as ImportRequest;
  if (value.version !== 1 || value.name !== name || JSON.stringify(value.namespace) !== JSON.stringify(namespace) || !Array.isArray(value.files)) throw new Error('Invalid saved Table import.');
  return value;
}
export function save(value: ImportRequest) {
  const table = value.table && { ...value.table, config: Object.fromEntries(Object.entries(value.table.config ?? {}).filter(([name]) => ['client.refresh-credentials-endpoint', 'crowdb.native-injection'].includes(name))) };
  const text = JSON.stringify({ ...value, table });
  if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) throw new Error('Saved Table import exceeds 4 MiB.');
  const existing = load(value.namespace, value.name);
  if (existing && existing.identity !== value.identity) throw new Error('Resume or discard the saved import for this Table first.');
  localStorage.setItem(key(value.namespace, value.name), text);
}
export function forget(value: ImportRequest) { if (load(value.namespace, value.name)?.identity === value.identity) localStorage.removeItem(key(value.namespace, value.name)); }
