// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Operation } from '../operation';
import type { LocalValue } from '../import/mapping';
import { patchSample } from '../import/draft';
import { encoded } from '../identity';
import * as api from '../api';
/** New columns do not exist in the parent; fetch only inherited definitions. */
export async function parentRows(op: Operation, ids: string[], fields: string[], signal?: AbortSignal) {
  const inherited = fields.filter(name => op.parentSchema?.fields.some(field => field.name === name));
  const rows = inherited.length ? await api.matrix(op.dataset, op.source!.parent!, ids, inherited, signal) : ids.map(id => ({ id, fields: [] }));
  return rows.map(row => ({ ...row, fields: fields.map(name => row.fields[inherited.indexOf(name)] ?? null) }));
}
export const limits = { edits: 1000, fields: 64, fileBytes: 100 * 1024 * 1024, batch: 20, inputBytes: 4 * 1024 * 1024 };
export function checkLimits(op: Operation) {
  if ((op.mapping?.samples.length ?? 0) > limits.edits) throw new Error(`The editor supports ${limits.edits} Sample changes per Draft.`);
  if ((op.mapping?.schema?.fields.length ?? 0) > limits.fields) throw new Error(`The editor supports ${limits.fields} Fields. Larger Snapshots remain available in View.`);
  if (new TextEncoder().encode(JSON.stringify(op)).length > limits.inputBytes) throw new Error('Draft metadata exceeds 4 MiB. Use file values for large content.');
}
export function setCell(op: Operation, id: string, field: string, value: LocalValue): Operation {
  const next = { ...op, mapping: patchSample(op.mapping!, { kind: 'upsert', id, fields: [{ name: field, value }] }) };
  checkLimits(next); return next;
}
export function cell(op: Operation, id: string, field: string) {
  const sample = op.mapping?.samples.find(value => value.id === id);
  return sample?.kind === 'upsert' ? sample.fields.find(value => value.name === field)?.value : undefined;
}
export function uploadMetadata(op: Operation, value?: LocalValue) {
  if (value?.kind === 'file') return op.uploads?.[value.file];
  // Reselecting a file can resume the same upload under a newer local attempt.
  return value?.kind === 'upload' ? Object.values(op.uploads ?? {}).reverse().find(file => file.id === value.id && file.md5 === value.md5 && String(file.size) === value.length) : undefined;
}
export const textValue = (text: string): LocalValue => {
  const value = new TextEncoder().encode(text);
  if (value.length > 65536) throw new Error('Text values are limited to 64 KiB. Upload a file for larger content.');
  return { kind: 'inline', data: encoded(value) };
};
export function displayValue(value?: LocalValue): string {
  if (!value) return 'Missing';
  if (value.kind === 'inline') {
    try { return new TextDecoder('utf-8', { fatal: true }).decode(Uint8Array.from(atob(value.data), c => c.charCodeAt(0))) || '(empty text)'; }
    catch { return `Binary text · ${atob(value.data).length} B`; }
  }
  if (value.kind === 'tombstone') return 'Cleared';
  return value.kind === 'upload' ? `File · ${value.length} B` : 'File pending';
}
