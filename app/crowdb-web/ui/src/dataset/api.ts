// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { getApiBase } from '../api';
import type { Page, DatasetInfo, Head, SnapshotSummary, FieldDefinition, MatrixRow, Preview, SplitHead, SplitRevision, RevisionSummary } from './types';

export const controlPath = '/access/dataset/api/dataset/v2';
export const namespacePath = '/namespaces/dataset-ns/datasets';
export const datasetPath = (name: string) => `${namespacePath}/${encodeURIComponent(name)}`;
export const snapshotPath = (name: string, snapshot: string) => `${datasetPath(name)}/snapshots/${opaqueId(snapshot)}`;
export function opaqueId(value: string): string {
  if (!/^[0-9a-f]{32}$/.test(value) || /^0+$/.test(value)) throw new Error('Expected a nonzero 32-character Snapshot or operation ID.');
  return value;
}
export function decimal(value: string): bigint {
  if (!/^(0|[1-9][0-9]*)$/.test(value) || BigInt(value) > 18446744073709551615n) throw new Error('Invalid exact unsigned integer.');
  return BigInt(value);
}
export class DatasetError extends Error {
  constructor(readonly status: number, message: string, readonly unknownOutcome = false) { super(message); }
}
async function readBounded(response: Response): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) throw new DatasetError(response.status, 'Dataset returned an empty response.');
  let count = 0; let text = ''; const decoder = new TextDecoder('utf-8', { fatal: true });
  try {
    for (;;) {
      const chunk = await reader.read(); if (chunk.done) break;
      count += chunk.value.length;
      if (count > 1024 * 1024) throw new DatasetError(502, 'Dataset metadata exceeds the 1 MiB limit.');
      text += decoder.decode(chunk.value, { stream: true });
    }
    text += decoder.decode();
  } finally { await reader.cancel(); }
  let result: unknown;
  try { result = JSON.parse(text); } catch { throw new DatasetError(response.status || 502, 'Dataset returned an invalid response.'); }
  if (!response.ok) {
    const error = result as { error?: string | { message?: string; code?: string } };
    const message = typeof error.error === 'string' ? error.error : error.error?.message;
    throw new DatasetError(response.status, message ?? `Dataset HTTP ${response.status}`, response.status >= 500);
  }
  return result;
}
export async function control<T>(path: string, method = 'GET', body?: unknown, signal?: AbortSignal): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${getApiBase()}${controlPath}${path}`, {
      method, signal, cache: 'no-store',
      ...(body === undefined ? {} : { body: body instanceof Blob ? body : JSON.stringify(body),
        headers: { 'Content-Type': body instanceof Blob ? 'application/octet-stream' : 'application/json' } }),
    });
  } catch (reason) {
    if (signal?.aborted) throw reason;
    throw new DatasetError(0, 'Dataset service is unavailable. Reconcile any pending operation before retrying.', method !== 'GET');
  }
  return await readBounded(response) as T;
}
export async function capabilities(signal?: AbortSignal): Promise<string[]> {
  const result = await control<{ api_version: string | number; capabilities: string[] }>('/capabilities', 'GET', undefined, signal);
  if (String(result.api_version) !== '2' || !Array.isArray(result.capabilities) || !result.capabilities.every(value => typeof value === 'string')) {
    throw new DatasetError(501, 'This Dataset service does not support control API v2.');
  }
  return result.capabilities;
}
export async function mutate<T>(capability: string, path: string, method: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  if (!(await capabilities(signal)).includes(capability)) throw new DatasetError(501, `Dataset capability unavailable: ${capability}`);
  return control<T>(path, method, body, signal);
}
export async function page<T>(path: string, after?: string, limit = 20, signal?: AbortSignal): Promise<Page<T>> {
  if (limit < 1 || limit > 100 || !Number.isInteger(limit) || (after && after.length > 43692)) throw new Error('Invalid Dataset page request.');
  const query = new URLSearchParams({ limit: String(limit) }); if (after) query.set('after', after);
  const result = await control<Page<T>>(`${path}?${query}`, 'GET', undefined, signal);
  if (!Array.isArray(result.entries) || result.entries.length > limit || !(result.after === null || typeof result.after === 'string')
      || result.after !== null && (!result.after || result.after.length > 43692 || result.after === after)) {
    throw new DatasetError(502, 'Invalid Dataset page or continuation. Refresh this scope.');
  }
  return result;
}
export const listDatasets = (after?: string, signal?: AbortSignal) => page<DatasetInfo>(namespacePath, after, 20, signal);
export const head = (name: string, signal?: AbortSignal) => control<Head>(`${datasetPath(name)}/head`, 'GET', undefined, signal);
export const summary = (name: string, snapshot: string, signal?: AbortSignal) => control<SnapshotSummary>(`${snapshotPath(name, snapshot)}/summary`, 'GET', undefined, signal);
export const schemaPage = (name: string, snapshot: string, after?: string, signal?: AbortSignal) => page<FieldDefinition>(`${snapshotPath(name, snapshot)}/schema`, after, 12, signal);
export const samplePage = (name: string, snapshot: string, after?: string, revision?: string, split?: string, signal?: AbortSignal) => page<string>(
  `${snapshotPath(name, snapshot)}${revision && split !== undefined ? `/split-revisions/${opaqueId(revision)}/splits/${encodeURIComponent(split)}/members` : '/samples'}`, after, 20, signal);
export async function matrix(name: string, snapshot: string, samples: string[], fields: string[], signal?: AbortSignal): Promise<MatrixRow[]> {
  if (samples.length > 20 || fields.length > 12) throw new Error('Matrix window exceeds 20 Samples × 12 fields.');
  const result = await control<{ entries: MatrixRow[] }>(`${snapshotPath(name, snapshot)}/matrix`, 'POST', { sample_ids: samples, fields }, signal);
  if (!Array.isArray(result.entries) || result.entries.length !== samples.length || result.entries.some((row, index) => row.id !== samples[index] || row.fields.length !== fields.length)) {
    throw new DatasetError(502, 'Dataset matrix identities do not match the requested window.');
  }
  return result.entries;
}
export const preview = (name: string, snapshot: string, sample: string, field: string, signal?: AbortSignal) => control<Preview>(
  `${snapshotPath(name, snapshot)}/preview?${new URLSearchParams({ sample, field })}`, 'GET', undefined, signal);
export const splitHead = (name: string, snapshot: string, signal?: AbortSignal) => control<SplitHead>(`${snapshotPath(name, snapshot)}/split-head`, 'GET', undefined, signal);
export const revision = (name: string, snapshot: string, id: string, signal?: AbortSignal) => control<SplitRevision>(`${snapshotPath(name, snapshot)}/split-revisions/${opaqueId(id)}`, 'GET', undefined, signal);
export const history = (name: string, snapshot: string, after?: string, signal?: AbortSignal) => page<RevisionSummary>(`${snapshotPath(name, snapshot)}/split-revisions`, after, 20, signal);
