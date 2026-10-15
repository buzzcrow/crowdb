// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { getApiBase } from '../api';
import * as api from './api';
import type { CompositionDraft, Receipt, SplitDraft } from './types';
import { newId } from './import/upload';
export interface Operation {
  dataset: string; kind: 'composition' | 'split'; operation: string; snapshot?: string;
  conflict?: boolean;
  mappingDigest?: string; importsComplete?: boolean;
  expectedHead: string | null; attempt?: string; pending?: { path: string; method: string; body?: unknown };
}
const key = (dataset: string) => `crowdb.dataset.operation:${getApiBase()}:${dataset}`;
export function savedOperation(dataset: string): Operation | null {
  const text = localStorage.getItem(key(dataset));
  if (!text) return null;
  if (text.length > 1024 * 1024) throw new Error('Saved draft exceeds recovery bound.');
  const value = JSON.parse(text) as Operation;
  if (value.dataset !== dataset || !['composition', 'split'].includes(value.kind)) throw new Error('Invalid saved draft.');
  api.opaqueId(value.operation); return value;
}
export function saveOperation(value: Operation) {
  const text = JSON.stringify(value);
  if (new TextEncoder().encode(text).length > 1024 * 1024) throw new Error('Draft request exceeds recovery bound.');
  localStorage.setItem(key(value.dataset), text);
}
export const operationPath = (op: Operation) => `${api.datasetPath(op.dataset)}/${op.kind === 'composition' ? 'compositions' : 'split-drafts'}/${api.opaqueId(op.operation)}`;
export async function request<T>(op: Operation, path: string, method: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  if (op.pending) throw new Error('A previous request needs reconciliation before another change.');
  op.pending = { path, method, body }; saveOperation(op);
  try {
    const result = await api.mutate<T>(op.kind === 'composition' ? 'composition' : 'split-revisions', path, method, body, signal);
    delete op.pending; delete op.conflict; saveOperation(op); return result;
  } catch (reason) {
    if (reason instanceof api.DatasetError && reason.status >= 400 && reason.status < 500) {
      if (reason.status === 409 && path.endsWith('/publish')) op.conflict = true;
      else delete op.pending;
      saveOperation(op);
    }
    throw reason;
  }
}
export async function reconcile(op: Operation): Promise<{ draft: CompositionDraft | SplitDraft; receipt: Receipt | null }> {
  const receipt = op.attempt ? await api.control<{ receipt: Receipt | null }>(`${api.datasetPath(op.dataset)}/commits/${op.attempt}`) : { receipt: null };
  const draft = await api.control<CompositionDraft | SplitDraft>(operationPath(op));
  if (receipt.receipt || !op.pending) { delete op.pending; saveOperation(op); }
  return { draft, receipt: receipt.receipt };
}
export async function retryIdentical<T>(op: Operation): Promise<T> {
  if (!op.pending) throw new Error('No pending request.');
  const { path, method, body } = op.pending;
  const result = await api.mutate<T>(op.kind === 'composition' ? 'composition' : 'split-revisions', path, method, body);
  delete op.pending; saveOperation(op); return result;
}
export function newAttempt(op: Operation, observed: string | null) {
  if (!op.conflict) throw new Error('A new attempt requires a confirmed head conflict.');
  delete op.conflict; op.expectedHead = observed; op.attempt = newId(); delete op.pending; saveOperation(op);
}
export function forgetOperation(op: Operation) { localStorage.removeItem(key(op.dataset)); }
