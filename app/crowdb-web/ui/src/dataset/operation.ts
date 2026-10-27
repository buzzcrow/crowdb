// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { getApiBase } from '../api';
import * as api from './api';
import type { Head, PrepareSnapshot, PreparedSnapshot, PrepareSplit, PublicationResult, SplitHead, SplitRevision } from './types';
import type { Mapping } from './import/mapping';
import { readRequest, removeRequest, writeRequest } from './operation/storage';

type Observation = Head | (SplitHead & { snapshot: string });
type Pending = { kind: 'prepare' } | { kind: 'publish'; expectedHead: string | null; expectedRevision?: string | null };
export interface Operation {
  version: 2; dataset: string; datasetId?: string; kind: 'snapshot' | 'split'; resource: string; snapshot?: string;
  editing?: boolean; parentSchema?: import('./types').Schema;
  parentMode?: 'all' | 'replace' | 'selection';
  fileChecks?: Record<string, { size: number; md5: string }>;
  fileSamples?: Array<{ file: string; sample: string }>;
  expectedHead: string | null; expectedRevision?: string | null;
  source?: Omit<PrepareSnapshot, 'samples'>; mapping?: Mapping;
  inheritedSource?: Omit<PrepareSnapshot, 'samples'>;
  input?: PrepareSnapshot | PrepareSplit; prepared?: PreparedSnapshot | SplitRevision;
  pending?: Pending; conflict?: Observation; fileKeys?: Record<string, string>;
}
const key = (dataset: string) => `${getApiBase()}:${dataset}`;
export async function savedOperation(dataset: string): Promise<Operation | undefined> {
  const value = await readRequest(key(dataset));
  if (!value) return;
  if (value.version !== 2 || value.dataset !== dataset || !['snapshot', 'split'].includes(value.kind)) throw new Error('Invalid saved Dataset request.');
  api.opaqueId(value.resource); if (value.kind === 'split') api.opaqueId(value.snapshot!);
  return value;
}
export async function saveOperation(op: Operation) {
  if (new TextEncoder().encode(JSON.stringify(op)).length > 10 * 1024 * 1024) throw new Error('Saved Dataset request exceeds 10 MiB.');
  await writeRequest(key(op.dataset), op);
}
export const forgetOperation = (op: Operation) => removeRequest(key(op.dataset), op.resource);
export const resourcePath = (op: Operation) => op.kind === 'snapshot' ? api.snapshotPath(op.dataset, op.resource)
  : `${api.snapshotPath(op.dataset, op.snapshot!)}/split-revisions/${api.opaqueId(op.resource)}`;
export const headPath = (op: Operation) => op.kind === 'snapshot' ? `${api.datasetPath(op.dataset)}/head` : `${api.snapshotPath(op.dataset, op.snapshot!)}/split-head`;
export const target = (op: Operation): PublicationResult => ({ target: op.kind === 'snapshot' ? { kind: 'snapshot', snapshot: op.resource }
  : { kind: 'split_revision', snapshot: op.snapshot!, revision: op.resource } });
export async function prepare(op: Operation, signal?: AbortSignal) {
  if (!op.input || op.pending) throw new Error('Keep the complete input and resolve its pending request first.');
  const limit = op.kind === 'snapshot' ? 8 * 1024 * 1024 : 1024 * 1024;
  if (new TextEncoder().encode(JSON.stringify(op.input)).length > limit) throw new Error('Complete Dataset input exceeds its resource limit.');
  op.pending = { kind: 'prepare' }; await retryIdentical(op, signal);
}
export async function publish(op: Operation): Promise<PublicationResult> {
  if (!op.prepared || op.pending || op.conflict) throw new Error('Prepare the result and review any changed head first.');
  op.pending = { kind: 'publish', expectedHead: op.expectedHead, expectedRevision: op.expectedRevision };
  return (await retryIdentical(op))!;
}
export async function retryIdentical(op: Operation, signal?: AbortSignal): Promise<PublicationResult | undefined> {
  const pending = op.pending; if (!pending) throw new Error('No pending request.');
  await saveOperation(op);
  const preparing = pending.kind === 'prepare';
  const body = preparing ? op.input : op.kind === 'snapshot'
    ? { snapshot: op.resource, expected_head: pending.expectedHead }
    : { revision: op.resource, expected_revision: pending.expectedRevision, expected_head: pending.expectedHead };
  const capability = op.kind === 'snapshot' ? (preparing ? 'composition-preparation' : 'snapshot-head-update')
    : (preparing ? 'split-preparation' : 'split-head-update');
  try {
    const result = await api.mutate<PreparedSnapshot | SplitRevision | Head | SplitHead>(capability, preparing ? resourcePath(op) : headPath(op), 'PUT', body, signal, op.datasetId);
    if (preparing) {
      if (op.kind === 'snapshot' ? !('sample_count' in result) || result.snapshot !== op.resource
        : !('revision' in result) || result.revision !== op.resource || !('snapshot' in result) || result.snapshot !== op.snapshot) throw new api.DatasetError(502, 'Prepared resource identity does not match the request.', true);
      op.prepared = result as PreparedSnapshot | SplitRevision;
    } else if (op.kind === 'snapshot' ? !('latest' in result) || result.latest !== op.resource
      : !('revision' in result) || result.revision !== op.resource) throw new api.DatasetError(502, 'Published pointer does not match the request.', true);
    delete op.pending; delete op.conflict; await saveOperation(op);
    return preparing ? undefined : target(op);
  } catch (reason) {
    if (reason instanceof api.DatasetError && reason.status >= 400 && reason.status < 500) {
      if (!preparing && reason.status === 409) op.conflict = observation(op, reason.observedHead);
      delete op.pending; await saveOperation(op);
    }
    throw reason;
  }
}
export async function useObservedHead(op: Operation) {
  if (!op.conflict || op.pending) throw new Error('A definite conflict observation is required.');
  op.expectedHead = op.conflict.generation;
  if (op.kind === 'split' && 'revision' in op.conflict) op.expectedRevision = op.conflict.revision;
  delete op.conflict; await saveOperation(op);
}
function observation(op: Operation, value: unknown): Observation | undefined {
  if (!value || typeof value !== 'object') return;
  const head = value as Record<string, unknown>;
  const id = (value: unknown, length: number) => value === null || typeof value === 'string' && new RegExp(`^[0-9a-f]{${length}}$`).test(value);
  if (!id(head.generation, 64)) return;
  if (op.kind === 'snapshot' && id(head.latest, 32) && id(head.stable, 32)) return head as unknown as Head;
  if (op.kind === 'split' && head.snapshot === op.snapshot && id(head.revision, 32)) return head as unknown as SplitHead & { snapshot: string };
}
