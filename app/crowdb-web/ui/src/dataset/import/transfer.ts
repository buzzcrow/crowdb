// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import type { CompositionDraft, CompositionSource, DraftSplit, Predicate, SplitPolicy, SplitSource } from '../types';
import { operationPath, request, saveOperation, type Operation } from '../operation';
import { editWindows, resolveSample, type Mapping } from './mapping';
import { Md5 } from './digest';
import { newId, upload } from './upload';

export async function saveImport({ dataset, parent, inherit, mode, predicate, frozenSource, filterSplit, splits, policy, op, mapping, files, signal, setOp, setDraft, setProgress }: {
  dataset: string; parent: string; inherit: boolean; mode: string; predicate: Predicate; frozenSource?: SplitSource; filterSplit: boolean;
  splits: DraftSplit[]; policy: SplitPolicy; op?: Operation; mapping?: Mapping; files: File[]; signal: AbortSignal;
  setOp: (value: Operation) => void; setDraft: (value: CompositionDraft) => void; setProgress: (value: string) => void;
}) {
  if (!mapping) throw new Error('Review the mapping first.');
  const mappingDigest = new Md5().update(new TextEncoder().encode(JSON.stringify(mapping))).finish();
  if (op?.mappingDigest && op.mappingDigest !== mappingDigest) throw new Error('Reselect the original mapping; this operation cannot accept a different import.');
  if (op?.pending) throw new Error('Reconcile the pending request first.');
  const current = await api.head(dataset, signal);
  const parentHead = parent && inherit ? await api.splitHead(dataset, parent, signal) : null;
  const source: CompositionSource = mode === 'replace' ? { kind: 'selection', predicate: { kind: 'explicit', ids: [] } }
    : mode === 'selection' ? { kind: 'selection', predicate }
    : mode === 'split' && frozenSource ? { kind: 'split', source: frozenSource, selection: filterSplit ? predicate : null } : { kind: 'all' };
  const operation: Operation = op ?? { dataset, kind: 'composition', operation: newId(), expectedHead: current.generation, mappingDigest, importsComplete: false };
  saveOperation(operation); setOp(operation);
  let value = op ? await api.control<CompositionDraft>(operationPath(op), 'GET', undefined, signal) : await request<CompositionDraft>(operation, `${api.datasetPath(dataset)}/compositions`, 'POST', {
    operation: operation.operation, parent: parent || null, source, schema: mapping.schema,
    inherit_parent_splits: inherit && !!parent, parent_split_revision: (mode === 'split' ? frozenSource?.revision : parentHead?.revision) ?? null, splits, policy,
  }, signal);
  setDraft(value);
  const uploaded = new Map<string, string>();
  for (const sample of mapping.samples) {
    if (sample.kind !== 'upsert') continue;
    for (const field of sample.fields) {
      if (field.value.kind !== 'file' || uploaded.has(field.value.file)) continue;
      const token = field.value.file; const file = files.find(file => (file.webkitRelativePath || file.name) === token)!;
      uploaded.set(token, await upload(dataset, file, setProgress, signal, (id, key) => {
        operation.fileKeys ??= {};
        if (operation.fileKeys[token] && operation.fileKeys[token] !== key) throw new Error(`Reselected file ${token} has different bytes; choose the original file.`);
        operation.fileKeys[token] = key;
        operation.uploads ??= [];
        if (!operation.uploads.some(value => value.id === id)) operation.uploads.push({ id, key });
        saveOperation(operation);
      }));
    }
  }
  let saved = 0;
  for (const samples of editWindows(mapping.samples.map(sample => resolveSample(sample, uploaded)))) {
    value = await request<CompositionDraft>(operation, operationPath(operation), 'PUT', { edit: newId(), expected_revision: value.revision, samples }, signal);
    saved += samples.length; setDraft(value); setProgress(`Saved ${saved}/${mapping.samples.length} Sample edits`);
  }
  operation.importsComplete = true; saveOperation(operation); setDraft({ ...value });
  setProgress('All reviewed edits are saved. Evaluate complete membership before preparation.');
}
