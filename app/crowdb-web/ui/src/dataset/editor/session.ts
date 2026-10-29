// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import { openDraft, validateDraft } from '../import/draft';
import { parseMapping, validatePreparation } from '../import/mapping';
import { forgetOperation, prepare, publish, retryIdentical, saveOperation, type Operation } from '../operation';
import type { useDraft } from '../operation/useDraft';
import type { SplitSource } from '../types';
import { checkLimits } from './model';
import { checkUpload } from './uploads';
import { ensureDataset, newDatasetDraft } from './createDataset';

export async function loadSnapshotDraft(dataset: string, parent: string | null | undefined, source: SplitSource | undefined, recovery: Operation | undefined, signal: AbortSignal, creatingDataset = false) {
  const value = recovery ?? await (creatingDataset ? newDatasetDraft(dataset) : openDraft(dataset, parent, source, signal));
  if (!value.creatingDataset || value.datasetId) {
    const info = await api.control<{ id: string }>(api.datasetPath(dataset), 'GET', undefined, signal);
    if (value.datasetId && value.datasetId !== info.id) throw new Error('This Draft belongs to a deleted or replaced Dataset.');
  }
  if (recovery && value.source?.parent) {
    await api.summary(dataset, value.source.parent, signal);
    if (value.source.source.kind === 'split') await api.revision(dataset, value.source.parent, value.source.source.source.revision, signal);
  }
  if (!value.input && value.mapping && value.source && !value.pending && !value.prepared) value.editing = true;
  if (value.editing) checkLimits(value);
  const errors: string[] = value.creationAttempt ? ['Dataset creation outcome is unknown. Retry the same name or choose a different name; an existing Dataset will not be adopted.'] : []; const checked = new Set<string>();
  if (recovery?.editing) for (const sample of value.mapping?.samples ?? []) if (sample.kind === 'upsert') for (const field of sample.fields) {
    if (field.value.kind === 'file') { errors.push(`${field.name}: reselect the file to resume its upload.`); continue; }
    if (field.value.kind !== 'upload' || checked.has(field.value.id)) continue;
    checked.add(field.value.id);
    try { await checkUpload(dataset, value.datasetId, field.value.id, field.value.length, field.value.md5, signal); }
    catch (reason) {
      errors.push(`${field.name}: ${String(reason)}`);
      for (const file of Object.values(value.uploads ?? {})) if (file.id === field.value.id) { file.state = 'failed'; file.error = String(reason); }
    }
  }
  return { value, error: errors.join('\n') };
}

/** Freeze one complete input before any mutation; never recreate it after an unknown reply. */
export async function commitDraft(draft: ReturnType<typeof useDraft>, signal?: AbortSignal) {
  await draft.saving.current;
  if (draft.current.current?.creatingDataset && draft.current.current.editing) {
    checkLimits(draft.current.current); validateDraft(draft.current.current);
    parseMapping(JSON.stringify(draft.current.current.mapping), []);
    await ensureDataset(draft, signal);
  }
  const value = draft.current.current; if (!value) throw new Error('Draft is unavailable.');
  if (value.editing) {
    if (value.kind === 'snapshot') {
      checkLimits(value); validateDraft(value);
      const mapping = parseMapping(JSON.stringify(value.mapping), []); const checked = new Set<string>();
      for (const sample of mapping.samples) if (sample.kind === 'upsert') for (const field of sample.fields) {
        if (field.value.kind === 'upload' && !checked.has(field.value.id)) {
          checked.add(field.value.id); await checkUpload(value.dataset, value.datasetId, field.value.id, field.value.length, field.value.md5, signal);
        }
      }
      value.input = { ...value.source!, schema: mapping.schema, samples: mapping.samples as import('../types').SampleChange[] };
    }
    if (!value.input) throw new Error('Complete input is unavailable.');
    validatePreparation(value.input, value.kind === 'snapshot' ? 8 * 1024 * 1024 : 1024 * 1024);
    delete value.editing; await saveOperation(value); draft.install({ ...value });
  }
  try {
    const retried = value.pending ? await retryIdentical(value, signal) : undefined;
    if (retried) { await forgetOperation(value); return retried; }
    if (!value.prepared) await prepare(value, signal);
    const result = await publish(value); await forgetOperation(value); return result;
  } finally { draft.install({ ...value }); }
}
