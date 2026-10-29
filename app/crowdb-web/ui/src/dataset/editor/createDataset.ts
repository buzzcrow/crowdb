// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import { newId } from '../import/upload';
import { saveOperation, type Operation } from '../operation';
import type { useDraft } from '../operation/useDraft';
import { defaultPolicy, type DatasetInfo } from '../types';

/** Keep the local identity stable while the user edits the Dataset name. */
export async function newDatasetDraft(dataset: string): Promise<Operation> {
  const schema = { version: 1, fields: [{ name: 'content', required: false }] };
  const op: Operation = { version: 2, dataset, creatingDataset: true, kind: 'snapshot', resource: newId(), expectedHead: null, editing: true,
    parentSchema: schema, mapping: { version: 1, schema, samples: [] },
    source: { parent: null, source: { kind: 'all' }, schema, splits: [], policy: defaultPolicy } };
  await saveOperation(op); return op;
}

/** Allocate once before uploading or publishing; never adopt a same-name Dataset. */
export async function ensureDataset(draft: ReturnType<typeof useDraft>, signal?: AbortSignal) {
  await draft.saving.current;
  const op = draft.current.current;
  if (!op?.creatingDataset || op.datasetId) return;
  if (!op.dataset.trim() || op.dataset !== op.dataset.trim() || new TextEncoder().encode(op.dataset).length > 255) throw new Error('Use a Dataset name of 1–255 bytes without surrounding spaces.');
  if (!(await api.capabilities(signal)).includes('dataset-identity-fence')) throw new Error('Dataset identity protection is unavailable. Upgrade the service before editing.');
  op.creationAttempt = op.dataset; await saveOperation(op); draft.install({ ...op });
  try {
    const info = await api.mutate<DatasetInfo>('dataset-management', api.datasetPath(op.dataset), 'PUT', undefined, signal);
    api.opaqueId(info.id);
    if (info.name !== op.dataset || info.namespace !== 'dataset-ns') throw new Error('Created Dataset identity does not match the request.');
    op.datasetId = info.id; delete op.creationAttempt;
    await saveOperation(op);
  } catch (reason) {
    if (!op.datasetId && reason instanceof api.DatasetError && reason.status >= 400 && reason.status < 500) {
      delete op.creationAttempt; await saveOperation(op);
    }
    throw reason;
  } finally { draft.install({ ...op }); }
}
