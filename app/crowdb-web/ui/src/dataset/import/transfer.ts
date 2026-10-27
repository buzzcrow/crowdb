// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import type { CompositionSource, DraftSplit, Predicate, SplitPolicy, SplitSource, UploadDescriptor } from '../types';
import { prepare, saveOperation, type Operation } from '../operation';
import { resolveSample, validatePreparation, type Mapping } from './mapping';
import { newId, upload } from './upload';

export async function saveImport({ dataset, parent, inherit, mode, predicate, frozenSource, filterSplit, splits, policy, op, mapping, files, signal, setOp, setProgress }: {
  dataset: string; parent: string; inherit: boolean; mode: string; predicate: Predicate; frozenSource?: SplitSource; filterSplit: boolean;
  splits: DraftSplit[]; policy: SplitPolicy; op?: Operation; mapping?: Mapping; files: File[]; signal: AbortSignal;
  setOp: (value: Operation) => void; setProgress: (value: string) => void;
}) {
  if (!mapping) throw new Error('Review the mapping first.');
  if (op?.pending) throw new Error('Retry the saved pending request first.');
  if (op?.mapping && JSON.stringify(op.mapping) !== JSON.stringify(mapping)) throw new Error('Reselect the original mapping for this saved request.');
  let operation = op;
  if (!operation) {
    const current = await api.head(dataset, signal);
    const source: CompositionSource = !parent || mode === 'all' ? { kind: 'all' }
      : mode === 'replace' ? { kind: 'selection', predicate: { kind: 'explicit', ids: [] } }
      : mode === 'selection' ? { kind: 'selection', predicate }
      : frozenSource ? { kind: 'split', source: frozenSource, selection: filterSplit ? predicate : null } : { kind: 'all' };
    const inherited = await frozenSplits(dataset, parent, inherit, mode === 'split' ? frozenSource : undefined, splits, signal);
    operation = { version: 2, dataset, kind: 'snapshot', resource: newId(), expectedHead: current.generation,
      mapping, source: { parent: parent || null, source, schema: mapping.schema, splits: inherited, policy } };
    const placeholders = new Map(files.map(file => [file.webkitRelativePath || file.name,
      { id: '1'.repeat(32), length: String(file.size), md5: '0'.repeat(32) }]));
    validatePreparation({ ...operation.source, samples: mapping.samples.map(sample => resolveSample(sample, placeholders)) });
    await saveOperation(operation);
  }
  setOp(operation);
  try {
    if (!operation.input) {
      const uploaded = new Map<string, UploadDescriptor>();
      for (const sample of mapping.samples) {
        if (sample.kind !== 'upsert') continue;
        for (const field of sample.fields) {
          if (field.value.kind !== 'file' || uploaded.has(field.value.file)) continue;
          const token = field.value.file;
          const file = files.find(file => (file.webkitRelativePath || file.name) === token);
          if (!file) throw new Error(`Reselect the original local file ${token}.`);
          uploaded.set(token, await upload(dataset, file, setProgress, signal, async (_id, key) => {
            const check = operation.fileChecks?.[token];
            if (check && !key.endsWith(`:${check.size}:${check.md5}`)) throw new Error(`Reselected file ${token} has different bytes.`);
            operation.fileKeys ??= {};
            if (operation.fileKeys[token] && operation.fileKeys[token] !== key) throw new Error(`Reselected file ${token} has different bytes.`);
            operation.fileKeys[token] = key; await saveOperation(operation);
          }, operation.datasetId));
        }
      }
      if (!operation.source) throw new Error('Saved input source is unavailable.');
      operation.input = { ...operation.source, samples: mapping.samples.map(sample => resolveSample(sample, uploaded)) };
      delete operation.source; delete operation.mapping; await saveOperation(operation);
    }
    setProgress('Preparing the complete Snapshot…');
    await prepare(operation, signal);
    setProgress('Complete Snapshot ready. Review it before publishing.');
  } finally { setOp({ ...operation }); }
}

export async function frozenSplits(dataset: string, parent: string, inherit: boolean, source: SplitSource | undefined, explicit: DraftSplit[], signal: AbortSignal): Promise<DraftSplit[]> {
  if (!parent || !inherit) return explicit;
  const revision = source?.revision ?? (await api.splitHead(dataset, parent, signal)).revision;
  if (!revision) return explicit;
  const frozen = await api.revision(dataset, parent, revision, signal);
  const definitions = new Map<string, DraftSplit>(frozen.splits.map(split => [split.name, { name: split.name, definition: split.definition,
    origin: { kind: 'inherited', source: { snapshot: parent, revision, name: split.name } } }]));
  for (const split of explicit) definitions.set(split.name, split);
  return [...definitions.values()];
}
