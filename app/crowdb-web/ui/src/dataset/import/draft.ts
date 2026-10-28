// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import type { DatasetInfo, DraftSplit, Head, Schema, SplitSource } from '../types';
import { defaultPolicy } from '../types';
import { saveOperation, type Operation } from '../operation';
import { newId } from './upload';
import type { LocalSample, Mapping } from './mapping';

/** Freeze identities and mutable pointers when opening, before any local edits. */
export async function openDraft(dataset: string, parent?: string, source?: SplitSource, signal?: AbortSignal): Promise<Operation> {
  if (!(await api.capabilities(signal)).includes('dataset-identity-fence')) throw new Error('Dataset identity protection is unavailable. Upgrade the service before editing.');
  const info = await api.control<DatasetInfo>(api.datasetPath(dataset), 'GET', undefined, signal);
  api.opaqueId(info.id);
  const head = await api.control<Head>(`${api.datasetPath(dataset)}/head`, 'GET', undefined, signal, info.id);
  const base = parent ?? head.latest;
  let schema: Schema = { version: 1, fields: [{ name: 'content', required: false }] };
  if (base) {
    const summary = await api.summary(dataset, base, signal);
    schema = { version: summary.schema_version, fields: [] };
    let after: string | undefined;
    do {
      const page = await api.schemaPage(dataset, base, after, signal);
      schema.fields.push(...page.entries);
      if (schema.fields.length > 64) throw new Error('Snapshot exceeds the 64-field editor limit. Use View for larger Schemas.');
      after = page.after ?? undefined;
    } while (after);
  }
  const revisionId = base ? source?.revision ?? (await api.splitHead(dataset, base, signal)).revision : null;
  const revision = base && revisionId ? await api.revision(dataset, base, revisionId, signal) : undefined;
  const splits: DraftSplit[] = revision?.splits.map(({ name, definition }) => ({ name, definition, origin: { kind: 'inherited', source: { snapshot: base!, revision: revisionId!, name } } })) ?? [];
  const op: Operation = { version: 2, dataset, datasetId: info.id, kind: 'snapshot', resource: newId(), snapshot: base ?? undefined,
    expectedHead: head.generation, editing: true, parentSchema: schema, parentSplits: { splits, policy: revision?.policy ?? defaultPolicy },
    mapping: { version: 1, schema, samples: [] }, source: { parent: base, source: source ? { kind: 'split', source, selection: null } : { kind: 'all' }, schema, splits: [], policy: defaultPolicy } };
  await saveOperation(op); return op;
}

/** Upserts are field patches: changing a payload must keep other pending fields. */
export function patchSample(mapping: Mapping, value: LocalSample): Mapping {
  const existing = mapping.samples.find(sample => sample.id === value.id);
  const merged = existing?.kind === 'upsert' && value.kind === 'upsert'
    ? { ...value, fields: [...existing.fields.filter(field => !value.fields.some(update => update.name === field.name)), ...value.fields] } : value;
  return { ...mapping, samples: [...mapping.samples.filter(sample => sample.id !== value.id), merged] };
}

export function independentRoot(op: Operation, independent: boolean): Operation {
  if (!op.source) throw new Error('The editing source is unavailable.');
  return independent
    ? { ...op, inheritedSource: op.source, source: { ...op.source, parent: null, source: { kind: 'all' }, splits: [] } }
    : { ...op, source: op.inheritedSource ?? op.source, inheritedSource: undefined };
}

export function validateDraft(op: Operation) {
  const mapping = op.mapping;
  if (!mapping?.schema || !op.source) throw new Error('The complete editing source is unavailable.');
  if (op.source.parent) {
    for (const field of op.parentSchema?.fields ?? []) {
      if (field.required && !mapping.schema.fields.some(value => value.name === field.name && value.required)) throw new Error(`Required parent field ${field.name} must remain required.`);
    }
  } else if (!mapping.samples.some(sample => sample.kind === 'upsert')) throw new Error('A Snapshot needs at least one Sample.');
  for (const sample of mapping.samples) {
    if (sample.kind !== 'upsert') continue;
    for (const field of sample.fields) if (!mapping.schema.fields.some(value => value.name === field.name)) throw new Error(`Sample edit uses unknown field ${field.name}.`);
    if (!op.source.parent) for (const field of mapping.schema.fields.filter(value => value.required)) {
      if (!sample.fields.some(value => value.name === field.name)) throw new Error(`Sample is missing required field ${field.name}.`);
    }
  }
}
