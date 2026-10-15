// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { bytes, encoded } from '../identity';
import type { SampleChange, Schema } from '../types';
export type LocalValue = { kind: 'inline'; data: string } | { kind: 'file'; file: string } | { kind: 'tombstone' };
export type LocalSample = { kind: 'remove'; id: string } | { kind: 'upsert'; id: string; fields: Array<{ name: string; value: LocalValue }> };
export interface Mapping { version: 1; schema: Schema | null; samples: LocalSample[] }
export function parseMapping(text: string, files: File[]): Mapping {
  if (new TextEncoder().encode(text).length > 8 * 1024 * 1024) throw new Error('Mapping exceeds 8 MiB.');
  const value = JSON.parse(text) as Mapping;
  if (value.version !== 1 || !Array.isArray(value.samples) || value.samples.length > 10000) throw new Error('Expected mapping version 1, at most 10000 Sample edits.');
  if (value.schema !== null && (!Number.isInteger(value.schema?.version) || value.schema.version < 1 || !Array.isArray(value.schema.fields) || value.schema.fields.length > 256)) throw new Error('Invalid Schema.');
  const fieldNames = new Set<string>();
  for (const field of value.schema?.fields ?? []) {
    if (!field.name || new TextEncoder().encode(field.name).length > 1024 || typeof field.required !== 'boolean' || fieldNames.has(field.name)) throw new Error('Invalid or duplicate Schema field.');
    fieldNames.add(field.name);
  }
  const ids = new Set<string>();
  for (const sample of value.samples) {
    bytes(sample.id); if (ids.has(sample.id)) throw new Error('Duplicate Sample ID.'); ids.add(sample.id);
    if (sample.kind === 'remove') continue;
    if (sample.kind !== 'upsert' || !Array.isArray(sample.fields) || sample.fields.length > 256) throw new Error('Invalid Sample edit.');
    const names = new Set<string>();
    for (const field of sample.fields) {
      if (!field.name || names.has(field.name)) throw new Error('Duplicate or empty field name.'); names.add(field.name);
      if (field.value.kind === 'inline') {
        const data = Uint8Array.from(atob(field.value.data), c => c.charCodeAt(0));
        if (data.length > 65536 || encoded(data) !== field.value.data) throw new Error('Inline field must be canonical base64, at most 64 KiB.');
      } else if (field.value.kind === 'file') {
        const token = field.value.file;
        if (files.filter(file => (file.webkitRelativePath || file.name) === token).length !== 1) throw new Error(`Select exactly one local file matching ${field.value.file}.`);
      } else if (field.value.kind !== 'tombstone') throw new Error('Unsupported field source.');
    }
  }
  return value;
}
export function resolveSample(sample: LocalSample, uploaded: Map<string, string>): SampleChange {
  if (sample.kind === 'remove') return sample;
  return { ...sample, fields: sample.fields.map(field => {
    if (field.value.kind !== 'file') return { name: field.name, value: field.value };
    const id = uploaded.get(field.value.file); if (!id) throw new Error('File upload is not complete.');
    return { name: field.name, value: { kind: 'upload', id } };
  }) };
}
