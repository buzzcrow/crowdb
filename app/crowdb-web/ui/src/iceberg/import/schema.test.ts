// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect, it } from 'vitest';
import { schemaFromParquet, nameMapping, type IceSchema } from './schema';
import { exactJson, requestKey } from './request';
import { commitBody } from './commit';
import type { ImportRequest } from './storage';
const metadata = { schema: [{ name: 'root', num_children: 2 }, { name: 'id', type: 'INT64' as const, repetition_type: 'REQUIRED' as const }, { name: 'text', type: 'BYTE_ARRAY' as const, repetition_type: 'OPTIONAL' as const, converted_type: 'UTF8' as const }] };
it('uses stable Table IDs for files without IDs and rejects conflicting explicit IDs and nullability', () => {
  const expected: IceSchema = { type: 'struct', 'schema-id': 7, fields: [{ name: 'id', id: 12, required: true, type: 'long' }, { name: 'text', id: 19, required: false, type: 'string' }] };
  expect(schemaFromParquet(metadata, expected)).toEqual(expected);
  expect(() => schemaFromParquet({ schema: metadata.schema.map(field => field.name === 'id' ? { ...field, field_id: 1 } : field) }, expected)).toThrow('conflicting field ID');
  expect(() => schemaFromParquet({ schema: metadata.schema.map(field => field.name === 'id' ? { ...field, repetition_type: 'OPTIONAL' } : field) }, expected)).toThrow('required Table field');
  expect(JSON.parse(nameMapping(expected))).toEqual([{ 'field-id': 12, names: ['id'] }, { 'field-id': 19, names: ['text'] }]);
});
it('maps nested standard list fields and detects mismatching nested types', () => {
  const file = { schema: [{ name: 'root', num_children: 1 }, { name: 'items', num_children: 1, converted_type: 'LIST' as const, repetition_type: 'OPTIONAL' as const },
    { name: 'list', num_children: 1, repetition_type: 'REPEATED' as const }, { name: 'element', type: 'INT32' as const, repetition_type: 'OPTIONAL' as const }] };
  const schema = schemaFromParquet(file);
  expect(schema.fields[0].type).toEqual({ type: 'list', 'element-id': 2, element: 'int', 'element-required': false });
  expect(() => schemaFromParquet(file, { ...schema, fields: [{ ...schema.fields[0], type: 'long' }] })).toThrow('list type');
});
it('serializes snapshot identities exactly while retaining numeric-looking property strings', () => {
  expect(exactJson({ snapshot: 9223372036854775807n, property: '9223372036854775807' })).toBe('{"snapshot":9223372036854775807,"property":"9223372036854775807"}');
  expect(requestKey()).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
});
it('append fences the original main ref and emits one Snapshot; empty creation has no data Snapshot', () => {
  const schema = schemaFromParquet(metadata);
  const op: ImportRequest = { version: 1, namespace: ['ns'], name: 'events', create: false, schema, identity: requestKey(), snapshot: '9223372036854775806', list: 's3://table/list.avro', files: [{ name: 'a.parquet', size: 50, md5: '0'.repeat(32), rows: '2', identity: requestKey() }], table: { 'metadata-location': 's3://table/meta.json', metadata: { 'table-uuid': 'uuid', 'current-schema-id': 0, 'default-spec-id': 0, 'last-sequence-number': '9223372036854775805', refs: { main: { type: 'branch', 'snapshot-id': '9223372036854775804' } }, properties: {}, 'partition-specs': [{ 'spec-id': 0, fields: [] }], 'sort-orders': [{ 'order-id': 0, fields: [] }] } } };
  const text = commitBody(op);
  expect(text).toContain('"snapshot-id":9223372036854775804');
  expect(text).toContain('"snapshot-id":9223372036854775806');
  expect(text.match(/"action":"add-snapshot"/g)).toHaveLength(1);
  expect(commitBody({ ...op, create: true, files: [] })).not.toContain('add-snapshot');
});
