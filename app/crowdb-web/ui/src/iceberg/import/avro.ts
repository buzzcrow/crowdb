// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
/** Bounded, uncompressed Avro OCF writer for Iceberg v2 manifests. */
type Type = string | Type[] | { type: 'record'; name: string; fields: Field[] };
interface Field { name: string; type: Type; 'field-id': number }
const field = (name: string, id: number, type: Type): Field => ({ name, 'field-id': id, type });
export const manifestSchema: Type = { type: 'record', name: 'manifest_entry', fields: [
  field('status', 0, 'int'), field('snapshot_id', 1, ['null', 'long']), field('sequence_number', 3, ['null', 'long']), field('file_sequence_number', 4, ['null', 'long']),
  field('data_file', 2, { type: 'record', name: 'r2', fields: [field('content', 134, 'int'), field('file_path', 100, 'string'), field('file_format', 101, 'string'),
    field('partition', 102, { type: 'record', name: 'r102', fields: [] }), field('record_count', 103, 'long'), field('file_size_in_bytes', 104, 'long')] }),
] };
export const listSchema: Type = { type: 'record', name: 'manifest_file', fields: [
  field('manifest_path', 500, 'string'), field('manifest_length', 501, 'long'), field('partition_spec_id', 502, 'int'), field('added_snapshot_id', 503, 'long'),
  field('content', 517, 'int'), field('sequence_number', 515, 'long'), field('min_sequence_number', 516, 'long'),
  field('added_files_count', 504, 'int'), field('existing_files_count', 505, 'int'), field('deleted_files_count', 506, 'int'),
  field('added_rows_count', 512, 'long'), field('existing_rows_count', 513, 'long'), field('deleted_rows_count', 514, 'long'),
] };
class Encoder {
  bytes: number[] = [];
  raw(value: Uint8Array) { for (const byte of value) this.bytes.push(byte); if (this.bytes.length > 8 * 1024 * 1024) throw new Error('Manifest exceeds the 8 MiB import budget.'); }
  long(value: string | number | bigint) {
    const number = BigInt(value); if (number < -(1n << 63n) || number >= 1n << 63n) throw new Error('Avro integer exceeds i64.');
    let encoded = (number << 1n) ^ (number >> 63n);
    while (encoded > 127n) { this.bytes.push(Number(encoded & 127n) | 128); encoded >>= 7n; }
    this.bytes.push(Number(encoded));
  }
  string(value: string) { const bytes = new TextEncoder().encode(value); this.long(bytes.length); this.raw(bytes); }
  datum(type: Type, value: unknown) {
    if (Array.isArray(type)) { this.long(value === null ? 0 : 1); this.datum(type[value === null ? 0 : 1], value); }
    else if (typeof type === 'object') { for (const field of type.fields) this.datum(field.type, (value as Record<string, unknown>)[field.name]); }
    else if (type === 'string') this.string(String(value));
    else if (type === 'int' || type === 'long') this.long(value as string | number | bigint);
    else if (type !== 'null') throw new Error(`Unsupported manifest type ${type}.`);
  }
}
export function ocf(schema: Type, rows: Record<string, unknown>[], metadata: Record<string, string>, identity: string): Blob {
  if (rows.length > 10000) throw new Error('An import supports at most 10000 manifest entries.');
  const sync = Uint8Array.from(identity.replaceAll('-', '').match(/../g)!.map(value => parseInt(value, 16)));
  if (sync.length !== 16) throw new Error('Invalid manifest identity.');
  const output = new Encoder(); output.raw(Uint8Array.of(79, 98, 106, 1));
  const properties = { 'avro.schema': JSON.stringify(schema), 'avro.codec': 'null', ...metadata };
  output.long(Object.keys(properties).length);
  for (const [name, value] of Object.entries(properties)) { output.string(name); output.string(value); }
  output.long(0); output.raw(sync);
  if (rows.length) {
    const data = new Encoder(); for (const row of rows) data.datum(schema, row);
    output.long(rows.length); output.long(data.bytes.length); output.raw(Uint8Array.from(data.bytes)); output.raw(sync);
  }
  return new Blob([Uint8Array.from(output.bytes)], { type: 'application/octet-stream' });
}
