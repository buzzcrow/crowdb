// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { parquetMetadataAsync, type SchemaElement, type FileMetaData } from 'hyparquet';
export type IceType = string | { type: 'struct'; fields: IceField[] } | { type: 'list'; 'element-id': number; element: IceType; 'element-required': boolean }
  | { type: 'map'; 'key-id': number; key: IceType; 'value-id': number; value: IceType; 'value-required': boolean };
export interface IceField { id: number; name: string; required: boolean; type: IceType }
export interface IceSchema { type: 'struct'; 'schema-id': number; fields: IceField[] }
interface Node { element: SchemaElement; children: Node[] }

export async function footer(file: File): Promise<FileMetaData> {
  if (file.size < 12) throw new Error(`${file.name}: invalid Parquet size.`);
  const last = new DataView(await file.slice(-8).arrayBuffer());
  if (last.getUint32(4, true) !== 0x31524150 || last.getUint32(0, true) > 1024 * 1024) throw new Error(`${file.name}: invalid Parquet magic or footer exceeds 1 MiB.`);
  const metadata = await parquetMetadataAsync({ byteLength: file.size, slice: (start, end) => file.slice(start, end).arrayBuffer() }, { geoparquet: false });
  if (metadata.schema.length > 4096 || metadata.row_groups.length > 100000 || metadata.num_rows < 0n) throw new Error(`${file.name}: Parquet metadata exceeds import limits.`);
  return metadata;
}

function primitive(value: SchemaElement): string {
  const logical = value.logical_type; const converted = value.converted_type;
  if (logical?.type === 'DECIMAL' || converted === 'DECIMAL') {
    const precision = logical?.type === 'DECIMAL' ? logical.precision : value.precision;
    const scale = logical?.type === 'DECIMAL' ? logical.scale : value.scale;
    if (!precision || precision > 38 || scale === undefined || scale < 0 || scale > precision) throw new Error(`${value.name}: unsupported decimal.`);
    return `decimal(${precision},${scale})`;
  }
  if (logical?.type === 'STRING' || converted === 'UTF8') return 'string';
  if (logical?.type === 'DATE' || converted === 'DATE') return 'date';
  if (logical?.type === 'UUID') return 'uuid';
  if (logical?.type === 'TIME') { if (logical.unit !== 'MICROS') throw new Error(`${value.name}: time must use microseconds.`); return 'time'; }
  if (logical?.type === 'TIMESTAMP') { if (logical.unit !== 'MICROS') throw new Error(`${value.name}: timestamp must use microseconds.`); return logical.isAdjustedToUTC ? 'timestamptz' : 'timestamp'; }
  if (logical?.type === 'INTEGER') { if (!logical.isSigned) throw new Error(`${value.name}: unsigned integer is unsupported.`); return logical.bitWidth <= 32 ? 'int' : 'long'; }
  if (converted === 'TIME_MICROS') return 'time';
  if (converted === 'TIMESTAMP_MICROS') return 'timestamp';
  if (logical || converted && !['INT_8', 'INT_16', 'INT_32', 'INT_64'].includes(converted)) throw new Error(`${value.name}: unsupported logical type ${logical?.type ?? converted}.`);
  const result = ({ BOOLEAN: 'boolean', INT32: 'int', INT64: 'long', FLOAT: 'float', DOUBLE: 'double', BYTE_ARRAY: 'binary', FIXED_LEN_BYTE_ARRAY: `fixed[${value.type_length}]` } as Record<string, string>)[value.type ?? 'INT96'];
  if (!result) throw new Error(`${value.name}: unsupported physical type ${value.type}.`);
  return result;
}

export function schemaFromParquet(metadata: Pick<FileMetaData, 'schema'>, expected?: IceSchema): IceSchema {
  let offset = 0; let generated = 1;
  const used = new Set(metadata.schema.flatMap(value => value.field_id === undefined ? [] : [value.field_id]));
  const claimed = new Set<number>();
  const read = (depth: number): Node => {
    const element = metadata.schema[offset++];
    if (!element || depth > 32 || (element.num_children ?? 0) > 4096) throw new Error('Invalid or excessively nested Parquet schema.');
    return { element, children: Array.from({ length: element.num_children ?? 0 }, () => read(depth + 1)) };
  };
  const root = read(0); if (offset !== metadata.schema.length) throw new Error('Invalid Parquet schema hierarchy.');
  const id = (node: Node, expected?: number): number => {
    while (used.has(generated) || claimed.has(generated)) generated++;
    const value = node.element.field_id ?? expected ?? generated++;
    if (!Number.isInteger(value) || value < 1 || value > 2147483647 || claimed.has(value) || expected !== undefined && value !== expected) throw new Error(`${node.element.name}: conflicting field ID.`);
    claimed.add(value); return value;
  };
  const type = (node: Node, expected?: IceType): IceType => {
    const element = node.element; const kind = element.logical_type?.type ?? element.converted_type;
    if (kind === 'LIST') {
      if (expected && (typeof expected === 'string' || expected.type !== 'list')) throw new Error(`${element.name}: list type does not match the Table field.`);
      const wrapper = node.children[0];
      if (node.children.length !== 1 || wrapper.element.repetition_type !== 'REPEATED' || wrapper.children.length !== 1) throw new Error(`${element.name}: only standard three-level LIST encoding is supported.`);
      const child = wrapper.children[0]; const base = typeof expected === 'object' && expected.type === 'list' ? expected : undefined;
      const result: IceType = { type: 'list', 'element-id': id(child, base?.['element-id']), element: type(child, base?.element), 'element-required': child.element.repetition_type === 'REQUIRED' };
      if (base?.['element-required'] && !result['element-required']) throw new Error(`${element.name}: optional list element cannot fill a required element.`);
      return result;
    }
    if (kind === 'MAP') {
      if (expected && (typeof expected === 'string' || expected.type !== 'map')) throw new Error(`${element.name}: map type does not match the Table field.`);
      const wrapper = node.children[0];
      if (node.children.length !== 1 || wrapper.children.length !== 2 || wrapper.element.repetition_type !== 'REPEATED' || wrapper.children[0].element.repetition_type !== 'REQUIRED') throw new Error(`${element.name}: unsupported MAP encoding.`);
      const [key, value] = wrapper.children; const base = typeof expected === 'object' && expected.type === 'map' ? expected : undefined;
      if (base?.['value-required'] && value.element.repetition_type !== 'REQUIRED') throw new Error(`${element.name}: optional map value cannot fill a required value.`);
      return { type: 'map', 'key-id': id(key, base?.['key-id']), key: type(key, base?.key), 'value-id': id(value, base?.['value-id']), value: type(value, base?.value), 'value-required': value.element.repetition_type === 'REQUIRED' };
    }
    if (node.children.length) {
      if (expected && (typeof expected === 'string' || expected.type !== 'struct')) throw new Error(`${element.name}: struct type does not match the Table field.`);
      return { type: 'struct', fields: fields(node.children, typeof expected === 'object' && expected.type === 'struct' ? expected.fields : undefined) };
    }
    if (element.repetition_type === 'REPEATED') throw new Error(`${element.name}: legacy repeated fields need an explicit schema conversion.`);
    const result = primitive(element);
    if (expected !== undefined && result !== expected) throw new Error(`${element.name}: ${result} does not match ${JSON.stringify(expected)}.`);
    return result;
  };
  const fields = (nodes: Node[], base?: IceField[]): IceField[] => {
    if (new Set(nodes.map(value => value.element.name)).size !== nodes.length || base && (base.length !== nodes.length || base.some(value => !nodes.some(node => node.element.name === value.name)))) throw new Error('Parquet field names do not match the Table schema.');
    return nodes.map(node => {
      const field = base?.find(value => value.name === node.element.name); const required = node.element.repetition_type === 'REQUIRED';
      if (field?.required && !required) throw new Error(`${node.element.name}: optional Parquet field cannot fill a required Table field.`);
      const fieldId = id(node, field?.id);
      return { id: fieldId, name: node.element.name, required: field?.required ?? required, type: type(node, field?.type) };
    });
  };
  return { type: 'struct', 'schema-id': expected?.['schema-id'] ?? 0, fields: fields(root.children, expected?.fields) };
}

export function nameMapping(schema: IceSchema): string {
  const children = (type: IceType): unknown[] => typeof type === 'string' ? [] : type.type === 'struct' ? type.fields.map(field)
    : type.type === 'list' ? [field({ id: type['element-id'], name: 'element', required: type['element-required'], type: type.element })]
    : [field({ id: type['key-id'], name: 'key', required: true, type: type.key }), field({ id: type['value-id'], name: 'value', required: type['value-required'], type: type.value })];
  const field = (value: IceField): unknown => ({ 'field-id': value.id, names: [value.name], ...(typeof value.type === 'string' ? {} : { fields: children(value.type) }) });
  return JSON.stringify(schema.fields.map(field));
}
