// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState, type ReactNode } from 'react';
import { buttonClass } from '../access/Workbench';
export const missing = 'Not recorded';
export function scalar(value: unknown): string {
  if (value == null) return missing;
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  return String(value);
}
export function byteSize(value: string | number | undefined): string {
  if (value == null) return missing;
  const n = Number(value);
  if (!Number.isFinite(n)) return missing;
  if (n < 1024) return `${value} B`;
  const power = Math.min(4, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** power).toFixed(2)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][power]}`;
}
export function Fields({ values, stacked = false }: { values: Record<string, unknown>; stacked?: boolean }) {
  if (Object.keys(values).length > 50) return <Structured value={values} />;
  return <dl className={stacked ? "tw-space-y-3" : "tw-grid tw-grid-cols-2 xl:tw-grid-cols-3 tw-gap-x-5 tw-gap-y-3"} aria-label="Metadata fields">{Object.entries(values).slice(0, 50).map(([key, value]) => <div key={key} className="tw-min-w-0"><dt className="tw-text-xs tw-text-muted">{key.replaceAll('_', ' ').replaceAll('-', ' ')}</dt><dd className="tw-text-sm tw-break-words">{key === 'metrics' && Array.isArray(value) && value.every(item => asObject(item)) ? <ObjectArrayTable label="File metrics" values={value.map(item => asObject(item)!)} /> : typeof value === 'object' && value !== null ? stacked ? <Expandable value={value} /> : <Structured value={value} /> : scalar(value)}</dd></div>)}</dl>;
}
function ObjectArrayTable({ label, values }: { label: string; values: SchemaNode[] }) {
  const headings = [...new Set(values.flatMap(value => Object.keys(value)))];
  return <div className="tw-overflow-auto"><table aria-label={label} className="tw-w-full tw-text-xs"><thead><tr><th className="tw-p-2 tw-text-left tw-font-medium tw-text-muted">#</th>{headings.map(heading => <th key={heading} className="tw-p-2 tw-text-left tw-font-medium tw-text-muted">{heading.replaceAll('_', ' ')}</th>)}</tr></thead><tbody>{values.map((value, index) => <tr key={index} className="tw-border-t tw-border-border"><td className="tw-p-2 tw-align-top">{index}</td>{headings.map(heading => <td key={heading} className="tw-p-2 tw-align-top tw-break-words">{typeof value[heading] === 'object' && value[heading] !== null ? JSON.stringify(value[heading]) : scalar(value[heading])}</td>)}</tr>)}</tbody></table></div>;
}
export function Structured({ value }: { value: unknown }) {
  const [page, setPage] = useState(0);
  if (value == null || typeof value !== 'object') return <>{scalar(value)}</>;
  if (Array.isArray(value) && value.length > 0 && value.every(item => asObject(item))) return <ObjectArrayTable label="Structured records" values={value.map(item => asObject(item)!)} />;
  const entries = Object.entries(value);
  const start = Math.min(page * 30, Math.max(0, Math.floor((entries.length - 1) / 30) * 30));
  return <><dl className="tw-space-y-1">{entries.slice(start, start + 30).map(([key, item]) => <div key={key} className="tw-pl-2 tw-border-l tw-border-border"><dt className="tw-text-xs tw-text-muted">{key}</dt><dd>{item && typeof item === 'object' ? <Expandable value={item} /> : scalar(item)}</dd></div>)}</dl>{entries.length > 30 && <div className="tw-flex tw-gap-2 tw-text-xs"><button className={buttonClass} disabled={!start} onClick={() => setPage(Math.max(0, start / 30 - 1))}>Previous fields</button><span>{start + 1}–{Math.min(start + 30, entries.length)} of {entries.length}</span><button className={buttonClass} disabled={start + 30 >= entries.length} onClick={() => setPage(start / 30 + 1)}>Next fields</button></div>}</>;
}
type SchemaNode = Record<string, unknown>;
const asObject = (value: unknown): SchemaNode | null => value && typeof value === 'object' && !Array.isArray(value) ? value as SchemaNode : null;
const schemaFields = (value: unknown): SchemaNode[] => {
  if (Array.isArray(value)) return value.flatMap(item => schemaFields(item));
  const object = asObject(value);
  if (!object || !Array.isArray(object.fields)) return [];
  return object.fields.filter((field): field is SchemaNode => !!asObject(field)).map(field => asObject(field)!);
};
const schemaType = (value: unknown): string => {
  if (Array.isArray(value)) return value.map(schemaType).join(' | ');
  if (typeof value === 'string') return value;
  const object = asObject(value);
  if (!object) return scalar(value);
  if (typeof object.type === 'string') {
    if (object.logicalType) return `${object.type} (${String(object.logicalType)})`;
    if (object.type === 'array' && object.items) return `array<${schemaType(object.items)}>`;
    if (object.type === 'map' && object.values) return `map<string, ${schemaType(object.values)}>`;
    return object.name ? `${object.type} ${String(object.name)}` : object.type;
  }
  if (object.fields) return object.name ? `record ${String(object.name)}` : 'record';
  return JSON.stringify(value);
};
const schemaChildren = (field: SchemaNode): SchemaNode[] => {
  const direct = schemaFields(field);
  if (direct.length) return direct;
  return schemaFields(field.type);
};
function SchemaFieldRows({ fields, depth = 0 }: { fields: SchemaNode[]; depth?: number }) {
  return <>{fields.map((field, index) => <SchemaFieldRow key={`${depth}:${String(field.name ?? index)}`} field={field} depth={depth} />)}</>;
}
function SchemaFieldRow({ field, depth }: { field: SchemaNode; depth: number }) {
  const [open, setOpen] = useState(false);
  const children = schemaChildren(field);
  const fieldType = schemaType(field.type ?? field);
  const required = field.required === true || (Array.isArray(field.type) && !field.type.some(item => item === 'null'));
  return <>
    <tr className="tw-border-t tw-border-border">
      <td className="tw-p-2 tw-align-top" style={{ paddingLeft: `${8 + depth * 20}px` }}>{children.length > 0 ? <button type="button" className="tw-text-accent tw-underline" aria-label={`${open ? 'Collapse' : 'Expand'} field details`} onClick={() => setOpen(value => !value)}>{open ? '▾' : '▸'} {String(field.name ?? 'field')}</button> : String(field.name ?? 'field')}</td>
      <td className="tw-p-2 tw-align-top tw-break-words">{fieldType}</td>
      <td className="tw-p-2 tw-align-top">{required ? 'Yes' : 'No'}</td>
      <td className="tw-p-2 tw-align-top">{scalar(field.id ?? field['field-id'])}</td>
      <td className="tw-p-2 tw-align-top tw-break-words">{field.doc ? String(field.doc) : children.length ? `${children.length} nested fields` : '—'}</td>
    </tr>
    {open && <SchemaFieldRows fields={children} depth={depth + 1} />}
  </>;
}
export function SchemaTable({ value }: { value: unknown }) {
  const fields = schemaFields(value);
  if (!fields.length) return <Structured value={value} />;
  return <div className="tw-overflow-auto"><table aria-label="Writer schema" className="tw-w-full tw-text-xs"><thead><tr>{['Field', 'Type', 'Required', 'ID', 'Details'].map(heading => <th key={heading} className="tw-p-2 tw-text-left tw-font-medium tw-text-muted">{heading}</th>)}</tr></thead><tbody><SchemaFieldRows fields={fields} /></tbody></table></div>;
}
function Expandable({ value }: { value: object }) {
  const [open, setOpen] = useState(false);
  return <details onToggle={event => setOpen(event.currentTarget.open)}><summary className="tw-text-xs tw-cursor-pointer">{Object.keys(value).length} {Array.isArray(value) ? 'items' : 'fields'}</summary>{open && <Structured value={value} />}</details>;
}
export function Records({ label, headings, rows }: { label: string; headings: string[]; rows: ReactNode[][] }) {
  const [page, setPage] = useState(0);
  const start = Math.min(page * 100, Math.max(0, Math.floor((rows.length - 1) / 100) * 100));
  return <div className="tw-overflow-auto"><table aria-label={label} className="tw-w-full tw-text-xs"><thead><tr>{headings.map(h => <th key={h} className="tw-text-left tw-p-2 tw-text-muted tw-font-medium">{h}</th>)}</tr></thead><tbody>{rows.slice(start, start + 100).map((row, i) => <tr key={i} className="tw-border-t tw-border-border">{row.map((cell, j) => <td key={j} className="tw-p-2 tw-align-top tw-break-words">{cell}</td>)}</tr>)}</tbody></table>{!rows.length && <p className="tw-text-xs tw-text-muted">No records.</p>}{rows.length > 100 && <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!start} onClick={() => setPage(start / 100 - 1)}>Previous {label}</button><span>{start + 1}–{Math.min(start + 100, rows.length)} of {rows.length}</span><button className={buttonClass} disabled={start + 100 >= rows.length} onClick={() => setPage(start / 100 + 1)}>Next {label}</button></div>}</div>;
}
