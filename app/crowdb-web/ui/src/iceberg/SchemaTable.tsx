// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
type Field = { id?: number; name: string; required?: boolean; type: unknown };
type Complex = { type: string; fields?: Field[]; element?: unknown; key?: unknown; value?: unknown; 'element-id'?: number; 'key-id'?: number; 'value-id'?: number; 'element-required'?: boolean; 'value-required'?: boolean };
function children(field: Field): Field[] {
  if (!field.type || typeof field.type !== 'object') return [];
  const t = field.type as Complex;
  if (t.type === 'struct') return t.fields ?? [];
  if (t.type === 'list') return [{ name: 'element', id: t['element-id'], type: t.element, required: t['element-required'] }];
  if (t.type === 'map') return [{ name: 'key', id: t['key-id'], type: t.key, required: true }, { name: 'value', id: t['value-id'], type: t.value, required: t['value-required'] }];
  return [];
}
export function SchemaTable({ fields }: { fields: Field[] }) {
  const [closed, setClosed] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(0);
  const rows: Array<{ field: Field; key: string; depth: number; branch: boolean }> = [];
  function visit(items: Field[], parent = '', depth = 0) {
    items.forEach((field, index) => {
      const key = `${parent}/${index}`;
      const nested = children(field);
      rows.push({ field, key, depth, branch: nested.length > 0 });
      if (!closed.has(key)) visit(nested, key, depth + 1);
    });
  }
  visit(fields);
  const current = Math.min(page, Math.max(0, Math.ceil(rows.length / 100) - 1));
  return <div className="tw-space-y-2 tw-overflow-x-auto"><table aria-label="Table schema" className="tw-w-full tw-text-xs tw-text-left">
    <thead><tr>{['Field', 'ID', 'Type', 'Required'].map(label => <th key={label} className="tw-p-2 tw-text-muted tw-border-b tw-border-border">{label}</th>)}</tr></thead>
    <tbody>{rows.slice(current * 100, (current + 1) * 100).map(({ field, key, depth, branch }) => <tr key={key} className="tw-border-b tw-border-border" aria-expanded={branch ? !closed.has(key) : undefined}>
      <td className="tw-p-2" style={{ paddingLeft: 8 + depth * 18 }}>{branch ? <button aria-label={`${closed.has(key) ? 'Expand' : 'Collapse'} field ${field.name}`} onClick={() => { setClosed(previous => { const next = new Set(previous); if (next.has(key)) next.delete(key); else next.add(key); return next; }); setPage(0); }}>{closed.has(key) ? '▸' : '▾'} {field.name}</button> : field.name}</td>
      <td className="tw-p-2">{field.id ?? '—'}</td><td className="tw-p-2">{typeof field.type === 'object' && field.type ? (field.type as Complex).type : String(field.type ?? '—')}</td><td className="tw-p-2">{field.required ? 'Yes' : 'No'}</td>
    </tr>)}</tbody></table>{rows.length > 100 && <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={current === 0} onClick={() => setPage(current - 1)}>Previous fields</button><span>{current * 100 + 1}–{Math.min(rows.length, (current + 1) * 100)} / {rows.length}</span><button className={buttonClass} disabled={(current + 1) * 100 >= rows.length} onClick={() => setPage(current + 1)}>Next fields</button></div>}</div>;
}
