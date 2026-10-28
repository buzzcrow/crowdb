// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { sampleLabel, encoded } from '../identity';
import type { Operation } from '../operation';
import type { MatrixRow } from '../types';
import { cell, uploadMetadata } from './model';
import { CellEditor } from './CellEditor';
import type { LocalValue } from '../import/mapping';

export function Grid({ op, selectedId, rows, fields, apply, upload, rename, remove, restore, select, selectField, removeField }: {
  op: Operation; selectedId?: string; rows: MatrixRow[]; fields: string[]; apply: (id: string, field: string, value: LocalValue) => void; upload: (id: string, field: string, file: File) => void;
  rename: (old: string, id: string) => void; remove: (id: string) => void; restore: (id: string) => void;
  select: (id: string, field?: string) => void; selectField: (name: string) => void; removeField: (name: string) => void;
}) {
  return <div className="tw-overflow-auto tw-max-h-[60vh]"><table aria-label="Snapshot editor table" className="tw-w-full tw-text-sm tw-text-left">
    <thead className="tw-sticky tw-top-0 tw-bg-panel tw-z-10"><tr><th className="tw-p-3 tw-sticky tw-left-0 tw-bg-panel">Sample ID</th>
      {fields.map(field => <th key={field} className="tw-p-3 tw-min-w-[180px]"><button onClick={() => selectField(field)}>{field}{op.mapping!.schema!.fields.find(value => value.name === field)?.required ? ' *' : ''}</button>
        <button aria-label={`Remove field ${field}`} title={`Remove field ${field}`} className="tw-ml-3 tw-text-muted" disabled={!!op.source?.parent && !!op.parentSchema?.fields.some(value => value.name === field && value.required)} onClick={() => removeField(field)}>×</button></th>)}<th className="tw-p-3">Change</th></tr></thead>
    <tbody>{rows.map(row => {
      const patch = op.mapping!.samples.find(value => value.id === row.id); const removed = patch?.kind === 'remove';
      return <tr key={row.id} aria-selected={selectedId === row.id} className={`tw-border-t tw-border-border ${selectedId === row.id ? 'tw-bg-accent/10' : ''} ${removed ? 'tw-opacity-60' : ''}`} onClick={event => { if (!(event.target as HTMLElement).closest('button,input,textarea')) select(row.id); }}>
        <th className="tw-p-3 tw-sticky tw-left-0 tw-bg-panel tw-font-normal">
          {patch?.kind === 'upsert' && (!op.source?.parent || op.newSamples?.includes(row.id))
            ? <SampleId key={row.id} id={row.id} select={() => select(row.id)} change={id => rename(row.id, id)} />
            : <button className="tw-text-accent" onClick={() => select(row.id)}>{sampleLabel(row.id)}</button>}</th>
        {fields.map((field, index) => {
          const value = cell(op, row.id, field); const inherited = row.fields[index];
          const uploaded = uploadMetadata(op, value);
          const label = uploaded ? `${uploaded.name} · ${uploaded.state === 'ready' ? `${uploaded.size} B` : uploaded.state}` : undefined;
          return <td key={field} className="tw-p-3"><CellEditor dataset={op.dataset} parent={op.source?.parent ?? undefined} id={row.id} sample={sampleLabel(row.id)} field={field} value={value} inherited={inherited} fileLabel={label} disabled={removed}
            select={() => select(row.id, field)} apply={value => apply(row.id, field, value)} upload={file => upload(row.id, field, file)} /></td>;
        })}
        <td className="tw-p-3 tw-whitespace-nowrap">{removed ? <button className={buttonClass} onClick={() => restore(row.id)}>Undo removal</button> : <button className={buttonClass} aria-label={`Remove Sample ${sampleLabel(row.id)}`} onClick={() => remove(row.id)}>Remove</button>}</td>
      </tr>;
    })}</tbody>
  </table>{!rows.length && <p className="tw-p-6 tw-text-muted">Add a Sample or upload files to start.</p>}</div>;
}
function SampleId({ id, change, select }: { id: string; change: (id: string) => void; select: () => void }) {
  const [text, setText] = useState(sampleLabel(id));
  const canonical = encoded(new TextEncoder().encode(sampleLabel(id))) === id;
  return canonical ? <input aria-label={`Sample ID for ${sampleLabel(id)}`} className={`${inputClass} tw-w-44`} value={text} onFocus={select} onChange={event => setText(event.target.value)} onBlur={() => { if (text && text !== sampleLabel(id)) change(encoded(new TextEncoder().encode(text))); setText(sampleLabel(id)); }} /> : <button className="tw-text-accent" onClick={select}>{sampleLabel(id)}</button>;
}
