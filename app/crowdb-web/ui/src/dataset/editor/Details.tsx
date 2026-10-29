// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import * as api from '../api';
import { sampleLabel } from '../identity';
import type { Operation } from '../operation';
import { useQuery } from '../query';
import { FieldPreview } from '../Properties';
import { cell, displayValue, parentRows, uploadMetadata } from './model';
export function DraftDetails({ op, id, field }: { op: Operation; id?: string; field?: string }) {
  const [page, setPage] = useState(0);
  const schema = op.mapping?.schema?.fields ?? [];
  const fields = field ? schema.filter(value => value.name === field) : schema.slice(page * 12, page * 12 + 12);
  const parent = op.source?.parent;
  const inherited = useQuery(JSON.stringify([op.dataset, parent, id, fields]), !!parent && !!id && !op.newSamples?.includes(id), async signal => {
    const member = await api.control<{ contains: boolean }>(`${api.snapshotPath(op.dataset, parent!)}/contains?${new URLSearchParams({ sample: id! })}`, 'GET', undefined, signal);
    return member.contains ? (await parentRows(op, [id!], fields.map(value => value.name), signal))[0] : { id: id!, fields: fields.map(() => null) };
  });
  return <section aria-label="Draft properties" className="tw-space-y-3">
    <h2 className="tw-font-semibold">Properties · {id ? sampleLabel(id) : field ?? 'Draft'}</h2>
    <p className="tw-text-xs">Local Draft · editable</p>
    {id && <label className="tw-block tw-text-xs">Canonical Sample ID<input aria-label="Canonical Sample ID" className={`${inputClass} tw-w-full`} readOnly value={id} /></label>}
    <p className="tw-text-xs tw-break-all">Draft: {op.resource}</p>{parent && <p className="tw-text-xs tw-break-all">Parent: {parent}</p>}
    {inherited.error && <p role="alert">{inherited.error}</p>}
    {fields.map((definition, index) => {
      const value = id ? cell(op, id, definition.name) : undefined;
      const upload = uploadMetadata(op, value);
      const metadata = inherited.value?.fields[index];
      return <div key={definition.name} className="tw-border-t tw-border-border tw-pt-2 tw-text-xs tw-space-y-2">
        <h3 className="tw-font-semibold tw-break-all">{definition.name} · {definition.required ? 'Required' : 'Optional'}</h3>
        {id && (value ? <p className="tw-whitespace-pre-wrap tw-break-all">{displayValue(value)}</p> : <p>{metadata ? `${metadata.storage} · ${metadata.length} B (inherited)` : parent && inherited.loading ? 'Loading…' : 'Missing'}</p>)}
        {upload && <div className="tw-break-all"><p>{upload.name} · {upload.size} B · {upload.state}</p><p>Checksum: {upload.md5 ?? 'Pending'}</p>{upload.error && <p>{upload.error}</p>}{upload.expiresAt && <p>Unclaimed upload expires: {new Date(Number(upload.expiresAt) * 1000).toISOString()}</p>}</div>}
        {id && parent && metadata && !value && <FieldPreview selected={{ kind: 'cell', dataset: op.dataset, snapshot: parent, sample: id, field: definition.name }} field={metadata} active />}
      </div>;
    })}
    {!field && <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!page} onClick={() => setPage(page - 1)}>Previous fields</button><button className={buttonClass} disabled={(page + 1) * 12 >= schema.length} onClick={() => setPage(page + 1)}>Next fields</button></div>}
  </section>;
}
