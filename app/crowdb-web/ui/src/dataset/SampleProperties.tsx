// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import * as api from './api';
import { useQuery } from './query';
import { FieldPreview } from './Properties';
import type { DatasetSelection } from './types';

/** The selected row has its own field window, independent of center columns. */
export function SampleProperties({ selected, active }: { selected: Extract<DatasetSelection, { kind: 'sample' }>; active: boolean }) {
  return <Fields key={JSON.stringify([selected.dataset, selected.snapshot, selected.sample])} selected={selected} active={active} />;
}
function Fields({ selected, active }: { selected: Extract<DatasetSelection, { kind: 'sample' }>; active: boolean }) {
  const [after, setAfter] = useState<string>(); const [previous, setPrevious] = useState<Array<string | undefined>>([]);
  const [open, setOpen] = useState<string>();
  const result = useQuery(JSON.stringify([selected.dataset, selected.snapshot, selected.sample, after]), active, async signal => {
    const schema = await api.schemaPage(selected.dataset, selected.snapshot, after, signal);
    const row = (await api.matrix(selected.dataset, selected.snapshot, [selected.sample], schema.entries.map(field => field.name), signal))[0];
    return { schema, row };
  });
  return <section aria-label="Sample fields" className="tw-space-y-3">
    <h3 className="tw-font-semibold">Fields</h3>
    {result.loading && <p role="status">Loading Sample fields…</p>}{result.error && <p role="alert">{result.error}</p>}
    {result.value?.schema.entries.map((field, index) => {
      const metadata = result.value!.row.fields[index];
      return <details key={field.name} open={open === field.name} onToggle={event => { if (event.currentTarget.open) setOpen(field.name); else if (open === field.name) setOpen(undefined); }}>
        <summary className="tw-cursor-pointer tw-break-all">{field.name} · {metadata ? `${metadata.storage} · ${metadata.length} B` : 'Missing'}</summary>
        {open === field.name && <div className="tw-space-y-2 tw-py-2 tw-text-xs"><p>{field.required ? 'Required' : 'Optional'}</p>
          {metadata && <><p className="tw-break-all">Source Snapshot: {metadata.source_snapshot}</p><p className="tw-break-all">Checksum: {metadata.md5 ?? 'None'}</p></>}
          <FieldPreview selected={{ ...selected, kind: 'cell', field: field.name }} field={metadata} active={active} />
        </div>}
      </details>;
    })}
    <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!previous.length || result.loading} onClick={() => { setAfter(previous.at(-1)); setPrevious(previous.slice(0, -1)); setOpen(undefined); }}>Previous fields</button>
      <button className={buttonClass} disabled={!result.value?.schema.after || result.loading} onClick={() => { setPrevious([...previous, after]); setAfter(result.value!.schema.after!); setOpen(undefined); }}>Next fields</button></div>
  </section>;
}
