// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createPortal } from 'react-dom';
import { buttonClass } from '../access/Workbench';
import type { JournalObservation, TreeObservation } from './useRuntimeObservation';

function Fields({ values }: { values: Record<string, string | boolean> }) {
  return <dl className="tw-grid tw-grid-cols-2 tw-gap-3 tw-text-sm">{Object.entries(values).map(([name, value]) => <div key={name}><dt className="tw-text-muted">{name}</dt><dd className="tw-font-mono tw-break-all">{String(value)}</dd></div>)}</dl>;
}

export function TreeStorage({ value }: { value?: TreeObservation }) {
  if (!value || value.error) return <p role="status">Tree storage observation unavailable{value?.error ? `: ${value.error}` : '.'}</p>;
  return <section aria-label="Tree storage" className="tw-space-y-4">
    <h3 className="tw-font-semibold">Opened tree checkpoint</h3>
    <Fields values={{ 'Checkpoint manifest': value.checkpoint_manifest, 'Checkpoint applied sequence': value.checkpoint_applied_seq }} />
    <p className="tw-text-xs tw-text-muted">Independently sampled opened state. A local checkpoint does not prove that its recovery reference or retention watermark was published.</p>
    <h3 className="tw-font-semibold">Page and memory counters</h3>
    {value.runtime ? <Fields values={Object.fromEntries(Object.entries(value.runtime).map(([name, value]) => [name.replaceAll('_', ' '), value]))} /> : <p>Native page and memory counters unavailable for this tree backend.</p>}
    <h3 className="tw-font-semibold">Maintenance and materialization</h3>
    <Fields values={Object.fromEntries(Object.entries(value.maintenance).map(([name, value]) => [name.replaceAll('_', ' '), value]))} />
    <p className="tw-text-xs tw-text-muted">Counters describe this open handle. The Page explorer above reads structural base pages separately; these counters do not establish retention pins or serving authority.</p>
  </section>;
}

export function JournalStorage({ value, disabled, onChunk, propertyHost, selected, onSelected }: { selected: string | null; onSelected: (id: string) => void; propertyHost: HTMLDivElement | null; onChunk: (id: string) => void; value?: JournalObservation | null; disabled: boolean }) {
  if (!value) return <p role="status">Journal storage observation unavailable.</p>;
  const record = value.chunk_record;
  const fields = { 'Chunk ID': record.chunk_id, 'Chunk state': record.state, 'Logical start (bytes)': record.logical_start,
    'Logical end (exclusive bytes)': record.logical_end, 'Append mappings': record.mapping_count, 'Compressed mapping bytes': record.mapping_bytes };
  return <section aria-label="Journal storage" className="tw-space-y-4">
    <Fields values={{ 'Stream chunk record generation': value.generation, 'Stream writer epoch': value.writer_epoch, 'Metadata group': value.metadata_group_id,
      'Trim offset (logical bytes)': value.trim_offset, 'Published tail (logical bytes)': value.published_tail, 'Stream state': value.closed ? 'Closed' : 'Open' }} />
    <h3 className="tw-font-semibold">Stream chunk record</h3>
    <p className="tw-text-xs tw-text-muted">Published tail record. Logical bounds describe its checkpoint; current appends may extend beyond it.</p>
    <div aria-label="Stream chunk record"><Fields values={fields} />
      <button className={buttonClass} aria-pressed={selected === record.chunk_id} onClick={() => onSelected(record.chunk_id)}>Select stream chunk record</button>
      <button className={buttonClass} disabled={disabled} onClick={() => onChunk(record.chunk_id)}>Inspect Chunk</button>
    </div>
    {selected === record.chunk_id && propertyHost && createPortal(<aside aria-label="Selected stream chunk record" className="tw-mt-5 tw-space-y-3"><h3 className="tw-font-semibold">Stream chunk record</h3><Fields values={fields} /></aside>, propertyHost)}
  </section>;
}
