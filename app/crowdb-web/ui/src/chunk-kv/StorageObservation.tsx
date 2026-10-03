// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
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
    <p className="tw-text-xs tw-text-muted">Counters describe this open handle. Current root-catalog layout, retention pins and page child links require a separate bounded metadata inspection API.</p>
  </section>;
}

export function JournalStorage({ value, disabled, onPage, onChunk }: { onChunk: (id: string) => void; value?: JournalObservation | null; disabled: boolean; onPage: (offset: number) => void }) {
  const [selected, setSelected] = useState<string | null>(null);
  if (!value) return <p role="status">Journal storage observation unavailable.</p>;
  const extent = value.extent_pages.find(page => page.page_index === selected);
  return <section aria-label="Journal storage" className="tw-space-y-4">
    <Fields values={{ 'Stream manifest generation': value.generation, 'Stream writer epoch': value.writer_epoch, 'Metadata group': value.metadata_group_id,
      'Trim offset (logical bytes)': value.trim_offset, 'Sealed tail (logical bytes)': value.sealed_tail, 'Stream state': value.closed ? 'Closed' : 'Open' }} />
    <div className="tw-border tw-border-accent tw-rounded tw-p-3 tw-space-y-3"><h3 className="tw-font-semibold">Active chunk</h3>
      {value.active ? <Fields values={{ 'Chunk ID': value.active.chunk_id, 'Logical start (bytes)': value.active.logical_start, 'Physical start (bytes)': value.active.physical_start,
        'Acknowledged physical cursor (bytes)': value.active.acknowledged_cursor, 'Physical capacity (bytes)': value.active.capacity }} /> : <p>No active chunk in this manifest.</p>}
      {value.active && <button className={buttonClass} disabled={disabled} onClick={() => onChunk(value.active!.chunk_id)}>Inspect active Chunk</button>}
    </div>
    <h3 className="tw-font-semibold">Published extent index</h3>
    <p className="tw-text-xs tw-text-muted">{value.extent_pages.length} loaded page fences · offset {value.offset}. Ordered blocks show logical byte ranges; widths do not represent sizes. Extent records and journal payloads are not read.</p>
    <div aria-label="Extent page map" className="tw-grid tw-grid-cols-[repeat(auto-fill,minmax(180px,1fr))] tw-gap-2">{value.extent_pages.map(page => <button key={page.page_index} className={`${buttonClass} tw-text-left`} aria-pressed={selected === page.page_index} onClick={() => setSelected(page.page_index)}>
      <span className="tw-block">Extent page {page.page_index}</span><span className="tw-font-mono tw-break-all">[{page.first_logical}, {page.end_logical})</span>
    </button>)}</div>
    {!value.extent_pages.length && <p>No published extent pages in this window.</p>}
    {extent && <aside aria-label="Selected extent page"><Fields values={{ 'Page index': extent.page_index, 'First logical byte': extent.first_logical, 'End logical byte (exclusive)': extent.end_logical }} /></aside>}
    <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={disabled || value.offset === 0} onClick={() => onPage(Math.max(0, value.offset - 100))}>Previous extent pages</button>
      <button className={buttonClass} disabled={disabled || value.next_offset === null} onClick={() => onPage(value.next_offset!)}>Next extent pages</button></div>
  </section>;
}
