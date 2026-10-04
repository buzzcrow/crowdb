// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { buttonClass } from '../access/Workbench';
import { byteSize, Records } from '../iceberg/Fields';
import type { useObjectLocations } from './useObjectLocations';
const interval = (offset: string, length: string) => `[${byteSize(offset)}, ${byteSize((BigInt(offset) + BigInt(length)).toString())})`;

export function StorageLocations({ inspection, onChunk }: { inspection: ReturnType<typeof useObjectLocations>; onChunk: (id: string) => void }) {
  const { page, selected, busy, error, stale } = inspection;
  return <section aria-label="Storage locations" className="tw-space-y-3">
    <div className="tw-flex tw-items-center tw-gap-3"><h2 className="tw-font-semibold">Storage locations</h2>
      <button className={buttonClass} disabled={busy} onClick={inspection.refresh}>Refresh locations</button></div>
    {busy && <p role="status" className="tw-text-xs tw-text-muted">Loading object storage metadata…</p>}
    {error && <p role="alert" className="tw-text-xs tw-text-failed">{stale ? 'Stale locations · ' : page ? 'Previous observation is stale · ' : ''}{error}</p>}
    {page && !page.locations.length && !busy && !error ? <p>No storage extents</p> : page && <Records label="Object storage extents"
      headings={['Extent', 'Logical interval', 'Chunk', 'Chunk interval', 'Physical length']}
      rows={page.locations.map(extent => [
        <button className="tw-text-accent" aria-label={`Select extent ${extent.index}`} aria-pressed={selected?.index === extent.index} disabled={busy || !!error} onClick={() => inspection.select(extent)}>{extent.index}</button>,
        interval(extent.logical_offset, extent.logical_length),
        extent.chunk_id ? <button className="tw-font-mono tw-text-accent tw-break-all tw-text-left" aria-label={`Open Chunk ${extent.chunk_id}`} disabled={busy || !!error} onClick={() => { inspection.select(extent); onChunk(extent.chunk_id!); }}>{extent.chunk_id}</button> : 'Location unavailable',
        interval(extent.offset, extent.length), byteSize(extent.length),
      ])} />}
    <nav aria-label="Storage location pages" className="tw-flex tw-gap-2">
      <button className={buttonClass} disabled={busy || !!error || !inspection.canPrevious} onClick={inspection.previous}>Previous locations</button>
      <button className={buttonClass} disabled={busy || !!error || !page?.next_cursor} onClick={inspection.next}>Next locations</button>
    </nav>
    <p className="tw-text-xs tw-text-muted">Metadata only · no object payload or disk placement reads</p>
  </section>;
}
