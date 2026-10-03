// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useRef } from 'react';
import { getApiBase } from '../api';
import { readJson } from '../access/native';
import { Workbench, JsonView, inputClass, buttonClass } from '../access/Workbench';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { Domain } from '../types';

interface Segment { disk_id: { high: string | number; low: string | number } | null; zone_index: number; unit_offset: string | number; unit_count: number; allocation_ts: string | number }
interface Strip { strip_sequence: number; chunk_offset: number; capacity: number; unit_kb: number; sealed_length: number; strip_type: number; strip: { MirrorStrip?: { segments: Segment[] }; EcStrip?: { segments: Segment[]; data_num: number; code_num: number; ec_state: number } } | null; unavailable_segments: Segment[]; placement_repair_required: boolean; placement_assessment: unknown }
interface Chunk { id_hex: string; chunk_type: number; state: number; modify_ts: string | number; capacity: number; sealed_length: number; strips: Strip[]; acknowledged_cursor: string | number; writer_epoch: string | number; owner?: string }
interface Placement { disk_id: string; rack_id: string; node_id: string; disk_group_id: string; unit_size: number }
interface ChunkDetail { chunk: Chunk; layout_validity_ms: number; observed_at_ms: number; placement_observed_at_ms: number; placements: Placement[]; placement_error: string | null }
interface ChunkPage { chunks: Chunk[]; scanned: number; next: string | null; owners: number; failures: Array<{ owner: string; error: string }>; observed_at_ms: number }
const kinds = ['Repo', 'WAL', 'Btree page', 'Page index', 'Stream', 'S3', 'Iceberg table'];
const states = ['Init', 'Active', 'Sealed', 'Deleted'];
const diskId = (segment: Segment): string | null => segment.disk_id ? BigInt(segment.disk_id.high).toString(16).padStart(16, '0') + BigInt(segment.disk_id.low).toString(16).padStart(16, '0') : null;
const identity = (segment: Segment): string => `${diskId(segment)}/${segment.zone_index}/${segment.unit_offset}/${segment.allocation_ts}`;

export function ChunkBrowser({ onPlacement }: { onPlacement: (entity: SelectedEntity) => void }) {
  const [kind, setKind] = useState('');
  const [prefix, setPrefix] = useState('');
  const [lookup, setLookup] = useState('');
  const [page, setPage] = useState<ChunkPage | null>(null);
  const [rows, setRows] = useState<Chunk[]>([]);
  const [detail, setDetail] = useState<ChunkDetail | null>(null);
  const [stripSequence, setStripSequence] = useState<number | null>(null);
  const [stripStart, setStripStart] = useState(0);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const revision = useRef(0);
  const query = async (after?: string) => {
    const version = ++revision.current;
    setBusy(true); setError('');
    if (!after) { setRows([]); setPage(null); setDetail(null); }
    try {
      const search = new URLSearchParams({ prefix, limit: '100', ...(kind ? { chunk_type: kind } : {}), ...(after ? { after } : {}) });
      const result = await readJson<ChunkPage>(await fetch(`${getApiBase()}/chunks?${search}`));
      if (version === revision.current) { setPage(result); setRows(previous => after ? [...previous, ...result.chunks] : result.chunks); }
    } catch (error) { if (version === revision.current) setError(String(error)); }
    finally { if (version === revision.current) setBusy(false); }
  };
  const inspect = async (id: string) => {
    const version = ++revision.current;
    setDetail(null); setStripSequence(null); setStripStart(0); setBusy(true); setError('');
    try {
      const result = await readJson<ChunkDetail>(await fetch(`${getApiBase()}/chunks/${encodeURIComponent(id)}`));
      if (version === revision.current) setDetail(result);
    } catch (error) { if (version === revision.current) setError(String(error)); }
    finally { if (version === revision.current) setBusy(false); }
  };
  const chunk = detail?.chunk;
  const strip = chunk?.strips.find(strip => strip.strip_sequence === stripSequence);
  const ordered = [...(chunk?.strips ?? [])].sort((a, b) => a.chunk_offset - b.chunk_offset || a.strip_sequence - b.strip_sequence);
  const fragments = (strip: Strip) => strip.strip?.MirrorStrip?.segments ?? strip.strip?.EcStrip?.segments ?? [];
  const layout = (strip: Strip) => strip.strip?.MirrorStrip ? `Mirror ×${strip.strip.MirrorStrip.segments.length}` : strip.strip?.EcStrip ? `EC ${strip.strip.EcStrip.data_num}+${strip.strip.EcStrip.code_num} · ${strip.strip.EcStrip.ec_state === 1 ? 'Parity' : 'No parity'}` : `Unknown layout (${strip.strip_type})`;
  return <Workbench sidebar={<>
    <h2 className="tw-font-semibold">ChunkDB</h2><p className="tw-text-xs tw-text-muted">Type and hexadecimal ID prefix</p>
    <form className="tw-space-y-3" onSubmit={event => { event.preventDefault(); void query(); }}>
      <label className="tw-block tw-text-xs">Chunk type<select className={`${inputClass} tw-w-full`} value={kind} onChange={event => setKind(event.target.value)}><option value="">All types</option>{kinds.map((name, type) => <option key={name} value={type}>{name} (0x{type.toString(16).padStart(2, '0')})</option>)}</select></label>
      <label className="tw-block tw-text-xs">Chunk ID prefix<input className={`${inputClass} tw-w-full tw-font-mono`} value={prefix} onChange={event => setPrefix(event.target.value)} pattern="[0-9a-fA-F]{0,32}" maxLength={32} /></label><button className={buttonClass} disabled={busy}>Query chunks</button>
    </form>
    <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void inspect(lookup.toLowerCase()); }}><label className="tw-block tw-text-xs">Exact Chunk ID<input className={`${inputClass} tw-w-full tw-font-mono`} required pattern="[0-9a-fA-F]{32}" value={lookup} onChange={event => setLookup(event.target.value)} /></label><button className={buttonClass} disabled={busy}>Lookup ID</button></form>
    <p className="tw-text-xs tw-text-muted">Read-only diagnostics. Type prefixes classify content; range ownership uses a separate hash.</p>
  </>} detail={strip && detail ? <>
    <h3 className="tw-font-semibold">Strip sequence {strip.strip_sequence}</h3><p className="tw-text-xs">{layout(strip)}</p><JsonView value={{ offset_kib: strip.chunk_offset, capacity_kib: strip.capacity, unit_kib: strip.unit_kb, sealed_length_kib: strip.sealed_length, placement_assessment: strip.placement_assessment }} />
    {fragments(strip).map((segment, index) => {
      const id = diskId(segment); const placement = detail.placements.find(value => value.disk_id.replace(/-/g, '').toLowerCase() === id);
      const ec = strip.strip?.EcStrip; const role = ec ? index < ec.data_num ? `Data ${index}` : `Parity ${index - ec.data_num}` : `Copy ${index}`;
      const unavailable = strip.unavailable_segments.some(value => identity(value) === identity(segment));
      return <div key={identity(segment)} className="tw-rounded tw-border tw-border-border tw-p-3 tw-space-y-2 tw-text-xs">
        <div className="tw-font-semibold">{role} {unavailable ? '· unavailable' : '· allocated'}</div><p className="tw-font-mono tw-break-all">Disk {id ?? 'Unknown'}</p>
        <p>Zone {segment.zone_index} · unit offset {segment.unit_offset} · {segment.unit_count} units</p>
        {placement ? <><p>Rack {placement.rack_id} / Node {placement.node_id} / DG {placement.disk_group_id}</p><p>Unit {placement.unit_size} bytes · offset within zone {(BigInt(segment.unit_offset) * BigInt(placement.unit_size)).toString()} bytes</p>
          <button className={buttonClass} onClick={() => onPlacement({ domain: Domain.Capacity, type: 'Disk', id: placement.disk_id, parentIds: { rack_id: placement.rack_id, node_id: placement.node_id, disk_group_id: placement.disk_group_id, disk_id: placement.disk_id } })}>Show disk capacity</button>
          <button className={buttonClass} onClick={() => onPlacement({ domain: Domain.Cluster, type: 'Node', id: placement.node_id, parentIds: { rack_id: placement.rack_id } })}>Show node</button></> : <p className="tw-text-muted">Placement unknown; Disk ID retained.</p>}
      </div>;
    })}
  </> : undefined}>
    <h1 className="tw-text-lg tw-font-semibold">Chunks / {kind ? kinds[Number(kind)] : 'All types'} / {prefix || 'All prefixes'}</h1>
    {busy && <p role="status" className="tw-text-xs tw-text-muted">Querying ChunkDB…</p>}{error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
    {page && <p className="tw-text-xs tw-text-muted">{page.owners} owners · scanned {page.scanned} records this page · observed {new Date(page.observed_at_ms).toLocaleTimeString()}</p>}
    {page?.owners === 0 && <p className="tw-text-sm tw-text-muted">No live ChunkDB service is registered. Deploy ChunkDB in Cluster before browsing chunks.</p>}
    {!!page?.failures.length && <div role="alert" className="tw-text-xs tw-text-failed">Partial result. Retry this scope after owner recovery; no continuation is issued while an owner is missing.<JsonView value={page.failures} /></div>}
    <table className="tw-w-full tw-text-sm" aria-label="Chunks"><thead className="tw-text-left tw-text-muted"><tr><th>Chunk ID</th><th>Type</th><th>State</th><th>Strips</th></tr></thead><tbody>{rows.map(chunk => <tr key={chunk.id_hex} className="tw-border-t tw-border-border"><td><button className="tw-py-2 tw-font-mono tw-text-xs tw-text-accent" disabled={busy} onClick={() => void inspect(chunk.id_hex)}>{chunk.id_hex}</button></td><td>{kinds[chunk.chunk_type] ?? `Unknown (${chunk.chunk_type})`}{parseInt(chunk.id_hex.slice(0, 2), 16) !== chunk.chunk_type && ' · type mismatch'}</td><td>{states[chunk.state] ?? chunk.state}</td><td>{chunk.strips.length}</td></tr>)}</tbody></table>
    {page && !rows.length && !busy && <p className="tw-text-xs tw-text-muted">No matching chunks in this scan window.{page.next && ' Continue scanning to find later matches.'}</p>}
    {page?.next && <button className={buttonClass} disabled={busy} onClick={() => void query(page.next!)}>Scan next window</button>}
    {chunk && detail && <section className="tw-space-y-3 tw-border-t tw-border-border tw-pt-4">
      <div className="tw-flex tw-justify-between"><h2 className="tw-font-semibold tw-font-mono tw-text-sm">{chunk.id_hex}</h2><button className={buttonClass} disabled={busy} onClick={() => void inspect(chunk.id_hex)}>Refresh layout</button></div>
      <JsonView value={{ state: states[chunk.state] ?? chunk.state, revision: chunk.modify_ts, capacity_kib: chunk.capacity, sealed_length_kib: chunk.sealed_length, acknowledged_cursor_bytes: chunk.acknowledged_cursor, writer_epoch: chunk.writer_epoch, strips: chunk.strips.length }} />
      <p className="tw-text-xs tw-text-muted">Layout observed {new Date(detail.observed_at_ms).toLocaleTimeString()} · validity {detail.layout_validity_ms} ms. Placement observed separately at {new Date(detail.placement_observed_at_ms).toLocaleTimeString()}. Refresh during conversion or migration.</p>
      {detail.placement_error && <p className="tw-text-xs tw-text-degraded">Placement unavailable: {detail.placement_error}</p>}
      <div className="tw-space-y-2" aria-label="Chunk strips">{ordered.slice(stripStart, stripStart + 20).map(strip => <button key={strip.strip_sequence} className={`${buttonClass} tw-w-full tw-text-left tw-space-y-2 ${stripSequence === strip.strip_sequence ? 'tw-border-accent' : ''}`} onClick={() => setStripSequence(strip.strip_sequence)}>
        <div>Sequence {strip.strip_sequence} · offset {strip.chunk_offset} KiB · {layout(strip)} {strip.placement_repair_required && '· repair required'}</div><div className="tw-flex tw-gap-2 tw-flex-wrap">{fragments(strip).slice(0, 32).map((fragment, index) => <span key={identity(fragment)} className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-2 tw-bg-accent/10">{strip.strip?.EcStrip ? index < strip.strip.EcStrip.data_num ? `D${index}` : `P${index - strip.strip.EcStrip.data_num}` : `Copy ${index}`} · {diskId(fragment)?.slice(0, 8) ?? 'Unknown'}</span>)}</div>
      </button>)}</div>
      {stripStart > 0 && <button className={buttonClass} onClick={() => setStripStart(value => Math.max(0, value - 20))}>Previous strips</button>}
      {ordered.length > stripStart + 20 && <button className={buttonClass} onClick={() => setStripStart(value => value + 20)}>Next 20 strips</button>}
    </section>}
  </Workbench>;
}
