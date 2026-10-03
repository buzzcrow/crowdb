// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useRef, useEffect, useCallback } from 'react';
import { getApiBase } from '../api';
import { readJson } from '../access/native';
import { Workbench, JsonView, inputClass, buttonClass } from '../access/Workbench';
import './chunk-browser.css';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { Domain, type Rack, type Node } from '../types';
import type { ServerSummary } from '../api';
import { ChunkHierarchy } from './ChunkHierarchy';

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

export function ChunkBrowser({ active, onPlacement, openRequest, racks, nodes, servers }: { racks: Rack[]; nodes: Node[]; servers: ServerSummary[]; openRequest?: { id: string; nonce: number }; active: boolean; onPlacement: (entity: SelectedEntity) => void }) {
  const [blockIndex, setBlockIndex] = useState<number | null>(null);
  const [filter, setFilter] = useState('');
  const [kind, setKind] = useState('');
  const [lookup, setLookup] = useState('');
  const [page, setPage] = useState<ChunkPage | null>(null);
  const [rows, setRows] = useState<Chunk[]>([]);
  const [windowStarts, setWindowStarts] = useState<Array<string | undefined>>([undefined]);
  const [windowIndex, setWindowIndex] = useState(0);
  const [detail, setDetail] = useState<ChunkDetail | null>(null);
  const [stripSequence, setStripSequence] = useState<number | null>(null);
  const [stripStart, setStripStart] = useState(0);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const revision = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const loadedKind = useRef<string | null>(null);
  const openedRequest = useRef<number | null>(null);
  const query = useCallback(async (after?: string, index = 0) => {
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    const version = ++revision.current;
    setBusy(true); setError('');
    if (!after) { setRows([]); setPage(null); setDetail(null); }
    try {
      const search = new URLSearchParams({ limit: '10', ...(kind ? { chunk_type: kind } : {}), ...(after ? { after } : {}) });
      const result = await readJson<ChunkPage>(await fetch(`${getApiBase()}/chunks?${search}`, { signal: request.signal }));
      if (version === revision.current) {
        loadedKind.current = kind; setPage(result); setRows(result.chunks); setDetail(null);
        setWindowIndex(index);
        setWindowStarts(previous => [...previous.slice(0, index), after]);
      }
    } catch (error) { if (version === revision.current) setError(String(error)); }
    finally { if (version === revision.current) setBusy(false); }
  }, [kind]);
  const inspect = useCallback(async (id: string) => {
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    const version = ++revision.current;
    setDetail(null); setStripSequence(null); setBlockIndex(null); setStripStart(0); setBusy(true); setError('');
    try {
      const result = await readJson<ChunkDetail>(await fetch(`${getApiBase()}/chunks/${encodeURIComponent(id)}`, { signal: request.signal }));
      if (version === revision.current) setDetail(result);
    } catch (error) { if (version === revision.current) setError(String(error)); }
    finally { if (version === revision.current) setBusy(false); }
  }, []);
  useEffect(() => {
    if (active) {
      if (openRequest && openedRequest.current !== openRequest.nonce) {
        if (kind) { setKind(''); return; }
        openedRequest.current = openRequest.nonce;
        loadedKind.current = kind;
        setLookup(openRequest.id); setRows([]); setPage(null);
        void inspect(openRequest.id);
      } else if (loadedKind.current !== kind) void query();
    }
    return () => { controller.current?.abort(); ++revision.current; setBusy(false); };
  }, [active, kind, query, inspect, openRequest]);
  const chunk = detail?.chunk;
  const strip = chunk?.strips.find(strip => strip.strip_sequence === stripSequence);
  const ordered = [...(chunk?.strips ?? [])].sort((a, b) => a.chunk_offset - b.chunk_offset || a.strip_sequence - b.strip_sequence);
  const fragments = (strip: Strip) => strip.strip?.MirrorStrip?.segments ?? strip.strip?.EcStrip?.segments ?? [];
  const layout = (strip: Strip) => strip.strip?.MirrorStrip ? `Mirror ×${strip.strip.MirrorStrip.segments.length}` : strip.strip?.EcStrip ? `EC ${strip.strip.EcStrip.data_num}+${strip.strip.EcStrip.code_num} · ${strip.strip.EcStrip.ec_state === 1 ? 'Parity' : 'No parity'}` : `Unknown layout (${strip.strip_type})`;
  const visibleRows = rows.filter(value => `${value.id_hex} ${kinds[value.chunk_type]} ${states[value.state]}`.toLowerCase().includes(filter.toLowerCase()));
  const fields = (values: Record<string, unknown>) => <dl className="chunk-properties">{Object.entries(values).map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{value == null ? 'Unknown' : typeof value === 'object' ? JSON.stringify(value) : String(value)}</dd></div>)}</dl>;
  return <Workbench showActivity={false} sidebar={<ChunkHierarchy active={active} racks={racks} nodes={nodes} servers={servers} />} detail={chunk && detail ? <section aria-label="Chunk properties" className="tw-space-y-3">
    <h3 className="tw-font-semibold">{strip ? blockIndex === null ? `Strip sequence ${strip.strip_sequence}` : `${layout(strip)} · Block ${blockIndex + 1}` : 'Chunk properties'}</h3>
    {strip ? <>
      {fields({ Sequence: strip.strip_sequence, Layout: layout(strip), 'Logical offset (KiB)': strip.chunk_offset, 'Capacity (KiB)': strip.capacity, 'Sealed (KiB)': strip.sealed_length, 'Unit (KiB)': strip.unit_kb, 'Repair required': strip.placement_repair_required })}
      {fragments(strip).map((segment, index) => {
        if (blockIndex !== null && blockIndex !== index) return null;
        const id = diskId(segment); const placement = detail.placements.find(value => value.disk_id.replace(/-/g, '').toLowerCase() === id);
        const ec = strip.strip?.EcStrip; const role = ec ? index < ec.data_num ? `Data ${index}` : `Parity ${index - ec.data_num}` : `Mirror ${index + 1}`;
        const unavailable = strip.unavailable_segments.some(value => identity(value) === identity(segment));
        return <section key={identity(segment)} className="tw-border-t tw-border-border tw-pt-3 tw-space-y-2">
          <h4 className="tw-text-sm tw-font-semibold">{role} · {unavailable ? 'unavailable' : 'allocated'}</h4>
          {fields({ Disk: id, Rack: placement?.rack_id, Node: placement?.node_id, Diskgroup: placement?.disk_group_id, Zone: segment.zone_index, 'Zone offset (units)': segment.unit_offset, 'Unit count': segment.unit_count, 'Unit size (bytes)': placement?.unit_size, 'Zone offset': placement ? `${BigInt(segment.unit_offset) * BigInt(placement.unit_size)} bytes` : undefined, 'Allocation timestamp': segment.allocation_ts })}
          {placement && <><button className={buttonClass} onClick={() => onPlacement({ domain: Domain.Capacity, type: 'Disk', id: placement.disk_id, parentIds: { rack_id: placement.rack_id, node_id: placement.node_id, disk_group_id: placement.disk_group_id, disk_id: placement.disk_id } })}>Show disk capacity</button><button className={buttonClass} onClick={() => onPlacement({ domain: Domain.Cluster, type: 'Node', id: placement.node_id, parentIds: { rack_id: placement.rack_id } })}>Show node</button></>}
        </section>;
      })}
    </> : fields({ ID: chunk.id_hex, Type: kinds[chunk.chunk_type], State: states[chunk.state], 'Capacity (KiB)': chunk.capacity, 'Sealed (KiB)': chunk.sealed_length, 'Acknowledged (bytes)': chunk.acknowledged_cursor, Revision: chunk.modify_ts, 'Writer epoch': chunk.writer_epoch, Strips: chunk.strips.length })}
    {fields({ 'Layout observed': new Date(detail.observed_at_ms).toLocaleTimeString(), 'Placement observed': new Date(detail.placement_observed_at_ms).toLocaleTimeString(), 'Validity (ms)': detail.layout_validity_ms })}
  </section> : undefined}>
    <div className="chunk-toolbar"><h1 className="tw-text-lg tw-font-semibold">Chunks</h1>
      <select aria-label="Chunk type" className={inputClass} value={kind} onChange={event => { setKind(event.target.value); setFilter(''); }}><option value="">All types</option>{kinds.map((name, type) => <option key={name} value={type}>{name}</option>)}</select>
      <input aria-label="Filter current window" placeholder="Filter this window" className={inputClass} value={filter} onChange={event => setFilter(event.target.value)} />
      <button className={buttonClass} disabled={busy} onClick={() => void query()}>Refresh chunks</button>
    </div>
    <form className="chunk-toolbar" aria-label="Exact chunk lookup" onSubmit={event => { event.preventDefault(); void inspect(lookup.toLowerCase()); }}>
      <label className="tw-flex tw-items-center tw-gap-2 tw-text-xs">Exact Chunk ID<input className={`${inputClass} tw-font-mono`} style={{ width: '34ch' }} required pattern="[0-9a-fA-F]{32}" value={lookup} onChange={event => setLookup(event.target.value)} placeholder="32-digit chunk ID" /></label>
      <button className={buttonClass} disabled={busy}>Lookup ID</button>
    </form>
    {busy && <p role="status" className="tw-text-xs tw-text-muted">Querying ChunkDB…</p>}{error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
    {page && <p className="tw-text-xs tw-text-muted">{page.owners} owners · scanned {page.scanned} records this page · observed {new Date(page.observed_at_ms).toLocaleTimeString()}</p>}
    {page?.owners === 0 && <p className="tw-text-sm tw-text-muted">No live ChunkDB service is registered. Deploy ChunkDB in Cluster before browsing chunks.</p>}
    {!!page?.failures.length && <div role="alert" className="tw-text-xs tw-text-failed">Partial result. Retry this scope after owner recovery; no continuation is issued while an owner is missing.<JsonView value={page.failures} /></div>}
    <div className="chunk-list"><table className="tw-w-full tw-text-sm" aria-label="Chunks"><thead><tr><th>Chunk ID</th><th>Type</th><th>State</th><th>Strips</th></tr></thead><tbody>{visibleRows.map(value => <tr key={value.id_hex} aria-selected={chunk?.id_hex === value.id_hex}><td><button className="tw-font-mono tw-text-xs" disabled={busy} onClick={() => void inspect(value.id_hex)}>{value.id_hex}</button></td><td>{kinds[value.chunk_type] ?? `Unknown (${value.chunk_type})`}</td><td>{states[value.state] ?? value.state}</td><td>{value.strips.length}</td></tr>)}</tbody></table></div>
    {!!rows.length && !visibleRows.length && <p className="tw-text-xs tw-text-muted">No matches in this window. Clear the filter or continue to the next window.</p>}
    {page && !rows.length && !busy && <p className="tw-text-xs tw-text-muted">No matching chunks in this scan window.{page.next && ' Continue scanning to find later matches.'}</p>}
    {page && <nav aria-label="Chunk page window" className="tw-flex tw-items-center tw-gap-3">
      <button className={buttonClass} disabled={busy || windowIndex === 0} onClick={() => void query(windowStarts[windowIndex - 1], windowIndex - 1)}>Prev</button>
      <span className="tw-text-xs tw-text-muted">Window {windowIndex + 1} · {rows.length} chunks · at most 10 scanned</span>
      <button className={buttonClass} disabled={busy || !page.next} onClick={() => void query(page.next!, windowIndex + 1)}>Next</button>
    </nav>}
    {chunk && detail && <section aria-label="Chunk layout" className="tw-space-y-3 tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4">
      <div className="chunk-toolbar"><button className="tw-text-left" onClick={() => { setStripSequence(null); setBlockIndex(null); }}><h2 aria-label={chunk.id_hex} className="tw-font-mono tw-text-sm tw-break-all">{chunk.id_hex}</h2><span className="tw-text-xs tw-text-muted">{kinds[chunk.chunk_type]} · {states[chunk.state]} · {chunk.capacity / 1024} MiB · {chunk.strips.length} strips</span></button><button className={`${buttonClass} tw-ml-auto`} disabled={busy} onClick={() => void inspect(chunk.id_hex)}>Refresh layout</button></div>
      {detail.placement_error && <p className="tw-text-xs tw-text-degraded">Placement unavailable: {detail.placement_error}</p>}
      <div className="chunk-strip-list" aria-label="Chunk strips">{ordered.slice(stripStart, stripStart + 16).map(strip => <section key={strip.strip_sequence} data-testid="chunk-strip" className={`chunk-strip ${stripSequence === strip.strip_sequence ? 'is-selected' : ''}`}>
        <button className="chunk-strip-heading" aria-label={`Sequence ${strip.strip_sequence} · ${layout(strip)}`} onClick={() => { setStripSequence(strip.strip_sequence); setBlockIndex(null); }}>
          <strong>Strip {strip.strip_sequence} · {strip.strip?.MirrorStrip ? 'Mirror' : strip.strip?.EcStrip ? `EC ${strip.strip.EcStrip.data_num}+${strip.strip.EcStrip.code_num}` : 'Unknown'}</strong><span>[{strip.chunk_offset / 1024}M, {(strip.chunk_offset + strip.capacity) / 1024}M){strip.placement_repair_required && <span className="tw-text-degraded" title="Placement repair required" aria-label="Placement repair required"> ⚠</span>}</span>
        </button>
        <div className="chunk-blocks">{fragments(strip).slice(0, 32).map((fragment, index) => {
          const ec = strip.strip?.EcStrip;
          const parity = ec && index >= ec.data_num;
          const role = ec ? parity ? `Parity ${index - ec.data_num}` : `Data ${index}` : `Mirror ${index + 1}`;
          const unavailable = strip.unavailable_segments.some(value => identity(value) === identity(fragment));
          return <button key={identity(fragment)} data-testid="chunk-disk-block" aria-label={`${role}${unavailable ? ' · unavailable' : ''}`} title={`${role}${unavailable ? ' · unavailable' : ''} — click for properties`} aria-pressed={stripSequence === strip.strip_sequence && blockIndex === index} className={`chunk-block ${ec ? parity ? 'parity' : 'data' : 'mirror'} ${unavailable ? 'unavailable' : ''}`} onClick={() => { setStripSequence(strip.strip_sequence); setBlockIndex(index); }}>
            <strong>{role}</strong>
          </button>;
        })}</div>
        {fragments(strip).length === 0 && <span className="tw-text-xs tw-text-muted">No allocated disk blocks</span>}
        {fragments(strip).length > 32 && <span className="tw-text-xs tw-text-muted">First 32 blocks; select strip for all details.</span>}
      </section>)}</div>
      <nav aria-label="Strip pages" className="chunk-toolbar"><button className={buttonClass} disabled={stripStart === 0} onClick={() => setStripStart(value => Math.max(0, value - 16))}>Previous strips</button><span className="tw-text-xs tw-text-muted">{ordered.length ? stripStart + 1 : 0}–{Math.min(stripStart + 16, ordered.length)} / {ordered.length} strips</span><button className={buttonClass} disabled={ordered.length <= stripStart + 16} onClick={() => setStripStart(value => value + 16)}>Next 16 strips</button></nav>
    </section>}
  </Workbench>;
}
