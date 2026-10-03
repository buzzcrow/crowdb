// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useState } from 'react';
import { getApiBase, getManagementToken, type ServerSummary } from '../api';
import { readJson } from '../access/native';
import { Workbench, buttonClass, inputClass } from '../access/Workbench';
import type { Node, Rack } from '../types';
import { range, type CatalogPage, type Cursor, type Partition } from './catalog';
import { PartitionDetail } from './PartitionDetail';

const endpointKey = (endpoint: string) => endpoint.replace(/^[a-z]+:\/\//, '').replace(/\/$/, '');

export function ChunkKvView({ active, racks, nodes, servers }: { active: boolean; racks: Rack[]; nodes: Node[]; servers: ServerSummary[] }) {
  const [page, setPage] = useState<CatalogPage | null>(null);
  const [cursor, setCursor] = useState<Cursor>({ page: 0, offset: 0 });
  const [refresh, setRefresh] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [filter, setFilter] = useState('');
  const [owner, setOwner] = useState('');
  const [selected, setSelected] = useState<{ partition: Partition; generation: string; catalogPage: number; catalogOffset: number } | null>(null);
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    setBusy(true); setError('');
    const query = new URLSearchParams({ page: String(cursor.page), offset: String(cursor.offset), ...(cursor.generation ? { generation: cursor.generation } : {}) });
    const token = getManagementToken();
    fetch(`${getApiBase()}/chunk-kv/catalog?${query}`, { signal: abort.signal, headers: token ? { Authorization: `Bearer ${token}` } : {} })
      .then(response => readJson<CatalogPage>(response))
      .then(result => { if (!abort.signal.aborted) setPage(result); })
      .catch(error => { if (!abort.signal.aborted) setError(String(error)); })
      .finally(() => { if (!abort.signal.aborted) setBusy(false); });
    return () => abort.abort();
  }, [active, cursor, refresh]);
  const owners = useMemo(() => {
    const groups = new Map<string, { node?: Node; entries: Partition[] }>();
    for (const entry of page?.entries ?? []) {
      const server = servers.find(server => ['chunk-kv', 'chunk_kv'].includes(server.service_type) && endpointKey(server.rpc_url ?? server.endpoint ?? '') === endpointKey(entry.endpoint));
      const node = nodes.find(node => node.id === server?.node_id);
      const group = groups.get(entry.owner_id) ?? { node, entries: [] };
      group.entries.push(entry); groups.set(entry.owner_id, group);
    }
    return [...groups.entries()];
  }, [page, servers, nodes]);
  const placements = useMemo(() => {
    const groups = new Map<string, Map<string, typeof owners>>();
    for (const entry of owners) {
      const node = entry[1].node;
      const rackKey = node ? String(node.rack_id) : 'Unresolved placement';
      const nodeKey = node ? String(node.id) : 'Unknown node';
      const rack = groups.get(rackKey) ?? new Map<string, typeof owners>();
      rack.set(nodeKey, [...(rack.get(nodeKey) ?? []), entry]); groups.set(rackKey, rack);
    }
    return [...groups.entries()];
  }, [owners]);
  const entries = (page?.entries ?? []).filter(entry => (!owner || entry.owner_id === owner) && entry.id.includes(filter.trim().toLowerCase()));
  const select = (partition: Partition) => setSelected({ partition, generation: page!.generation, catalogPage: page!.page, catalogOffset: page!.offset });
  return <Workbench sidebar={<>
    <h2 className="tw-font-semibold">Chunk-KV</h2>
    <p className="tw-text-xs tw-text-muted">Assigned partitions on the loaded catalog page</p>
    <button className={buttonClass} onClick={() => setOwner('')}>All loaded servers</button>
    <nav aria-label="Partition placement" className="tw-space-y-3">
      {placements.map(([rackId, rackNodes]) => <details key={rackId} open><summary className="tw-text-sm">{rackId === 'Unresolved placement' ? rackId : `Rack ${racks.find(rack => String(rack.id) === rackId)?.name || rackId}`}</summary>
        {[...rackNodes].map(([nodeId, members]) => <details key={nodeId} open className="tw-pl-3"><summary className="tw-text-sm">{nodeId === 'Unknown node' ? nodeId : `Node ${nodeId}`}</summary>
          {members.map(([id, group]) => <details key={id} open className="tw-pl-3"><summary><button className="tw-text-xs tw-text-accent" aria-pressed={owner === id} onClick={() => setOwner(id)}>Chunk-KV Server {id}</button></summary>
            {group.entries.map(entry => <button key={entry.id} className="tw-block tw-py-1 tw-text-xs tw-font-mono tw-break-all tw-text-left" aria-pressed={selected?.partition.id === entry.id} onClick={() => select(entry)}>{entry.id.slice(-8)} · {range(entry)}</button>)}
          </details>)}
        </details>)}
      </details>)}
    </nav>
  </>}>
    <div className="tw-flex tw-items-center tw-justify-between"><h1 className="tw-text-lg tw-font-semibold">Chunk-KV range distribution</h1><button className={buttonClass} disabled={busy} onClick={() => { setCursor({ page: 0, offset: 0 }); setRefresh(value => value + 1); }}>Refresh catalog</button></div>
    {busy && <p role="status">Loading catalog…</p>}
    {error && <p role="alert" className="tw-text-failed">{error}{page && ' Previous observation remains visible.'}</p>}
    {!page && !busy && error && <p className="tw-text-sm tw-text-muted">Catalog unavailable. Check Group 0 and the current cluster's Chunk-KV deployment in Cluster.</p>}
    {page && <>
      <p className="tw-text-xs tw-text-muted">Generation {page.generation} · catalog page {page.page + 1} / {page.catalog_pages} · {page.entries.length} loaded partitions. Catalog assignment; live serving state is not observed.</p>
      <label className="tw-flex tw-gap-2 tw-items-center tw-text-sm">Filter loaded partition IDs<input className={inputClass} value={filter} onChange={event => setFilter(event.target.value)} /></label>
      <p className="tw-text-xs tw-text-muted">Ordered range blocks. Width does not represent data size. Only the loaded catalog window is shown.</p>
      <div aria-label="Partition range map" className="tw-space-y-4">
        {owners.filter(([id]) => !owner || owner === id).map(([id]) => <section key={id}><h2 className="tw-text-sm tw-mb-2">Server {id}</h2><div className="tw-grid tw-grid-cols-[repeat(auto-fill,minmax(220px,1fr))] tw-gap-2">
          {entries.filter(entry => entry.owner_id === id).map(entry => <button key={entry.id} aria-label={`Partition ${entry.id}`} aria-pressed={selected?.partition.id === entry.id} disabled={busy || !!error} className={`${buttonClass} tw-text-left tw-space-y-2 ${selected?.partition.id === entry.id ? 'tw-border-accent tw-bg-accent/10' : ''}`} onClick={() => select(entry)}>
            <span className="tw-block tw-font-mono tw-break-all">{entry.id}</span><span className="tw-block tw-font-mono tw-break-all">{range(entry)}</span><span className="tw-block">{entry.state} · epoch {entry.epoch}</span>{entry.artifact.tail_overlay && <span className="tw-block tw-text-degraded">Parent recovery dependency</span>}
          </button>)}
        </div></section>)}
      </div>
      {!entries.length && <p>No matches in the loaded catalog window.</p>}
      {page.next && <button className={buttonClass} disabled={busy || !!error} onClick={() => { setOwner(''); setCursor({ ...page.next!, generation: page.generation }); }}>Next partitions</button>}
    </>}
    {selected && <PartitionDetail key={`${selected.partition.id}/${selected.generation}`} {...selected} active={active && !error && selected.generation === page?.generation} currentGeneration={page?.generation} onBack={() => setSelected(null)} />}
  </Workbench>;
}
