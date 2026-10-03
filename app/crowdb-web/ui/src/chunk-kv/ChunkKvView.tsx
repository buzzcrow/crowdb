// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense, useEffect, useState } from 'react';
import { getApiBase, getManagementToken, type ServerSummary } from '../api';
import { readJson } from '../access/native';
import { Workbench, buttonClass } from '../access/Workbench';
import type { Node, Rack } from '../types';
import { range, type CatalogPage, type Cursor, type Partition } from './catalog';
const PartitionGraph = lazy(() => import('./PartitionGraph').then(module => ({ default: module.PartitionGraph })));
import { PartitionDetail } from './PartitionDetail';
import { DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { Building2, FolderTree, Monitor, Cog, Layers } from 'lucide-react';
import { Tree, type TreeNode } from '../components/Tree';
import { serviceInstanceLabel } from '../services/client';

const endpointKey = (endpoint: string) => endpoint.replace(/^[a-z]+:\/\//, '').replace(/\/$/, '');

export function ChunkKvView({ active, racks, nodes, servers, onChunk }: { onChunk: (id: string) => void; active: boolean; racks: Rack[]; nodes: Node[]; servers: ServerSummary[] }) {
  const [detailView, setDetailView] = useState({ tab: 'Overview', revision: 0 });
  const [propertyHost, setPropertyHost] = useState<HTMLDivElement | null>(null);
  const [page, setPage] = useState<CatalogPage | null>(null);
  const [cursor, setCursor] = useState<Cursor>({ page: 0, offset: 0 });
  const [refresh, setRefresh] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
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
  const iconClass = 'tw-h-4 tw-w-4 tw-text-muted';
  const registered = servers.filter(server => ['chunk-kv', 'chunk_kv'].includes(server.service_type));
  const serverNodes = (nodeId?: number): TreeNode[] => registered.filter(server => server.node_id === nodeId).map(server => {
    const partitions = (page?.entries ?? []).filter(entry => endpointKey(entry.endpoint) === endpointKey(server.rpc_url ?? server.endpoint ?? ''));
    return { id: `ckv-server-${server.id ?? server.rpc_url}`, label: serviceInstanceLabel('chunk-kv', server.id ?? String(nodeId)), type: 'Server',
      title: `${partitions.length} splits in loaded catalog window`, icon: <Cog className={iconClass} />,
      children: partitions.map(entry => ({ id: `ckv-split-${entry.id}`, rawId: entry.id, label: `Split ${entry.id.slice(0, 4)}…${entry.id.slice(-4)}`,
        title: `${entry.id} · ${range(entry)} · ${entry.state}`, type: 'Partition', icon: <Layers className={iconClass} /> })) };
  });
  const tree: TreeNode[] = [{ id: 'ckv-datacenter', label: DEFAULT_DC_NAME, type: 'Datacenter', icon: <Building2 className={iconClass} />,
    children: racks.map(rack => ({ id: `ckv-rack-${rack.id}`, label: `R-${rack.id}`, type: 'Rack', icon: <FolderTree className={iconClass} />,
      children: nodes.filter(node => node.rack_id === rack.id).map(node => ({ id: `ckv-node-${node.id}`, label: `N-${node.id}`, type: 'Node',
        icon: <Monitor className={iconClass} />, children: serverNodes(node.id) })) })) }];
  const expanded = ['ckv-datacenter', ...racks.map(rack => `ckv-rack-${rack.id}`), ...nodes.map(node => `ckv-node-${node.id}`)];
  const entries = page?.entries ?? [];
  const select = (partition: Partition) => setSelected({ partition, generation: page!.generation, catalogPage: page!.page, catalogOffset: page!.offset });
  return <Workbench showActivity={false} detail={<div ref={setPropertyHost} aria-label="Chunk-KV properties">{!selected && <p className="tw-text-sm tw-text-muted">Select a Split to inspect its properties.</p>}</div>} sidebar={<nav aria-label="Partition placement" className="tw--mx-4">
    <Tree nodes={tree} defaultExpandedIds={expanded} onNodeClick={node => {
      if (node.type === 'Partition') {
        const entry = page?.entries.find(entry => entry.id === node.rawId);
        if (entry && !busy && !error) select(entry);
      }
    }} />
  </nav>}>
    <div className="tw-flex tw-items-center tw-justify-between"><h1 className="tw-text-lg tw-font-semibold">Chunk-KV range distribution</h1><button className={buttonClass} disabled={busy} onClick={() => { setCursor({ page: 0, offset: 0 }); setRefresh(value => value + 1); }}>Refresh catalog</button></div>
    {busy && <p role="status">Loading catalog…</p>}
    {error && <p role="alert" className="tw-text-failed">{error}{page && ' Previous observation remains visible.'}</p>}
    {!page && !busy && error && <p className="tw-text-sm tw-text-muted">Catalog unavailable. Check Group 0 and the current cluster's Chunk-KV deployment in Cluster.</p>}
    {page && <>
      <p className="tw-text-xs tw-text-muted">Generation {page.generation} · catalog page {page.page + 1} / {page.catalog_pages} · {page.entries.length} loaded partitions. Catalog assignment; live serving state is not observed.</p>
      <Suspense fallback={<p role="status">Loading topology…</p>}><PartitionGraph key={`${page.generation}/${page.page}/${page.offset}`} entries={entries} servers={registered}
        selectedId={selected?.partition.id} disabled={busy || !!error} onSelect={select}
        onTree={entry => { select(entry); setDetailView(value => ({ tab: 'Tree', revision: value.revision + 1 })); }} /></Suspense>
      {!entries.length && <p>No splits in this catalog window.</p>}
      {page.next && <button className={buttonClass} disabled={busy || !!error} onClick={() => { setCursor({ ...page.next!, generation: page.generation }); }}>Next partitions</button>}
    </>}
    {selected && <PartitionDetail requestedView={detailView} propertyHost={propertyHost} key={`${selected.partition.id}/${selected.generation}`} {...selected} active={active && !error && selected.generation === page?.generation} currentGeneration={page?.generation} onChunk={onChunk} onBack={() => setSelected(null)} />}
  </Workbench>;
}
