// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense, useEffect, useState, useRef } from 'react';
import { getApiBase, getManagementToken, type ServerSummary } from '../api';
import { readJson } from '../access/native';
import { DocsHelp, Workbench, buttonClass } from '../access/Workbench';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import { initialSplitQuery, initialGraphQuery, type SplitQuery, type GraphQuery } from './query';
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
  const { checkpoint } = useDomain();
  const [query, setQuery] = useState<SplitQuery>(initialSplitQuery);
  const queryRef = useRef(query); queryRef.current = query;
  const changeQuery = (value: SplitQuery) => { queryRef.current = value; setQuery(value); };
  const [graph, setGraph] = useState<GraphQuery>(initialGraphQuery);
  const [expandedTree, setExpandedTree] = useState<string[] | undefined>();
  const restoreSelection = useRef<{ id: string; generation: string } | null>(null);
  const [propertyHost, setPropertyHost] = useState<HTMLDivElement | null>(null);
  const [page, setPage] = useState<CatalogPage | null>(null);
  const [cursor, setCursor] = useState<Cursor>({ page: 0, offset: 0 });
  const [refresh, setRefresh] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState<{ partition: Partition; generation: string; catalogPage: number; catalogOffset: number } | null>(null);
  useNavigationSnapshot(Domain.ChunkKV, 'catalog-query', () => {
    const window = { ...cursor, generation: page?.generation ?? cursor.generation };
    const selection = selected ? { id: selected.partition.id, generation: selected.generation } : null;
    const detail = { ...queryRef.current, stream: { ...queryRef.current.stream } };
    const chart = { ...graph, offsets: { ...graph.offsets }, collapsed: [...graph.collapsed] };
    const expansion = expandedTree ? [...expandedTree] : undefined;
    return () => {
      restoreSelection.current = selection; setCursor(window); changeQuery(detail); setGraph(chart); setExpandedTree(expansion);
      if (!selection) setSelected(null);
      setRefresh(value => value + 1);
    };
  });
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    setBusy(true); setError('');
    const query = new URLSearchParams({ page: String(cursor.page), offset: String(cursor.offset), ...(cursor.generation ? { generation: cursor.generation } : {}) });
    const token = getManagementToken();
    fetch(`${getApiBase()}/chunk-kv/catalog?${query}`, { signal: abort.signal, headers: token ? { Authorization: `Bearer ${token}` } : {} })
      .then(response => readJson<CatalogPage>(response))
      .then(result => {
        if (abort.signal.aborted) return;
        setPage(result);
        const restore = restoreSelection.current;
        if (!restore) return;
        restoreSelection.current = null;
        if (restore.generation !== result.generation) throw new Error('Catalog generation changed; refresh and select the current Split');
        const partition = result.entries.find(entry => entry.id === restore.id);
        if (!partition) { setSelected(null); throw new Error('Previously selected Split is no longer in this catalog window'); }
        setSelected({ partition, generation: result.generation, catalogPage: result.page, catalogOffset: result.offset });
      })
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
  const select = (partition: Partition) => {
    if (selected?.partition.id !== partition.id || selected.generation !== page!.generation) { checkpoint(); changeQuery(initialSplitQuery()); }
    setSelected({ partition, generation: page!.generation, catalogPage: page!.page, catalogOffset: page!.offset });
  };
  return <Workbench showActivity={false} help={<DocsHelp href="https://crowdb.dev/docs/deploy/chunk/" title="ChunkKV" description="Inspect partition placement and the current catalog generation." />} detail={<div ref={setPropertyHost} aria-label="ChunkKV properties">{!selected && <p className="tw-text-sm tw-text-muted">Select a Split to inspect its properties.</p>}</div>} sidebar={<nav aria-label="Partition placement" className="tw--mx-4">
    <Tree nodes={tree} expandedIds={expandedTree ?? expanded} onExpansionChange={setExpandedTree} onNodeClick={node => {
      if (node.type === 'Partition') {
        const entry = page?.entries.find(entry => entry.id === node.rawId);
        if (entry && !busy && !error) select(entry);
      }
    }} />
  </nav>}>
    <div className="tw-flex tw-items-center tw-justify-between"><h1 className="tw-text-lg tw-font-semibold">ChunkKV range distribution</h1><button className={buttonClass} disabled={busy} onClick={() => { restoreSelection.current = null; setCursor({ page: 0, offset: 0 }); setRefresh(value => value + 1); }}>Refresh catalog</button></div>
    {busy && <p role="status">Loading catalog…</p>}
    {error && <p role="alert" className="tw-text-failed">{error}{page && ' Previous observation remains visible.'}</p>}
    {!page && !busy && error && <p className="tw-text-sm tw-text-muted">Catalog unavailable. Check Group 0 and the current cluster's ChunkKV deployment in Cluster.</p>}
    {page && <>
      <p className="tw-text-xs tw-text-muted">Generation {page.generation} · catalog page {page.page + 1} / {page.catalog_pages} · {page.entries.length} loaded partitions. Catalog assignment; live serving state is not observed.</p>
      <Suspense fallback={<p role="status">Loading topology…</p>}><PartitionGraph entries={entries} servers={registered}
        query={graph} onQuery={setGraph} selectedId={selected?.partition.id} disabled={busy || !!error} onSelect={select}
        onTree={entry => { select(entry); changeQuery({ ...queryRef.current, tab: 'Tree' }); }} /></Suspense>
      {!entries.length && <p>No splits in this catalog window.</p>}
      {page.next && <button className={buttonClass} disabled={busy || !!error} onClick={() => { checkpoint(); setCursor({ ...page.next!, generation: page.generation }); }}>Next partitions</button>}
    </>}
    {selected && <PartitionDetail query={query} onQuery={changeQuery} propertyHost={propertyHost} key={`${selected.partition.id}/${selected.generation}`} {...selected} active={active && !error && selected.generation === page?.generation} currentGeneration={page?.generation} onChunk={onChunk} onBack={() => { checkpoint(); setSelected(null); }} />}
  </Workbench>;
}
