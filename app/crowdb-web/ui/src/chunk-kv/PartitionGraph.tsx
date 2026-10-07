// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef } from 'react';
import type { GraphQuery } from './query';
import ReactFlow, { ReactFlowProvider, Background, Handle, Position, type Node, type Edge, type NodeProps, useNodesInitialized, useReactFlow, useNodesState } from 'reactflow';
import 'reactflow/dist/style.css';
import { Boxes, ChevronDown, ChevronRight, Cog, Database, FolderTree, ScanSearch } from 'lucide-react';
import type { ServerSummary } from '../api';
import { buttonClass } from '../access/Workbench';
import { serviceInstanceLabel } from '../services/client';
import { range, type Partition } from './catalog';

const endpointKey = (value: string) => value.replace(/^[a-z]+:\/\//, '').replace(/\/$/, '');
const SERVER_LIMIT = 8;
const SPLIT_LIMIT = 5;

interface Card {
  label: string;
  subtitle: string;
  title?: string;
  accessible?: string;
  selected?: boolean;
  disabled?: boolean;
  collapsed?: boolean;
  kind: 'root' | 'server' | 'split' | 'tree';
  click?: () => void;
  previous?: () => void;
  next?: () => void;
}

const iconForKind = {
  root: Database,
  server: Cog,
  split: Boxes,
  tree: FolderTree,
} as const;
const layerY = { root: 0, server: 130, split: 260, tree: 390 } as const;

function GraphNode({ data }: NodeProps<Card>) {
  const Icon = iconForKind[data.kind];
  return <div style={{ pointerEvents: 'auto' }} className={`tw-relative tw-border tw-rounded-lg tw-px-3 tw-py-2 tw-w-[160px] tw-min-w-[160px] tw-shadow-sm tw-transition-all tw-bg-panel ${data.selected ? 'tw-border-accent tw-ring-2 tw-ring-accent/30' : 'tw-border-border'}`}>
    {data.kind !== 'root' && <Handle type="target" position={Position.Top} isConnectable={false} />}
    <div className="tw-flex tw-items-stretch tw-justify-center">
      <button className="nodrag tw-min-w-0 tw-flex-1 tw-p-0 tw-text-center disabled:tw-opacity-50" title={data.title} disabled={data.disabled || !data.click}
        aria-label={data.accessible ?? data.label} aria-pressed={data.selected} onClick={data.click}>
        <span className="tw-flex tw-items-center tw-justify-center tw-gap-1.5">
          <Icon data-testid={`chunk-kv-icon-${data.kind}`} className={`tw-h-3.5 tw-w-3.5 tw-flex-shrink-0 ${data.kind === 'split' ? 'tw-text-accent' : data.kind === 'tree' ? 'tw-text-healthy' : 'tw-text-accent2'}`} />
          <span className="tw-block tw-text-sm tw-font-medium tw-truncate tw-text-text">{data.label}</span>
        </span>
        <span className="tw-block tw-mt-1 tw-text-[10px] tw-text-muted tw-truncate">{data.subtitle}</span>
      </button>
      {data.kind === 'server' && data.click && <button className="nodrag tw-absolute tw-right-1 tw-top-1/2 tw-p-1 tw-text-muted hover:tw-text-text" style={{ transform: 'translateY(-50%)' }} title={data.collapsed ? `Expand ${data.label}` : `Collapse ${data.label}`} aria-label={data.collapsed ? `Expand ${data.label}` : `Collapse ${data.label}`} onClick={data.click}>
        {data.collapsed ? <ChevronRight className="tw-h-4 tw-w-4" /> : <ChevronDown className="tw-h-4 tw-w-4" />}
      </button>}
    </div>
    {(data.previous || data.next) && <div className="nodrag tw-flex tw-justify-between tw-px-2 tw-pb-2">
      <button className={buttonClass} aria-label={`Previous splits for ${data.label}`} disabled={!data.previous} onClick={data.previous}>Prev</button>
      <button className={buttonClass} aria-label={`Next splits for ${data.label}`} disabled={!data.next} onClick={data.next}>Next</button>
    </div>}
    {data.kind !== 'tree' && <Handle type="source" position={Position.Bottom} isConnectable={false} />}
  </div>;
}
const nodeTypes = { chunkKv: GraphNode };

function FitVisibleGraph({ layoutKey }: { layoutKey: string }) {
  const initialized = useNodesInitialized();
  const { fitView } = useReactFlow();
  const marker = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    const container = marker.current?.closest<HTMLElement>('[data-testid="chunk-kv-graph"]');
    if (!initialized || !container) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry.contentRect.width > 0 && entry.contentRect.height > 0) void fitView({ padding: 0.15 });
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, [initialized, fitView, layoutKey]);
  return <span ref={marker} hidden />;
}

function GraphCanvas({ layout, edges, layoutKey }: { layout: Node<Card>[]; edges: Edge[]; layoutKey: string }) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Card>(layout);
  const { fitView } = useReactFlow();
  useEffect(() => {
    setNodes(previous => {
      // Keep measured dimensions across catalog/selection updates; otherwise
      // unchanged cards can remain hidden awaiting a size change that never occurs.
      const measured = new Map(previous.map(node => [node.id, node]));
      return layout.map(node => ({ ...node, width: measured.get(node.id)?.width, height: measured.get(node.id)?.height }));
    });
  }, [layout, setNodes]);
  return <ReactFlow id="chunk-kv" nodes={nodes} edges={edges} onNodesChange={onNodesChange} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.15 }}
    minZoom={0.1} maxZoom={2} nodesFocusable={false} edgesFocusable={false} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false} preventScrolling={false} proOptions={{ hideAttribution: true }}>
    <FitVisibleGraph layoutKey={layoutKey} />
    <Background gap={24} color="#2e3440" />
    <div className="tw-absolute tw-top-3 tw-right-3 tw-z-10">
      <button className={buttonClass} data-testid="chunk-kv-fit-all" aria-label="Fit all" onClick={() => void fitView({ padding: 0.15, duration: 250 })}>
        <ScanSearch className="tw-mr-1.5 tw-h-3.5 tw-w-3.5" /> Fit All
      </button>
    </div>
  </ReactFlow>;
}

export function PartitionGraph({ entries, servers, selectedId, disabled, onSelect, onTree, query, onQuery }: {
  query: GraphQuery; onQuery: (query: GraphQuery) => void;
  entries: Partition[]; servers: ServerSummary[]; selectedId?: string; disabled: boolean;
  onSelect: (partition: Partition) => void; onTree: (partition: Partition) => void;
}) {
  const { serverPage, offsets } = query;
  const collapsed = new Set(query.collapsed);
  const setServerPage = (serverPage: number) => onQuery({ ...query, serverPage });
  const setOffsets = (update: (value: Record<string, number>) => Record<string, number>) => onQuery({ ...query, offsets: update(offsets) });
  const setCollapsed = (update: (value: Set<string>) => Set<string>) => onQuery({ ...query, collapsed: [...update(collapsed)] });
  const groups = servers.map(server => ({
    id: `server-${server.id ?? server.rpc_url ?? server.endpoint}`,
    label: serviceInstanceLabel('chunk-kv', server.id ?? String(server.node_id)),
    entries: entries.filter(entry => endpointKey(entry.endpoint) === endpointKey(server.rpc_url ?? server.endpoint ?? '')),
  }));
  const matched = new Set(groups.flatMap(group => group.entries.map(entry => entry.id)));
  for (const entry of entries) {
    if (matched.has(entry.id)) continue;
    const id = `owner-${entry.owner_id}`;
    let group = groups.find(group => group.id === id);
    if (!group) { group = { id, label: `CKV-${entry.owner_id}`, entries: [] }; groups.push(group); }
    group.entries.push(entry);
  }
  const page = Math.min(serverPage, Math.max(0, Math.ceil(groups.length / SERVER_LIMIT) - 1));
  const visible = groups.slice(page * SERVER_LIMIT, (page + 1) * SERVER_LIMIT);
  const nodes: Node<Card>[] = [];
  const edges: Edge[] = [];
  let x = 0;
  const connect = (source: string, target: string, type: Edge['type'] = 'smoothstep') => edges.push({ id: `${source}/${target}`, source, target, type });
  for (const group of visible) {
    const offset = Math.min(offsets[group.id] ?? 0, Math.max(0, Math.floor((group.entries.length - 1) / SPLIT_LIMIT) * SPLIT_LIMIT));
    const children = collapsed.has(group.id) ? [] : group.entries.slice(offset, offset + SPLIT_LIMIT);
    const width = children.length ? (children.length - 1) * 195 + 160 : 160;
    nodes.push({ id: group.id, type: 'chunkKv', position: { x: x + (width - 160) / 2, y: layerY.server },
      data: { kind: 'server', label: group.label, collapsed: collapsed.has(group.id), subtitle: `${group.entries.length} splits in window · ${collapsed.has(group.id) ? 'Expand' : 'Collapse'}`,
        click: () => setCollapsed(previous => { const next = new Set(previous); if (next.has(group.id)) next.delete(group.id); else next.add(group.id); return next; }),
        previous: offset > 0 ? () => setOffsets(value => ({ ...value, [group.id]: offset - SPLIT_LIMIT })) : undefined,
        next: offset + SPLIT_LIMIT < group.entries.length ? () => setOffsets(value => ({ ...value, [group.id]: offset + SPLIT_LIMIT })) : undefined } });
    connect('root', group.id);
    children.forEach((entry, index) => {
      const id = `split-${entry.id}`;
      nodes.push({ id, type: 'chunkKv', position: { x: x + index * 195, y: layerY.split }, data: {
        kind: 'split', label: `Split ${entry.id.slice(0, 4)}…${entry.id.slice(-4)}`, subtitle: entry.state,
        accessible: `Partition ${entry.id}`, title: `${entry.id} · ${range(entry)}`, disabled, selected: entry.id === selectedId, click: () => onSelect(entry),
      } });
      nodes.push({ id: `tree-${entry.id}`, type: 'chunkKv', position: { x: x + index * 195, y: layerY.tree }, data: {
        kind: 'tree', label: 'KV Tree', subtitle: entry.artifact.tree_id, accessible: `KV Tree for ${entry.id}`,
        title: 'Inspect base pages, checkpoint and counters.', disabled, click: () => onTree(entry),
      } });
      connect(group.id, id); connect(id, `tree-${entry.id}`, 'straight');
    });
    x += width + 35;
  }
  nodes.unshift({ id: 'root', type: 'chunkKv', position: { x: Math.max(0, (x - 205) / 2), y: 0 },
    data: { kind: 'root', label: 'ChunkKV', subtitle: `${groups.length} servers · loaded catalog window` } });
  const layoutKey = nodes.map(node => node.id).join('/');
  return <section aria-label="Partition range map" className="tw-space-y-2">
    <div className="tw-flex tw-items-center tw-gap-3 tw-text-xs tw-text-muted">
      <span>Click server to expand/collapse · Split for details · KV Tree for checkpoint</span>
      {groups.length > SERVER_LIMIT && <>
        <button className={buttonClass} disabled={page === 0} onClick={() => setServerPage(page - 1)}>Previous servers</button>
        <span>{page + 1} / {Math.ceil(groups.length / SERVER_LIMIT)}</span>
        <button className={buttonClass} disabled={(page + 1) * SERVER_LIMIT >= groups.length} onClick={() => setServerPage(page + 1)}>Next servers</button>
      </>}
    </div>
    <div className="tw-rounded-lg tw-border tw-border-border tw-overflow-hidden" style={{ height: 640 }} data-testid="chunk-kv-graph">
      <ReactFlowProvider><GraphCanvas layout={nodes} edges={edges} layoutKey={layoutKey} /></ReactFlowProvider>
    </div>
    <p className="tw-text-xs tw-text-muted">At most 8 servers × 5 splits per canvas window. Select KV Tree for bounded base-page inspection.</p>
  </section>;
}
