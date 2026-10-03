// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { GraphQuery } from './query';
import ReactFlow, { Background, Controls, Handle, Position, type Node, type Edge, type NodeProps } from 'reactflow';
import 'reactflow/dist/style.css';
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
  kind: 'root' | 'server' | 'split' | 'tree';
  click?: () => void;
  previous?: () => void;
  next?: () => void;
}

function GraphNode({ data }: NodeProps<Card>) {
  return <div style={{ pointerEvents: 'auto' }} className={`tw-w-[170px] tw-rounded-lg tw-border tw-bg-panel tw-shadow-sm ${data.selected ? 'tw-border-accent' : 'tw-border-border'}`}>
    {data.kind !== 'root' && <Handle type="target" position={Position.Top} isConnectable={false} />}
    <button className="nodrag tw-w-full tw-p-3 tw-text-left disabled:tw-opacity-50" title={data.title} disabled={data.disabled || !data.click}
      aria-label={data.accessible ?? data.label} aria-pressed={data.selected} onClick={data.click}>
      <span className={`tw-block tw-text-sm tw-font-medium ${data.kind === 'split' ? 'tw-text-accent' : 'tw-text-text'}`}>{data.label}</span>
      <span className="tw-block tw-mt-1 tw-text-xs tw-text-muted">{data.subtitle}</span>
    </button>
    {(data.previous || data.next) && <div className="nodrag tw-flex tw-justify-between tw-px-2 tw-pb-2">
      <button className={buttonClass} aria-label={`Previous splits for ${data.label}`} disabled={!data.previous} onClick={data.previous}>Prev</button>
      <button className={buttonClass} aria-label={`Next splits for ${data.label}`} disabled={!data.next} onClick={data.next}>Next</button>
    </div>}
    {data.kind !== 'tree' && <Handle type="source" position={Position.Bottom} isConnectable={false} />}
  </div>;
}
const nodeTypes = { chunkKv: GraphNode };

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
  const connect = (source: string, target: string) => edges.push({ id: `${source}/${target}`, source, target, type: 'smoothstep', style: { stroke: '#64748b', strokeWidth: 1.5 } });
  for (const group of visible) {
    const offset = Math.min(offsets[group.id] ?? 0, Math.max(0, Math.floor((group.entries.length - 1) / SPLIT_LIMIT) * SPLIT_LIMIT));
    const children = collapsed.has(group.id) ? [] : group.entries.slice(offset, offset + SPLIT_LIMIT);
    const width = Math.max(1, children.length) * 195;
    nodes.push({ id: group.id, type: 'chunkKv', position: { x: x + (width - 170) / 2, y: 190 },
      data: { kind: 'server', label: group.label, subtitle: `${group.entries.length} splits in window · ${collapsed.has(group.id) ? 'Expand' : 'Collapse'}`,
        click: () => setCollapsed(previous => { const next = new Set(previous); if (next.has(group.id)) next.delete(group.id); else next.add(group.id); return next; }),
        previous: offset > 0 ? () => setOffsets(value => ({ ...value, [group.id]: offset - SPLIT_LIMIT })) : undefined,
        next: offset + SPLIT_LIMIT < group.entries.length ? () => setOffsets(value => ({ ...value, [group.id]: offset + SPLIT_LIMIT })) : undefined } });
    connect('root', group.id);
    children.forEach((entry, index) => {
      const id = `split-${entry.id}`;
      nodes.push({ id, type: 'chunkKv', position: { x: x + index * 195, y: 380 }, data: {
        kind: 'split', label: `Split ${entry.id.slice(0, 4)}…${entry.id.slice(-4)}`, subtitle: entry.state,
        accessible: `Partition ${entry.id}`, title: `${entry.id} · ${range(entry)}`, disabled, selected: entry.id === selectedId, click: () => onSelect(entry),
      } });
      nodes.push({ id: `tree-${entry.id}`, type: 'chunkKv', position: { x: x + index * 195, y: 530 }, data: {
        kind: 'tree', label: 'KV Tree', subtitle: entry.artifact.tree_id, accessible: `KV Tree for ${entry.id}`,
        title: 'Inspect checkpoint and counters; subtree/page inspection API unavailable', disabled, click: () => onTree(entry),
      } });
      connect(group.id, id); connect(id, `tree-${entry.id}`);
    });
    x += width + 35;
  }
  nodes.unshift({ id: 'root', type: 'chunkKv', position: { x: Math.max(0, (x - 205) / 2), y: 0 },
    data: { kind: 'root', label: 'Chunk-KV', subtitle: `${groups.length} servers · loaded catalog window` } });
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
      <ReactFlow key={layoutKey} nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.15 }}
        minZoom={0.1} maxZoom={2} nodesFocusable={false} edgesFocusable={false} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false} preventScrolling={false}>
        <Background gap={24} color="#2e3440" />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
    <p className="tw-text-xs tw-text-muted">At most 8 servers × 5 splits per canvas window. Subtree/Page links require a server inspection API.</p>
  </section>;
}
