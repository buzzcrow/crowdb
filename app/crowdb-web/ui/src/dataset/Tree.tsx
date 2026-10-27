// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Tree, type TreeNode } from '../components/Tree';
import type { MouseEvent } from 'react';
import { buttonClass } from '../access/Workbench';
import * as api from './api';
import type { DatasetSelection, Head, SnapshotSummary } from './types';
import { useQuery } from './query';

export interface TreeEntry { expanded?: boolean; splits?: boolean; cursor?: string }
export type TreeState = Record<string, TreeEntry>;
type Props = { selected: DatasetSelection; onContextMenu: (event: MouseEvent, selected: DatasetSelection) => void; onSplitsContextMenu?: (event: MouseEvent, selected: Exclude<DatasetSelection, { kind: 'root' }>) => void; dataset: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void; tree: TreeState; updateTree: (key: string, value: TreeEntry) => void; head?: Head };
export function SnapshotTree(props: Props) {
  const head = useQuery(`${props.dataset}:${props.refresh}`, props.active, signal => api.head(props.dataset, signal));
  return <Branches {...props} head={head.value} />;
}
function Branches(props: Props & { parent?: string; depth?: number }) {
  const key = `${props.dataset}:children:${props.parent ?? 'roots'}`;
  const cursor = props.tree[key]?.cursor;
  const setCursor = (cursor?: string) => props.updateTree(key, { cursor });
  const path = props.parent ? `${api.snapshotPath(props.dataset, props.parent)}/children` : `${api.datasetPath(props.dataset)}/roots`;
  const result = useQuery(`${path}:${cursor}:${props.refresh}`, props.active, signal => api.page<SnapshotSummary>(path, cursor, 20, signal));
  return <ul aria-label={props.parent ? `Children of ${props.parent}` : 'Root Snapshots'} className="tw-pl-3 tw-space-y-2">
    {result.error && <li role="alert">{result.error}</li>}
    {result.value?.entries.map(item => <SnapshotNode key={item.id} {...props} item={item} depth={props.depth ?? 0} />)}
    {result.value?.entries.length === 0 && <li className="tw-text-muted">No {props.parent ? 'children' : 'root Snapshots'}</li>}
    {result.value?.after && <li><button className={buttonClass} onClick={() => setCursor(result.value!.after!)}>Next {props.parent ? 'children' : 'roots'}</button></li>}
    {cursor && <li><button className={buttonClass} onClick={() => setCursor(undefined)}>First page</button></li>}
  </ul>;
}
function SnapshotNode(props: Props & { item: SnapshotSummary; depth: number }) {
  const key = `${props.dataset}:node:${props.item.id}`;
  const entry = props.tree[key] ?? {};
  const { expanded, splits } = entry;
  const setExpanded = (expanded: boolean) => props.updateTree(key, { ...entry, expanded });
  const setSplits = (splits: boolean) => props.updateTree(key, { ...entry, splits });
  const scope = { dataset: props.dataset, snapshot: props.item.id };
  const current = useQuery(`${props.dataset}:${props.item.id}:${props.refresh}`, props.active && !!splits,
    async signal => { const h = await api.splitHead(props.dataset, props.item.id, signal); return h.revision ? api.revision(props.dataset, props.item.id, h.revision, signal) : null; });
  const selected: DatasetSelection = { kind: 'snapshot', ...scope };
  const splitsNode: TreeNode = { id: `${key}:splits`, type: 'Dataset', label: 'Splits', expandable: true, activate: () => setSplits(!splits),
    onExpand: () => setSplits(true), children: current.value ? [
      { id: `${key}:revision`, type: 'Dataset', label: `Revision ${current.value.revision.slice(0, 8)}`, activate: () => props.select({ kind: 'revision', ...scope, revision: current.value!.revision }) },
      ...current.value.splits.map(split => ({ id: `${key}:split:${split.name}`, type: 'Dataset' as const, label: `${split.name} (${split.count})`, activate: () => props.select({ kind: 'split', ...scope, revision: current.value!.revision, split: split.name }) })),
    ] : [], footer: current.error ? <p role="alert">{current.error}</p> : current.value === null ? <p>No Split revision</p> : undefined };
  return <li><Tree nodes={[{ id: key, type: 'Dataset', label: props.item.id.slice(0, 10), title: props.item.id,
    selected: props.selected.kind !== 'root' && props.selected.kind !== 'dataset' && props.selected.snapshot === props.item.id,
    activate: () => props.select(selected), expandable: true,
    suffix: <>{props.head?.latest === props.item.id && <span className="tw-text-xs tw-text-accent">Latest</span>}{props.head?.stable === props.item.id && <span className="tw-text-xs tw-text-healthy">Stable</span>}</>,
    childrenContent: <><Tree nodes={[splitsNode]} expandedIds={splits ? [splitsNode.id] : []} onExpansionChange={ids => setSplits(ids.includes(splitsNode.id))} onNodeContextMenu={(node, event) => {
      if (node.id === splitsNode.id) (props.onSplitsContextMenu ?? props.onContextMenu)(event, { ...selected, revision: current.value?.revision });
      else if (current.value) {
        const split = current.value.splits.find(value => node.id === `${key}:split:${value.name}`);
        props.onContextMenu(event, { ...scope, kind: split ? 'split' : 'revision', revision: current.value.revision, ...(split ? { split: split.name } : {}) });
      }
    }} />{props.depth < 32 ? <Branches {...props} parent={props.item.id} depth={props.depth + 1} /> : <p>Tree depth limit. Open a Snapshot directly to continue.</p>}</>,
  }]} expandedIds={expanded ? [key] : []} onExpansionChange={ids => setExpanded(ids.includes(key))} onNodeContextMenu={(_, event) => props.onContextMenu(event, selected)} /></li>;
}
