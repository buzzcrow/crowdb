// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Tree } from '../components/Tree';
import type { MouseEvent } from 'react';
import { buttonClass } from '../access/Workbench';
import * as api from './api';
import type { DatasetSelection, Head, SnapshotSummary } from './types';
import { useQuery } from './query';
import { Drafts } from './Drafts';
import { SplitTree } from './splits/Tree';
import type { Operation } from './operation';

export interface TreeEntry { expanded?: boolean; splits?: boolean; cursor?: string }
export type TreeState = Record<string, TreeEntry>;
export type SnapshotTreeProps = { openDraft?: (op: Operation) => void; selected: DatasetSelection; onContextMenu: (event: MouseEvent, selected: DatasetSelection) => void; onSplitsContextMenu?: (event: MouseEvent, selected: Exclude<DatasetSelection, { kind: 'root' }>) => void; dataset: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void; tree: TreeState; updateTree: (key: string, value: TreeEntry) => void; head?: Head };
export function SnapshotTree(props: SnapshotTreeProps) {
  const head = useQuery(`${props.dataset}:${props.refresh}`, props.active, signal => api.head(props.dataset, signal));
  return <>{props.openDraft && <Drafts dataset={props.dataset} active={props.active} open={props.openDraft} />}<Branches {...props} head={head.value} /></>;
}
function Branches(props: SnapshotTreeProps & { parent?: string; depth?: number }) {
  const key = `${props.dataset}:children:${props.parent ?? 'roots'}`;
  const cursor = props.tree[key]?.cursor;
  const setCursor = (cursor?: string) => props.updateTree(key, { cursor });
  const path = props.parent ? `${api.snapshotPath(props.dataset, props.parent)}/children` : `${api.datasetPath(props.dataset)}/roots`;
  const result = useQuery(`${path}:${cursor}:${props.refresh}`, props.active, signal => api.page<SnapshotSummary>(path, cursor, 20, signal));
  return <ul aria-label={props.parent ? `Children of ${props.parent}` : 'Root Snapshots'} className="tw-space-y-0">
    {result.error && <li role="alert">{result.error}</li>}
    {result.value?.entries.map(item => <SnapshotNode key={item.id} {...props} item={item} depth={props.depth ?? 0} />)}
    {result.value?.entries.length === 0 && <li className="tw-text-muted">No {props.parent ? 'children' : 'root Snapshots'}</li>}
    {result.value?.after && <li><button className={buttonClass} onClick={() => setCursor(result.value!.after!)}>Next {props.parent ? 'children' : 'roots'}</button></li>}
    {cursor && <li><button className={buttonClass} onClick={() => setCursor(undefined)}>First page</button></li>}
  </ul>;
}
function SnapshotNode(props: SnapshotTreeProps & { item: SnapshotSummary; depth: number }) {
  const key = `${props.dataset}:node:${props.item.id}`;
  const entry = props.tree[key] ?? {};
  const { expanded, splits } = entry;
  const setExpanded = (expanded: boolean) => props.updateTree(key, { ...entry, expanded });
  const setSplits = (splits: boolean) => props.updateTree(key, { ...entry, splits });
  const scope = { dataset: props.dataset, snapshot: props.item.id };
  const selected: DatasetSelection = { kind: 'snapshot', ...scope };
  const splitsKey = `${key}:splits`;
  return <li><Tree nodes={[{ id: key, type: 'Dataset', label: props.item.id.slice(0, 10), title: props.item.id,
    selected: props.selected.kind === 'snapshot' && props.selected.snapshot === props.item.id,
    activate: () => props.select(selected), expandable: true,
    suffix: <>{props.head?.latest === props.item.id && <span className="tw-text-xs tw-text-accent">Latest</span>}{props.head?.stable === props.item.id && <span className="tw-text-xs tw-text-healthy">Stable</span>}</>,
    childrenContent: <><Tree nodes={[{ id: splitsKey, type: 'Dataset', label: 'Splits', expandable: true,
      activate: () => setSplits(!splits), childrenContent: <SplitTree {...props} snapshot={props.item.id} />,
    }]} expandedIds={splits ? [splitsKey] : []} onExpansionChange={ids => setSplits(ids.includes(splitsKey))}
      onNodeContextMenu={(_, event) => (props.onSplitsContextMenu ?? props.onContextMenu)(event, selected)} />{props.depth < 32 ? <Branches {...props} parent={props.item.id} depth={props.depth + 1} /> : <p>Tree depth limit. Open a Snapshot directly to continue.</p>}</>,
  }]} expandedIds={expanded ? [key] : []} onExpansionChange={ids => setExpanded(ids.includes(key))} onNodeContextMenu={(_, event) => props.onContextMenu(event, selected)} /></li>;
}
