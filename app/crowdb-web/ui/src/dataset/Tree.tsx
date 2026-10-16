// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { buttonClass } from '../access/Workbench';
import * as api from './api';
import type { DatasetSelection, Head, SnapshotSummary } from './types';
import { useQuery } from './query';

export interface TreeEntry { expanded?: boolean; splits?: boolean; cursor?: string }
export type TreeState = Record<string, TreeEntry>;
type Props = { dataset: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void; tree: TreeState; updateTree: (key: string, value: TreeEntry) => void; head?: Head };
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
  return <li>
    <div className="tw-flex tw-gap-1 tw-items-center">
      <button aria-label={`Expand Snapshot ${props.item.id}`} aria-expanded={expanded} onClick={() => setExpanded(!expanded)}> {expanded ? '▾' : '▸'} </button>
      <button title={props.item.id} className="tw-font-mono tw-text-xs tw-break-all" onClick={() => props.select({ kind: 'snapshot', ...scope })}>{props.item.id.slice(0, 10)}</button>
      {props.head?.latest === props.item.id && <span className="tw-text-xs tw-text-accent">Latest</span>}
      {props.head?.stable === props.item.id && <span className="tw-text-xs tw-text-healthy">Stable</span>}
    </div>
    {expanded && <div className="tw-pl-2">
      <button aria-expanded={splits} onClick={() => setSplits(!splits)}>{splits ? '▾' : '▸'} Splits</button>
      {splits && <ul className="tw-pl-3">
        {current.error && <li role="alert">{current.error}</li>}
        {current.value === null && <li>No Split revision</li>}
        {current.value && <li><button onClick={() => props.select({ kind: 'revision', ...scope, revision: current.value!.revision })}>Revision {current.value.revision.slice(0, 8)}</button></li>}
        {current.value?.splits.map(split => <li key={split.name}><button onClick={() => props.select({ kind: 'split', ...scope, revision: current.value!.revision, split: split.name })}>{split.name} ({split.count})</button></li>)}
      </ul>}
      {props.depth < 32 ? <Branches {...props} parent={props.item.id} depth={props.depth + 1} /> : <p>Tree depth limit. Open a Snapshot directly to continue.</p>}
    </div>}
  </li>;
}
