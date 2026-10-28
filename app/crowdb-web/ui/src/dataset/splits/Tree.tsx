// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Tree } from '../../components/Tree';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { useQuery } from '../query';
import { Drafts } from '../Drafts';
import type { SnapshotTreeProps } from '../Tree';
import type { DatasetSelection } from '../types';

type Props = SnapshotTreeProps & { snapshot: string };
export function SplitTree(props: Props) {
  const { dataset, snapshot, active, refresh, tree, updateTree } = props;
  const key = `${dataset}:${snapshot}:revisions`;
  const cursor = tree[key]?.cursor;
  const head = useQuery(`${key}:${refresh}`, active, signal => api.splitHead(dataset, snapshot, signal));
  const history = useQuery(`${key}:${cursor}:${refresh}`, active, signal => api.history(dataset, snapshot, cursor, signal));
  const revisions = [...new Set([head.value?.revision, ...history.value?.entries.map(item => item.revision) ?? []].filter((id): id is string => !!id))];
  return <>
    {head.error && <p role="alert">{head.error}</p>}{history.error && <p role="alert">{history.error}</p>}
    {revisions.map(revision => <RevisionNode key={revision} {...props} revision={revision} current={head.value?.revision === revision} />)}
    {history.value?.after && <button className={buttonClass} onClick={() => updateTree(key, { cursor: history.value!.after! })}>Next revisions</button>}
    {cursor && <button className={buttonClass} onClick={() => updateTree(key, {})}>First revisions</button>}
    {!revisions.length && history.value && <p className="tw-px-4 tw-text-muted">No Split revisions</p>}
    {props.openDraft && <Drafts dataset={dataset} snapshot={snapshot} active={active} open={props.openDraft} />}
  </>;
}
function RevisionNode(props: Props & { revision: string; current: boolean }) {
  const { dataset, snapshot, revision, selected, tree, updateTree } = props;
  const key = `${dataset}:${snapshot}:revision:${revision}`;
  const selectedRevision = 'revision' in selected && selected.snapshot === snapshot && selected.revision === revision;
  const expanded = tree[key]?.expanded ?? (props.current || selectedRevision);
  const scope = { dataset, snapshot, revision };
  const target: DatasetSelection = { kind: 'revision', ...scope };
  const result = useQuery(`${key}:${props.refresh}`, props.active && expanded, signal => api.revision(dataset, snapshot, revision, signal));
  return <Tree expandedIds={expanded ? [key] : []} onExpansionChange={ids => updateTree(key, { expanded: ids.includes(key) })}
    nodes={[{ id: key, type: 'Dataset', label: revision.slice(0, 8), title: revision,
      selected: selectedRevision && selected.kind === 'revision', activate: () => props.select(target), expandable: true,
      suffix: props.current ? <span className="tw-text-xs tw-text-accent">Current</span> : undefined,
      children: result.value?.splits.map(split => ({ id: `${key}:${split.name}`, type: 'Dataset', label: split.name,
        selected: selectedRevision && selected.kind === 'split' && selected.split === split.name,
        activate: () => props.select({ kind: 'split', ...scope, split: split.name }),
      })), footer: result.error ? <p role="alert">{result.error}</p> : undefined,
    }]} onNodeContextMenu={(node, event) => props.onContextMenu(event, node.id === key ? target : { kind: 'split', ...scope, split: node.label })} />;
}
