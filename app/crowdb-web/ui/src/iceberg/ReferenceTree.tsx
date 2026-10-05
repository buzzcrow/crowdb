// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { FileText, History, FileSpreadsheet } from 'lucide-react';
import type { TreeNode } from '../components/Tree';
import type { Inspector } from './useInspection';
import type { FileEntry, Inspection, Manifest, Selection, TableLoad } from './types';

export type IcebergNode = TreeNode & { activate?: () => void };
export const filename = (path: string) => path.split('/').pop() || path;
const short = (value: string) => value.length > 24 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value;
const iconClass = 'tw-h-4 tw-w-4 tw-text-muted';
export function referenceNodes(loaded: TableLoad, inspector: Inspector, page: number): IcebergNode[] {
  const snapshots = [...(loaded.metadata.snapshots ?? [])].reverse();
  const current = String(loaded.metadata['current-snapshot-id']);
  const node = (selection: Selection, label: string, icon: TreeNode['icon']): IcebergNode => ({
    id: `ice-${selection.kind}-${inspector.key(selection)}`, type: 'Iceberg', label, icon,
    title: `${selection.kind} · ${selection.file?.location ?? selection.manifest?.location ?? selection.snapshot['snapshot-id']}`,
    selected: inspector.selection?.kind === selection.kind && inspector.key(inspector.selection) === inspector.key(selection),
    activate: () => { void inspector.select(selection); },
  });
  const result: IcebergNode[] = snapshots.slice(page * 20, page * 20 + 20).map(snapshot => {
      const selection: Selection = { snapshot, kind: 'snapshot' };
      const listSelection: Selection = { snapshot, kind: 'list' };
      const list = inspector.cache[inspector.key(listSelection)];
      const children = (list?.rows as Manifest[] ?? []).map(manifest => {
        const s: Selection = { snapshot, kind: 'manifest', manifest };
        const files = inspector.cache[inspector.key(s)];
        return { ...node(s, `Manifest · ${short(filename(manifest.location))}`, <FileText className={iconClass} />),
          expandable: true, onExpand: () => { void inspector.select(s, undefined, true); },
          footer: files && <ReferencePages data={files} selection={s} inspector={inspector} />,
          children: (files?.rows as FileEntry[] ?? []).map(file => node({ ...s, kind: 'file', file }, `${file.content === 'Data' ? 'Data' : 'Delete'} · ${short(filename(file.location))}`, <FileSpreadsheet className={iconClass} />)),
        };
      });
      return { ...node(selection, `Snapshot ${snapshot['snapshot-id']}${String(snapshot['snapshot-id']) === current ? ' · Current' : ''}`, <History className={iconClass} />), expandable: !!snapshot['manifest-list'], onExpand: () => { void inspector.select(selection, undefined, true); }, footer: list && <ReferencePages data={list} selection={selection} inspector={inspector} />, children };
    });
  return result;
}
function ReferencePages({ data, selection, inspector }: { data: Inspection; selection: Selection; inspector: Inspector }) {
  return <div className="tw-flex tw-flex-wrap tw-gap-2 tw-text-xs tw-text-muted tw-py-2"><span>{data.rows?.length ?? 0} loaded</span>{Number(data.offset) > 0 && <button disabled={inspector.busy} className="tw-text-accent" onClick={() => void inspector.select(selection, data.previous ?? '0', true)}>Previous page</button>}{data.next != null && <button disabled={inspector.busy} className="tw-text-accent" onClick={() => void inspector.select(selection, data.next!, true)}>Next page</button>}</div>;
}
