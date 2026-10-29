// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { History } from 'lucide-react';
import type { TreeNode } from '../components/Tree';
import type { Inspector } from './useInspection';
import type { Selection, TableLoad } from './types';

export type IcebergNode = TreeNode & { activate?: () => void; resource?: import('./ResourceMenu').CatalogTarget };
export const filename = (path: string) => path.split('/').pop() || path;
const iconClass = 'tw-h-4 tw-w-4 tw-text-muted';
export function referenceNodes(loaded: TableLoad, inspector: Inspector, page: number, resource?: { namespace: string[]; table: string }): IcebergNode[] {
  const snapshots = [...(loaded.metadata.snapshots ?? [])].reverse();
  const current = String(loaded.metadata['current-snapshot-id']);
  const node = (selection: Selection, label: string, icon: TreeNode['icon']): IcebergNode => ({
    resource: resource && { ...resource, loaded, selection },
    id: `ice-${selection.kind}-${inspector.key(selection)}`, type: 'Iceberg', label, icon,
    title: `${selection.kind} · ${selection.file?.location ?? selection.manifest?.location ?? selection.snapshot['snapshot-id']}`,
    selected: inspector.selection?.kind === selection.kind && inspector.key(inspector.selection) === inspector.key(selection),
    activate: () => { void inspector.select(selection); },
  });
  const result: IcebergNode[] = snapshots.slice(page * 20, page * 20 + 20).map(snapshot => {
    const selection: Selection = { snapshot, kind: 'snapshot' };
    return node(selection, `Snapshot ${snapshot['snapshot-id']}${String(snapshot['snapshot-id']) === current ? ' · Current' : ''}`, <History className={iconClass} />);
  });
  return result;
}
