// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useSelection, type SelectedEntity } from '../contexts/SelectionContext';
import { Domain } from '../types';

export function ClusterBreadcrumb() {
  const { scopeEntity: scope, selectEntity } = useSelection();
  const parents = scope?.parentIds ?? {};
  const rack = scope?.type === 'Rack' ? scope.id : parents.rack_id;
  const node = scope?.type === 'Node' ? scope.id : parents.node_id;
  const group = scope?.type === 'DiskGroup' ? scope.id : parents.disk_group_id;
  const entries: Array<{ label: string; entity: SelectedEntity | null }> = [{ label: 'Cluster', entity: null }];
  if (rack !== undefined) entries.push({ label: `R-${rack}`, entity: { domain: Domain.Cluster, type: 'Rack', id: String(rack) } });
  if (node !== undefined) entries.push({ label: `N-${node}`, entity: { domain: Domain.Cluster, type: 'Node', id: String(node),
    parentIds: rack !== undefined ? { rack_id: rack } : undefined } });
  if (group !== undefined) entries.push({ label: `DG-${group}`, entity: { domain: Domain.Cluster, type: 'DiskGroup', id: String(group),
    parentIds: { ...(rack !== undefined ? { rack_id: rack } : {}), ...(node !== undefined ? { node_id: node } : {}), disk_group_id: group } } });
  if (scope?.section || scope?.type === 'Disk' || scope?.type === 'Server') {
    entries.push({ label: scope.section ? scope.section[0].toUpperCase() + scope.section.slice(1) : scope.name ?? scope.id, entity: scope });
  }
  return <nav aria-label="Cluster scope" className="tw-flex tw-shrink-0 tw-flex-wrap tw-items-center tw-gap-2 tw-border-b tw-border-border tw-px-4 tw-py-2 tw-text-sm">
    {entries.map((entry, index) => <span key={`${entry.label}-${index}`} className="tw-flex tw-min-w-0 tw-items-center tw-gap-2">
      {index > 0 && <span aria-hidden="true" className="tw-text-muted">/</span>}
      <button aria-current={index === entries.length - 1 ? 'page' : undefined}
        className="tw-max-w-64 tw-truncate tw-text-accent hover:tw-underline" title={entry.label}
        onClick={() => selectEntity(entry.entity)}>{entry.label}</button>
    </span>)}
  </nav>;
}
