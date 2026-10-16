// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Button } from '../components/ui/Button';
import { formatBytes } from '../utils/capacity';
import type { DiskInventory, RemovedDisk } from './types';
interface Props {
  disk: RemovedDisk; inventory?: DiskInventory; ready: boolean; readonly: boolean;
  onInspect: () => void; onRejoin: () => void;
}
export function RemovedDiskRow({ disk, inventory, ready, readonly, onInspect, onRejoin }: Props) {
  const local = inventory?.disks.find(item => item.path === disk.container_path);
  const capacity = BigInt(disk.capacity_units) * BigInt(disk.unit_size_bytes);
  const issue = !inventory ? 'Discovery unavailable; refresh before Rejoin.'
    : !local ? 'Missing from latest discovery' : local.error
    || (local.backing_identity !== disk.backing_identity || local.backend !== disk.backend ? 'Original backing identity or type changed'
      : !Number.isSafeInteger(local.capacity_bytes) || BigInt(local.capacity_bytes!) !== capacity ? 'Capacity differs from original geometry' : undefined);
  const media = disk.media_type === null ? 'Unknown' : ['HDD', 'SSD', 'Zoned SSD', 'SMR HDD'][disk.media_type] ?? 'Unknown';
  return <article aria-label={`Removed disk ${disk.disk_id}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm tw-space-y-1">
    <p role="status">Removed · DG-{disk.disk_group_id} · {disk.disk_id}</p>
    <p className="tw-break-all">{disk.container_path}</p>
    <p>{disk.backend} · {formatBytes(Number(capacity))} · Media {media} · {disk.sparse ? 'Sparse' : disk.backend === 'file' ? 'Preallocated' : 'Raw device'} · Medium and allocation metadata retained.</p>
    <p className="tw-text-muted">{issue || 'Explicit Rejoin required; never automatically readmitted.'}</p>
    <button className="tw-text-accent" onClick={onInspect}>Properties</button>
    <Button size="sm" disabled={readonly || !ready || !!issue} onClick={onRejoin}>Rejoin disk</Button>
  </article>;
}
