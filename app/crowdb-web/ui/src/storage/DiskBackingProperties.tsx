// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { Button } from '../components/ui/Button';
import { diskBacking, nodeDiskFile } from '../api';
import type { DiskBackingInfo, FilePreparation } from './types';
import { filePhaseLabel, pendingFilePreparation } from './types';

export function DiskBackingProperties({ nodeId, groupId, diskId }: { nodeId: number; groupId: number; diskId: string }) {
  const [record, setRecord] = useState<DiskBackingInfo | null>();
  const [operation, setOperation] = useState<FilePreparation>();
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    setError('');
    const refresh = async () => {
      try {
        const backing = await diskBacking(nodeId, groupId, diskId, { signal: controller.signal });
        if (controller.signal.aborted) return;
        setRecord(backing); setError('');
        if (backing?.container_path.endsWith(`/managed-${backing.operation_id}.img`)) {
          const current = await nodeDiskFile(nodeId, backing.operation_id, { signal: controller.signal });
          if (controller.signal.aborted) return;
          setOperation(current);
          if (pendingFilePreparation(current)) timer = setTimeout(refresh, 1000);
        }
      } catch (cause) { if (!controller.signal.aborted) setError(String(cause)); }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [nodeId, groupId, diskId, revision]);
  const fields = record ? [
    ['Container path', record.container_path], ['Backend', record.backend === 'file' ? 'File' : 'Block device'],
    ['Media', record.media_type === null ? 'Unknown' : ['HDD', 'SSD', 'Zoned SSD', 'SMR HDD'][record.media_type] ?? 'Unknown'],
    ['Backing identity', record.backing_identity], ['Allocation mode', record.backend === 'block-device' ? 'Raw device' : record.sparse ? 'Sparse' : 'Preallocated'],
    ['Retained membership', record.removed ? 'Removed' : 'Registered'], ['DiskDB owner', record.owner ?? 'Unassigned'],
    ['KV binding', record.binding ?? 'Unbound'], ['Allocation units', record.capacity_units],
    ['Unit size (bytes)', String(record.unit_size_bytes)], ['Zone units', record.zone_size_units], ['Zone count', String(record.zone_count)],
    ['Authority observed', new Date(record.observed_at_ms).toLocaleString()], ['Operation ID', record.operation_id],
  ] : [];
  return <section aria-label="Disk backing properties" className="tw-space-y-2">
    <Button size="sm" variant="secondary" onClick={() => setRevision(value => value + 1)}>Refresh backing properties</Button>
    {record === undefined && !error && <p role="status" className="tw-text-xs tw-text-muted">Loading backing metadata…</p>}
    {record === null && <p className="tw-text-xs tw-text-muted">Backing identity and backend are unknown for this legacy disk.</p>}
    {error && <p role="alert" className="tw-text-xs tw-text-failed">{error} · {record ? 'Last-known backing properties retained.' : 'Backing properties unavailable.'}</p>}
    {record && <dl className="tw-border tw-border-border tw-rounded tw-divide-y tw-divide-border">{fields.map(([label, value]) =>
      <div key={label} className="tw-p-2 tw-text-xs tw-space-y-1"><dt className="tw-text-muted">{label}</dt><dd className="tw-font-mono tw-break-all">{value}</dd></div>)}</dl>}
    {operation && <div className="tw-text-xs tw-space-y-1"><p role="status">Operation: {filePhaseLabel[operation.phase]}</p>
      {operation.error && <p>{operation.error}</p>}<p className="tw-text-muted">Operation completion does not assert current I/O health.</p></div>}
  </section>;
}
