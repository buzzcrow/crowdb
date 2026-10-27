// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { nodeAdmissions, type NodeAdmission } from '../api';
import type { Rack } from '../types';
import { AdmitCandidateDialog } from './AdmitCandidateDialog';
import { NodeSetupProgress } from './NodeSetupProgress';
import { Button } from '../components/ui/Button';

export function NodeAdmissionProperties({ node, racks, readonly }: { node: number; racks: Rack[]; readonly?: boolean }) {
  const [record, setRecord] = useState<NodeAdmission>();
  const [editing, setEditing] = useState(false);
  const [revision, setRevision] = useState(0);
  const [error, setError] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    setRecord(undefined); setError(''); setEditing(false);
    void nodeAdmissions({ signal: controller.signal }).then(records => {
      if (!controller.signal.aborted) setRecord(records.find(row => row.node_id === node && row.confirmed && !row.cancelled));
    }).catch(error => { if (!controller.signal.aborted) setError(String(error)); });
    return () => controller.abort();
  }, [node, revision]);
  return <div className="tw-p-3 tw-space-y-3">
    {error && <p role="status" className="tw-text-xs tw-text-muted">Node configuration unavailable: {error}</p>}
    {record && <Button size="sm" disabled={readonly} onClick={() => setEditing(true)}>Edit node…</Button>}
    <NodeSetupProgress node={node} readonly={!!readonly} />
    {record && editing && <AdmitCandidateDialog discoveryId={record.discovery_id} current={record} racks={racks}
      onClose={() => setEditing(false)} onSuccess={() => { setEditing(false); setRevision(value => value + 1); }} />}
  </div>;
}
