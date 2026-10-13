// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { cleanupNodeCluster, initCluster, type NodeDeploymentStatus } from '../api';
import { Dialog } from '../components/Dialog';

export function NodeRecoveryDialog({ status, cleanup, onClose, onChange }: { status: NodeDeploymentStatus; cleanup: boolean; onClose: () => void; onChange?: () => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const execute = async () => {
    setBusy(true); setError('');
    try {
      if (cleanup) {
        const result = await cleanupNodeCluster(status.operation_id ?? status.cleanup!.operation_id);
        if (result.pending.length) throw new Error(`Cleanup pending on nodes ${result.pending.join(', ')}. ${result.errors.join('; ')}`);
      } else {
        await initCluster({ nodes: status.members! });
      }
      onChange?.(); onClose();
    } catch (error) { setError(error instanceof Error ? error.message : String(error)); }
    finally { setBusy(false); }
  };
  return <Dialog isOpen title={cleanup ? 'Delete Group 0 and system store' : 'Resume fixed bootstrap'} onClose={() => { if (!busy) onClose(); }} onConfirm={execute} destructive={cleanup} confirmLabel={cleanup ? 'Delete and release bindings' : 'Resume'} confirmLoading={busy} confirmDisabled={busy}>
    <p className="tw-text-xs tw-break-all">Cluster {status.cluster_id}</p>
    <p className="tw-text-sm tw-mt-3">{cleanup ? 'This deletes store 0, including all its groups, on the retained cluster nodes and clears their bindings. Unreachable nodes remain pending and cannot be reused. Old bootstrap commands remain fenced.' : `Resume the submitted operation with fixed members ${status.members?.join(', ')}.`}</p>
    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-mt-3">{error}</p>}
  </Dialog>;
}
