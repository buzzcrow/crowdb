// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { listNodeCandidates, type NodeCandidates } from '../api';

const labels = {
  unbound: 'Available', same_cluster: 'Cluster member', foreign_cluster: 'Another cluster',
  incompatible: 'Incompatible version', identity_conflict: 'Identity conflict',
};

/** Discovery observations remain separate from the confirmed hardware tree. */
export function CandidateNodes() {
  const [snapshot, setSnapshot] = useState<NodeCandidates>();
  const [unavailable, setUnavailable] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      try {
        const result = await listNodeCandidates({ signal: controller.signal });
        if (!controller.signal.aborted) { setSnapshot(result); setUnavailable(false); }
      } catch {
        if (!controller.signal.aborted) setUnavailable(true);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, 2000);
      }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, []);

  return <section aria-label="Candidate nodes" className="tw-border-t tw-border-border tw-p-3 tw-overflow-y-auto tw-max-h-[40vh] tw-shrink-0">
    <h3 className="tw-text-sm tw-font-semibold tw-mb-2">Candidate nodes</h3>
    {unavailable && <p role="status" className="tw-text-xs tw-text-muted">Discovery unavailable{snapshot ? ' — last observations shown' : ''}.</p>}
    {!snapshot && !unavailable && <p className="tw-text-xs tw-text-muted">Discovering nodes…</p>}
    {snapshot?.candidates.length === 0 && <p className="tw-text-xs tw-text-muted">No nodes discovered.</p>}
    <ul className="tw-space-y-2">
      {snapshot?.candidates.map(node => <li key={node.advertisement.discovery_id} data-testid={`candidate-${node.advertisement.discovery_id}`} className="tw-border tw-border-border tw-rounded tw-p-2 tw-text-xs">
        <div className="tw-break-all">{node.advertisement.discovery_id}</div>
        <div className="tw-text-muted">{labels[node.state]}{node.advertisement.discovery_id === snapshot.local.advertisement.discovery_id ? ' · This node' : ''}</div>
        {node.advertisement.cluster_id && <div className="tw-break-all tw-text-muted">Cluster {node.advertisement.cluster_id}</div>}
        {node.advertisement.monitor_endpoints.map(endpoint => <div key={endpoint} className="tw-break-all tw-text-muted">{endpoint}</div>)}
      </li>)}
    </ul>
    {snapshot?.diagnostics.map((diagnostic, index) => <p key={index} className="tw-text-xs tw-text-muted tw-mt-2">{diagnostic}</p>)}
  </section>;
}
