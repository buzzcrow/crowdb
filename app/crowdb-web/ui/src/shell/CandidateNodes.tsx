// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { listNodeCandidates, nodeDeploymentStatus, nodeAdmissions, cancelNodeAdmission, type NodeAdmission, type NodeCandidates, type NodeDeploymentStatus } from '../api';
import { AdmitCandidateDialog } from './AdmitCandidateDialog';
import { Button } from '../components/ui/Button';
import { NodeRecoveryDialog } from './NodeRecoveryDialog';
import type { Rack } from '../types';

const labels = {
  unbound: 'Available', same_cluster: 'Cluster member', foreign_cluster: 'Another cluster',
  incompatible: 'Incompatible version', identity_conflict: 'Identity conflict',
};

/** Discovery observations remain separate from the confirmed hardware tree. */
export function CandidateNodes({ racks, readonly, onChange }: { racks: Rack[]; readonly?: boolean; onChange?: () => void }) {
  const section = useRef<HTMLElement>(null);
  const dialogRoot = section.current?.closest('.crowdb-console');
  const [snapshot, setSnapshot] = useState<NodeCandidates>();
  const [actionError, setActionError] = useState('');
  const [unavailable, setUnavailable] = useState(false);
  const [deployment, setDeployment] = useState<NodeDeploymentStatus>();
  const [admissions, setAdmissions] = useState<NodeAdmission[]>([]);
  const [recovery, setRecovery] = useState<'resume' | 'cleanup'>();
  const [selected, setSelected] = useState<string>();
  const [updating, setUpdating] = useState(false);
  const [clusterFilter, setClusterFilter] = useState('all');
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      try {
        const [result, deployment, admissions] = await Promise.all([listNodeCandidates({ signal: controller.signal }), nodeDeploymentStatus({ signal: controller.signal }), nodeAdmissions({ signal: controller.signal })]);
        if (!controller.signal.aborted) { setSnapshot(result); setDeployment(deployment); setAdmissions(admissions); setUnavailable(false); }
      } catch {
        if (!controller.signal.aborted) setUnavailable(true);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, 2000);
      }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, []);

  return <section ref={section} aria-label="Candidate nodes" className="tw-border-t tw-border-border tw-p-3 tw-overflow-y-auto tw-max-h-[40vh] tw-shrink-0">
    <h3 className="tw-text-sm tw-font-semibold tw-mb-2">Candidate nodes</h3>
    {deployment && <p data-testid="deployment-phase" className="tw-text-xs tw-text-muted tw-mb-2">{deployment.phase === 'unbound_draft' ? 'Uncommitted local draft' : deployment.phase === 'active' ? 'Active cluster' : deployment.phase === 'authority_unavailable' ? 'Group 0 unavailable · confirmed view is stale' : deployment.phase === 'topology_publishing' ? 'Publishing confirmed topology' : deployment.phase === 'bootstrap_in_progress' ? 'Bootstrap in progress · submitted inputs are fixed' : 'Recovery required'}</p>}
    {deployment?.cluster_id && <p className="tw-text-xs tw-break-all tw-text-muted tw-mb-2">Cluster {deployment.cluster_id}</p>}
    {deployment && ['bootstrap_in_progress', 'topology_publishing'].includes(deployment.phase) && <Button size="sm" disabled={readonly} onClick={() => setRecovery('resume')}>Resume bootstrap</Button>}
    {(deployment?.operation_id || deployment?.cleanup) && <Button size="sm" disabled={readonly} onClick={() => setRecovery('cleanup')}>{deployment.phase === 'cleanup_in_progress' ? 'Retry cleanup' : 'Clean up Group 0'}</Button>}
    {deployment?.nodes?.map(node => <p key={node.node_id} className="tw-text-xs tw-text-muted">Node {node.node_id}: store 0 {node.store_created ? 'created' : 'pending'} · Group 0 {node.group_ready ? 'ready' : 'pending'}</p>)}
    {deployment?.cleanup?.pending.map(id => <p key={id} className="tw-text-xs tw-text-muted">Node {id}: cleanup pending</p>)}
    {actionError && <p role="alert" className="tw-text-xs tw-text-failed">{actionError}</p>}
    {unavailable && <p role="status" className="tw-text-xs tw-text-muted">Discovery unavailable{snapshot ? ' — last observations shown' : ''}.</p>}
    {!snapshot && !unavailable && <p className="tw-text-xs tw-text-muted">Discovering nodes…</p>}
    {snapshot?.candidates.length === 0 && <p className="tw-text-xs tw-text-muted">No nodes discovered.</p>}
    {snapshot && <label className="tw-block tw-text-xs tw-mb-2">Discovered cluster
      <select aria-label="Discovered cluster" value={clusterFilter} onChange={event => setClusterFilter(event.target.value)} className="tw-block tw-w-full tw-bg-surface tw-border tw-border-border">
        <option value="all">All clusters and candidates</option>
        <option value="unbound">Unbound candidates</option>
        {[...new Set(snapshot.candidates.map(node => node.advertisement.cluster_id).filter((id): id is string => !!id))].map(id => <option key={id} value={id}>{id}</option>)}
      </select>
    </label>}
    <ul className="tw-space-y-2">
      {snapshot?.candidates.filter(node => clusterFilter === 'all' || (node.advertisement.cluster_id ?? 'unbound') === clusterFilter).map(node => <li key={node.advertisement.discovery_id} data-testid={`candidate-${node.advertisement.discovery_id}`} className="tw-border tw-border-border tw-rounded tw-p-2 tw-text-xs">
        <div className="tw-break-all">{node.advertisement.discovery_id}</div>
        <div className="tw-text-muted">{labels[node.state]}{node.advertisement.discovery_id === snapshot.local.advertisement.discovery_id ? ' · This node' : ''}</div>
        {node.state === 'foreign_cluster' && node.advertisement.monitor_endpoints[0] && <a className="tw-text-accent" href={clusterConsole(node.advertisement.monitor_endpoints[0])} target="_blank" rel="noreferrer">Open this cluster UI</a>}
        {node.advertisement.cluster_id && <div className="tw-break-all tw-text-muted">Cluster {node.advertisement.cluster_id}</div>}
        {node.advertisement.monitor_endpoints.map(endpoint => <div key={endpoint} className="tw-break-all tw-text-muted">{endpoint}</div>)}
        {admissions.some(entry => entry.discovery_id === node.advertisement.discovery_id) && <div className="tw-text-muted">{admissions.find(entry => entry.discovery_id === node.advertisement.discovery_id)?.confirmed ? 'Admitted' : admissions.find(entry => entry.discovery_id === node.advertisement.discovery_id)?.cancelled ? 'Cancelled · cleanup can be retried' : 'Prepared or joining · retry available'}</div>}
        {admissions.filter(entry => entry.discovery_id === node.advertisement.discovery_id && !entry.confirmed).map(entry => <Button key={entry.operation_id} size="sm" disabled={readonly || unavailable || !deployment || !['unbound_draft', 'active'].includes(deployment.phase)} onClick={async () => { try { const result = await cancelNodeAdmission(entry.discovery_id, entry.operation_id); setActionError(result.pending.length ? `Cancellation pending: ${result.pending.join('; ')}` : ''); onChange?.(); } catch (error) { setActionError(String(error)); } }}>Cancel admission</Button>)}
        {(node.state === 'unbound' || (node.state === 'same_cluster' && admissions.some(entry => entry.discovery_id === node.advertisement.discovery_id && !entry.confirmed))) && <Button size="sm" disabled={readonly || unavailable || racks.length === 0 || !deployment || !['unbound_draft', 'active'].includes(deployment.phase)} onClick={() => { setUpdating(false); setSelected(node.advertisement.discovery_id); }}>Move to cluster</Button>}
        {node.state === 'same_cluster' && admissions.some(entry => entry.discovery_id === node.advertisement.discovery_id && entry.confirmed && !entry.cancelled) && <Button size="sm" disabled={readonly || unavailable || deployment?.phase !== 'active'} onClick={() => { setUpdating(true); setSelected(node.advertisement.discovery_id); }}>Update node</Button>}
      </li>)}
    </ul>
    {snapshot?.diagnostics.map((diagnostic, index) => <p key={index} className="tw-text-xs tw-text-muted tw-mt-2">{diagnostic}</p>)}
    {recovery && deployment && dialogRoot && createPortal(<NodeRecoveryDialog status={deployment} cleanup={recovery === 'cleanup'} onClose={() => setRecovery(undefined)} onChange={onChange} />, dialogRoot)}
    {selected && dialogRoot && createPortal(<AdmitCandidateDialog discoveryId={selected} racks={racks} current={updating ? admissions.find(entry => entry.discovery_id === selected) : undefined} onClose={() => setSelected(undefined)} onSuccess={onChange} />, dialogRoot)}
  </section>;
}

function clusterConsole(endpoint: string): string {
  const url = new URL(endpoint);
  url.port = '9090';
  url.pathname = '/';
  url.search = '?domain=Cluster';
  return url.toString();
}
