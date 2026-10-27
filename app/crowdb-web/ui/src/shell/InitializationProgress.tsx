// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { clusterInitialization, initCluster, type NodeDeploymentStatus, type InitializationStatus } from '../api';
import { NodeRecoveryDialog } from './NodeRecoveryDialog';
import { Button } from '../components/ui/Button';

const phases: Record<InitializationStatus['phase'], string> = {
  idle: 'Not initialized', preparing: 'Preparing selected nodes',
  initializing: 'Initializing Store 0 / Group 0', binding: 'Publishing cluster membership',
  'data-group': 'Creating initial data group', active: 'Initialization complete',
  failed: 'Initialization failed',
};

/** The browser observes the durable operation; unmounting never cancels it. */
export function InitializationProgress({ readonly = false, deployment }: { readonly?: boolean; deployment?: NodeDeploymentStatus }) {
  const [status, setStatus] = useState<InitializationStatus>();
  const [unavailable, setUnavailable] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState('');
  const [resuming, setResuming] = useState(false);
  const recoverable = !!deployment?.operation_id && !!deployment.members?.length
    && ['bootstrap_in_progress', 'recovery_required'].includes(deployment.phase);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      let interval = 2000;
      try {
        const result = await clusterInitialization({ signal: controller.signal });
        if (!['idle', 'active', 'failed'].includes(result.phase)) interval = 500;
        if (!controller.signal.aborted) { setStatus(result); setUnavailable(false); }
      } catch {
        if (!controller.signal.aborted) setUnavailable(true);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, interval);
      }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, []);

  const retry = async () => {
    if (!status?.members?.length) return;
    setRetrying(true); setError('');
    try {
      await initCluster({ nodes: status.members });
      setStatus(await clusterInitialization());
      setUnavailable(false);
    } catch (error) { setError(error instanceof Error ? error.message : String(error)); }
    finally { setRetrying(false); }
  };
  if ((status?.phase === 'idle' || status?.phase === 'active') && !unavailable && !recoverable) return null;
  return <section aria-label="Cluster initialization" className="tw-border-b tw-border-border tw-px-4 tw-py-3 tw-text-sm">
    <p role="status">{recoverable && status?.phase === 'idle' ? 'Bootstrap recovery required' : status ? phases[status.phase] : unavailable ? 'Initialization status unavailable' : 'Loading initialization status…'}</p>
    {status?.members && <p className="tw-text-xs tw-text-muted">Selected voters: {status.members.join(', ')}</p>}
    {status?.operation_id && <p className="tw-text-xs tw-text-muted tw-break-all">Operation {status.operation_id}</p>}
    {status && !['idle', 'active', 'failed'].includes(status.phase) && <p className="tw-text-xs tw-text-muted">The node monitor continues if you close this page.</p>}
    {unavailable && status && <p className="tw-text-xs tw-text-muted">Status unavailable · last observation shown</p>}
    {status?.error && <p role="alert" className="tw-text-failed">{status.error}</p>}
    {error && <p role="alert" className="tw-text-failed">{error}</p>}
    {recoverable && <Button size="sm" disabled={readonly} onClick={() => setResuming(true)}>Resume bootstrap</Button>}
    {resuming && deployment && <NodeRecoveryDialog status={deployment} cleanup={false} onClose={() => setResuming(false)} />}
    {!recoverable && status?.phase === 'failed' && <Button size="sm" disabled={readonly || unavailable || retrying || !status.members?.length} onClick={() => void retry()}>Retry accepted initialization</Button>}
  </section>;
}
