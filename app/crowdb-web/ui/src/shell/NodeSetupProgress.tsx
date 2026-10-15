// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { nodeSetupStatus, retryNodeSetup } from '../api';
import { Button } from '../components/ui/Button';
import { serviceLabels } from '../services/useNodeServicePlans';
import type { NodeSetupStatus } from './nodeSetup';

export function NodeSetupProgress({ node, readonly }: { node: number; readonly: boolean }) {
  const [status, setStatus] = useState<NodeSetupStatus | null>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const generation = useRef(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      const captured = generation.current;
      try {
        const observation = await nodeSetupStatus(node, { signal: controller.signal });
        if (!controller.signal.aborted && captured === generation.current) { setStatus(observation); setError(''); }
      } catch (cause) { if (!controller.signal.aborted && captured === generation.current) setError(String(cause)); }
      finally { if (!controller.signal.aborted) timer = setTimeout(() => { void refresh(); }, 2000); }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [node]);
  const retry = async () => {
    setBusy(true); setError(''); generation.current++;
    try { setStatus(await retryNodeSetup(node)); }
    catch (cause) { setError(String(cause)); }
    finally { generation.current++; setBusy(false); }
  };
  if (status === null && !error) return null;
  const warning = status?.services.some(service => service.phase === 'warning');
  return <section aria-label={`Node ${node} setup`} className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-2">
    <h2 className="tw-font-semibold">Node setup</h2>
    <p role="status" className="tw-text-sm">{status ? status.phase === 'ready' ? warning ? 'Completed with warnings' : 'Completed' : status.phase.replaceAll('-', ' ') : error ? 'Setup observation unavailable' : 'Loading setup…'}</p>
    {error && <p role="alert" className="tw-text-xs tw-text-failed">{error}{status ? ' · Last confirmed progress retained.' : ''}</p>}
    {status && <>
      <p className="tw-text-xs tw-text-muted">Accepted work continues without this browser. Operation {status.operation_id}</p>
      <p className="tw-text-xs">Storage: {status.storage_mode === 'diskless' ? 'Diskless · no disk creation or initialization' : status.storage_mode === 'automatic' ? 'Managed file' : status.storage_mode === 'supplied' ? 'Selected discovered media' : 'Unknown'}</p>
      {status.error && <p role="status" className="tw-text-xs tw-text-failed">{status.error}</p>}
      <ul className="tw-text-xs tw-space-y-1">{status.services.filter(service => service.phase !== 'disabled').map(service => <li key={service.kind}>{serviceLabels[service.kind]} · {service.phase}{service.detail ? ` · ${service.detail}` : ''}</li>)}</ul>
      {status.disks.map(disk => <p key={disk.operation_id} className="tw-text-xs tw-break-all">{disk.path} · {disk.phase}{disk.detail ? ` · ${disk.detail}` : ''}</p>)}
      {status.phase === 'failed' && <Button size="sm" disabled={readonly || busy} onClick={() => { void retry(); }}>Retry accepted setup</Button>}
    </>}
  </section>;
}
