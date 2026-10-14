// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { nodeAutoJoin, setNodeAutoJoin, type AutoJoinStatus } from '../api';

const phases: Record<string, string> = {
  disabled: 'Automatic admission paused', 'needs-rack': 'Create a default rack to prepare nodes',
  'needs-target': 'Choose target', preparing: 'Preparing authenticated node',
  initializing: 'Initializing Store 0 / Group 0', ready: 'Ready',
  'joining-nonvoter': 'Joining cluster (nonvoter)', 'authority-unavailable': 'Group 0 unavailable · waiting for recovery',
  'recovery-required': 'Recovery required', failed: 'Auto join blocked',
};

export function AutoJoin({ readonly = false }: { readonly?: boolean }) {
  const [status, setStatus] = useState<AutoJoinStatus>();
  const [unavailable, setUnavailable] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const revision = useRef(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      const observedRevision = revision.current;
      try {
        const result = await nodeAutoJoin({ signal: controller.signal });
        if (!controller.signal.aborted && observedRevision === revision.current) { setStatus(result); setUnavailable(false); }
      } catch {
        if (!controller.signal.aborted && observedRevision === revision.current) setUnavailable(true);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, 2000);
      }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, []);
  const update = async (enabled: boolean) => {
    revision.current += 1;
    setSaving(true); setError('');
    try { setStatus(await setNodeAutoJoin(enabled)); }
    catch (error) { setError(error instanceof Error ? error.message : String(error)); }
    finally { revision.current += 1; setSaving(false); }
  };
  const observation = status?.observation;
  const phase = observation?.phase;
  return <section aria-label="Automatic node admission" className="tw-border-b tw-border-border tw-px-4 tw-py-3 tw-text-sm">
    <label className="tw-flex tw-items-center tw-gap-2">
      <input type="checkbox" checked={status?.enabled ?? false} disabled={readonly || !status || unavailable || saving}
        onChange={event => void update(event.target.checked)} />Auto join
    </label>
    {status && <p className="tw-text-xs tw-text-muted">Standard mode · Target: {status.target_cluster_id ?? 'Initial local draft'} · {status.required_voters} initial voters</p>}
    <p role="status">{!status ? unavailable ? 'Auto join status unavailable' : 'Loading Auto join…'
      : !status.enabled ? phases.disabled
      : phase === 'waiting' ? `Waiting (${observation?.members?.length ?? 0}/${status.required_voters})`
      : phases[phase ?? ''] ?? phase}</p>
    {observation?.members?.length ? <p className="tw-text-xs tw-text-muted">Initial voters: {observation.members.join(', ')}</p> : null}
    {status && <p className="tw-text-xs tw-text-muted">Accepted initialization continues when Auto join is off. Later nodes join without changing Group 0 voters.</p>}
    {unavailable && status && <p className="tw-text-xs tw-text-muted">Status unavailable · last observation shown</p>}
    {observation?.needs_access?.map(node => <p key={node.discovery_id} className="tw-text-xs tw-text-muted">Needs access: {node.discovery_id}{node.error ? ` · ${node.error}` : ''}</p>)}
    {(error || observation?.error) && <p role="alert" className="tw-text-failed">{error || observation?.error}</p>}
  </section>;
}
