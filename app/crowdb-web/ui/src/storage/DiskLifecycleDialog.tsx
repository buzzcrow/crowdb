// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useCallback, useEffect, useRef, useState } from 'react';
import { Dialog } from '../components/Dialog';
import { Button } from '../components/ui/Button';
import { diskLifecycle, offlineNodeDisk, removeNodeDisk } from '../api';
import type { DiskLifecycle } from './types';

interface Props {
  nodeId: number; groupId: number; diskId: string;
  onClose: () => void;
  onCommitted: (state: DiskLifecycle) => void;
}
interface Submission { action: 'offline' | 'removal'; revision: string }

/** Only observations are cancelled on close; accepted mutations remain server-owned. */
export function DiskLifecycleDialog({ nodeId, groupId, diskId, onClose, onCommitted }: Props) {
  const [state, setState] = useState<DiskLifecycle>();
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [fresh, setFresh] = useState(false);
  const [acknowledged, setAcknowledged] = useState(false);
  const [submission, setSubmission] = useState<Submission>();
  const controller = useRef(new AbortController());
  const pending = useRef<Promise<void>>();
  const callbacks = useRef({ onCommitted, onClose });
  callbacks.current = { onCommitted, onClose };
  const submitted = useRef<Submission>();
  submitted.current = submission;
  const observe = useCallback((current: DiskLifecycle) => {
    setState(current); setFresh(true); setError('');
    const attempt = submitted.current;
    if (attempt?.action === 'offline' && current.offline && !current.removed) {
      submitted.current = undefined; setSubmission(undefined); callbacks.current.onCommitted(current);
    }
    if (attempt?.action === 'removal' && current.removed && current.removed_from_revision === attempt.revision) {
      submitted.current = undefined; callbacks.current.onCommitted(current); callbacks.current.onClose();
    }
  }, []);
  const refresh = useCallback(() => {
    if (pending.current) return pending.current;
    const signal = controller.current.signal;
    setLoading(true);
    const work = diskLifecycle(nodeId, groupId, diskId, { signal })
      .then(current => { if (!signal.aborted) observe(current); })
      .catch(cause => { if (!signal.aborted) { setFresh(false); setError(String(cause)); } })
      .finally(() => { if (!signal.aborted) setLoading(false); if (pending.current === work) pending.current = undefined; });
    pending.current = work;
    return work;
  }, [nodeId, groupId, diskId, observe]);
  useEffect(() => {
    controller.current = new AbortController();
    pending.current = undefined;
    void refresh();
    return () => { controller.current.abort(); };
  }, [refresh]);
  useEffect(() => {
    if (!state?.offline || state.removed || !state.remove_conditions.length || busy || error) return;
    const timer = setTimeout(() => { void refresh(); }, 250);
    return () => clearTimeout(timer);
  }, [state, busy, error, refresh]);
  useEffect(() => { setAcknowledged(false); }, [state?.disk_revision]);
  const action = submission?.action ?? (state?.offline ? 'removal' : 'offline');
  const sameRevision = !submission || state?.disk_revision === submission.revision;
  const canSubmit = fresh && !busy && !loading && state && !state.removed && sameRevision && state.disk_revision
    && (action === 'offline' || (state.offline && state.remove_conditions.length === 0 && acknowledged));
  const submit = async () => {
    if (!canSubmit || !state?.disk_revision) return;
    const attempt = submission ?? { action, revision: state.disk_revision };
    submitted.current = attempt; setSubmission(attempt); setBusy(true); setError('');
    try {
      if (attempt.action === 'offline') {
        const current = await offlineNodeDisk(nodeId, groupId, diskId, attempt.revision);
        if (!controller.current.signal.aborted) {
          observe(current);
        }
      } else {
        const receipt = await removeNodeDisk(nodeId, groupId, diskId, attempt.revision);
        if (!controller.current.signal.aborted) observe({ ...state, removed: true, offline: true,
          disk_revision: null, removed_from_revision: receipt.offline_revision, operation_id: receipt.operation_id });
      }
    } catch (cause) {
      if (!controller.current.signal.aborted) {
        setFresh(false);
        setError(`${String(cause)} Outcome unconfirmed. Refresh authority before retrying the same revision.`);
      }
    } finally { if (!controller.current.signal.aborted) setBusy(false); }
  };
  return <Dialog isOpen title="Disk lifecycle" onClose={onClose} description={`Disk ${diskId} · Node ${nodeId} · DG-${groupId}`}
    confirmLabel={submission ? 'Retry same operation' : action === 'offline' ? 'Mark Offline' : 'Remove disk'}
    onConfirm={submit} confirmLoading={busy} confirmDisabled={!canSubmit} destructive={action === 'removal'}>
    <div className="tw-space-y-3 tw-text-sm">
      <Button size="sm" variant="secondary" disabled={loading || busy} onClick={() => { void refresh(); }}>Refresh authority</Button>
      {!state && <p role="status">{loading ? 'Loading Group 0 authority…' : 'Authority unavailable'}</p>}
      {state && <p role="status">{state.removed ? 'Removed · medium retained' : state.offline ? 'Offline' : 'Registered'} · Revision {state.disk_revision ?? state.removed_from_revision}</p>}
      {error && <p role="alert" className="tw-text-failed">{error}{state ? ' Last-known state retained.' : ''}</p>}
      {!sameRevision && <p role="alert">The disk revision changed. Close and inspect the current state before another operation.</p>}
      {state?.operation_id && <p className="tw-text-xs tw-break-all">Operation: {state.operation_id}</p>}
      {action === 'offline' && <p>Stop this disk's allocation and data participation. Removal will wait for its DiskDB owner and DiskIO to confirm exclusion and drain outstanding I/O.</p>}
      {state?.offline && !state.removed && <>
        {state.remove_conditions.length > 0 ? <ul aria-label="Removal conditions" className="tw-list-disc tw-pl-5">{state.remove_conditions.map(reason => <li key={reason}>{reason}</li>)}</ul>
          : <p role="status">Owner and DiskIO exclusion confirmed for this revision.</p>}
        <p>Removing this disk does not migrate or recover data. Referenced data may become unavailable. The medium, disk identity and allocation metadata are retained.</p>
        <label className="tw-flex tw-gap-2"><input type="checkbox" checked={acknowledged} disabled={busy}
          onChange={event => setAcknowledged(event.target.checked)} />I understand that data may become unavailable.</label>
      </>}
    </div>
  </Dialog>;
}
