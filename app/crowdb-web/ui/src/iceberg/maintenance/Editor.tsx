// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import type { Selection, TableLoad } from '../types';
import { begin, forget, load, protection, submit, type Maintenance } from './operation';

export function MaintenanceEditor({ namespace, name, table, target, action, close, complete }: {
  namespace: string[]; name: string; table: TableLoad; target: Selection; action: Maintenance['action']; close: () => void; complete: () => Promise<void>;
}) {
  const [op, setOp] = useState<Maintenance>(); const [error, setError] = useState(''); const [busy, setBusy] = useState(false);
  const controller = useRef<AbortController>();
  useEffect(() => {
    try { setOp(load(namespace, name) ?? begin(namespace, name, table, target, action)); } catch (reason) { setError(String(reason)); }
    return () => controller.current?.abort();
  }, []);
  const submitRequest = async () => {
    if (!op || busy) return;
    setBusy(true); setError(''); const request = new AbortController(); controller.current = request;
    try { await submit(op, request.signal); if (!request.signal.aborted) await complete(); }
    catch (reason) { if (!request.signal.aborted) setError(String(reason)); }
    finally { if (!request.signal.aborted) { setBusy(false); setOp({ ...op }); } }
  };
  const title = (op?.action ?? action) === 'remove-file' ? 'Remove from table' : 'Expire snapshot';
  const problem = op && protection(op.table!, op.target, op.action);
  return <section aria-label={title} className="tw-space-y-5">
    <h1 className="tw-text-lg tw-font-semibold">{title}</h1>
    <p className="tw-text-sm">{namespace.join(' / ')} / {name}</p>
    {op && <>
      <p className="tw-text-sm tw-break-all">Snapshot {String(op.target.snapshot['snapshot-id'])}{op.target.file && ` · ${op.target.file.location}`}</p>
      {op.action === 'remove-file' ? <p className="tw-text-sm">Publish a new Snapshot without this file ({op.target.file!.records} records). Existing Snapshots keep their contents.</p>
        : <p className="tw-text-sm">Remove this historical Snapshot. Current Snapshots and Branch/Tag references are protected.</p>}
      <p className="tw-text-sm tw-text-muted">Snapshot expiry and reference removal do not physically delete files.</p>
      {op.uncertain && <p role="status">The publication result is unknown. Retry sends the same request; Close keeps it for recovery.</p>}
      <details><summary>Request details</summary><p className="tw-break-all">{op.table!['metadata-location']}</p><p>{op.identity}</p>{op.action === 'remove-file' && <p>New Snapshot: {op.snapshot}</p>}</details>
    </>}
    {(error || problem) && <p role="alert" className="tw-text-failed">{error || problem}</p>}
    <div className="tw-flex tw-gap-3"><button className={buttonClass} onClick={close}>Close</button>
      {op && <button className={buttonClass} disabled={busy || op.uncertain} onClick={() => { forget(op); close(); }}>Discard request</button>}
      <button className={`${buttonClass} tw-text-failed`} disabled={busy || !op || !!problem} onClick={() => void submitRequest()}>{busy ? 'Publishing…' : op?.uncertain ? 'Retry same request' : title}</button>
    </div>
  </section>;
}
