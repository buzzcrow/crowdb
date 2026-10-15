// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass, JsonView } from '../access/Workbench';
import * as api from './api';

export function InspectionPreparation({ dataset, snapshot, readonly, refreshed }: {
  dataset: string; snapshot: string; readonly: boolean; refreshed: () => void;
}) {
  const [busy, setBusy] = useState(false); const [status, setStatus] = useState<unknown>(); const [error, setError] = useState('');
  const path = `${api.snapshotPath(dataset, snapshot)}/inspection`;
  const run = async (advance: boolean) => {
    setBusy(true); setError('');
    try { const value = advance ? await api.mutate('inspection-preparation', path, 'POST') : await api.control(path); setStatus(value); refreshed(); }
    catch (reason) { setError(String(reason)); } finally { setBusy(false); }
  };
  return <div className="tw-space-y-2">
    <p>Older Snapshots may require bounded metadata preparation. Unsupported records remain explicit.</p>
    <button className={buttonClass} disabled={busy} onClick={() => void run(false)}>Inspect metadata preparation</button>
    {!readonly && <button className={buttonClass} disabled={busy} onClick={() => void run(true)}>Advance metadata preparation</button>}
    {error && <p role="alert">{error}</p>}{status !== undefined && <JsonView value={status} />}
  </div>;
}
