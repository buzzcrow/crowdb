// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState, useEffect } from 'react';
import { setManagementToken } from '../api';
import { buttonClass, inputClass } from '../access/Workbench';

export function ManagementSession({ apiPrefix, onAuthorized }: { apiPrefix: string; onAuthorized: (authorized: boolean) => void }) {
  const [token, setToken] = useState('');
  const [authorized, setAuthorized] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => () => setManagementToken(''), []);
  return <div className="tw-px-4 tw-py-2 tw-bg-panel tw-border-b tw-border-border tw-flex tw-items-center tw-gap-3 tw-text-xs" data-testid="managed-preview">
    <span data-testid="managed-source">Source: Group 0</span><span data-testid="managed-readonly">Hardware topology is read-only</span>
    <form className="tw-flex tw-items-center tw-gap-2" onSubmit={event => { event.preventDefault(); setBusy(true); void (async () => {
      const response = await fetch(`${apiPrefix}/management/check`, { method: 'POST', headers: { Authorization: `Bearer ${token}` } });
      if (!response.ok) throw new Error(`Management authorization failed (${response.status})`);
      setManagementToken(token); setAuthorized(true); onAuthorized(true); setError('');
    })().catch(error => { setManagementToken(''); setAuthorized(false); onAuthorized(false); setError(String(error)); }).finally(() => setBusy(false)); }}>
      <label>Management token <input className={inputClass} type="password" autoComplete="off" value={token} disabled={busy} onChange={event => { setToken(event.target.value); setManagementToken(''); setAuthorized(false); onAuthorized(false); }} /></label>
      <button className={buttonClass} disabled={!token || busy}>Authorize</button>{authorized && <span>Logical operations enabled</span>}
    </form>{error && <span role="alert" className="tw-text-failed">{error}</span>}
  </div>;
}
