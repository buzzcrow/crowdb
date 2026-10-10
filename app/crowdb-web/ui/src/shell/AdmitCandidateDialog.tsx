// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { admitCandidate, updateCandidate, type NodeAdmission } from '../api';
import { Dialog } from '../components/Dialog';
import { Input, Select } from '../components/ui/Input';
import type { Rack } from '../types';

export function AdmitCandidateDialog({ discoveryId, racks, current, onClose, onSuccess }: { discoveryId: string; racks: Rack[]; current?: NodeAdmission; onClose: () => void; onSuccess?: () => void }) {
  const [rack, setRack] = useState(String(current?.rack_id ?? racks[0]?.id ?? ''));
  const [user, setUser] = useState(current?.ssh_user ?? 'crowdb');
  const [password, setPassword] = useState(current ? '' : 'crowdb');
  const [port, setPort] = useState(String(current?.ssh_port ?? 2222));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const submit = async () => {
    if (busy) return;
    setBusy(true); setError('');
    const initialPassword = password;
    setPassword('');
    try {
      await (current ? updateCandidate : admitCandidate)({ discovery_id: discoveryId, rack_id: Number(rack), ssh_user: user, ssh_port: Number(port), ssh_password: initialPassword || null });
      onSuccess?.(); onClose();
    } catch (error) { setError(error instanceof Error ? error.message : String(error)); }
    finally { setBusy(false); }
  };
  return <Dialog isOpen title={current ? "Update cluster node" : "Move candidate to cluster"} onClose={() => { if (!busy) onClose(); }} onConfirm={submit} confirmLabel={busy ? 'Verifying SSH…' : current ? 'Verify and update' : 'Verify and move'} confirmDisabled={busy || !rack || !user || !Number(port)}>
    <div className="tw-space-y-3">
      <p className="tw-text-xs tw-break-all">{discoveryId}</p>
      <Select label="Rack" value={rack} onChange={event => setRack(event.target.value)}>{racks.map(rack => <option key={rack.id} value={rack.id}>{rack.name || `R-${rack.id}`}</option>)}</Select>
      <Input label="SSH user" value={user} onChange={event => setUser(event.target.value)} autoComplete="username" />
      <Input label="SSH port" type="number" value={port} onChange={event => setPort(event.target.value)} />
      <Input label="Initial SSH password" type="password" value={password} onChange={event => setPassword(event.target.value)} autoComplete="new-password" />
      <p className="tw-text-xs tw-text-muted">Docker test login: crowdb / crowdb. Leave the password empty for existing Ed25519 access. Passwords are discarded after setup.</p>
      {error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
    </div>
  </Dialog>;
}
