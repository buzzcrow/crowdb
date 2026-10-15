// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useRef, useState } from 'react';
import { admitCandidate, prepareCandidate, discardCandidateReview, updateCandidate, type NodeAdmission, type PreparedNodeReference } from '../api';
import { Dialog } from '../components/Dialog';
import { Input, Select } from '../components/ui/Input';
import type { Rack } from '../types';
import { NodeSetupOptions } from './NodeSetupOptions';
import { defaultNodeSetup, discoveredStorage, validNodeSetup, type PreparedNodeAccess, type NodeSetupChoices, type NodeAdmissionRequest } from './nodeSetup';

export function AdmitCandidateDialog({ discoveryId, racks, current, preparedNode, onClose, onSuccess }: { discoveryId: string; racks: Rack[]; current?: NodeAdmission; preparedNode?: PreparedNodeReference; onClose: () => void; onSuccess?: () => void }) {
  const [rack, setRack] = useState(String(current?.rack_id ?? preparedNode?.rack_id ?? racks[0]?.id ?? ''));
  const [user, setUser] = useState(current?.ssh_user ?? preparedNode?.ssh_user ?? 'crowdb');
  const [password, setPassword] = useState(current || preparedNode ? '' : 'crowdb');
  const [port, setPort] = useState(String(current?.ssh_port ?? preparedNode?.ssh_port ?? 2222));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [prepared, setPrepared] = useState<PreparedNodeAccess>();
  const [choices, setChoices] = useState<NodeSetupChoices>(defaultNodeSetup);
  const [submitted, setSubmitted] = useState<NodeAdmissionRequest>();
  const credential = useRef<string | null>(null);
  const close = async () => {
    if (busy) return;
    const operation = prepared?.operation_id ?? preparedNode?.operation_id;
    setBusy(true); setError('');
    try {
      if (!submitted && operation) await discardCandidateReview(discoveryId, operation);
      credential.current = null; onClose();
    } catch (cause) { setError(String(cause)); }
    finally { setBusy(false); }
  };
  const submit = async () => {
    if (busy) return;
    setBusy(true); setError('');
    const initialPassword = password;
    setPassword('');
    try {
      const access = { discovery_id: discoveryId, rack_id: Number(rack), ssh_user: user, ssh_port: Number(port), ssh_password: initialPassword || null };
      if (!current && !prepared) {
        const result = await prepareCandidate({ ...access, preflight_operation_id: preparedNode?.operation_id });
        credential.current = initialPassword || null;
        setPrepared(result); setChoices(previous => result.accepted_choices ?? { ...previous, storage: discoveredStorage(result.inventory) });
        return;
      }
      if (current) await updateCandidate(access);
      else {
        const request = submitted ?? { ...access, ssh_password: null, preflight_operation_id: prepared!.operation_id, setup: choices };
        setSubmitted(request);
        await admitCandidate({ ...request, ssh_password: credential.current });
      }
      credential.current = null;
      onSuccess?.(); onClose();
    } catch (error) { setError(error instanceof Error ? error.message : String(error)); }
    finally { setBusy(false); }
  };
  return <Dialog isOpen title={current ? "Update cluster node" : "Move candidate to cluster"} onClose={() => { void close(); }} onConfirm={submit} confirmLabel={busy ? prepared ? 'Submitting setup…' : 'Verifying SSH…' : current ? 'Verify and update' : prepared ? submitted ? 'Retry same setup' : 'Join and start' : 'Verify and review'} confirmDisabled={busy || !rack || !user || !Number(port) || (!!prepared && !submitted && !validNodeSetup(choices))}>
    <div className="tw-space-y-3">
      <p className="tw-text-xs tw-break-all">{discoveryId}</p>
      <h3 className="tw-font-semibold">Node</h3>
      <Select label="Rack" value={rack} disabled={busy || !!prepared} onChange={event => setRack(event.target.value)}>{racks.map(rack => <option key={rack.id} value={rack.id}>{rack.name || `R-${rack.id}`}</option>)}</Select>
      <Input label="SSH user" value={user} disabled={busy || !!prepared} onChange={event => setUser(event.target.value)} autoComplete="username" />
      <Input label="SSH port" type="number" value={port} disabled={busy || !!prepared} onChange={event => setPort(event.target.value)} />
      <Input label="Initial SSH password" type="password" value={password} disabled={busy || !!prepared} onChange={event => setPassword(event.target.value)} autoComplete="new-password" />
      <p className="tw-text-xs tw-text-muted">Docker test login: crowdb / crowdb. Leave the password empty for existing Ed25519 access. Passwords are discarded after setup.</p>
      {!current && !prepared && <p className="tw-text-xs tw-text-muted">Verify access first to review actual disks. This step does not admit the node or initialize storage.</p>}
      {prepared && <><p role="status" className="tw-text-sm">Access verified · {prepared.accepted_choices ? 'resuming the previously accepted setup.' : 'node remains a candidate until you submit.'} Accepted setup continues after this dialog or browser closes.</p>
        {prepared.accepted_choices && <p className="tw-text-xs tw-text-muted">Previously accepted choices are fixed. This submission resumes the same setup.</p>}
        <NodeSetupOptions choices={choices} inventory={prepared.inventory} disabled={busy || !!submitted || !!prepared.accepted_choices} onChange={setChoices} />
        {!validNodeSetup(choices) && <p role="status" className="tw-text-xs tw-text-failed">Resolve listener conflicts, storage capacity or selected-device agreement before joining.</p>}
      </>}
      {error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
    </div>
  </Dialog>;
}
