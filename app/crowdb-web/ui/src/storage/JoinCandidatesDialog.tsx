// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { joinNodeCandidate } from '../api';
import { Dialog } from '../components/Dialog';
import { Button } from '../components/ui/Button';
import type { DiskGroupEntry } from '../types';
import { formatBytes } from '../utils/capacity';
import { candidateIneligible, storageOperationId, type DiskObservation, type JoinCandidateRequest, type SuppliedOperation } from './types';

interface Props {
  nodeId: number; disks: DiskObservation[]; groups: DiskGroupEntry[];
  onClose: () => void; onAccepted: (operation: SuppliedOperation) => void;
}
interface Choice { selected: boolean; group: string; media: string; rawConsent: boolean }
interface Row { request: JoinCandidateRequest; phase: 'submitting' | 'accepted' | 'unknown'; error?: string }

export function JoinCandidatesDialog({ nodeId, disks, groups, onClose, onAccepted }: Props) {
  const [reviewed] = useState(disks);
  const [choices, setChoices] = useState<Record<string, Choice>>({});
  const [consent, setConsent] = useState(false);
  const [sealed, setSealed] = useState<JoinCandidateRequest[]>();
  const [rows, setRows] = useState<Record<string, Row>>({});
  const [busy, setBusy] = useState(false);
  const closed = useRef(false);
  useEffect(() => () => { closed.current = true; }, []);
  const choice = (path: string): Choice => choices[path] ?? { selected: false, group: '', media: '', rawConsent: false };
  const update = (path: string, change: Partial<Choice>) => setChoices(previous => ({ ...previous, [path]: { ...choice(path), ...change } }));
  const selected = reviewed.filter(disk => choice(disk.path).selected);
  const valid = consent && selected.length > 0 && selected.length <= 16 && selected.every(disk => !candidateIneligible(disk)
    && groups.some(group => group.id === Number(choice(disk.path).group)) && choice(disk.path).group !== ''
    && (disk.backend !== 'block-device' || choice(disk.path).rawConsent));
  const submit = async () => {
    if (busy || (!sealed && !valid)) return;
    const requests = sealed ?? selected.map(disk => ({ operation_id: storageOperationId(), disk_group_id: Number(choice(disk.path).group),
      medium: { path: disk.path, backend: disk.backend!, backing_identity: disk.backing_identity!, resource_identity: disk.resource_identity!,
        capacity_bytes: disk.capacity_bytes!, media_type: choice(disk.path).media === '' ? null : Number(choice(disk.path).media), initialize: true } }));
    setSealed(requests); setBusy(true);
    const todo = requests.filter(request => rows[request.operation_id]?.phase !== 'accepted');
    let offset = 0;
    const worker = async () => {
      while (!closed.current && offset < todo.length) {
        const request = todo[offset++];
        setRows(previous => ({ ...previous, [request.operation_id]: { request, phase: 'submitting' } }));
        try {
          const accepted = await joinNodeCandidate(nodeId, request);
          onAccepted(accepted);
          if (!closed.current) setRows(previous => ({ ...previous, [request.operation_id]: { request, phase: 'accepted' } }));
        } catch (cause) {
          if (!closed.current) setRows(previous => ({ ...previous, [request.operation_id]: { request, phase: 'unknown', error: String(cause) } }));
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(4, todo.length) }, worker));
    if (!closed.current) setBusy(false);
  };
  const allAccepted = !!sealed && sealed.every(request => rows[request.operation_id]?.phase === 'accepted');
  return <Dialog isOpen title="Join Candidates" onClose={onClose} onConfirm={submit}
    confirmLabel={sealed ? 'Retry unresolved submissions' : 'Join selected disks'} confirmLoading={busy}
    confirmDisabled={busy || allAccepted || (!sealed && !valid)}>
    <div className="tw-space-y-3 tw-text-sm">
      <p>Select at most 16 supplied media and their existing target groups. A group has one media class; backend file/device does not imply HDD/SSD.</p>
      <p>Every usable byte must read as zero before registration. No wipe is performed. Usable capacity contains whole zones; any physical tail is retained.</p>
      <p className="tw-text-xs tw-text-muted">Accepted operations continue on the node after closing. Closing stops any submissions that have not yet been sent.</p>
      {reviewed.map(disk => { const state = choice(disk.path); const reason = candidateIneligible(disk); return <fieldset key={disk.path} disabled={!!sealed} className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-2">
        <label className="tw-flex tw-gap-2 tw-break-all"><input type="checkbox" aria-label={`Join ${disk.path}`} disabled={!!reason} checked={state.selected} onChange={event => update(disk.path, { selected: event.target.checked })} />{disk.path}</label>
        <p>{disk.backend ?? 'Unknown backend'} · {disk.capacity_bytes === null ? 'Unknown capacity' : formatBytes(disk.capacity_bytes)}</p>
        <p className="tw-text-xs tw-break-all">{disk.resource_identity ?? 'Unverified resource'}</p>
        {reason && <p role="status">{reason}</p>}
        {state.selected && <>
          <label>Disk group <select aria-label={`Disk group for ${disk.path}`} value={state.group} onChange={event => update(disk.path, { group: event.target.value })} className="tw-bg-surface tw-border tw-border-border">
            <option value="">Select group</option>{groups.map(group => <option key={group.id} value={group.id}>{group.name || `DG-${group.id}`} · DG-{group.id}</option>)}
          </select></label>
          <label>Media <select aria-label={`Media for ${disk.path}`} value={state.media} onChange={event => update(disk.path, { media: event.target.value })} className="tw-bg-surface tw-border tw-border-border">
            <option value="">Unknown</option><option value="0">HDD</option><option value="1">SSD</option><option value="2">Zoned SSD</option><option value="3">SMR HDD</option>
          </select></label>
          {disk.backend === 'block-device' && <label className="tw-block"><input type="checkbox" checked={state.rawConsent} onChange={event => update(disk.path, { rawConsent: event.target.checked })} />Authorize initialization of {disk.path}</label>}
        </>}
      </fieldset>; })}
      {groups.length === 0 && <p role="status">Create and bind a compatible DiskGroup before joining media.</p>}
      <label className="tw-block"><input type="checkbox" checked={consent} disabled={!!sealed} onChange={event => setConsent(event.target.checked)} />Initialize the selected blank media and register them in Group 0</label>
      {selected.length > 16 && <p role="alert">Select at most 16 disks per submission.</p>}
      {Object.values(rows).map(row => <div key={row.request.operation_id} className="tw-break-all"><p role="status">{row.request.medium.path} · {row.phase === 'accepted' ? 'Accepted · node continues verification' : row.phase}</p>
        {row.error && <p role="alert">{row.error} · Reconcile or retry the same operation {row.request.operation_id}; successful rows are retained.</p>}</div>)}
      {allAccepted && <Button size="sm" onClick={onClose}>View node progress</Button>}
    </div>
  </Dialog>;
}
