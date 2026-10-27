// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Fields } from '../iceberg/Fields';
import { useNodeCandidates } from './NodeCandidatesContext';

export function CandidateProperties({ id, readonly }: { id: string; readonly?: boolean }) {
  const { snapshot, candidates, admissions, policy, policyUnavailable, unavailable, saving, changeAutoJoin, actionError } = useNodeCandidates();
  const node = candidates.find(entry => entry.advertisement.discovery_id === id);
  const record = admissions.find(entry => entry.discovery_id === id);
  const enabled = policy ? policy.candidates?.[id] ?? policy.enabled : undefined;
  return <section aria-label="Candidate properties" className="tw-p-4 tw-space-y-3">
    <Fields stacked values={{ 'Discovery ID': id, State: node?.state ?? (record?.confirmed ? 'Joined' : 'Not currently discovered'),
      Addresses: node?.advertisement.monitor_endpoints, 'This node': snapshot?.local.advertisement.discovery_id === id,
      Admission: record ? record.confirmed ? 'Confirmed' : record.cancelled ? 'Cancelled' : 'Accepted' : 'Not accepted',
      Operation: record?.operation_id, 'Target Rack': record?.rack_id ?? 'Default Rack',
      Error: policy?.observation.needs_access?.find(entry => entry.discovery_id === id)?.error }} />
    <label className="tw-block tw-text-sm">Auto join <select aria-label="Auto join" value={enabled === undefined ? 'unknown' : String(enabled)} disabled={readonly || unavailable || policyUnavailable || enabled === undefined || saving !== undefined || node?.state !== 'unbound'} onChange={event => void changeAutoJoin(id, event.target.value === 'true')} className="tw-bg-bg tw-border tw-border-border tw-rounded tw-p-1">
      {enabled === undefined && <option value="unknown">Unknown</option>}<option value="true">Enabled</option><option value="false">Disabled</option>
    </select></label>
    {record && !record.cancelled && !record.confirmed && <p className="tw-text-xs tw-text-muted">Accepted admission continues independently of Auto join.</p>}
    {actionError && <p role="alert">{actionError}</p>}
  </section>;
}
