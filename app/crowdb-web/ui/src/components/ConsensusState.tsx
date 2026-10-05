// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ElectionState, ReadState } from '../types';

/** Election state region. */
export function ElectionStateRegion({ state }: { state: ElectionState }) {
  const rows: { label: string; value: string }[] = [
    { label: 'Term', value: String(state.current_term) },
    ...(state.election_count != null
      ? [{ label: 'Elections', value: String(state.election_count) }]
      : []),
    ...(state.last_heartbeat_age_ms != null
      ? [{ label: 'Heartbeat Age', value: `${state.last_heartbeat_age_ms}ms` }]
      : []),
    ...(state.lease_remaining_ms != null
      ? [{ label: 'Lease Remaining', value: `${state.lease_remaining_ms}ms` }]
      : []),
    { label: 'Phase 1 In-Flight', value: String(state.bulk_phase1_in_flight_slots) },
    ...(state.step_downs_higher_term != null
      ? [{ label: 'Step-downs (higher term)', value: String(state.step_downs_higher_term) }]
      : []),
    ...(state.step_downs_lease_unrenewable != null
      ? [{ label: 'Step-downs (lease)', value: String(state.step_downs_lease_unrenewable) }]
      : []),
    ...(state.step_downs_admin != null
      ? [{ label: 'Step-downs (admin)', value: String(state.step_downs_admin) }]
      : []),
  ];
  return (
    <Section title="Election State">
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {rows.map((r) => (
          <div
            key={r.label}
            className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs tw-gap-2"
          >
            <dt className="tw-text-muted tw-flex-shrink-0">{r.label}</dt>
            <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text">{r.value}</dd>
          </div>
        ))}
      </dl>
    </Section>
  );
}

/** Read-path state region. */
export function ReadStateRegion({ state }: { state: ReadState }) {
  const rows: { label: string; value: string }[] = [
    { label: 'Lease Valid', value: state.lease_valid ? 'Yes' : 'No' },
    { label: 'Contiguous Applied', value: String(state.contiguous_applied) },
    { label: 'Safe Slot', value: String(state.safe_slot) },
  ];
  return (
    <Section title="Read State">
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {rows.map((r) => (
          <div
            key={r.label}
            className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs tw-gap-2"
          >
            <dt className="tw-text-muted tw-flex-shrink-0">{r.label}</dt>
            <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text">{r.value}</dd>
          </div>
        ))}
      </dl>
    </Section>
  );
}

/** Section wrapper with a heading. */
function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="tw-space-y-1">
      <h4 className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted tw-px-1">{title}</h4>
      {children}
    </div>
  );
}
