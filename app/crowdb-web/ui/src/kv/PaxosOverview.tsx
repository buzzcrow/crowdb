// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { Domain, type EnrichedStoreView, type GroupView } from '../types';
import { useSelection, type SelectedEntity } from '../contexts/SelectionContext';
import { useDomain } from '../contexts/DomainContext';

const WINDOW = 100;
const button = 'tw-rounded tw-border tw-border-border tw-px-3 tw-py-1 tw-text-sm disabled:tw-opacity-40';

export function PaxosOverview({ stores, selectedEntity, loading, backendError }: {
  stores: EnrichedStoreView[]; selectedEntity: SelectedEntity | null;
  loading: boolean; backendError: boolean;
}) {
  const { selectEntity } = useSelection();
  const [offset, setOffset] = useState(0);
  const sid = selectedEntity?.type === 'Store' ? selectedEntity.id : selectedEntity?.parentIds?.store_id;
  const gid = selectedEntity?.type === 'Group' ? selectedEntity.id : selectedEntity?.parentIds?.group_id;
  const groups = stores.filter(store => sid == null || String(store.store_id) === String(sid))
    .flatMap(store => store.groups);
  const group = groups.find(item => String(item.group_id) === String(gid));
  const start = Math.min(offset, Math.max(0, Math.ceil(groups.length / WINDOW) - 1) * WINDOW);
  return <section className="tw-h-full tw-overflow-auto tw-p-4 tw-space-y-4" data-testid="paxos-overview">
    <header>
      <h2 className="tw-text-lg tw-font-semibold">Paxos groups</h2>
      <p className="tw-text-sm tw-text-muted">Store → Group → Replica. Select a group to inspect membership and read progress.</p>
      {backendError && <p role="status">Backend unreachable — observations may be stale.</p>}
    </header>
    {loading && !stores.length ? <p>Loading topology…</p> : !stores.length ?
      <p>No stores yet. Deploy KV servers in Cluster, then initialize Group 0 here.</p> : <>
        <div className="tw-flex tw-gap-3 tw-items-center">
          <span>{sid == null ? 'All stores' : `Store ${sid}`} · {groups.length} groups loaded</span>
          {sid != null && <button className={button} onClick={() => selectEntity(null)}>All stores</button>}
        </div>
        <div className="tw-overflow-auto">
          <table className="tw-w-full tw-text-sm tw-text-left" aria-label="Paxos groups">
            <thead><tr>{['Store / Group', 'Health', 'Leader', 'Replicas'].map(label => <th key={label} className="tw-p-2">{label}</th>)}</tr></thead>
            <tbody>{groups.slice(start, start + WINDOW).map(item => <tr key={`${item.store_id}/${item.group_id}`} className="tw-border-t tw-border-border">
              <td className="tw-p-2"><button className={button} onClick={() => selectEntity({ domain: Domain.KV, type: 'Group', id: String(item.group_id), parentIds: { store_id: String(item.store_id) } })}>
                {item.store_id} / {item.group_id}
              </button></td>
              <td className="tw-p-2">{item.state ?? 'unknown'}</td>
              <td className="tw-p-2">{item.leader ?? 'Unknown'}</td>
              <td className="tw-p-2">{item.replicas.length}</td>
            </tr>)}</tbody>
          </table>
        </div>
        {groups.length > WINDOW && <nav aria-label="Group pages" className="tw-flex tw-gap-3">
          <button className={button} disabled={start === 0} onClick={() => setOffset(start - WINDOW)}>Previous</button>
          <span>{start + 1}–{Math.min(start + WINDOW, groups.length)} of {groups.length}</span>
          <button className={button} disabled={start + WINDOW >= groups.length} onClick={() => setOffset(start + WINDOW)}>Next</button>
        </nav>}
        {group && <GroupDetail key={`${group.store_id}/${group.group_id}`} group={group} selected={selectedEntity} />}
      </>}
  </section>;
}

function GroupDetail({ group, selected }: { group: GroupView; selected: SelectedEntity | null }) {
  const { selectEntity } = useSelection();
  const { setDomain } = useDomain();
  const [offset, setOffset] = useState(0);
  const start = Math.min(offset, Math.max(0, Math.ceil(group.replicas.length / WINDOW) - 1) * WINDOW);
  const read = group.read_state;
  return <section className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-3" data-testid="paxos-group-detail">
    <h3 className="tw-font-semibold">Store {group.store_id} / Group {group.group_id}</h3>
    <p className="tw-text-sm">Membership: {group.membership_state ?? 'unknown'}{group.membership_epoch == null ? '' : ` · epoch ${group.membership_epoch}`}</p>
    {String(group.store_id) === '0' && String(group.group_id) === '0' && <p>System topology group · Data is read-only.</p>}
    <dl className="tw-grid tw-grid-cols-3 tw-gap-3 tw-text-sm">
      <div><dt>Read lease</dt><dd>{read ? (read.lease_valid ? 'Valid' : 'Invalid') : 'Unknown'}</dd></div>
      <div><dt>Contiguous applied</dt><dd>{read?.contiguous_applied ?? 'Unknown'}</dd></div>
      <div><dt>Safe slot</dt><dd>{read?.safe_slot ?? 'Unknown'}</dd></div>
    </dl>
    <div className="tw-overflow-auto"><table className="tw-w-full tw-text-left tw-text-sm" aria-label="Group replicas">
      <thead><tr>{['Replica', 'Node', 'Role / State', 'Engine', 'Term', 'Lease remaining'].map(label => <th className="tw-p-2" key={label}>{label}</th>)}</tr></thead>
      <tbody>{group.replicas.slice(start, start + WINDOW).map(replica => <tr key={replica.replica_id} className="tw-border-t tw-border-border">
        <td className="tw-p-2"><button className={button} aria-pressed={selected?.type === 'Replica' && selected.id === String(replica.replica_id)} onClick={() => selectEntity({
          domain: Domain.KV, type: 'Replica', id: String(replica.replica_id),
          parentIds: { store_id: String(group.store_id), group_id: String(group.group_id), node_id: replica.node_id },
        })}>{replica.replica_id}</button></td>
        <td className="tw-p-2"><button className={button} onClick={() => {
          selectEntity({ domain: Domain.Cluster, type: 'Node', id: String(replica.node_id) });
          setDomain(Domain.Cluster);
        }}>Node {replica.node_id}</button></td>
        <td className="tw-p-2">{replica.role} / {replica.state}</td>
        <td className="tw-p-2">{replica.engine_healthy == null ? 'Unknown' : replica.engine_healthy ? 'Healthy' : 'Unhealthy'}</td>
        <td className="tw-p-2">{replica.election?.current_term ?? 'Unknown'}</td>
        <td className="tw-p-2">{replica.election?.lease_remaining_ms == null ? 'Unknown' : `${replica.election.lease_remaining_ms} ms`}</td>
      </tr>)}</tbody>
    </table></div>
    {group.replicas.length > WINDOW && <nav aria-label="Replica pages" className="tw-flex tw-gap-3">
      <button className={button} disabled={!start} onClick={() => setOffset(start - WINDOW)}>Previous</button>
      <button className={button} disabled={start + WINDOW >= group.replicas.length} onClick={() => setOffset(start + WINDOW)}>Next</button>
    </nav>}
  </section>;
}
