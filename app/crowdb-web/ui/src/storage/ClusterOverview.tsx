// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import type { ServerSummary } from '../api';
import { Domain, NodeHealth, type Node, type Rack, type HardwareCapacitySummary, type CapacityUsageResponse } from '../types';
import { useSelection } from '../contexts/SelectionContext';
import { useNavigationSnapshot } from '../contexts/DomainContext';
import { Button } from '../components/ui/Button';
import { observeCapacity } from '../panels/capacity/observation';
import { toUiHealth } from '../utils/entityDisplay';
import { formatBytes } from '../utils/capacity';
import type { NodeSetupSummary } from '../shell/nodeSetup';
import type { CandidateSummary } from './types';

export interface ClusterOverviewProps {
  racks: Rack[]; nodes: Node[]; nodeHealthById: Record<string, NodeHealth>;
  services: ServerSummary[]; hardware: HardwareCapacitySummary | null; usage: CapacityUsageResponse | null;
  setups?: Record<number, NodeSetupSummary>;
  candidates: Record<number, CandidateSummary>; capacity?: boolean; readonly: boolean;
  onAddRack: () => void; onAddNode: (rackId: number) => void;
}
type Sort = 'node' | 'capacity' | 'busy' | 'free';

/** A summary of existing observations; opening this page never discovers every node's disks. */
export function ClusterOverview({ racks, nodes, nodeHealthById, services, hardware, usage, candidates, setups = {}, capacity, readonly, onAddRack, onAddNode }: ClusterOverviewProps) {
  const { scopeEntity, selectedEntity, selectEntity, inspectEntity } = useSelection();
  const [query, setQuery] = useState('');
  const [health, setHealth] = useState('all');
  const [candidatesOnly, setCandidatesOnly] = useState(false);
  const [sort, setSort] = useState<Sort>('node');
  const [descending, setDescending] = useState(false);
  useNavigationSnapshot(Domain.Cluster, capacity ? 'cluster-overview-capacity' : 'cluster-overview-topology', () => {
    const saved = { query, health, candidatesOnly, sort, descending };
    return () => { setQuery(saved.query); setHealth(saved.health); setCandidatesOnly(saved.candidatesOnly); setSort(saved.sort); setDescending(saved.descending); };
  });
  const rackId = scopeEntity?.type === 'Rack' ? Number(scopeEntity.id) : undefined;
  const visibleRacks = racks.filter(rack => rackId === undefined || rack.id === rackId);
  const rows = nodes.filter(node => (rackId === undefined || node.rack_id === rackId)
    && `${node.id} ${node.host}`.toLowerCase().includes(query.toLowerCase())
    && (health === 'all' || (nodeHealthById[node.id] ?? 'unknown') === health)
    && (!candidatesOnly || (candidates[node.id]?.count ?? 0) > 0)).map(node => ({
      node, ...observeCapacity(hardware, usage, { nodeId: node.id, rackId: node.rack_id }),
    }));
  rows.sort((a, b) => {
    const x = sort === 'node' ? a.node.id : a[sort]; const y = sort === 'node' ? b.node.id : b[sort];
    if (x === null || y === null) return x === y ? a.node.id - b.node.id : x === null ? 1 : -1;
    return (descending ? y - x : x - y) || a.node.id - b.node.id;
  });
  const entity = (node: Node) => ({ domain: Domain.Cluster, type: 'Node' as const, id: String(node.id), parentIds: { rack_id: node.rack_id } });
  const chosen = (node: Node) => selectedEntity?.type === 'Node' && selectedEntity.id === String(node.id);
  const summary = (node: Node) => {
    const actual = services.filter(service => service.node_id === node.id);
    const healthy = actual.filter(service => toUiHealth(service.health) === 'Healthy').length;
    const observed = candidates[node.id];
    return { health: nodeHealthById[node.id] ?? 'unknown', services: actual.length ? `${healthy}/${actual.length} services healthy` : 'No services observed',
      candidates: observed ? `${observed.count}${observed.partial ? '+' : ''}${observed.stale ? ' · Stale' : ' · Last observed'}` : 'Not observed',
      alerts: hardware?.disk_groups.filter(group => group.node_id === node.id).flatMap(group => group.disks).filter(disk => disk.status !== 1).length };
  };
  const setup = (node: Node) => {
    const state = setups[node.id];
    return state ? `Setup: ${state.phase === 'ready' ? state.warning ? 'Completed with warnings' : 'Completed' : state.phase}${state.stale ? ' · Stale' : ' · Last observed'}${state.error ? ` · ${state.error}` : ''}` : 'Setup: Not observed';
  };
  const bytes = (value: number | null) => value === null ? 'Unknown' : formatBytes(value);
  return <section aria-label={capacity ? 'Cluster capacity workspace' : 'Cluster node workspace'} className={capacity ? "tw-space-y-4" : "tw-h-full tw-overflow-auto tw-p-4 tw-space-y-4"}>
    <header className="tw-flex tw-items-center tw-justify-between tw-gap-3">
      <h1 className="tw-text-lg tw-font-semibold">{capacity ? 'Capacity Overview' : 'Cluster nodes'}{rackId === undefined ? '' : ` · Rack ${rackId}`}</h1>
      {!readonly && (rackId === undefined ? <Button size="sm" disabled={readonly} onClick={onAddRack}>Add Rack</Button>
        : <Button size="sm" disabled={readonly} onClick={() => onAddNode(rackId)}>Add Node</Button>)}
    </header>
    <div className="tw-flex tw-flex-wrap tw-gap-3 tw-text-sm">
      <label>Find node <input aria-label="Find node" value={query} onChange={event => setQuery(event.target.value)} className="tw-bg-surface tw-border tw-border-border tw-rounded tw-p-1" /></label>
      <label>Health <select aria-label="Node health" value={health} onChange={event => setHealth(event.target.value)} className="tw-bg-surface tw-border tw-border-border tw-p-1">
        <option value="all">All</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option>
      </select></label>
      <label><input type="checkbox" checked={candidatesOnly} onChange={event => setCandidatesOnly(event.target.checked)} /> With observed candidates</label>
    </div>
    <p className="tw-text-xs tw-text-muted">Registered logical capacity and last observed usage. Unknown values require an observation; open a node to discover candidates. Disk alerts reflect the last hardware observation.</p>
    {capacity && <nav aria-label="Capacity racks" className="tw-text-sm tw-space-y-2">
      <p>Racks ({visibleRacks.length}) · {hardware ? hardware.disk_groups.filter(group => rackId === undefined || group.rack_id === rackId).length : 'Unknown'} DG(s) · {nodes.filter(node => rackId === undefined || node.rack_id === rackId).length} node(s)</p>
      <div className="tw-flex tw-gap-3">{visibleRacks.map(rack => <button key={rack.id} className="tw-text-accent" onClick={() => selectEntity({ domain: Domain.Cluster, type: 'Rack', id: String(rack.id) })}>R-{rack.id} {rack.name || 'Open rack'}</button>)}</div>
    </nav>}
    {capacity ? <div className="tw-overflow-x-auto"><table className="tw-w-full tw-text-sm tw-text-left" aria-label="Node capacity">
      <thead><tr>{(['node', 'capacity', 'busy', 'free'] as const).map(field => <th key={field} aria-sort={sort === field ? descending ? 'descending' : 'ascending' : 'none'}>
        <button onClick={() => { if (sort === field) setDescending(value => !value); else { setSort(field); setDescending(false); } }}>{field === 'busy' ? 'Used' : field[0].toUpperCase() + field.slice(1)}</button>
      </th>)}<th>Health / services</th><th>Candidates</th><th>Actions</th></tr></thead>
      <tbody>{rows.map(row => { const state = summary(row.node); return <tr key={row.node.id} aria-selected={chosen(row.node)} tabIndex={0}
        onClick={() => inspectEntity(entity(row.node))} onKeyDown={event => { if (event.target === event.currentTarget && (event.key === 'Enter' || event.key === ' ')) { event.preventDefault(); inspectEntity(entity(row.node)); } }} className="tw-border-t tw-border-border">
        <td className="tw-py-3"><button className="tw-text-accent" onClick={event => { event.stopPropagation(); selectEntity(entity(row.node)); }}>N-{row.node.id}</button><p className="tw-text-xs tw-text-muted">{row.node.host} · Rack {row.node.rack_id}</p></td>
        <td>{bytes(row.capacity)}</td><td>{bytes(row.busy)}</td><td>{bytes(row.free)}</td><td>{state.health} · {state.services}<p className="tw-text-xs tw-text-muted">{setup(row.node)}</p>{state.alerts ? ` · ${state.alerts} disk alerts` : ''}</td><td>{state.candidates}</td>
        <td><button onClick={() => inspectEntity(entity(row.node))}>Properties</button></td>
      </tr>; })}</tbody>
    </table></div> : visibleRacks.map(rack => <section key={rack.id} aria-label={`Rack ${rack.id} nodes`} className="tw-space-y-3">
      <header className="tw-flex tw-justify-between tw-items-center tw-gap-3"><button className="tw-font-semibold tw-text-accent" onClick={() => selectEntity({ domain: Domain.Cluster, type: 'Rack', id: String(rack.id) })}>{rack.name || `Rack ${rack.id}`}</button>
        {!readonly && rackId === undefined && <Button size="sm" variant="secondary" onClick={() => onAddNode(rack.id)}>Add Node</Button>}</header>
      <div className="tw-grid tw-grid-cols-1 xl:tw-grid-cols-2 tw-gap-3">{rows.filter(row => row.node.rack_id === rack.id).map(row => {
        const state = summary(row.node);
        return <article key={row.node.id} aria-label={`Node ${row.node.id} summary`} tabIndex={0} aria-current={chosen(row.node) ? 'true' : undefined}
          onClick={() => inspectEntity(entity(row.node))} onKeyDown={event => { if (event.target === event.currentTarget && (event.key === 'Enter' || event.key === ' ')) { event.preventDefault(); inspectEntity(entity(row.node)); } }} className="tw-border tw-border-border tw-rounded tw-p-4 tw-space-y-2">
          <div className="tw-flex tw-justify-between"><button className="tw-text-accent tw-font-semibold" onClick={event => { event.stopPropagation(); selectEntity(entity(row.node)); }}>N-{row.node.id}</button><span>{state.health}</span></div>
          <p className="tw-text-sm tw-break-all">{row.node.host}</p><p className="tw-text-sm">{state.services}</p><p className="tw-text-xs tw-text-muted">{setup(row.node)}</p>
          <p className="tw-text-sm">{bytes(row.busy)} used / {bytes(row.capacity)} total</p><p className="tw-text-xs tw-text-muted">Candidates: {state.candidates}{state.alerts ? ` · ${state.alerts} disk alerts` : ''}</p>
          <button className="tw-text-xs tw-text-accent" onClick={event => { event.stopPropagation(); selectEntity({ ...entity(row.node), section: 'candidates' }); }}>Open candidates</button>
        </article>;
      })}</div>
      {!rows.some(row => row.node.rack_id === rack.id) && <p className="tw-text-sm tw-text-muted">No matching nodes in this rack.</p>}
    </section>)}
    {visibleRacks.length === 0 && <p className="tw-text-sm tw-text-muted">No racks. Add a rack to place nodes.</p>}
    {capacity && rows.length === 0 && <p className="tw-text-sm tw-text-muted">No matching nodes.</p>}
  </section>;
}
