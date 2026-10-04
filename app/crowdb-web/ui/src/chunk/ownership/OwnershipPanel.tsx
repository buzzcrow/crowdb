// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { getApiBase, getManagementToken, type ServerSummary } from '../../api';
import { readJson } from '../../access/native';
import { buttonClass } from '../../access/Workbench';
import type { SelectedEntity } from '../../contexts/SelectionContext';
import { useNavigationSnapshot } from '../../contexts/DomainContext';
import type { EnrichedStoreView, Node } from '../../types';
import { ownerColor, parseSnapshot, projectOwners, type Layer, type Snapshot } from './model';
import './ownership.css';

interface Props {
  active: boolean; selection: SelectedEntity; nodes: Node[]; servers: ServerSummary[]; stores: EnrichedStoreView[];
  onSelect?: (entity: SelectedEntity) => void;
}
export function OwnershipPanel(props: Props) {
  const { selection } = props;
  const [queries, setQueries] = useState<Record<string, { slot: number | null; legendPage: number }>>({});
  useNavigationSnapshot(selection.domain, 'chunk-ownership', () => {
    const saved = { ...queries };
    return () => setQueries(saved);
  });
  const layers: Layer[] = selection.type === 'Server' ? ['service'] : ['Group', 'Store'].includes(selection.type) ? ['storage'] : ['service', 'storage'];
  const missing = selection.type === 'Node' && !props.nodes.some(node => String(node.id) === selection.id);
  return <section aria-label="Chunk ownership" className="tw-space-y-3">
    <h2 className="tw-text-sm tw-font-semibold">Chunk ownership · {selection.name ?? `${selection.type} ${selection.id}`}</h2>
    {missing && <p role="alert">Selected Node is unavailable or has been removed. Refresh topology before interpreting ownership.</p>}
    <div className="ownership-layers">{layers.map(layer => <OwnershipLayer key={layer} {...props} layer={layer}
      query={queries[`${layer}/${selection.type}/${selection.id}`] ?? { slot: null, legendPage: 0 }}
      onQuery={query => setQueries(previous => Object.fromEntries([...Object.entries(previous).filter(([key]) => key !== `${layer}/${selection.type}/${selection.id}`).slice(-31), [`${layer}/${selection.type}/${selection.id}`, query]]))} />)}</div>
  </section>;
}
function OwnershipLayer({ active, selection, nodes, servers, stores, onSelect, layer, query, onQuery }: Props & {
  layer: Layer; query: { slot: number | null; legendPage: number }; onQuery: (query: { slot: number | null; legendPage: number }) => void;
}) {
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  const { slot, legendPage } = query;
  const setSlot = (slot: number) => onQuery({ ...query, slot });
  const [hover, setHover] = useState<string | null>(null);

  const [observed, setObserved] = useState('');
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    setBusy(true); setError('');
    const token = getManagementToken();
    void fetch(`${getApiBase()}/chunk-slots?layer=${layer}&view=bitmap`, {
      signal: controller.signal, headers: token ? { Authorization: `Bearer ${token}` } : {},
    }).then(readJson<Snapshot>).then(parseSnapshot).then(result => {
      if (result.layer !== layer) throw new Error('Ownership response layer mismatch');
      if (!controller.signal.aborted) { setSnapshot(result); setObserved(new Date().toLocaleTimeString()); }
    }).catch(cause => { if (!controller.signal.aborted) setError(String(cause)); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [active, revision, layer]);
  const owners = snapshot ? projectOwners(snapshot, selection, nodes, servers, stores) : [];
  const byId = new Map(owners.map(owner => [owner.id, owner]));
  const legend = owners.filter(owner => owner.scope !== 'outside');
  const title = layer === 'service' ? 'Serving ownership' : 'Storage ownership';
  const selectedOwner = snapshot && slot !== null ? byId.get(snapshot.owners[slot]) : undefined;
  const owned = snapshot?.owners.filter(id => byId.get(id)?.scope === 'inside').length ?? 0;
  const unknown = snapshot?.owners.filter(id => byId.get(id)?.scope === 'unknown').length ?? 0;
  const selectOwner = (id: string) => {
    const owner = byId.get(id)!;
    onSelect?.(layer === 'service'
      ? { domain: selection.domain, type: 'Server', serviceType: 'chunkdb', id: `chunkdb-${id}`, name: owner.label, parentIds: { node_id: owner.nodes[0] ?? '' } }
      : { domain: selection.domain, type: 'Group', id: id.split('/')[1], name: owner.label, parentIds: { store_id: id.split('/')[0] } });
  };
  return <section aria-label={title} className="ownership-card">
    <div className="tw-flex tw-justify-between tw-items-center tw-gap-2"><h3 className="tw-text-sm tw-font-semibold">{title}</h3><button className={buttonClass} disabled={busy} onClick={() => setRevision(value => value + 1)}>Refresh slots</button></div>
    {busy && <p role="status">{snapshot ? 'Refreshing observation…' : 'Loading ownership…'}</p>}
    {error && <p role="alert">Unavailable: {error}{snapshot && ' · Previous observation is stale.'}</p>}
    {snapshot && <>
      <p>{owned} / 1024 slots in scope · generation {snapshot.generation} · observed {observed}{unknown > 0 && ` · ${unknown} slots have unknown scope`}</p>
      <p className="tw-text-muted">0–1023 · left to right, top to bottom · gray: outside scope · patterned: unknown scope</p>
      <div className="ownership-grid" role="group" aria-label={`${title} bitmap`} data-generation={snapshot.generation} data-stale={!!error}>
        {snapshot.owners.map((id, index) => {
          const owner = byId.get(id)!;
          return <button key={index} type="button" data-slot={index} data-owner={id} data-scope={owner.scope} aria-label={`Slot ${index}: ${owner.label} · ${owner.scope}`} aria-pressed={slot === index}
            className={`ownership-cell ${owner.scope}`} title={`Slot ${index} · ${owner.label} · ${owner.nodes.map(node => `N-${node}`).join(', ') || 'Node association unknown'}`}
            style={{ backgroundColor: owner.scope === 'inside' ? ownerColor(`${layer}/${id}`) : undefined, opacity: hover && hover !== id ? 0.2 : 1 }}
            tabIndex={index === (slot ?? 0) ? 0 : -1} onClick={() => setSlot(index)}
            onKeyDown={event => {
              const delta = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -32, ArrowDown: 32 }[event.key];
              if (delta == null) return;
              event.preventDefault(); const next = Math.max(0, Math.min(1023, index + delta)); setSlot(next);
              (event.currentTarget.parentElement?.children[next] as HTMLButtonElement)?.focus();
            }} />;
        })}
      </div>
      <div className="ownership-legend" aria-label={`${title} owners`}>
        {legend.slice(legendPage * 12, legendPage * 12 + 12).map(owner => <button key={owner.id} className={buttonClass}
          onMouseEnter={() => setHover(owner.id)} onMouseLeave={() => setHover(null)} onFocus={() => setHover(owner.id)} onBlur={() => setHover(null)}
          disabled={!onSelect} onClick={() => selectOwner(owner.id)}><span className="ownership-swatch" style={{ backgroundColor: ownerColor(`${layer}/${owner.id}`) }} />{owner.label} · {snapshot.owners.filter(id => id === owner.id).length}{owner.scope === 'unknown' && ' · scope unknown'}</button>)}
      </div>
      {legend.length > 12 && <nav aria-label={`${title} legend pages`}><button disabled={!legendPage} onClick={() => onQuery({ ...query, legendPage: legendPage - 1 })}>Previous owners</button> {legendPage + 1} <button disabled={(legendPage + 1) * 12 >= legend.length} onClick={() => onQuery({ ...query, legendPage: legendPage + 1 })}>Next owners</button></nav>}
      {selectedOwner && <aside aria-label={`${title} slot properties`} className="ownership-properties">
        <strong>Slot {slot} · {selectedOwner.label}</strong>
        <p>Owner ID: {selectedOwner.id} · generation {snapshot.generation}</p>
        <p>{layer === 'storage' ? 'Replica nodes' : 'Serving node'}: {selectedOwner.nodes.map(node => `N-${node}`).join(', ') || 'Unknown'}</p>
        <p>Scope: {selectedOwner.scope} · Source: {snapshot.source}</p>
        <button className={buttonClass} disabled={!onSelect} onClick={() => selectOwner(selectedOwner.id)}>Open owner</button>
      </aside>}
      {layer === 'storage' && <p className="tw-text-muted">Chunk metadata ownership by Store/Group. Replica nodes share this ownership; payload placement is separate.</p>}
    </>}
  </section>;
}
