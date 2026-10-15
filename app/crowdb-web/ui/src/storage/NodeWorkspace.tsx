// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { MouseEvent } from 'react';
import { Button } from '../components/ui/Button';
import { Domain, type CapacityUsageResponse, type HardwareCapacitySummary, type Node } from '../types';
import { useSelection, type SelectedEntity } from '../contexts/SelectionContext';
import type { NodeDiskGroups } from '../data/useDiskInventory';
import type { ServerSummary } from '../api';
import type { MenuTarget } from '../topology/TopologyCanvas';
import { serviceOrder, serviceLabels, type NodeServicePlan } from '../services/useNodeServicePlans';
import { serviceInstanceLabel } from '../services/client';
import { NodeStorage } from './NodeStorage';
import type { CandidateSummary } from './types';

interface Props {
  node: Node; scope: SelectedEntity; capacity: boolean; readonly: boolean; ready: boolean;
  inventory?: NodeDiskGroups; hardware: HardwareCapacitySummary | null; usage: CapacityUsageResponse | null;
  services: ServerSummary[]; plan?: NodeServicePlan;
  loadGroup: (node: number, group: number) => Promise<void>;
  onAddGroup: () => void; onServicePlan: () => void;
  onMenu: (target: MenuTarget, event: MouseEvent) => void;
  onCandidates: (nodeId: number, summary: CandidateSummary) => void;
}

export function NodeWorkspace({ node, scope, capacity, readonly, ready, inventory, hardware, usage, services, plan, loadGroup, onAddGroup, onServicePlan, onMenu, onCandidates }: Props) {
  const { inspectEntity, selectEntity } = useSelection();
  const actual = new Map(services.filter(service => service.node_id === node.id).map(service => [service.service_type, service]));
  return <div className="tw-h-full tw-overflow-auto tw-p-4 tw-space-y-6" aria-label={`Node ${node.id} workspace`}>
    <header className="tw-flex tw-flex-wrap tw-items-start tw-justify-between tw-gap-3">
      <div><h1 className="tw-text-lg tw-font-semibold">Node {node.id}</h1><p className="tw-text-xs tw-text-muted">Rack {node.rack_id} · {node.host}</p></div>
      <Button size="sm" variant="secondary" disabled={readonly} onClick={event => onMenu({ type: 'Node', id: String(node.id), rawId: node.id, parentIds: { rack_id: node.rack_id }, label: `Node ${node.id}` }, event)}>Node actions</Button>
    </header>
    {!capacity && scope.section !== 'storage' && scope.section !== 'candidates' && <section aria-label={`Node ${node.id} services`} className="tw-space-y-3">
      <header className="tw-flex tw-justify-between tw-items-center tw-gap-3"><h2 className="tw-font-semibold">Services</h2>
        <Button size="sm" variant="secondary" disabled={readonly || !ready} onClick={onServicePlan}>Service plan</Button>
      </header>
      <div className="tw-grid tw-grid-cols-1 xl:tw-grid-cols-2 tw-gap-3">{serviceOrder.filter(kind => actual.has(kind) || (plan && plan[kind].state !== 'disabled')).map(kind => {
        const service = actual.get(kind); const step = plan?.[kind];
        const entity = { domain: Domain.Cluster, type: 'Server' as const, id: service?.id ?? `${kind}-${node.id}`, serviceType: kind, parentIds: { rack_id: node.rack_id, node_id: node.id } };
        return <article key={kind} aria-label={serviceLabels[kind]} className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-2">
          <div className="tw-flex tw-justify-between tw-gap-2">
            <button className="tw-text-accent tw-text-sm" onClick={() => selectEntity(entity)}>{serviceInstanceLabel(kind, entity.id)}</button>
            <button className="tw-text-muted tw-text-xs" onClick={() => inspectEntity(entity)}>Properties</button>
          </div>
          <p className="tw-text-xs">{serviceLabels[kind]} · {service?.health ?? step?.state ?? 'Unknown'}</p>
          {step?.detail && <p className="tw-text-xs tw-text-muted">{step.detail}</p>}
          {service?.id && <Button size="sm" variant="ghost" disabled={readonly} onClick={event => onMenu({ ...entity, rawId: entity.id, label: serviceInstanceLabel(kind, entity.id) }, event)}>Service actions</Button>}
        </article>;
      })}</div>
      {actual.size === 0 && !plan && <p className="tw-text-sm tw-text-muted">No observed services.</p>}
    </section>}
    <NodeStorage key={node.id} nodeId={node.id} rackId={node.rack_id} active readonly={readonly} ready={ready}
      inventory={inventory} hardware={hardware} usage={usage} candidatesOnly={scope.section === 'candidates'} loadGroup={loadGroup} onAddGroup={onAddGroup} onCandidates={onCandidates} />
  </div>;
}
