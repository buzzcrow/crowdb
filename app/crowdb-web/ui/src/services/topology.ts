// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Node, Edge } from 'reactflow';
import type { ServerSummary } from '../api';
import { Domain, type Node as PhysicalNode } from '../types';
import type { FlowNodeData } from '../topology/buildFlow';
import { isAuxiliaryKind, serviceNames } from './client';

export function addServiceNodes(domain: Domain, flow: { nodes: Node[]; edges: Edge[] }, services: ServerSummary[], nodes: PhysicalNode[]) {
  if (domain !== Domain.Cluster) return flow;
  const physical = new Map(nodes.map(node => [node.id, node]));
  for (const service of services) {
    if (!service.id || !isAuxiliaryKind(service.service_type) || service.node_id == null) continue;
    const owner = physical.get(service.node_id);
    if (!owner) continue;
    const id = `SERVICE-${service.id}`;
    const data: FlowNodeData = {
      kind: 'Server', label: service.id, sublabel: `${serviceNames[service.service_type]} · ${service.pid ? 'Running' : 'Stopped'}`,
      health: service.health, layer: 3,
      entity: { type: 'Server', id: service.id, serviceType: service.service_type, parentIds: { rack_id: owner.rack_id, node_id: owner.id } },
    };
    flow.nodes.push({ id, type: 'crowdbKv', position: { x: 0, y: 0 }, data });
    flow.edges.push({ id: `e-N-${owner.id}-${id}`, source: `N-${owner.id}`, target: id, type: 'smoothstep' });
  }
  return flow;
}
