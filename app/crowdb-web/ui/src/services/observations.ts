// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ServerSummary } from '../api';
import type { DiskdbInstanceInfo, Node } from '../types';

type ServiceObservation = Pick<ServerSummary, 'service_type' | 'node_id' | 'health' | 'endpoint' | 'rpc_url'>;
type PhysicalNode = Pick<Node, 'id' | 'diskdb_server'>;
type Instance = Pick<DiskdbInstanceInfo, 'instance_id' | 'rpc_endpoint'>;

export function diskdbHealthForNodes(servers: readonly ServiceObservation[], nodes: readonly PhysicalNode[]) {
  const result = new Map<number, string>();
  for (const server of servers) {
    if (server.service_type === 'diskdb' && server.node_id != null) result.set(server.node_id, server.health);
  }
  for (const node of nodes) {
    if (node.diskdb_server && !result.has(node.id)) result.set(node.id, node.diskdb_server.health);
  }
  return result;
}

function endpointKey(endpoint: string | undefined): string | undefined {
  if (!endpoint) return undefined;
  try {
    const url = new URL(endpoint.includes('://') ? endpoint : `tcp://${endpoint}`);
    const port = url.port || (url.protocol === 'http:' ? '80' : url.protocol === 'https:' ? '443' : '');
    if (!url.hostname || !port || url.username || url.password || url.search || url.hash
      || (url.pathname !== '' && url.pathname !== '/')) return undefined;
    return `${url.hostname.toLowerCase()}:${port}`;
  } catch {
    return undefined;
  }
}

export function diskdbInstancesForNodes(servers: readonly ServiceObservation[], nodes: readonly PhysicalNode[], instances: readonly Instance[]) {
  const byEndpoint = new Map<string, string | null>();
  for (const instance of instances) {
    const key = endpointKey(instance.rpc_endpoint);
    if (key == null) continue;
    const previous = byEndpoint.get(key);
    byEndpoint.set(key, previous === undefined || previous === instance.instance_id ? instance.instance_id : null);
  }
  const result = new Map<number, string>();
  const assign = (node: number, endpoint: string | undefined) => {
    const key = endpointKey(endpoint);
    const instance = key == null ? undefined : byEndpoint.get(key);
    if (instance != null) result.set(node, instance);
  };
  for (const node of nodes) assign(node.id, node.diskdb_server?.endpoint);
  for (const server of servers) {
    if (server.service_type === 'diskdb' && server.node_id != null) {
      result.delete(server.node_id);
      assign(server.node_id, server.endpoint || server.rpc_url);
    }
  }
  return result;
}
