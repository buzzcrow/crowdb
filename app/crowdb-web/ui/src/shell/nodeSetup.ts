// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ServiceKind } from '../services/useNodeServicePlans';
import type { DiskInventory, DiskBackend } from '../storage/types';

export interface SetupListeners {
  http_port?: number | null; rpc_port?: number | null; s3_port?: number | null; health_port?: number | null;
  keepalive_interval_secs?: number | null; dynamic_ownership?: boolean | null;
}
export interface SetupService { kind: ServiceKind; enabled: boolean; listeners: SetupListeners }
export interface SetupMedium { path: string; backend: DiskBackend; backing_identity: string; resource_identity?: string | null; capacity_bytes: number; media_type: number | null; initialize: boolean }
export type SetupStorage = { mode: 'automatic'; capacity_bytes: number; reserve_bytes: number; sparse: boolean }
  | { mode: 'diskless' } | { mode: 'supplied'; disks: SetupMedium[] };
export interface NodeSetupChoices { storage: SetupStorage; services: SetupService[] }
export interface PreparedNodeAccess { operation_id: string; inventory: DiskInventory; accepted_choices?: NodeSetupChoices | null }
export interface NodeAdmissionRequest { discovery_id: string; rack_id: number; ssh_user: string; ssh_port: number; ssh_password: string | null;
  preflight_operation_id?: string; setup?: NodeSetupChoices }
export type SetupStepPhase = 'pending' | 'waiting' | 'preparing' | 'accepted' | 'ready' | 'failed' | 'warning' | 'disabled';
export interface NodeSetupStatus {
  version: number; discovery_id: string; operation_id: string; node_id: number | null; cluster_id: string | null;
  phase: 'accepted' | 'waiting-for-binding' | 'running' | 'ready' | 'failed'; storage_mode?: 'automatic' | 'diskless' | 'supplied' | null;
  services: Array<{ kind: ServiceKind; phase: SetupStepPhase; detail: string | null; operation_id: string | null }>;
  disks: Array<{ path: string; phase: SetupStepPhase; detail: string | null; operation_id: string; disk_id: string | null }>;
  error: string | null;
}
export const setupKinds: ServiceKind[] = ['paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server'];
export const automaticStorage = (): SetupStorage => ({ mode: 'automatic', capacity_bytes: 16 * 1024 ** 3, reserve_bytes: 1024 ** 3, sparse: false });
export const defaultNodeSetup = (): NodeSetupChoices => ({ storage: automaticStorage(), services: setupKinds.map(kind => ({ kind, enabled: true, listeners: {} })) });

export function discoveredStorage(inventory: DiskInventory): SetupStorage {
  if (!inventory.disks.length) return automaticStorage();
  return { mode: 'supplied', disks: inventory.disks.filter(disk => disk.supplied && !disk.error && disk.backend && disk.backing_identity && disk.resource_identity &&
    disk.capacity_bytes !== null && disk.capacity_bytes >= 64 * 1024 ** 2).map(disk => ({ path: disk.path, backend: disk.backend!,
      backing_identity: disk.backing_identity!, resource_identity: disk.resource_identity!, capacity_bytes: disk.capacity_bytes!, media_type: null, initialize: disk.backend === 'file' })) };
}

export function validNodeSetup(choices: NodeSetupChoices): boolean {
  const ports = new Set<number>();
  for (const service of choices.services.filter(service => service.enabled)) {
    for (const [field, value] of Object.entries(service.listeners)) {
      if (field === 'dynamic_ownership' || value === undefined || value === null) continue;
      if (!Number.isInteger(value) || Number(value) < 1 || Number(value) > (field === 'keepalive_interval_secs' ? 0xffffffff : 65535)) return false;
      if (field !== 'keepalive_interval_secs') {
        if (ports.has(Number(value))) return false;
        ports.add(Number(value));
      }
    }
    if (service.kind === 'diskdb' && service.listeners.rpc_port != null) {
      const base = service.listeners.rpc_port;
      if (base > 65533 || ports.has(base + 1) || ports.has(base + 2)) return false;
      ports.add(base + 1); ports.add(base + 2);
    }
  }
  const storage = choices.storage;
  if (storage.mode === 'diskless') return true;
  if (storage.mode === 'supplied') return storage.disks.length > 0 && storage.disks.every(disk => disk.initialize);
  const zone = 32 * 1024 ** 3;
  return Number.isSafeInteger(storage.capacity_bytes) && storage.capacity_bytes > 0 && storage.capacity_bytes % (64 * 1024 ** 2) === 0
    && (storage.capacity_bytes <= zone || storage.capacity_bytes % zone === 0);
}

export interface NodeSetupSummary {
  phase: NodeSetupStatus['phase']; warning: boolean; error: string | null; observed_at_ms: number; stale: boolean;
}
