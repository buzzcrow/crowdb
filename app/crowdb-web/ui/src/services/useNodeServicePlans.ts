// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useCallback, useEffect, useRef, useState } from 'react';
import { deployServer, deployDiskdb, listServers } from '../api';
import type { EnrichedStoreView } from '../types';
import type { NodeDiskGroups } from '../data/useClusterTree';
import { serviceNames, serviceRequest } from './client';
import type { DeploymentDefaults } from './useDeploymentDefaults';

export const serviceOrder = ['kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'] as const;
export type ServiceKind = typeof serviceOrder[number];
export const serviceLabels = { kv: 'CrowDB Storage', diskdb: 'DiskDB', ...serviceNames };
export type ServiceStep = { state: 'pending' | 'waiting' | 'deploying' | 'deployed' | 'failed'; detail?: string };
export type NodeServicePlan = Record<ServiceKind, ServiceStep>;
const newPlan = (): NodeServicePlan => Object.fromEntries(serviceOrder.map(kind => [kind, { state: 'pending' }])) as NodeServicePlan;

/** Serializes default deployments across nodes so each step gets fresh port reservations.
 * Plans outlive dialogs; dependency changes resume waiting steps, failures need Retry.
 * Progress is durable; interrupted mutations require registration reconciliation.
 */
export function useNodeServicePlans(stores: EnrichedStoreView[], groups: Record<number, NodeDiskGroups>, refresh: () => Promise<void>, enabled: boolean) {
  const [plans, setPlans] = useState<Record<number, NodeServicePlan>>({});
  const plansRef = useRef(plans);
  const revisions = useRef<Record<number, number>>({});
  const recovery = useRef<Promise<void>>(Promise.resolve());
  const input = useRef({ stores, groups, refresh, enabled });
  input.current = { stores, groups, refresh, enabled };
  const wake = useRef<() => void>(() => {});
  const busy = useRef(false);
  const stopped = useRef(false);
  const running = useRef<Promise<void>>(Promise.resolve());
  const update = useCallback((id: number, plan: NodeServicePlan) => {
    plansRef.current = { ...plansRef.current, [id]: plan };
    setPlans(plansRef.current);
  }, []);
  const persist = useCallback(async (id: number, plan: NodeServicePlan) => {
    const result = await serviceRequest(`/nodes/${id}/service-plan`, 'PUT', { revision: revisions.current[id] ?? 0, steps: plan }) as { revision: number };
    revisions.current[id] = result.revision;
    update(id, plan);
  }, [update]);
  const failProgress = useCallback((id: number, detail: string) => {
    const previous = plansRef.current[id] ?? newPlan();
    update(id, Object.fromEntries(serviceOrder.map(kind => [kind, previous[kind].state === 'deployed'
      ? previous[kind] : { state: 'failed', detail }])) as NodeServicePlan);
  }, [update]);
  useEffect(() => {
    if (!enabled) return;
    let disposed = false;
    recovery.current = (async () => {
      const stored = await serviceRequest('/service-plans', 'GET') as Record<number, { revision: number; steps: NodeServicePlan }>;
      if (disposed) return;
      for (const [key, saved] of Object.entries(stored)) {
        const id = Number(key);
        revisions.current[id] = saved.revision;
        const steps = Object.fromEntries(serviceOrder.map(kind => [kind, saved.steps[kind].state === 'deploying'
          ? { state: 'failed', detail: 'Deployment was interrupted; verify the registered service, then Retry to reconcile.' } : saved.steps[kind]])) as NodeServicePlan;
        update(id, steps);
      }
    })();
    // Keep the rejection for Start; idle recovery itself must not emit an unhandled rejection.
    void recovery.current.catch(() => {});
    return () => { disposed = true; };
  }, [enabled, update]);
  const start = useCallback(async (id: number) => {
    stopped.current = false;
    try {
      await recovery.current;
      const previous = plansRef.current[id] ?? newPlan();
      await persist(id, Object.fromEntries(serviceOrder.map(kind => [kind, previous[kind].state === 'failed' ? { state: 'pending' } : previous[kind]])) as NodeServicePlan);
      wake.current();
    } catch (error) { failProgress(id, String(error)); }
  }, [persist, failProgress]);

  const stop = useCallback(async () => {
    stopped.current = true;
    await running.current;
    plansRef.current = {};
    setPlans({});
  }, []);

  useEffect(() => {
    let disposed = false;
    let timer: ReturnType<typeof setTimeout>;
    async function tick() {
      if (!busy.current && input.current.enabled && !stopped.current) {
        busy.current = true;
        try {
          for (const [key, initial] of Object.entries(plansRef.current)) {
            if (disposed || stopped.current) break;
            if (!serviceOrder.some(kind => ['pending', 'waiting'].includes(initial[kind].state))) continue;
            const id = Number(key);
            let plan = plansRef.current[id];
            const write = async (kind: ServiceKind, step: ServiceStep) => { plan = { ...plan, [kind]: step }; await persist(id, plan); };
            let existing;
            try { existing = await listServers(); }
            catch (error) {
              for (const kind of serviceOrder) if (plan[kind].state !== 'deployed') await write(kind, { state: 'failed', detail: String(error) });
              continue;
            }
            for (const kind of serviceOrder) {
              if (disposed || stopped.current || !input.current.enabled) break;
              if (plan[kind].state === 'failed') continue;
              const registered = existing.find(server => server.node_id === id && server.service_type === kind);
              if (registered) {
                const step: ServiceStep = registered.pid ? { state: 'deployed' } : { state: 'failed', detail: 'Registered service is stopped; restart it from the Node menu before resuming.' };
                if (plan[kind].state !== step.state) await write(kind, step);
                continue;
              }
              if (plan[kind].state === 'deployed') { await write(kind, { state: 'failed', detail: 'Previously deployed service is missing; Retry after checking its lifecycle.' }); continue; }
              const { stores: currentStores, groups: currentGroups } = input.current;
              const metadata = currentStores.flatMap(store => store.groups.filter(group => String(group.group_id) !== '0').map(group => ({ store: store.store_id, group: group.group_id })))[0];
              const disks = currentGroups[id];
              const diskGroup = disks?.diskGroups.find(group => disks.disksByDg[group.id]?.length && disks.disksByDg[group.id].every(disk => disk.device_path?.trim()));
              let waiting = '';
              if (kind !== 'kv' && kind !== 'diskdb' && !currentStores.some(store => String(store.store_id) === '0')) waiting = 'Waiting: initialize Group 0 in KV';
              else if (kind === 'chunkdb' && !metadata) waiting = 'Waiting: create an ordinary data group in KV for chunk storage slots';
              else if (kind === 'diskio' && !diskGroup) waiting = 'Waiting: add disks with device paths in Capacity';
              else if (kind === 'chunk-kv' && !metadata) waiting = 'Waiting: create a non-system metadata group in KV';
              else if (kind === 'chunk-kv' && new Set(existing.filter(server => server.service_type === 'diskio' && server.pid).map(server => server.node_id)).size < 2) waiting = 'Waiting: deploy DiskIO on at least two nodes for journal mirrors';
              else if (kind === 'access-server' && plan['chunk-kv'].state !== 'deployed' && !existing.some(server => server.service_type === 'chunk-kv' && server.pid)) waiting = 'Waiting: deploy Chunk-KV and initialize its catalog';
              if (waiting) { if (plan[kind].state !== 'waiting' || plan[kind].detail !== waiting) await write(kind, { state: 'waiting', detail: waiting }); continue; }
              await write(kind, { state: 'deploying' });
              try {
                const defaults = await serviceRequest('/deployment-defaults', 'GET') as Record<ServiceKind, DeploymentDefaults>;
                if (disposed || stopped.current) break;
                const value = defaults[kind];
                if (kind === 'kv') await deployServer(id, { rest_port: value.http_port!, rpc_port: value.rpc_port! });
                else if (kind === 'diskdb') await deployDiskdb(id, { rpc_port: value.rpc_port! });
                else await serviceRequest(`/nodes/${id}/services/deploy`, 'POST', {
                  kind, ...value, test_single_node: false,
                  ...(kind === 'diskio' ? { disk_group_id: diskGroup!.id } : {}),
                  ...(kind === 'chunk-kv' ? { metadata_store_id: Number(metadata!.store), bootstrap_group_id: Number(metadata!.group) } : {}),
                });
                await write(kind, { state: 'deployed' });
              } catch (error) { await write(kind, { state: 'failed', detail: String(error).slice(0, 4096) }); }
            }
            try { await input.current.refresh(); } catch { /* The shared data hooks report refresh errors. */ }
          }
        } catch (error) {
          for (const id of Object.keys(plansRef.current)) failProgress(Number(id), `Progress could not be saved: ${String(error)}. Reload to reconcile before retrying.`);
        } finally { busy.current = false; }
      }
      if (!disposed) timer = setTimeout(() => { running.current = tick(); }, 2000);
    }
    wake.current = () => { if (!busy.current) { clearTimeout(timer); running.current = tick(); } };
    running.current = tick();
    return () => { disposed = true; clearTimeout(timer); };
  }, [persist, failProgress]);
  return { plans, start, stop };
}
