// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { serviceLifecycle } from './serviceLifecycle';
import { useCallback } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square, HardDrive } from 'lucide-react';
import { removeRack, removeNode, removeStore, removeGroup, removeReplica, stopServer, restartServer, pingNode, setDiskStatus, setDiskGroupStatus, restartDiskdb, stopDiskdb, removeServer, removeDiskdb, removeDiskGroup, removeDisk } from '../api';
import { Domain } from '../types';
import type { MenuTarget } from '../topology/TopologyCanvas';
import type { MenuItemOrSeparator } from '../components/ContextMenu';
import { buildStatusSubmenu } from './status';
import type { MenuContext } from './context';
import { isAuxiliaryKind, serviceNames, type AuxiliaryKind } from '../services/client';
export function useClusterMenus({ readonly, managed, managementAuthorized, domain, physicalActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds, allServers, setDialog }: MenuContext) {
  /** Build per-layer context menu items for a normalized target. */
  return useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly || (managed && domain !== Domain.KV) || (managed && !managementAuthorized)) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};
      if (physicalActive && t.type === 'Server' && isAuxiliaryKind(t.serviceType)) {
        const id = String(t.rawId ?? t.id);
        return serviceLifecycle(allServers?.find(server => server.id === id) ?? { id, node_id: Number(p.node_id), service_type: t.serviceType, health: 'unknown' }, runMutation, requestDelete);
      }

      if (physicalActive) {
        if (t.type === 'Datacenter') {
          // The default DC is immutable — only Add Rack is offered.
          items.push({
            id: 'add-rack',
            label: 'Add Rack',
            icon: <Plus className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addRack: true })),
          });
        } else if (t.type === 'Rack' && modules?.nodes !== false) {
          const rackId = Number(t.rawId);
          items.push({
            id: 'add-node',
            label: 'Add Node',
            icon: <Server className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addNode: { rackId } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-rack',
            label: 'Delete Rack',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Rack', rackId, async () => { await runMutation('Delete Rack', `Rack ${rackId}`, () => removeRack(rackId)); }),
          });
        } else if (t.type === 'Node') {
          const nodeId = Number(t.rawId);
          const hasServer = serverNodeIds.has(nodeId);
          const hasDiskdb = diskdbNodeIds.has(nodeId);
          // Add Services — deploy CrowDB Storage and/or DiskDB.
          if (!hasServer) {
            items.push({
              id: 'deploy',
              label: 'Deploy CrowDB Storage',
              icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployServer: { nodeId } })),
            });
          }
          if (!hasDiskdb) {
            items.push({
              id: 'deploy-diskdb',
              label: 'Deploy DiskDB',
              icon: <HardDrive className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
            });
          }
          for (const kind of Object.keys(serviceNames) as AuxiliaryKind[]) {
            items.push({ id: `deploy-${kind}`, label: `Deploy ${serviceNames[kind]}`, icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog(dialog => ({ ...dialog, deployAuxiliary: { nodeId, kind } })),
            });
          }
          items.push({ id: 'default-services', label: 'Deploy default services', icon: <Server size={16} />, onSelect: () => setDialog(dialog => ({ ...dialog, defaultServices: { nodeId } })) });
          for (const server of allServers?.filter(server => server.node_id === nodeId) ?? []) {
            const label = isAuxiliaryKind(server.service_type) ? serviceNames[server.service_type] : server.service_type === 'diskdb' ? 'DiskDB' : 'CrowDB Storage';
            items.push({ id: `manage-${server.id}`, label: `${label} · ${server.id}`, hint: server.pid ? 'Running' : 'Stopped', submenu: serviceLifecycle(server, runMutation, requestDelete) });
          }
          items.push({
            id: 'ping',
            label: 'Ping',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () =>
              runMutation('Ping Node', t.label || t.id, async () => {
                const r = await pingNode(nodeId);
                if (!r.ok) throw new Error(r.error || 'unreachable');
              }),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            // Cascade: the backend's DELETE /api/nodes/:id handler
            // (http_remove_node) calls stop_and_remove_server_for_node
            // which stops the KV process, removes the server entry, and
            // purges topology — all before removing the node. Calling
            // removeServer separately here would hit check_require_empty
            // (which refuses if the node hosts group-0 replicas), blocking
            // the cascade. So we only remove diskdb explicitly (no
            // check_require_empty gate) and let removeNode handle the KV
            // cascade.
            onSelect: () => requestDelete('Node', nodeId, async () => {
              await runMutation('Delete Node', t.label || t.id, async () => {
                if (allServers?.some(server => server.node_id === nodeId && isAuxiliaryKind(server.service_type))) {
                  throw new Error('Remove auxiliary service deployments before removing this node');
                }
                if (hasDiskdb) await removeDiskdb(nodeId);
                await removeNode(nodeId);
              });
            }),
          });
        } else if (t.type === 'DiskGroup') {
          const dgId = Number(t.rawId);
          const nodeId = Number(p.node_id);
          const rackId = Number(p.rack_id);
          items.push({ id: 'add-disk', label: 'Add Disk', icon: <HardDrive className="tw-h-4 tw-w-4" />, onSelect: () => setDialog((d) => ({ ...d, addDisk: { nodeId, dgId } })) });
          items.push({ id: 'dg-status', label: 'Change Status', icon: <Activity className="tw-h-4 tw-w-4" />, submenu: buildStatusSubmenu((status) => runMutation(`Set DG ${status}`, t.label || t.id, () => setDiskGroupStatus(rackId, nodeId, dgId, status))) });
          items.push({ id: 'assign-dg', label: 'Assign to DiskDB', icon: <Server className="tw-h-4 tw-w-4" />, onSelect: () => setDialog((d) => ({ ...d, assignDiskGroup: { rackId, nodeId, dgId, dgName: t.label } })) });
          items.push({ id: 'del-dg', label: 'Delete Disk Group', icon: <Trash2 className="tw-h-4 tw-w-4" />, destructive: true, onSelect: () => requestDelete('Disk Group', dgId, async () => { await runMutation('Delete Disk Group', t.label || t.id, () => removeDiskGroup(nodeId, dgId)); }, 'All disks in this disk group will also be removed.') });
        } else if (t.type === 'Disk') {
          const diskId = String(p.disk_id || t.rawId || t.id);
          const nodeId = Number(p.node_id);
          const dgId = Number(p.disk_group_id);
          items.push({ id: 'disk-status', label: 'Change Status', icon: <Activity className="tw-h-4 tw-w-4" />, submenu: buildStatusSubmenu((status) => runMutation(`Set Disk ${status}`, t.label || t.id, () => setDiskStatus(diskId, status))) });
          items.push({ id: 'del-disk', label: 'Delete Disk', icon: <Trash2 className="tw-h-4 tw-w-4" />, destructive: true, onSelect: () => requestDelete('Disk', diskId, async () => { await runMutation('Delete Disk', diskId, () => removeDisk(nodeId, dgId, diskId)); }) });
        } else if (t.type === 'Server') {
          // Server context menu: dispatch on serviceType (KV vs DiskDB).
          const nodeId = Number(p.node_id);
          if (t.serviceType === 'diskdb') {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-ddb',
              label: 'Delete DiskDB',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
                await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
              }),
            });
          } else if (t.serviceType === 'kv') {
            // CrowdbKV service context menu: restart, stop, delete.
            items.push({
              id: 'restart',
              label: 'Restart CrowDB Storage',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart CrowDB Storage', t.label || t.id, () => restartServer(nodeId)),
            });
            items.push({
              id: 'stop',
              label: 'Stop CrowDB Storage',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop CrowDB Storage', t.label || t.id, () => stopServer(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-service',
              label: 'Delete CrowDB Storage',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('CrowDB Storage', t.label || t.id, async () => {
                await runMutation('Delete CrowDB Storage', t.label || t.id, () => removeServer(nodeId));
              }),
            });
          }
        }
      } else {
        if (t.type === 'Server') {
          // KV-domain server context menu: restart, stop, delete.
          const nodeId = Number(p.node_id);
          if (t.serviceType === 'diskdb') {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-ddb',
              label: 'Delete DiskDB',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
                await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
              }),
            });
          } else if (t.serviceType === 'kv') {
            items.push({
              id: 'restart',
              label: 'Restart CrowDB Storage',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart CrowDB Storage', t.label || t.id, () => restartServer(nodeId)),
            });
            items.push({
              id: 'stop',
              label: 'Stop CrowDB Storage',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop CrowDB Storage', t.label || t.id, () => stopServer(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-service',
              label: 'Delete CrowDB Storage',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('CrowDB Storage', t.label || t.id, async () => {
                await runMutation('Delete CrowDB Storage', t.label || t.id, () => removeServer(nodeId));
              }),
            });
          }
        } else if (t.type === 'Store' && modules?.groups !== false) {
          items.push({
            id: 'add-group',
            label: 'Add Group',
            icon: <Database className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addGroup: { storeId: t.id } })),
          });
          // System store (store 0) cannot be deleted individually.
          if (t.id !== '0') {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-store',
              label: 'Delete Store',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('Store', t.id, async () => { await runMutation('Delete Store', t.id, () => removeStore(t.id)); }),
            });
          }
        } else if (t.type === 'Group') {
          const storeId = p.store_id;
          const isSystemGroup = storeId === '0' && t.id === '0';
          if (modules?.replicas !== false) {
            items.push({
              id: 'add-replica',
              label: 'Add Replica',
              icon: <Plus className="tw-h-4 tw-w-4" />,
              onSelect: () => {
                if (storeId) setDialog((d) => ({ ...d, addReplica: { storeId: String(storeId), groupId: t.id } }));
              },
            });
          }
          // System group (store 0, group 0) cannot be deleted individually.
          if (!isSystemGroup) {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-group',
              label: 'Delete Group',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => {
                if (storeId)
                  requestDelete('Group', t.id, async () => {
                    await runMutation('Delete Group', `${storeId}/${t.id}`, () => removeGroup(String(storeId), t.id));
                  });
              },
            });
          }
        } else if (t.type === 'Replica') {
          const storeId = p.store_id;
          const groupId = p.group_id;
          items.push({
            id: 'del-replica',
            label: 'Delete Replica',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => {
              if (storeId && groupId)
                requestDelete('Replica', t.id, async () => {
                  await runMutation('Delete Replica', `${storeId}/${groupId}/${t.id}`, () => removeReplica(String(storeId), String(groupId), t.id));
                });
            },
          });
        }
      }
      return items;
    },
    [readonly, managed, managementAuthorized, domain, physicalActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds, allServers, buildStatusSubmenu],
  );

}
