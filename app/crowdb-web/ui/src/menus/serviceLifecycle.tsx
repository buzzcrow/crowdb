// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { RotateCw, Square, Trash2, Play } from 'lucide-react';
import { restartServer, stopServer, removeServer, restartDiskdb, stopDiskdb, removeDiskdb, type ServerSummary } from '../api';
import { serviceDisplayNames, serviceRequest } from '../services/client';
import type { MenuContext } from './context';
import type { MenuItemOrSeparator } from '../components/ContextMenu';
export function serviceLifecycle(server: ServerSummary, run: MenuContext['runMutation'], remove: MenuContext['requestDelete']): MenuItemOrSeparator[] {
  const kind = server.service_type;
  const label = serviceDisplayNames[kind as keyof typeof serviceDisplayNames] ?? kind;
  const id = server.id ?? '';
  const node = server.node_id!;
  const restart = () => kind === 'paxos-kv' ? restartServer(node) : kind === 'diskdb' ? restartDiskdb(node) : serviceRequest(`/services/${encodeURIComponent(id)}/restart`, 'POST');
  const stop = () => kind === 'paxos-kv' ? stopServer(node) : kind === 'diskdb' ? stopDiskdb(node) : serviceRequest(`/services/${encodeURIComponent(id)}/stop`, 'POST');
  const deletion = () => kind === 'paxos-kv' ? removeServer(node) : kind === 'diskdb' ? removeDiskdb(node) : serviceRequest(`/services/${encodeURIComponent(id)}`, 'DELETE');
  return [
    { id: 'start-restart', label: `${server.pid ? 'Restart' : 'Start'} ${label}`, icon: server.pid ? <RotateCw size={16} /> : <Play size={16} />, onSelect: () => run(`Start / Restart ${label}`, id, restart) },
    { id: 'stop', label: `Stop ${label}`, icon: <Square size={16} />, disabled: !server.pid, onSelect: () => run(`Stop ${label}`, id, stop) },
    { id: 'delete', label: `Delete ${label}`, icon: <Trash2 size={16} />, destructive: true, onSelect: () => remove(label, id, () => run(`Delete ${label}`, id, deletion), 'Stops this instance and removes its deployment record. Stored data is preserved.') },
  ];
}
