// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Dispatch, SetStateAction } from 'react';
import type { Domain, CapacityUsageResponse } from '../types';
import type { CrowdbConsoleProps } from '../App';
export interface ConsoleDialogState {
    defaultServices?: { nodeId: number };
    addRack?: boolean;
    addNode?: { rackId: number };
    addStore?: boolean;
    addGroup?: { storeId: string };
    addReplica?: { storeId: string; groupId: string };
    addDiskGroup?: { nodeId: number };
    addDisk?: { nodeId: number; dgId: number };
    assignDiskGroup?: { rackId: number; nodeId: number; dgId: number; dgName?: string };
    deployServer?: { nodeId: number };
    deployDiskdb?: { nodeId: number } | null;
    deployAuxiliary?: { nodeId: number; kind: import('../services/client').AuxiliaryKind };
    delete?: { type: string; id: string | number; onDelete: () => Promise<void>; cascadeWarning?: string };
    initCluster?: boolean;
    compactZones?: { diskId: string; zoneCount?: number };
    rebuildBitmap?: { diskId: string; zoneCount?: number };
}
export interface MenuContext {
  readonly: boolean;
  managed: boolean;
  managementAuthorized: boolean;
  domain: Domain;
  physicalActive: boolean;
  modules: CrowdbConsoleProps['modules'];
  setDialog: Dispatch<SetStateAction<ConsoleDialogState>>;
  requestDelete: (type: string, id: string | number, onDelete: () => Promise<void>, warning?: string) => void;
  runMutation: (action: string, target: string, operation: () => Promise<unknown>) => Promise<void>;
  serverNodeIds: Set<number>;
  diskdbNodeIds: Set<number>;
  allServers?: import('../api').ServerSummary[];
  capacityUsage: CapacityUsageResponse | null;
}
