// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export type DiskBackend = 'file' | 'block-device';
export interface CandidateSummary { count: number; observed_at_ms: number; stale: boolean; partial?: boolean }
export interface DiskObservation {
  path: string;
  backend: DiskBackend | null;
  capacity_bytes: number | null;
  allocated_bytes: number | null;
  backing_identity: string | null;
  resource_identity?: string | null;
  supplied: boolean;
  error: string | null;
  backing_filesystem: string | null;
}
export interface DiskInventory {
  observed_at_ms: number;
  disks: DiskObservation[];
  filesystems: Array<{ identity: string; capacity_bytes: number | null; available_bytes: number | null; error: string | null }>;
}
export interface CreateFileDiskRequest {
  operation_id: string;
  disk_group_id: number;
  capacity_bytes: number;
  sparse: boolean;
  reserve_bytes?: number;
}
export type FilePreparationPhase = 'accepted' | 'creating' | 'prepared' | 'publishing' | 'waiting-for-diskio' | 'registered' | 'unknown' | 'failed';
export interface FilePreparation {
  version: number;
  request: {
    operation_id: string; cluster_id: string; node_id: number;
    capacity_bytes: number; sparse: boolean; reserve_bytes: number;
    admission?: { rack_id: number; disk_group_id: number; unit_size_bytes: number; zone_size_bytes: number; media_type: number | null };
  };
  path: string;
  phase: FilePreparationPhase;
  backing_identity: string | null;
  preparation_complete: boolean;
  error: string | null;
}
export interface FilePreparationPage { items: FilePreparation[]; next: string | null }
export interface DiskBackingInfo {
  physical_backing?: { host_id: string; resource_identity: string; capacity_bytes: string; usable_bytes: string; tail_bytes: string };
  version: number; operation_id: string; backend: DiskBackend; backing_identity: string;
  media_type: number | null; sparse: boolean; removed: boolean; container_path: string;
  capacity_units: string; unit_size_bytes: number; zone_size_units: string; zone_count: number;
  owner: string | null; binding: string | null; observed_at_ms: number;
}
export const pendingFilePreparation = (state: FilePreparation) => ['accepted', 'creating', 'publishing', 'waiting-for-diskio'].includes(state.phase);
export const filePhaseLabel: Record<FilePreparationPhase, string> = {
  accepted: 'Accepted', creating: 'Creating file', prepared: 'Prepared · provisional', publishing: 'Registering in Group 0',
  'waiting-for-diskio': 'Waiting for DiskIO', registered: 'Registered · DiskIO verified', unknown: 'Outcome unknown', failed: 'Failed',
};

export function storageOperationId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export interface DiskLifecycle {
  version: number; disk_id: string; removed: boolean; offline: boolean;
  disk_revision: string | null; remove_conditions: string[]; operation_id: string | null;
  removed_from_revision: string | null;
  rejoin_operation_id?: string | null; rejoin_source_removal_operation_id?: string | null;
  online?: DiskOnlineOperation | null;
}
export interface DiskOnlineOperation { operation_id: string; offline_revision: string; state: 'pending' | 'failed' | 'complete'; error: string | null }
export interface DiskRemovalOutcome { operation_id: string; disk_id: string; removed: boolean; offline_revision: string }
export interface RemovedDisk {
  disk_id: string; disk_group_id: number; operation_id: string; container_path: string;
  backend: DiskBackend; backing_identity: string; media_type: number | null; sparse: boolean;
  capacity_units: string; unit_size_bytes: number;
}
export interface RemovedDiskPage { items: RemovedDisk[]; next: string | null }

export interface DiskRejoinOutcome { operation_id: string; source_removal_operation_id: string; state: DiskLifecycle }

export interface JoinCandidateRequest { operation_id: string; disk_group_id: number; medium: import('../shell/nodeSetup').SetupMedium }
export interface SuppliedOperation extends JoinCandidateRequest {
  node_id: number; disk_id: string; phase: 'accepted' | 'verifying' | 'publishing' | 'waiting-for-diskio' | 'registered' | 'unknown' | 'failed';
  verified_bytes: number; error: string | null;
}
export interface SuppliedOperationPage { items: SuppliedOperation[]; next: string | null }
export const pendingSupplied = (operation: SuppliedOperation) => !['registered', 'failed'].includes(operation.phase);
export function candidateIneligible(disk: DiskObservation): string | undefined {
  if (disk.error) return disk.error;
  if (!disk.supplied) return 'Not explicitly supplied by the launcher';
  if (!disk.backend || !disk.backing_identity || !disk.resource_identity) return 'Physical resource identity is not verified';
  if (!Number.isSafeInteger(disk.capacity_bytes) || disk.capacity_bytes! < 64 * 1024 ** 2) return 'Requires at least 64 MiB of verified capacity';
}
