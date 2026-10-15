// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export type DiskBackend = 'file' | 'block-device';
export interface CandidateSummary { count: number; observed_at_ms: number; stale: boolean }
export interface DiskObservation {
  path: string;
  backend: DiskBackend | null;
  capacity_bytes: number | null;
  allocated_bytes: number | null;
  backing_identity: string | null;
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
