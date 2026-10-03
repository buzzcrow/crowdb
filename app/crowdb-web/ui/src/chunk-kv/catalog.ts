// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export interface Identity { high: string; low: string }
export interface Overlay {
  source_partition_id: Identity;
  source_epoch: string;
  source_stream_name: Identity;
  source_stream_manifest_generation: string;
  replay_offset: string;
  cutover_offset: string;
  base_root_manifest_generation: string;
  base_tree_manifest: string;
  base_applied_seq: string;
  cutover_seq: string;
  target_stream_start_seq: string;
}
export interface Partition {
  id: string;
  start: string;
  end: string | null;
  owner_id: string;
  endpoint: string;
  epoch: string;
  state: string;
  transition_id: string | null;
  artifact: { tree_id: string; stream_name: Identity; tail_overlay?: Overlay };
}
export interface Cursor { page: number; offset: number; generation?: string }
export interface CatalogPage extends Cursor {
  generation: string;
  catalog_pages: number;
  entries: Partition[];
  next: Cursor | null;
  source: string;
}
export const identity = (id: Identity) => BigInt(id.high).toString(16).padStart(16, '0') + BigInt(id.low).toString(16).padStart(16, '0');
export const range = (partition: Partition) => `[${partition.start ? `0x${partition.start}` : '−∞'}, ${partition.end === null ? '+∞' : `0x${partition.end}`})`;
