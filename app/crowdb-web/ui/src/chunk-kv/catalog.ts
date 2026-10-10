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
export interface Weight { byte_units: string; count_units: string }
export interface BalanceOwner { instance_id: string; rpc_endpoint?: string; partition_count: string; estimated_bytes: string; weight: Weight }
export interface BalanceSummary {
  reason: string; policy_version?: string; generation?: string; observed_at_ms?: string; valid_for_ms?: string;
  policy?: { byte_weight_percent: string; imbalance_tolerance_percent: string; minimum_weighted_improvement_percent: string; cooldown_ms: string };
  deviation_percent_millionths?: string; loss_millionths?: string | null; owners?: BalanceOwner[];
  candidate?: { partition_id: Identity; source_id: string; target_id: string; improvement_percent_millionths: string; source_after: Weight; target_after: Weight } | null;
}
export interface Partition {
  balance?: { partition: { estimated_bytes: string; weight: Weight; reason: string }; owner: BalanceOwner } | null;
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
  balance?: BalanceSummary;
  generation: string;
  catalog_pages: number;
  entries: Partition[];
  next: Cursor | null;
  source: string;
}
export const identity = (id: Identity) => BigInt(id.high).toString(16).padStart(16, '0') + BigInt(id.low).toString(16).padStart(16, '0');
export const range = (partition: Partition) => `[${partition.start ? `0x${partition.start}` : '−∞'}, ${partition.end === null ? '+∞' : `0x${partition.end}`})`;

export const weightPercent = (weight?: Weight) => weight ? `${((Number(weight.byte_units) + Number(weight.count_units)) / 10_000_000).toFixed(2)}% (estimated)` : 'Unavailable';
