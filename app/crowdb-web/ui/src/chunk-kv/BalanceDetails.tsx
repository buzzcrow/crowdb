// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { identity, weightPercent, type BalanceSummary, type Partition } from './catalog';

export function BalanceDetails({ partition, summary }: { partition: Partition; summary?: BalanceSummary }) {
  const value = partition.balance;
  const policy = summary?.policy;
  const candidate = summary?.candidate;
  return <section aria-label="Balance explanation" className="tw-space-y-2 tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm">
    <h3 className="tw-font-semibold">Balance</h3>
    <p>{value ? '' : 'Weight unavailable · '}{value?.partition.reason ?? summary?.reason ?? 'Balance observation unavailable'}</p>
    {value && <dl className="tw-grid tw-grid-cols-[auto_minmax(0,1fr)] tw-gap-x-4 tw-gap-y-1">
      <dt>Owner weight</dt><dd>{weightPercent(value.owner.weight)}</dd>
      <dt>Owner partitions</dt><dd>{value.owner.partition_count}</dd>
      <dt>Estimated partition bytes</dt><dd>{value.partition.estimated_bytes}</dd>
      <dt>Estimated owner bytes</dt><dd>{value.owner.estimated_bytes}</dd>
      <dt>Partition byte contribution</dt><dd>{(Number(value.partition.weight.byte_units) / 10_000_000).toFixed(2)} percentage points</dd>
      <dt>Partition count contribution</dt><dd>{(Number(value.partition.weight.count_units) / 10_000_000).toFixed(2)} percentage points</dd>
    </dl>}
    {policy && <>
      <p>Policy version: {summary?.policy_version ?? 'Unavailable'}.</p>
      <p>Coefficients: data {policy.byte_weight_percent}% · count {100 - Number(policy.byte_weight_percent)}%.</p>
      {summary?.owners?.length && summary.owners.every(owner => owner.estimated_bytes === '0') ? <p>All byte estimates are known zero; this observation uses count-only weight.</p> : null}
      <p>Tolerance: {policy.imbalance_tolerance_percent}% relative to equal owner share · minimum global loss improvement: {policy.minimum_weighted_improvement_percent}%.</p>
      <p>Observed maximum deviation: {(Number(summary?.deviation_percent_millionths ?? 0) / 1_000_000).toFixed(2)}%.</p>
      {summary?.loss_millionths != null && <p>Global imbalance score: {(Number(summary.loss_millionths) / 1_000_000).toFixed(4)} (lower is better).</p>}
      <p>Cooldown: {Number(policy.cooldown_ms) / 1000}s · observation {summary?.observed_at_ms ? new Date(Number(summary.observed_at_ms)).toISOString() : 'Unavailable'} · freshness window {Number(summary?.valid_for_ms ?? 0) / 1000}s.</p>
    </>}
    {candidate && <p>Best candidate split {identity(candidate.partition_id)}: server {candidate.source_id} → {candidate.target_id}, global loss improvement {(Number(candidate.improvement_percent_millionths) / 1_000_000).toFixed(2)}%; predicted owner weights {weightPercent(candidate.source_after)} / {weightPercent(candidate.target_after)}. {summary?.reason}.</p>}
    <p className="tw-text-xs tw-text-muted">Weight uses the complete catalog and cached estimates. Range splitting does not guarantee equal sizes.</p>
  </section>;
}
