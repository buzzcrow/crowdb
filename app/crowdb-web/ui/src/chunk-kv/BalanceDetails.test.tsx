// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import { BalanceDetails } from './BalanceDetails';
import { weightPercent, type Partition, type BalanceSummary } from './catalog';
afterEach(cleanup);
const owner = { instance_id: '1', partition_count: '5', estimated_bytes: '80', weight: { byte_units: '400000000', count_units: '125000000' } };
const partition: Partition = { id: 'p', start: 'ff'.repeat(64), end: null, owner_id: '1', endpoint: 'localhost', epoch: '3', state: 'Serving', transition_id: null,
  artifact: { tree_id: '1', stream_name: { high: '0', low: '1' } },
  balance: { owner, partition: { estimated_bytes: '60', weight: { byte_units: '300000000', count_units: '25000000' }, reason: 'within tolerance' } } };
const summary: BalanceSummary = { generation: '3', reason: 'within tolerance', observed_at_ms: '0', deviation_percent_millionths: '5000000',
  policy: { byte_weight_percent: '80', imbalance_tolerance_percent: '20', minimum_weighted_improvement_percent: '25', cooldown_ms: '60000' } };
it('shows the backend contributions and percentage thresholds without using the visible window', () => {
  render(<BalanceDetails partition={partition} summary={summary} />);
  expect(weightPercent(partition.balance?.partition.weight)).toBe('32.50% (estimated)');
  expect(screen.getByText('52.50% (estimated)')).toBeVisible();
  expect(screen.getByText('30.00 percentage points')).toBeVisible();
  expect(screen.getByText('2.50 percentage points')).toBeVisible();
  expect(screen.getByText('Coefficients: data 80% · count 20%.')).toBeVisible();
  expect(screen.getByText(/Tolerance: 20% relative to equal owner share/)).toBeVisible();
});
it('shows unavailable and stale reasons without fabricating zero weight', () => {
  const { rerender } = render(<BalanceDetails partition={{ ...partition, balance: null }} summary={{ reason: 'Balance observation unavailable' }} />);
  expect(weightPercent()).toBe('Unavailable');
  expect(screen.queryByText('0.00% (estimated)')).toBeNull();
  rerender(<BalanceDetails partition={{ ...partition, balance: null }} summary={{ reason: 'Stale balance catalog generation' }} />);
  expect(screen.getByText(/Stale balance catalog generation/)).toBeVisible();
  expect(screen.queryByText('52.50% (estimated)')).toBeNull();
});
