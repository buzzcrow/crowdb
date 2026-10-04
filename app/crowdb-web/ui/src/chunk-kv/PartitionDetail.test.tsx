// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { PartitionDetail } from './PartitionDetail';
import type { Partition } from './catalog';
afterEach(cleanup);

it('keeps inherited and current journal identities and recovery dependencies separate', () => {
  const stream = { high: '18446744073709551615', low: '2' };
  const partition: Partition = {
    id: 'ffffffffffffffff0000000000000002', start: '80', end: null,
    owner_id: '42', endpoint: '127.0.0.1:45200', epoch: '9007199254740993',
    state: 'Serving', transition_id: 'transition-1', artifact: {
      tree_id: '9007199254740995', stream_name: stream, tail_overlay: {
        source_partition_id: { ...stream, low: '1' }, source_epoch: '9007199254740991',
        source_stream_name: { ...stream, low: '3' }, source_stream_manifest_generation: '15',
        replay_offset: '4096', cutover_offset: '8192', cutover_seq: '50',
        base_root_manifest_generation: '7', base_tree_manifest: '6', base_applied_seq: '40', target_stream_start_seq: '51',
      },
    },
  };
  const props = { partition, generation: '9007199254740997', currentGeneration: '9007199254740997', active: false,
    catalogPage: 0, catalogOffset: 0, onBack: vi.fn(), onChunk: vi.fn(), propertyHost: null,
    query: { tab: 'Journal', stream: { offset: 0 }, extent: null }, onQuery: vi.fn() };
  const { rerender } = render(<PartitionDetail {...props} />);
  const inherited = screen.getByRole('heading', { name: 'Inherited parent stream' }).parentElement!;
  expect(inherited).toHaveTextContent('ffffffffffffffff0000000000000003');
  expect(inherited).toHaveTextContent('4096'); expect(inherited).toHaveTextContent('8192');
  const current = screen.getByRole('heading', { name: 'Partition journal stream' }).parentElement!;
  expect(current).toHaveTextContent(partition.id); expect(current).toHaveTextContent('51');
  expect(current).not.toHaveTextContent('ffffffffffffffff0000000000000003');
  rerender(<PartitionDetail {...props} query={{ ...props.query, tab: 'Dependencies' }} />);
  expect(screen.getByRole('tabpanel')).toHaveTextContent('Parent recovery dependency is retained');
  expect(screen.getByRole('tabpanel')).toHaveTextContent('9007199254740991');
  rerender(<PartitionDetail {...props} query={{ ...props.query, tab: 'Overview' }} />);
  expect(screen.getByRole('tabpanel')).toHaveTextContent('9007199254740993');
});
