// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { cleanup, render, screen, fireEvent, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { PartitionGraph } from './PartitionGraph';
import type { GraphQuery } from './query';
import type { Partition } from './catalog';
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it('replaces bounded split windows and collapses only the selected server', async () => {
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  const entries: Partition[] = Array.from({ length: 12 }, (_, index) => ({
    id: index.toString(16).padStart(32, '0'), start: '', end: null, owner_id: '1',
    endpoint: '127.0.0.1:15201', epoch: '2', state: 'Serving', transition_id: null,
    artifact: { tree_id: String(100 + index), stream_name: { high: '1', low: String(index) } },
  }));
  function TestGraph() {
    const [query, onQuery] = useState<GraphQuery>({ serverPage: 0, offsets: {}, collapsed: [] });
    return <PartitionGraph entries={entries} servers={[{ id: 'chunk-kv-1', node_id: 1, rpc_url: '127.0.0.1:15201', service_type: 'chunk-kv', health: 'unknown' }]} disabled={false} onSelect={vi.fn()} onTree={vi.fn()} query={query} onQuery={onQuery} />;
  }
  render(<TestGraph />);
  expect(screen.queryByText(/Weight/)).toBeNull();
  // jsdom has no layout: inspect real graph cards by their accessible labels.
  // The native browser case separately verifies measured cards are visible.
  const cards = (prefix: string) => screen.getByTestId('chunk-kv-graph').querySelectorAll(`button[aria-label^="${prefix}"]`);
  const card = (label: string) => {
    const result = [...screen.getByTestId('chunk-kv-graph').querySelectorAll('button')].find(button => button.getAttribute('aria-label') === label);
    expect(result, label).toBeDefined();
    return result!;
  };
  await waitFor(() => expect(cards('Partition ')).toHaveLength(5));
  expect(cards('KV Tree for ')).toHaveLength(5);
  expect(screen.getAllByTestId('chunk-kv-icon-server')).toHaveLength(1);
  expect(screen.getAllByTestId('chunk-kv-icon-split')).toHaveLength(5);
  expect(screen.getAllByTestId('chunk-kv-icon-tree')).toHaveLength(5);
  fireEvent.click(card('Next splits for CKV-1'));
  await waitFor(() => expect(card(`Partition ${entries[5].id}`)).toBeInTheDocument());
  expect([...cards('Partition ')].map(button => button.getAttribute('aria-label'))).not.toContain(`Partition ${entries[0].id}`);
  fireEvent.click(card('CKV-1'));
  await waitFor(() => expect(cards('Partition ')).toHaveLength(0));
  fireEvent.click(card('CKV-1'));
  await waitFor(() => expect(cards('Partition ')).toHaveLength(5));
});
