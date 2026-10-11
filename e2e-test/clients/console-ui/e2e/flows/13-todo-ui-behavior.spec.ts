// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.93s (2026-10-12)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack } from '../fixtures/registrySetup';

test.describe('cluster · registry browser', () => {
  test('fixed-slot warning stays healthy and persists across reload', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 705, name: 'Slot warning' });
    const api = await apiContext(baseURL!);
    try {
      const node = await api.post('/api/nodes', { data: { id: 705, rack_id: 705, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } } });
      expect(node.ok(), await node.text()).toBeTruthy();
      const detail = 'CDB instance is outside the fixed slot plan; explicit slot migration is required';
      const steps = Object.fromEntries(['access-server', 'chunk-kv', 'chunkdb', 'diskdb', 'diskio', 'paxos-kv'].map(kind => [kind, kind === 'chunkdb' ? { state: 'warning', detail } : { state: 'disabled' }]));
      const saved = await api.put('/api/nodes/705/service-plan', { data: { revision: 0, steps } });
      expect(saved.ok(), await saved.text()).toBeTruthy();
      await page.goto('/?domain=Cluster');
      const item = page.getByTestId('tree-node-SERVICE-chunkdb-705');
      await expect(item.getByTitle('Healthy', { exact: true })).toBeVisible();
      await expect(item.getByRole('button', { name: 'CDB-705', exact: true })).toHaveAttribute('title', `CDB-705 · ${detail}`);
      await page.reload();
      await expect(item.getByTitle('Healthy', { exact: true })).toBeVisible();
      expect((await (await api.get('/api/service-plans')).json())['705'].steps.chunkdb.state).toBe('warning');
    } finally {
      await api.dispose();
    }
  });
});
