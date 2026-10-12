// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Page, APIRequestContext } from '@playwright/test';
import { expect } from './realBackend';
import { nativeProcessAlive } from './nativeProcess';

export type Service = { id: string; node_id: number; service_type: string; pid?: number | null; pid_node_id?: number };
export type LiveService = Service & { pid: number };

export async function service(request: APIRequestContext, kind: string): Promise<Service | undefined> {
  const response = await request.get('/api/servers');
  expect(response.ok(), await response.text()).toBe(true);
  const rows: Service[] = await response.json();
  const matches = rows.filter(row => row.node_id === 1 && row.service_type === kind);
  expect(matches.length).toBeLessThanOrEqual(1);
  return matches[0];
}

export async function liveService(request: APIRequestContext, kind: string, previous?: number): Promise<LiveService> {
  let row: Service | undefined;
  await expect.poll(async () => {
    row = await service(request, kind);
    return row?.pid && row.pid !== previous ? row.pid : 0;
  }, { timeout: 10_000, intervals: [100] }).toBeGreaterThan(0);
  expect(row!.pid_node_id).toBe(1);
  expect(await nativeProcessAlive(0, row!.pid!)).toBe(true);
  return { ...row!, pid: row!.pid! };
}

export async function unchangedService(request: APIRequestContext, original: LiveService) {
  const actual = await liveService(request, original.service_type);
  expect(actual.id).toBe(original.id);
  expect(actual.pid).toBe(original.pid);
}

export async function stoppedService(request: APIRequestContext, original: LiveService) {
  await expect.poll(async () => {
    const actual = await service(request, original.service_type);
    return { id: actual?.id, pid: actual?.pid ?? null };
  }, { timeout: 10_000, intervals: [100] }).toEqual({ id: original.id, pid: null });
  await expect.poll(() => nativeProcessAlive(0, original.pid), { intervals: [100] }).toBe(false);
}

export function serviceRow(page: Page, original: Service) {
  const id = original.service_type === 'diskdb' ? `DDB-${original.node_id}` : `SERVICE-${original.id}`;
  return page.getByRole('complementary', { name: 'Cluster tree sidebar' })
    .getByTestId(`tree-node-${id}`).locator(':scope > [role="treeitem"]');
}
