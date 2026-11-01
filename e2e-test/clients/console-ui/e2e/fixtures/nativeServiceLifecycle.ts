// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { APIRequestContext, Page } from '@playwright/test';
import { expect } from './realBackend';
import { nativeProcessAlive, nativeServices, observedNativeProcessAlive } from './nativeProcess';
import { step } from './stepTimer';

interface NativeServer {
  id: string;
  node_id: number;
  pid?: number;
  pid_node_id?: number;
  health: string;
  service_type: string;
  start_pending: boolean;
}

export async function nativeServerInventory(request: APIRequestContext): Promise<NativeServer[]> {
  const response = await request.get('/api/servers');
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

async function siblingIdentities(target: string) {
  const nodes = await Promise.all([0, 1, 2].map(node => nativeServices(node)));
  return nodes.flatMap((services, node) => Object.entries(services)
    .filter(([id]) => node !== 0 || id !== target)
    .map(([id, { pid, generation, operation_id }]) => ({ node, id, pid, generation, operation_id })))
    .sort((left, right) => `${left.node}/${left.id}`.localeCompare(`${right.node}/${right.id}`));
}

const registration = ({ pid: _pid, pid_node_id: _namespace, health: _health, start_pending: _pending, ...row }: NativeServer) => row;

async function stopAuxiliary(page: Page, request: APIRequestContext, original: NativeServer, kind: string, label: string, prefix: string) {
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await step(`native lifecycle: ${original.id} Stop response`, async () => {
    await sidebar.getByRole('button', { name: 'N-1', exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: `${prefix}-1 Running`, exact: true }).hover();
    const stopped = page.waitForResponse(response =>
      new URL(response.url()).pathname === `/api/services/${original.id}/stop`
      && response.request().method() === 'POST');
    await page.getByRole('menuitem', { name: `Stop ${label}`, exact: true }).click();
    const response = await stopped;
    expect(response.status(), await response.text()).toBe(200);
    expect(await response.json()).toMatchObject({ id: original.id, pid: null, removed: false, data_preserved: true });
  });
  await expect(page.locator('header').getByRole('button', { name: 'Refresh', exact: true }).locator('svg')).not.toHaveClass(/tw-animate-spin/);
  await step(`native lifecycle: ${original.id} stopped process and inventory`, async () => {
    await expect.poll(() => nativeProcessAlive(0, original.pid), { intervals: [100] }).toBe(false);
    await expect.poll(async () => {
      const current = (await nativeServerInventory(request)).find(row => row.id === original.id);
      return { id: current?.id, pid: current?.pid ?? null, ...(kind === 'access-server' ? { health: current?.health } : {}) };
    }, { intervals: [100] }).toEqual({ id: original.id, pid: null, ...(kind === 'access-server' ? { health: 'down' } : {}) });
  });
}

async function startAuxiliary(page: Page, request: APIRequestContext, original: NativeServer, kind: string, label: string, prefix: string, oldOperation: string) {
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  let submitted = false;
  try {
    await sidebar.getByRole('button', { name: 'N-1', exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: `${prefix}-1 Stopped`, exact: true }).hover();
    // Preserve the existing response budget; the combined process assertion stays 3s.
    const started = page.waitForResponse(response =>
      new URL(response.url()).pathname === `/api/services/${original.id}/restart`
      && response.request().method() === 'POST', { timeout: 5_000 });
    const startAt = performance.now();
    submitted = true;
    const response = await step(`native lifecycle: ${original.id} Start response`, async () => {
      await page.getByRole('menuitem', { name: `Start ${label}`, exact: true }).click();
      return started;
    });
    console.log(`[START] ${original.id}: ${(performance.now() - startAt).toFixed(0)}ms`);
    expect(response.status(), await response.text()).toBe(200);
    const accepted = await response.json();
    expect(accepted).toMatchObject({ id: original.id, removed: false, data_preserved: true });
    expect(Number.isInteger(accepted.pid)).toBe(true);
    expect(accepted.pid).toBeGreaterThan(1);
    expect(accepted.pid).not.toBe(original.pid);
    await step(`native lifecycle: ${original.id} new process, inventory and Access recovery`, () => expect.poll(async () => {
      const inventory = await nativeServerInventory(request);
      const current = inventory.find(row => row.id === original.id);
      const access = inventory.find(row => row.node_id === original.node_id && row.service_type === 'access-server');
      const actual = (await nativeServices(0))[original.id];
      // A new backend PID can precede dependent Access readiness. Close that
      // outage inside this same three-second assertion before the next cycle.
      return current?.pid === accepted.pid && current.pid_node_id === original.node_id
        && actual?.pid === accepted.pid && typeof actual.operation_id === 'string'
        && actual.operation_id !== oldOperation && await observedNativeProcessAlive(0, accepted.pid)
        && (kind !== 'access-server' || current.health === 'up') && access?.health === 'up';
    }, { intervals: [100] }).toBe(true));
    const current = (await nativeServerInventory(request)).filter(row => row.id === original.id);
    expect(current).toHaveLength(1);
    expect(registration(current[0])).toEqual(registration(original));
  } finally {
    if (!submitted) {
      const current = (await nativeServerInventory(request)).find(row => row.id === original.id);
      if (!current?.pid) expect((await request.post(`/api/services/${original.id}/restart`, { data: {} })).ok()).toBe(true);
    }
  }
}

export async function cycleAuxiliaryService(page: Page, request: APIRequestContext, kind: string, label: string, prefix: string) {
  const original = (await nativeServerInventory(request)).find(row => row.node_id === 1 && row.service_type === kind);
  expect(original).toBeDefined();
  expect(original.pid_node_id).toBe(original.node_id);
  if (kind === 'access-server') expect(original.health).toBe('up');
  expect(original.pid).toBeGreaterThan(1);
  const actual = (await nativeServices(0))[original.id];
  expect(actual?.pid).toBe(original.pid);
  expect(actual.operation_id).toEqual(expect.any(String));
  expect(await observedNativeProcessAlive(0, original.pid)).toBe(true);
  const siblings = await siblingIdentities(original.id);
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(sidebar.getByText(`${prefix}-1`, { exact: true })).toBeVisible();
  if (kind === 'access-server') await expect(sidebar.getByRole('button', { name: 'AS-access-1', exact: true })).toHaveCount(0);
  await stopAuxiliary(page, request, original, kind, label, prefix);
  await step(`native lifecycle: ${original.id} stopped siblings`, async () => expect(await siblingIdentities(original.id)).toEqual(siblings));
  await startAuxiliary(page, request, original, kind, label, prefix, actual.operation_id);
  await step(`native lifecycle: ${original.id} restarted siblings`, async () => expect(await siblingIdentities(original.id)).toEqual(siblings));
}
