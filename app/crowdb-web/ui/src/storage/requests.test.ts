// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, it, vi } from 'vitest';
import { InventoryRequests } from './requests';

it('shares a four-worker limit and coalesces the same inventory identity', async () => {
  const queue = new InventoryRequests();
  const finish: Array<(value: number) => void> = [];
  let active = 0; let maximum = 0;
  const load = vi.fn(() => new Promise<number>(resolve => {
    active++; maximum = Math.max(maximum, active);
    finish.push(value => { active--; resolve(value); });
  }));
  const requests = Array.from({ length: 8 }, (_, id) => queue.run(String(id), undefined, load));
  const duplicate = queue.run('0', undefined, load);
  await Promise.resolve();
  expect(load).toHaveBeenCalledTimes(4);
  for (let index = 0; index < 8; index++) {
    while (!finish[index]) await Promise.resolve();
    finish[index](index);
  }
  expect(await Promise.all(requests)).toEqual([0,1,2,3,4,5,6,7]);
  expect(await duplicate).toBe(0);
  expect(load).toHaveBeenCalledTimes(8);
  expect(maximum).toBe(4);
});

it('aborting one observer preserves another observer of the same request', async () => {
  const queue = new InventoryRequests();
  let finish!: (value: string) => void;
  let signal!: AbortSignal;
  const load = vi.fn((value: AbortSignal) => { signal = value; return new Promise<string>(resolve => { finish = resolve; }); });
  const controller = new AbortController();
  const first = queue.run('node', controller.signal, load).catch(error => error.name);
  const second = queue.run('node', undefined, load);
  await Promise.resolve();
  controller.abort();
  expect(await first).toBe('AbortError');
  expect(signal.aborted).toBe(false);
  finish('inventory');
  expect(await second).toBe('inventory');
  expect(load).toHaveBeenCalledTimes(1);
});

it('releases aborted queued observations and rejects excess work explicitly', async () => {
  const queue = new InventoryRequests(1, 2);
  let finish!: (value: string) => void;
  const active = queue.run('active', undefined, () => new Promise<string>(resolve => { finish = resolve; }));
  const controller = new AbortController();
  const never = vi.fn(async () => 'unused');
  const queued = queue.run('queued', controller.signal, never).catch(error => error.name);
  await expect(queue.run('excess', undefined, never)).rejects.toThrow('queue is full');
  controller.abort();
  expect(await queued).toBe('AbortError');
  const replacement = queue.run('replacement', undefined, async () => 'replacement');
  finish('done');
  expect(await active).toBe('done');
  expect(await replacement).toBe('replacement');
  expect(never).not.toHaveBeenCalled();
});
