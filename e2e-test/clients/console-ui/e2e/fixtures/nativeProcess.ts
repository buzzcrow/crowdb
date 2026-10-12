// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { expect } from './realBackend';

let nextRequest = 0;
const known = new Set<string>();
async function observation(body: object) {
  const root = process.env.CROWDB_NATIVE_OBSERVATION_CONTROL;
  if (!root) throw new Error('Owned native observation control is required');
  const id = `${process.pid}-${nextRequest++}`;
  const request = path.join(root, `${id}.request.json`);
  const temporary = `${request}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(body));
  await fs.rename(temporary, request);
  const response = path.join(root, `${id}.response.json`);
  let result: { ok: boolean; error?: string; result: any } | undefined;
  await expect.poll(async () => {
    try { result = JSON.parse(await fs.readFile(response, 'utf8')); return true; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false; throw error; }
  }, { intervals: [25] }).toBe(true);
  if (!result?.ok) throw new Error(`Native observation failed: ${result?.error}`);
  return result.result;
}

/** Observe only a PID recorded by this fixture's actual monitor. */
export async function nativeProcessAlive(node: number, pid: number): Promise<boolean> {
  const key = `${node}/${pid}`;
  if (!known.has(key)) {
    await expect.poll(async () => {
      const status = await observation({ action: 'observe-native-services', node });
      for (const service of Object.values(status.services) as { pid?: number }[]) {
        if (service.pid) known.add(`${node}/${service.pid}`);
      }
      return known.has(key);
    }, { intervals: [100] }).toBe(true);
  }
  return (await observation({ action: 'observe-native-process', node, pid })).alive;
}
