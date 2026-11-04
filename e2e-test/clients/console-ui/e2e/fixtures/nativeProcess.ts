// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect } from './realBackend';
import { containerControl } from './containerControl';

const known = new Set<string>();
const observation = (body: object) => containerControl(process.env.CROWDB_NATIVE_OBSERVATION_CONTROL, body);

export interface NativeServiceState {
  pid: number | null;
  generation: number;
  operation_id: string | null;
}

/** Read typed service identities from this fixture's owned monitor. */
export async function nativeServices(node: number): Promise<Record<string, NativeServiceState>> {
  const status = await observation({ action: 'observe-native-services', node });
  for (const service of Object.values(status.services) as NativeServiceState[]) {
    if (service.pid) known.add(`${node}/${service.pid}`);
  }
  return status.services;
}

/** One OS observation after the owning monitor has supplied the PID. */
export async function observedNativeProcessAlive(node: number, pid: number): Promise<boolean> {
  if (!known.has(`${node}/${pid}`)) throw new Error('Native PID has not been observed in this owned node');
  return (await observation({ action: 'observe-native-process', node, pid })).alive;
}

/** Observe only a PID recorded by this fixture's actual monitor. */
export async function nativeProcessAlive(node: number, pid: number): Promise<boolean> {
  const key = `${node}/${pid}`;
  if (!known.has(key)) {
    await expect.poll(async () => {
      await nativeServices(node);
      return known.has(key);
    }, { intervals: [100] }).toBe(true);
  }
  return observedNativeProcessAlive(node, pid);
}
