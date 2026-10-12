// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect } from './realBackend';
import { containerControl } from './containerControl';

const known = new Set<string>();
const observation = (body: object) => containerControl(process.env.CROWDB_NATIVE_OBSERVATION_CONTROL, body);

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
