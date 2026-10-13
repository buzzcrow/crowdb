// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { expect } from './realBackend';

let nextRequest = 0;
export async function containerControl(root: string | undefined, body: object): Promise<any> {
  if (!root) throw new Error('Owned container fixture control is required');
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
  if (!result?.ok) throw new Error(`Container fixture control failed: ${result?.error}`);
  return result.result;
}
