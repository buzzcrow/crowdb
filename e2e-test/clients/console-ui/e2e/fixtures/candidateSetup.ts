// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Locator, APIRequestContext } from '@playwright/test';
import { expect } from './realBackend';

/** Bootstrap fixtures review their target's fixed diskless startup settings. */
export async function reviewDisklessCandidate(dialog: Locator, request?: APIRequestContext, origin = '', discoveryId?: string) {
  await dialog.getByRole('button', { name: 'Verify and review', exact: true }).click();
  await expect(dialog.getByText(/Access verified/)).toBeVisible();
  if (request && discoveryId) {
    const response = await request.get(`${origin}/api/node/admissions`);
    expect(response.ok(), await response.text()).toBe(true);
    expect((await response.json()).some((node: { discovery_id: string }) => node.discovery_id === discoveryId)).toBe(false);
  }
  await expect(dialog.getByLabel('Storage setup', { exact: true })).toHaveValue('diskless');
  await expect(dialog.getByLabel('Storage setup', { exact: true })).toBeDisabled();
  await expect(dialog.getByLabel('Managed file capacity (GiB)', { exact: true })).toHaveCount(0);
  for (const name of ['crowdb-paxos-kv', 'crowdb-disk-db', 'crowdb-disk-io', 'crowdb-chunk-db', 'crowdb-chunk-kv', 'crowdb-access-server']) {
    const service = dialog.getByRole('checkbox', { name, exact: true });
    await expect(service).not.toBeChecked();
    await expect(service).toBeDisabled();
  }
  await dialog.getByRole('button', { name: 'Join and start', exact: true }).click();
}
