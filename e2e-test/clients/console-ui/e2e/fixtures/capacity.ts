// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect } from './realBackend';
import type { Page, Locator } from '@playwright/test';

export async function diskBranch(page: Page, node: number, group: number) {
  const response = await page.request.get(`/api/nodes/${node}/disk-groups/${group}/disks`);
  expect(response.ok(), await response.text()).toBe(true);
  const disks = await response.json();
  expect(disks).toHaveLength(1);
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const nodeRow = sidebar.getByTestId(`tree-node-N-${node}`).locator(':scope > [role="treeitem"]');
  if (await nodeRow.getAttribute('aria-expanded') !== 'true') {
    await nodeRow.getByRole('button', { name: 'Expand', exact: true }).click();
  }
  const storageRow = sidebar.getByTestId(`tree-node-STORAGE-${node}`).locator(':scope > [role="treeitem"]');
  if (await storageRow.getAttribute('aria-expanded') !== 'true') {
    await storageRow.getByRole('button', { name: 'Expand', exact: true }).click();
  }
  const groupRow = sidebar.getByTestId(`tree-node-CH-DG-${node}-${group}`).locator(':scope > [role="treeitem"]');
  await expect(groupRow).toBeVisible();
  if (await groupRow.getAttribute('aria-expanded') !== 'true') {
    await groupRow.getByRole('button', { name: 'Expand', exact: true }).click();
  }
  const disk = sidebar.getByTestId(`tree-node-CH-D-${node}-${group}-${disks[0].disk_id}`)
    .locator(':scope > [role="treeitem"]');
  await expect(disk).toBeVisible();
  return { disk, group: groupRow, id: String(disks[0].disk_id) };
}

export async function diskMenu(page: Page, row: Locator, name: string) {
  await row.click({ button: 'right' });
  const item = page.getByRole('menuitem', { name, exact: true });
  await expect(item).toBeVisible();
  await item.dispatchEvent('click');
}
