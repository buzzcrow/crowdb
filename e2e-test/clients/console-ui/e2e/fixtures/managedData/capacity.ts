// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { formatBytes } from '../../../src/utils/capacity';
import { expect } from '../realBackend';
import { diskBranch } from '../capacity';
import { step } from '../stepTimer';

export async function capacityWorkflow(page: Page, request: APIRequestContext) {
  await step('Capacity: runtime totals load automatically from Disk through Datacenter', async () => {
    const response = await request.get('/api/hardware/capacity'); expect(response.ok(), await response.text()).toBe(true);
    const hardware = await response.json();
    const selected = hardware.disk_groups.find((group: any) => group.node_id === 1 && group.disk_count === 1);
    expect(selected).toBeDefined();
    await page.getByTestId('domain-cluster').click();
    const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await sidebar.getByRole('button', { name: 'datacenter', exact: true }).click();
    const branch = await diskBranch(page, 1, selected.disk_group_id);
    const summary = page.getByTestId('capacity-summary');
    const scopes = [
      { row: branch.disk, groups: [selected] }, { row: branch.group, groups: [selected] },
      { row: sidebar.getByTestId('tree-node-STORAGE-1').locator(':scope > [role="treeitem"]'), groups: hardware.disk_groups.filter((group: any) => group.node_id === 1) },
      { row: sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]'), groups: hardware.disk_groups.filter((group: any) => group.node_id === 1) },
      { row: sidebar.getByTestId(`tree-node-R-${selected.rack_id}`).locator(':scope > [role="treeitem"]'), groups: hardware.disk_groups.filter((group: any) => group.rack_id === selected.rack_id) },
      { row: sidebar.getByRole('button', { name: 'datacenter', exact: true }), groups: hardware.disk_groups },
    ];
    for (const scope of scopes) {
      await scope.row.click();
      const total = scope.groups.reduce((sum: number, group: any) => sum + group.capacity_bytes, 0);
      await expect(summary.locator(':scope > div').filter({ hasText: 'Total Capacity' }).getByText(formatBytes(total), { exact: true })).toBeVisible();
      await expect(summary).not.toContainText('Unknown');
      const observed = await request.get('/api/diskdb/usage'); expect(observed.ok(), await observed.text()).toBe(true);
      const reports = (await observed.json()).disk_groups.filter((group: any) => scope.groups.some((item: any) => item.node_id === group.node_id && item.disk_group_id === group.disk_group_id));
      expect(reports).toHaveLength(scope.groups.length);
      for (const [label, field] of [['Busy', 'busy_bytes'], ['Free', 'free_bytes']]) {
        const expected = reports.reduce((sum: number, group: any) => sum + group[field], 0);
        await expect(summary.locator(':scope > div').filter({ hasText: label }).getByText(formatBytes(expected), { exact: true })).toBeVisible();
      }
    }
  });
}
