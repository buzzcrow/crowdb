// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Page, Locator } from '@playwright/test';
export async function resourceAction(page: Page, target: Locator, action: string) {
  await target.click({ button: 'right' });
  await page.getByRole('menuitem', { name: action, exact: true }).click();
}
export async function datasetAction(page: Page, action: string) {
  await resourceAction(page, page.getByTestId('dataset-scope'), action);
}
