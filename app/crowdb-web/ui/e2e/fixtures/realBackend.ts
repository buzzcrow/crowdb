// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, test as base } from '@playwright/test';

const rejectInterception = async () => {
  throw new Error('E2E response interception is forbidden. Provision real services/data; track unconverted scenarios in ui-todo.md (E2E-01).');
};

export const test = base.extend({
  context: async ({ context }, use) => {
    context.route = rejectInterception;
    context.routeFromHAR = rejectInterception;
    await use(context);
  },
  page: async ({ page }, use) => {
    page.route = rejectInterception;
    page.routeFromHAR = rejectInterception;
    await use(page);
  },
});
export { expect };

export function consoleBaseURL(): string {
  const port = Number(process.env.CROWDB_WEB_E2E_PORT ?? 4193);
  return process.env.CROWDB_WEB_E2E_BASE_URL ?? `http://127.0.0.1:${port}`;
}
