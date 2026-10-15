// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { existsSync } from "node:fs";
import { defineConfig, devices } from "@playwright/test";
const explicitExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const localBrowsers = [
  '/snap/bin/chromium',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable',
  '/Applications/Chromium.app/Contents/MacOS/Chromium',
  '/usr/bin/microsoft-edge',
  '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
];
const executablePath = explicitExecutable
  ?? localBrowsers.find((p) => existsSync(p));

if (!process.env.PLAYWRIGHT_CHANNEL && !executablePath) {
  throw new Error('No system browser found; set PLAYWRIGHT_CHANNEL or PLAYWRIGHT_CHROMIUM_EXECUTABLE');
}

// Owned local release endpoints must not wait for host PAC/WPAD discovery.
const chromiumUse = process.env.PLAYWRIGHT_CHANNEL
  ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL, launchOptions: { args: ['--no-proxy-server'] } }
  : executablePath
    ? { ...devices['Desktop Chrome'], launchOptions: { executablePath, args: ['--no-proxy-server'] } }
    : { ...devices['Desktop Chrome'] };

export default defineConfig({
  testDir: './e2e/flows',
  workers: 1,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 3000 },
  testMatch: "**/12-bridge-discovery.spec.ts",
  outputDir: process.env.CROWDB_WEB_E2E_OUTPUT,
  reporter: [['list'], ['json', { outputFile: `${process.env.CROWDB_WEB_E2E_OUTPUT}/results.json` }]],
  use: { ...chromiumUse, baseURL: process.env.CROWDB_WEB_E2E_BASE_URL, trace: 'retain-on-failure' },
});
