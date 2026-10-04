import { existsSync } from 'node:fs';
import { defineConfig, devices } from '@playwright/test';

const port = Number(process.env.CROWDB_WEB_E2E_PORT ?? 4193);
const baseURL = `http://127.0.0.1:${port}`;

// Browser selection, in priority order:
//   1. PLAYWRIGHT_CHANNEL (e.g. "chrome"/"msedge") — use Playwright's channel support.
//   2. PLAYWRIGHT_CHROMIUM_EXECUTABLE — explicit binary path override.
//   3. Local Chromium (Linux snap, Linux apt, macOS app).
//   4. Local Microsoft Edge (Linux /usr/bin, macOS app).
//   5. macOS Google Chrome (common dev install; Safari is the macOS default
//      but Playwright cannot drive it directly — no CDP support).
// Tests require an installed browser; never download a private test browser.
const explicitExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const localBrowsers = [
  '/snap/bin/chromium',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
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

const chromiumUse = process.env.PLAYWRIGHT_CHANNEL
  ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL }
  : executablePath
    ? { ...devices['Desktop Chrome'], launchOptions: { executablePath } }
    : { ...devices['Desktop Chrome'] };

export default defineConfig({
  testDir: './flows',
  // These cases require the isolated six-service native fixture.
  grepInvert: /native diagnostics/,
  testIgnore: ['**/fixtures/**', '**/71-s3-native.spec.ts', '**/72-managed-native.spec.ts'],
  globalSetup: './globalSetup.ts',
  globalTeardown: './globalTeardown.ts',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  timeout: 60_000,
  expect: { timeout: 3_000 },
  reporter: [['list'], ['./slowReporter.ts']],
  use: {
    baseURL,
    actionTimeout: 3_000,
    trace: 'retain-on-failure',
    headless: true,
  },
  projects: [
    {
      name: 'chromium',
      use: chromiumUse,
    },
  ],
  webServer: {
    command: `npm run build && cargo run -p crowdb-web -- --bind 127.0.0.1 --port ${port} --test-mode`,
    url: `${baseURL}/healthz`,
    reuseExistingServer: false,
    gracefulShutdown: { signal: 'SIGTERM', timeout: 60_000 },
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env } as Record<string, string>,
  },
});
