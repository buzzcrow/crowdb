// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Playwright reporter that logs [TEST] lines for slow tests (>= 10 s)
// and [TEST] VERY_SLOW for tests >= 30 s. Composes with the default
// 'list' reporter — see realBackend.config.ts.

import { mkdirSync, writeFileSync } from 'node:fs';
import type { Reporter, TestCase, TestResult, TestStep } from '@playwright/test/reporter';

const SLOW_MS = 10_000;
const VERY_SLOW_MS = 30_000;

export default class SlowReporter implements Reporter {
  private measurements: { test: string; phase: string; duration_ms: number; status: string }[] = [];
  onStepEnd(test: TestCase, _result: TestResult, step: TestStep) {
    if (step.category !== 'test.step') return;
    this.measurements.push({ test: test.titlePath().slice(1).join(' › '), phase: step.title,
      duration_ms: step.duration, status: step.error ? 'failed' : 'passed' });
  }
  onTestEnd(test: TestCase, result: TestResult) {
    const ms = result.duration;
    const tag = ms >= VERY_SLOW_MS ? 'VERY_SLOW' : ms >= SLOW_MS ? 'SLOW' : null;
    this.measurements.push({ test: test.titlePath().slice(1).join(' › '), phase: 'test',
      duration_ms: ms, status: result.status });
    if (tag) {
      const title = test.titlePath().slice(1).join(' › ');
      console.log(`[TEST] ${title}: ${ms}ms (${tag})`);
    }
  }
  onEnd() {
    mkdirSync('test-results', { recursive: true });
    writeFileSync('test-results/timings.json', JSON.stringify(this.measurements, null, 2));
  }
  printsToConsole() { return true; }
}
