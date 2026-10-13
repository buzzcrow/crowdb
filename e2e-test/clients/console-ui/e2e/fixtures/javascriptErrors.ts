// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Page } from '@playwright/test';
import { expect } from './realBackend';

export function javascriptErrors(page: Page) {
  const errors: string[] = [];
  page.on('console', message => {
    if (message.type() === 'error' && !/Failed to load resource/i.test(message.text())) errors.push(message.text());
  });
  page.on('pageerror', error => errors.push(error.message));
  return () => expect(errors, errors.join('\n')).toEqual([]);
}
