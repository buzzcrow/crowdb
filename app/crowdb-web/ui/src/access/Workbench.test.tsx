// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { DocsHelp } from './Workbench';

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
    s3: { endpoint: 'http://localhost:9091', region: 'us-east-1', access_key_id: 'access', secret_access_key: 'secret' },
    iceberg: { uri: 'http://localhost:9092', token: 'token' },
  }))));
  Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: vi.fn().mockResolvedValue(undefined) } });
});

it('reveals credentials and reports a successful copy', async () => {
  render(<DocsHelp href="https://example.test" title="iceberg" auth="External clients use ICEBERG_TOKEN." />);
  fireEvent.click(screen.getByText('Connection credentials'));
  fireEvent.click(screen.getByRole('button', { name: 'Show local credentials' }));
  await waitFor(() => expect(screen.getByText(/ICEBERG_URI=http:\/\/localhost:9092/)).toBeVisible());
  fireEvent.click(screen.getByRole('button', { name: 'Copy env' }));
  await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Copied environment variables'));
  expect(navigator.clipboard.writeText).toHaveBeenCalledWith(expect.stringContaining('AWS_ACCESS_KEY_ID=access'));
});
