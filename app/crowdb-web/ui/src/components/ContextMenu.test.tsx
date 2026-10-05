// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { ContextMenu } from './ContextMenu';

it('closes the selected menu before asynchronous work so completion cannot close a newer menu', async () => {
  let complete!: () => void;
  const operation = new Promise<void>(resolve => { complete = resolve; });
  const close = vi.fn();
  const { rerender } = render(<ContextMenu items={[{ id: 'old', label: 'Old action', onSelect: () => operation }]} position={{ x: 10, y: 10 }} onClose={close} />);
  fireEvent.click(screen.getByRole('menuitem', { name: 'Old action' }));
  expect(close).toHaveBeenCalledTimes(1);
  rerender(<ContextMenu items={[{ id: 'new', label: 'New action' }]} position={{ x: 20, y: 20 }} onClose={close} />);
  await act(async () => complete());
  expect(close).toHaveBeenCalledTimes(1);
  expect(screen.getByRole('menuitem', { name: 'New action' })).toBeVisible();
});
