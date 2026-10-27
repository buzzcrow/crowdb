// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import * as api from './api';
import { DeleteDataset } from './DeleteDataset';
vi.mock('./api', async original => ({ ...await original<typeof import('./api')>(), control: vi.fn(), mutate: vi.fn() }));
beforeEach(() => {
  vi.mocked(api.control).mockReset().mockResolvedValue({ name: 'original', id: '1'.repeat(32) });
  vi.mocked(api.mutate).mockReset();
});
it('pins the identity through a selection change and uncertain retry', async () => {
  const complete = vi.fn(); const close = vi.fn();
  const view = render(<DeleteDataset dataset="original" readonly={false} complete={complete} close={close} />);
  await screen.findByText(`Dataset ID: ${'1'.repeat(32)}`);
  view.rerender(<DeleteDataset dataset="replacement" readonly={false} complete={complete} close={close} />);
  vi.mocked(api.mutate).mockRejectedValueOnce(new api.DatasetError(0, 'Connection lost', true)).mockResolvedValueOnce({});
  fireEvent.click(screen.getByRole('button', { name: 'Delete Dataset' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('Retry uses the same Dataset ID');
  expect(complete).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Delete Dataset' }));
  await waitFor(() => expect(complete).toHaveBeenCalledOnce());
  for (const call of vi.mocked(api.mutate).mock.calls) expect(call).toEqual(['dataset-delete', api.datasetPath('original'), 'DELETE', { id: '1'.repeat(32) }]);
});
it('keeps nonempty errors in the dialog and disables readonly deletion', async () => {
  vi.mocked(api.mutate).mockRejectedValue(new api.DatasetError(409, 'dataset_not_empty'));
  const complete = vi.fn(); const close = vi.fn();
  const view = render(<DeleteDataset dataset="original" readonly={false} complete={complete} close={close} />);
  await screen.findByText(`Dataset ID: ${'1'.repeat(32)}`);
  fireEvent.click(screen.getByRole('button', { name: 'Delete Dataset' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('Dataset is not empty');
  expect(complete).not.toHaveBeenCalled();
  view.rerender(<DeleteDataset dataset="original" readonly complete={complete} close={close} />);
  expect(screen.getByRole('button', { name: 'Delete Dataset' })).toBeDisabled();
});
