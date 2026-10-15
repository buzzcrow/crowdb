// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { expect, it } from 'vitest';
import { Fields, SchemaTable } from './Fields';

it('renders file metrics records as a table', () => {
  render(<Fields stacked values={{ metrics: [
    { column: 'id', null_count: 0, lower_bound: '0', upper_bound: '9' },
    { column: 'name', null_count: 1, lower_bound: 'a', upper_bound: 'z' },
  ] }} />);
  const table = screen.getByRole('table', { name: 'File metrics' });
  expect(table).toHaveTextContent('null count');
  expect(table).toHaveTextContent('id');
  expect(table).toHaveTextContent('name');
  expect(table.querySelectorAll('tbody tr')).toHaveLength(2);
});

it('renders nested writer fields as an expandable table', () => {
  render(<SchemaTable value={{ type: 'record', name: 'manifest_entry', fields: [
    { name: 'status', id: 0, required: true, type: 'int' },
    { name: 'data_file', id: 1, type: { type: 'record', fields: [{ name: 'file_path', id: 2, type: 'string' }] } },
  ] }} />);
  const table = screen.getByRole('table', { name: 'Writer schema' });
  expect(table).toHaveTextContent('status');
  expect(table).toHaveTextContent('data_file');
  expect(table).not.toHaveTextContent('file_path');
  const button = screen.getByRole('button', { name: 'Expand field details' });
  fireEvent.click(button);
  return waitFor(() => expect(table).toHaveTextContent('file_path'));
});
