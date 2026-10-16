// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { DomainProvider, useDomain } from './DomainContext';
import { SelectionProvider, useSelection } from './SelectionContext';
import { Domain } from '../types';

function ScopeProbe() {
  const { domain, setDomain } = useDomain();
  const { selectedEntity, scopeEntity, inspectEntity, selectEntity, clearSelection } = useSelection();
  return <><div data-testid="scope">{scopeEntity?.id ?? 'empty'}</div>
    <button onClick={() => inspectEntity({ domain, type: 'Node', id: 'node-2' })}>Inspect second</button>
    <button onClick={() => selectEntity({ domain, type: 'Node', id: 'node-2' })}>Open second</button>
    <output>{selectedEntity?.id ?? 'empty'}</output>
    <button onClick={() => selectEntity({ domain, type: domain === Domain.KV ? 'Group' : 'Node', id: domain === Domain.KV ? 'group-7' : 'node-1' })}>Select</button>
    <button onClick={() => setDomain(Domain.KV)}>KV</button>
    <button onClick={() => setDomain(Domain.Capacity)}>Capacity</button>
    <button onClick={() => setDomain(Domain.Cluster)}>Cluster</button>
    <button onClick={clearSelection}>Clear</button></>;
}
describe('domain selection scope', () => {
  it('retains each domain selection and clears only the active domain', () => {
    render(<DomainProvider><SelectionProvider><ScopeProbe /></SelectionProvider></DomainProvider>);
    fireEvent.click(screen.getByRole('button', { name: 'Select' }));
    fireEvent.click(screen.getByRole('button', { name: 'KV' }));
    expect(screen.getByRole('status')).toHaveTextContent('empty');
    fireEvent.click(screen.getByRole('button', { name: 'Select' }));
    fireEvent.click(screen.getByRole('button', { name: 'Cluster' }));
    expect(screen.getByRole('status')).toHaveTextContent('node-1');
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    fireEvent.click(screen.getByRole('button', { name: 'KV' }));
    expect(screen.getByRole('status')).toHaveTextContent('group-7');
  });
});

it('shares the selected entity between both Cluster views', () => {
  render(<DomainProvider><SelectionProvider><ScopeProbe /></SelectionProvider></DomainProvider>);
  fireEvent.click(screen.getByRole('button', { name: 'Select' }));
  fireEvent.click(screen.getByRole('button', { name: 'Capacity' }));
  expect(screen.getByRole('status')).toHaveTextContent('node-1');
  fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
  fireEvent.click(screen.getByRole('button', { name: 'Cluster' }));
  expect(screen.getByRole('status')).toHaveTextContent('empty');
});

it('keeps center scope while inspecting another row or closing properties', () => {
  render(<DomainProvider><SelectionProvider><ScopeProbe /></SelectionProvider></DomainProvider>);
  fireEvent.click(screen.getByRole('button', { name: 'Select' }));
  fireEvent.click(screen.getByRole('button', { name: 'Inspect second' }));
  expect(screen.getByRole('status')).toHaveTextContent('node-2');
  expect(screen.getByTestId('scope')).toHaveTextContent('node-1');
  fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
  expect(screen.getByRole('status')).toHaveTextContent('empty');
  expect(screen.getByTestId('scope')).toHaveTextContent('node-1');
  fireEvent.click(screen.getByRole('button', { name: 'Open second' }));
  expect(screen.getByTestId('scope')).toHaveTextContent('node-2');
});
