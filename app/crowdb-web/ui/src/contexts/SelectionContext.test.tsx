// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { DomainProvider, useDomain } from './DomainContext';
import { SelectionProvider, useSelection } from './SelectionContext';
import { Domain } from '../types';

function ScopeProbe() {
  const { domain, setDomain } = useDomain();
  const { selectedEntity, selectEntity, clearSelection } = useSelection();
  return <><output>{selectedEntity?.id ?? 'empty'}</output>
    <button onClick={() => selectEntity({ domain, type: domain === Domain.KV ? 'Group' : 'Node', id: domain === Domain.KV ? 'group-7' : 'node-1' })}>Select</button>
    <button onClick={() => setDomain(Domain.KV)}>KV</button>
    <button onClick={() => setDomain(Domain.Cluster)}>Cluster</button>
    <button onClick={clearSelection}>Clear</button></>;
}
describe('domain selection scope', () => {
  it('retains each domain selection and clears only the active domain', () => {
    render(<DomainProvider><SelectionProvider><ScopeProbe /></SelectionProvider></DomainProvider>);
    fireEvent.click(screen.getByText('Select'));
    fireEvent.click(screen.getByText('KV'));
    expect(screen.getByRole('status')).toHaveTextContent('empty');
    fireEvent.click(screen.getByText('Select'));
    fireEvent.click(screen.getByText('Cluster'));
    expect(screen.getByRole('status')).toHaveTextContent('node-1');
    fireEvent.click(screen.getByText('Clear'));
    fireEvent.click(screen.getByText('KV'));
    expect(screen.getByRole('status')).toHaveTextContent('group-7');
  });
});
