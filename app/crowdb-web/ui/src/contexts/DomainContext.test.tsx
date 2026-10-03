// Copyright 2026-present Gian <crow.db@outlook.com>.

import { describe, it, expect, vi } from 'vitest';
import { render, renderHook, act } from '@testing-library/react';
import { DomainProvider, useDomain } from './DomainContext';
import { Domain } from '../types';

describe('DomainProvider', () => {
  it('defaults to Cluster domain when no initialDomain provided', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => <DomainProvider>{children}</DomainProvider>,
    });
    expect(result.current.domain).toBe(Domain.Cluster);
  });

  it('uses the provided initialDomain', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => (
        <DomainProvider initialDomain={Domain.KV}>{children}</DomainProvider>
      ),
    });
    expect(result.current.domain).toBe(Domain.KV);
  });

  it('setDomain updates the domain', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => <DomainProvider>{children}</DomainProvider>,
    });
    act(() => result.current.setDomain(Domain.Chunk));
    expect(result.current.domain).toBe(Domain.Chunk);
  });

  it('throws when useDomain is used outside a DomainProvider', () => {
    // Suppress the expected error output.
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    expect(() => renderHook(() => useDomain())).toThrow(
      'useDomain must be used within a DomainProvider',
    );
    spy.mockRestore();
  });

  it('renders children', () => {
    const { getByText } = render(
      <DomainProvider>
        <div>child-content</div>
      </DomainProvider>,
    );
    expect(getByText('child-content')).toBeTruthy();
  });
});


it('keeps a bounded return route, restores snapshots and drops forward on new navigation', async () => {
  const { result } = renderHook(() => useDomain(), {
    wrapper: ({ children }) => <DomainProvider>{children}</DomainProvider>,
  });
  let position = 'group-A';
  act(() => { result.current.register(Domain.Cluster, 'query', () => {
    const saved = position; return () => { position = saved; };
  }); });
  await act(async () => { result.current.setDomain(Domain.KV); await Promise.resolve(); });
  position = 'group-B';
  act(() => result.current.back());
  expect(position).toBe('group-A');
  expect(result.current.domain).toBe(Domain.Cluster);
  expect(result.current.canForward).toBe(true);
  act(() => result.current.forward());
  expect(result.current.domain).toBe(Domain.KV);
  act(() => result.current.back());
  await act(async () => { result.current.setDomain(Domain.S3); await Promise.resolve(); });
  expect(result.current.canForward).toBe(false);
  for (let index = 0; index < 40; index++) {
    await act(async () => { result.current.setDomain(index % 2 ? Domain.KV : Domain.Cluster); await Promise.resolve(); });
  }
  let count = 0;
  while (result.current.canBack) { act(() => result.current.back()); count++; }
  expect(count).toBe(32);
});
