// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, cleanup, render } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import type { ReactFlowInstance, useStoreApi } from 'reactflow';
import { TopologyCanvas } from './TopologyCanvas';
import { DomainProvider } from '../contexts/DomainContext';
import type { Node as HardwareNode } from '../types';
import { SelectionProvider } from '../contexts/SelectionContext';

let flow: ReactFlowInstance;
let store: ReturnType<typeof useStoreApi>;
vi.mock('reactflow', async () => {
  const actual = await vi.importActual<typeof import('reactflow')>('reactflow');
  return {
    ...actual,
    useReactFlow: () => {
      store = actual.useStoreApi();
      flow = actual.useReactFlow();
      return flow;
    },
  };
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it('keeps a finite viewport when its panel hides before the first zoom animation frame', async () => {
  vi.useFakeTimers();
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  let visible = true;
  for (const [name, size] of [['clientWidth', 500], ['clientHeight', 300], ['offsetWidth', 500], ['offsetHeight', 300]] as const) {
    vi.spyOn(HTMLElement.prototype, name, 'get').mockImplementation(() => visible ? size : 0);
  }
  const racks = [{ id: 1, name: 'rack', nodes: [] }];
  function TestCanvas({ active }: { active: boolean }) {
    return <DomainProvider><SelectionProvider><div style={{ display: active ? 'block' : 'none' }}>
      <TopologyCanvas active={active} racks={racks} nodes={[]} servers={[]} stores={[]} nodeStores={{}} />
    </div></SelectionProvider></DomainProvider>;
  }
  const mounted = render(<TestCanvas active />);
  store.setState({ width: 500, height: 300 });
  let fitted = false;
  await act(async () => { fitted = await flow.fitView({ duration: 250 }); });
  expect(fitted).toBe(true);
  visible = false;
  mounted.rerender(<TestCanvas active={false} />);
  await act(async () => { await vi.advanceTimersByTimeAsync(300); });
  expect(Object.values(flow.getViewport()).every(Number.isFinite)).toBe(true);
});

it('keeps an explicitly collapsed ancestor closed on refreshed data until a new navigation request', async () => {
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  const nodes: HardwareNode[] = [{ id: 2, rack_id: 1, host: 'fixture', ssh: { type: 'KeyDefault', user: 'fixture' } }];
  const racks = [{ id: 1, name: 'rack', nodes }];
  function TestCanvas({ nonce, refresh }: { nonce: number; refresh: number }) {
    return <DomainProvider><SelectionProvider><TopologyCanvas racks={racks} nodes={nodes} servers={[]} stores={[]} nodeStores={{}}
      allServers={[]} refreshToken={refresh} focusRequest={{ targetId: 'N-2', subtree: true, nonce }} /></SelectionProvider></DomainProvider>;
  }
  const mounted = render(<TestCanvas nonce={1} refresh={1} />);
  await act(async () => { store.setState({ width: 500, height: 300 }); });
  expect(flow.getNode('N-2')).toBeDefined();
  await act(async () => { flow.getNode('R-1')!.data.onToggle(); });
  expect(flow.getNode('N-2')).toBeUndefined();
  mounted.rerender(<TestCanvas nonce={1} refresh={2} />);
  expect(flow.getNode('N-2')).toBeUndefined();
  mounted.rerender(<TestCanvas nonce={2} refresh={2} />);
  expect(flow.getNode('N-2')).toBeDefined();
});
