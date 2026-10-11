// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { request, expect } from '@playwright/test';

// Public setup only: server lifecycle and isolation belong to the container fixture.
export async function apiContext(baseURL: string) {
  return request.newContext({ baseURL, timeout: 20_000 });
}

export async function createRack(baseURL: string, rack: { id: number; name: string }) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/racks', { data: rack });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function createNode(baseURL: string, node: { id: number; rack_id: number }) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/racks/${node.rack_id}/nodes`, {
      data: { ...node, host: '127.0.0.1' },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}
