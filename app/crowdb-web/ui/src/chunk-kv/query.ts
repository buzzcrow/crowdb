// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { PageCursor } from './PageExplorer';
export interface SplitQuery {
  tab: string;
  stream: { generation?: string; offset: number };
  extent: string | null;
  page?: PageCursor;
}
export const initialSplitQuery = (): SplitQuery => ({ tab: 'Overview', stream: { offset: 0 }, extent: null });
export interface GraphQuery { serverPage: number; offsets: Record<string, number>; collapsed: string[] }
export const initialGraphQuery = (): GraphQuery => ({ serverPage: 0, offsets: {}, collapsed: [] });
