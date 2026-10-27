// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useContextMenu, ContextMenu, type MenuItem } from '../components/ContextMenu';
import type { MouseEvent } from 'react';
import type { DatasetSelection } from './types';
import type { DatasetAction } from './Actions';
export function useDatasetMenu(readonly: boolean, capabilities: string[], create: () => void, action: (selected: Exclude<DatasetSelection, { kind: 'root' }>, action: DatasetAction) => void) {
  const menu = useContextMenu();
  const open = (event: MouseEvent, selected: DatasetSelection) => {
    const items: MenuItem[] = [];
    if (selected.kind === 'root') items.push({ id: 'create', label: 'Create Dataset…', disabled: readonly || !capabilities.includes('dataset-management'), onSelect: create });
    else {
      const add = (key: DatasetAction, label: string, required: string[] = []) => items.push({ id: key, label, disabled: key !== 'inspect' && (readonly || required.some(capability => !capabilities.includes(capability))), onSelect: () => action(selected, key) });
      add('composition', selected.kind === 'split' ? 'Create child from Split…' : 'snapshot' in selected ? 'Create child Snapshot…' : 'Import / New Snapshot…', ['composition-preparation', 'snapshot-head-update']);
      add('resume', 'Resume saved request…');
      if (selected.kind === 'dataset') add('delete', 'Delete Dataset…', ['dataset-delete']);
      if ('snapshot' in selected) { add('split', 'Edit Split revision…', ['split-preparation', 'split-head-update']); add('stable', 'Set stable', ['dataset-management']); add('retain', 'Retain Snapshot', ['dataset-management']); add('release', 'Release Snapshot', ['dataset-management']); add('reclaim', 'Reclaim Snapshot…', ['snapshot-reclaim']); add('inspect', 'Inspect reclaim status'); }
    }
    menu.openMenu(event, items);
  };
  return { open, overlay: menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} /> };
}
