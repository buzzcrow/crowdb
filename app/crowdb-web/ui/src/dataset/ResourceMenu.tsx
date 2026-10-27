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
      const add = (key: DatasetAction, label: string, required: string[] = []) => items.push({ id: key, label, destructive: ['delete', 'remove-split', 'reclaim'].includes(key), disabled: key !== 'inspect' && (readonly || required.some(capability => !capabilities.includes(capability))), onSelect: () => action(selected, key) });
      add('composition', selected.kind === 'split' ? 'Create child from Split…' : ['sample', 'cell', 'schema'].includes(selected.kind) ? 'Edit in child Snapshot…' : 'snapshot' in selected ? 'Create child Snapshot…' : 'Import / New Snapshot…', ['composition-preparation', 'snapshot-head-update']);
      add('resume', 'Resume saved request…');
      if (selected.kind === 'dataset') add('delete', 'Delete Dataset…', ['dataset-delete']);
      if ('snapshot' in selected) {
        add('create-split', 'Create Split…', ['split-preparation', 'split-head-update']);
        add('split', selected.kind === 'split' ? 'Edit Split…' : 'Edit Split revision…', ['split-preparation', 'split-head-update']);
        if (selected.kind === 'split') add('remove-split', 'Remove Split…', ['split-preparation', 'split-head-update']);
        add('stable', 'Set stable', ['dataset-management']); add('retain', 'Retain Snapshot', ['dataset-management']); add('release', 'Release Snapshot', ['dataset-management']); add('reclaim', 'Reclaim Snapshot…', ['snapshot-reclaim']); add('inspect', 'Inspect reclaim status');
      }
    }
    menu.openMenu(event, items);
  };
  const openSplits = (event: MouseEvent, selected: Exclude<DatasetSelection, { kind: 'root' }>) => menu.openMenu(event, [
    { id: 'create-split', label: 'Create Split…', disabled: readonly || !capabilities.includes('split-preparation') || !capabilities.includes('split-head-update'), onSelect: () => action(selected, 'create-split') },
    { id: 'split', label: 'Edit Split revision…', disabled: readonly || !capabilities.includes('split-preparation') || !capabilities.includes('split-head-update'), onSelect: () => action(selected, 'split') },
  ]);
  return { open, openSplits, overlay: menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} /> };
}
