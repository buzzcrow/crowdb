// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useCallback, ReactNode } from 'react';
import { Domain } from '../types';
import { useDomain } from './DomainContext';

export type EntityType = 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'DiskGroup' | 'Disk';

/**
 * The single selected entity. `parentIds` carries the ancestor chain in
 * snake_case (`rack_id`, `store_id`, `group_id`, `node_id`) so API calls and
 * cross-jumps can resolve the full path.
 */
export interface SelectedEntity {
  type: EntityType;
  id: string;
  parentIds?: Record<string, string | number>;
  domain: Domain;
  name?: string;
  /** Service flavor for `Server` entities: KV vs DiskDB. */
  serviceType?: 'kv' | 'diskdb';
}

interface SelectionContextType {
  selectedEntity: SelectedEntity | null;
  selectionForDomain: (domain: Domain) => SelectedEntity | null;
  selectEntity: (entity: SelectedEntity | null) => void;
  clearSelection: () => void;
  isSelected: (entityId: string) => boolean;
}

const SelectionContext = createContext<SelectionContextType | undefined>(undefined);

export function SelectionProvider({ children }: { children: ReactNode }) {
  const { domain } = useDomain();
  const [scopes, setScopes] = useState<Partial<Record<Domain, SelectedEntity | null>>>({});
  const selectedEntity = scopes[domain] ?? null;
  const selectionForDomain = useCallback((scope: Domain) => scopes[scope] ?? null, [scopes]);

  const selectEntity = useCallback((entity: SelectedEntity | null) => {
    setScopes(previous => ({ ...previous, [entity?.domain ?? domain]: entity }));
  }, [domain]);

  const clearSelection = useCallback(() => setScopes(previous => ({ ...previous, [domain]: null })), [domain]);

  const isSelected = useCallback(
    (entityId: string) => selectedEntity?.id === entityId,
    [selectedEntity],
  );

  return (
    <SelectionContext.Provider value={{ selectedEntity, selectionForDomain, selectEntity, clearSelection, isSelected }}>
      {children}
    </SelectionContext.Provider>
  );
}

export function useSelection() {
  const context = useContext(SelectionContext);
  if (context === undefined) {
    throw new Error('useSelection must be used within a SelectionProvider');
  }
  return context;
}
