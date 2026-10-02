// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { ReactNode } from 'react';
import { HelpCircle, CheckCircle2, Wrench, AlertTriangle, EyeOff, XCircle, PowerOff } from 'lucide-react';
import { HW_STATUS_NAMES } from '../utils/entityDisplay';
import type { MenuItemOrSeparator } from '../components/ContextMenu';
  const statusIcons: Record<string, ReactNode> = {
    Init: <HelpCircle className="tw-h-4 tw-w-4" />,
    Up: <CheckCircle2 className="tw-h-4 tw-w-4" />,
    Maintenance: <Wrench className="tw-h-4 tw-w-4" />,
    Suspect: <AlertTriangle className="tw-h-4 tw-w-4" />,
    Missing: <EyeOff className="tw-h-4 tw-w-4" />,
    Bad: <XCircle className="tw-h-4 tw-w-4" />,
    Offline: <PowerOff className="tw-h-4 tw-w-4" />,
  };


export function buildStatusSubmenu(onSet: (status: string) => Promise<void>): MenuItemOrSeparator[] {
  return HW_STATUS_NAMES.map(name => ({ id: `status-${name.toLowerCase()}`, label: name, icon: statusIcons[name], onSelect: () => onSet(name) }));
}
