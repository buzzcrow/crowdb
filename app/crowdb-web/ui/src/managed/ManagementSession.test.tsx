// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ManagementSession } from './ManagementSession';

describe('managed container scope', () => {
  it('labels simplified topology separately from monitor service health', () => {
    render(<ManagementSession />);
    expect(screen.getByTestId('managed-topology-scope')).toHaveTextContent('simplified view');
    expect(screen.getByTestId('managed-readonly')).toHaveTextContent('read-only');
  });
});
