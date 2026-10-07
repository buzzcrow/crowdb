// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ManagementSession } from './ManagementSession';

describe('managed container scope', () => {
  it('does not add internal container diagnostics to the managed workspace', () => {
    render(<ManagementSession />);
    expect(screen.queryByTestId('managed-preview')).toBeNull();
    expect(screen.queryByText(/Monitor service health/)).toBeNull();
  });
});
