// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { createContext, type ReactNode } from 'react';
export const EditorDetails = createContext<(content: ReactNode) => void>(() => {});
