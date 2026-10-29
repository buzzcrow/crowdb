// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect, it } from 'vitest';
import { advancedRequest, propertyRequest } from './metadataEdits';
it('submits only changed properties and removes renamed original keys', () => {
  const request = JSON.parse(propertyRequest('table-id', { keep: 'old', rename: 'x' }, [{ name: 'keep', value: 'old', remove: false }, { name: 'renamed', value: 'x', remove: false }]));
  expect(request.updates).toEqual([{ action: 'set-properties', updates: { renamed: 'x' } }, { action: 'remove-properties', removals: ['rename'] }]);
});
it('retains exact large integer tokens and refuses another Table identity', () => {
  const text = '{"requirements":[{"type":"assert-table-uuid","uuid":"table-id"}],"updates":[{"action":"set-snapshot-ref","snapshot-id":9223372036854775806}]}';
  expect(advancedRequest('table-id', text)).toBe(text);
  expect(() => advancedRequest('replacement-id', text)).toThrow('Table UUID');
});
