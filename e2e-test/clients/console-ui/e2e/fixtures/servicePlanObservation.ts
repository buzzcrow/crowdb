// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Locator } from '@playwright/test';

type ObservedDialog = HTMLElement & { deploymentObservations?: string[][]; deploymentObserver?: MutationObserver };

/** Retain transient rendered states without delaying or replacing real requests. */
export async function observeDeployingSteps(dialog: Locator) {
  await dialog.evaluate(element => {
    const target = element as ObservedDialog;
    const observations: string[][] = [];
    const capture = () => {
      const active = Array.from(target.querySelectorAll('li'))
        .filter(row => Array.from(row.querySelectorAll('span')).some(cell => cell.textContent === 'deploying'))
        .map(row => row.textContent ?? '');
      if (active.length && JSON.stringify(active) !== JSON.stringify(observations.at(-1)) && observations.length < 32) observations.push(active);
    };
    target.deploymentObservations = observations;
    target.deploymentObserver = new MutationObserver(capture);
    target.deploymentObserver.observe(target, { subtree: true, childList: true, characterData: true });
    capture();
  });
  return {
    read: () => dialog.evaluate(element => (element as ObservedDialog).deploymentObservations ?? []),
    stop: () => dialog.evaluate(element => (element as ObservedDialog).deploymentObserver?.disconnect()),
  };
}
