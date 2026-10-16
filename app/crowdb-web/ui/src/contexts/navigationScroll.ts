// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

let cancelRestore: (() => void) | undefined;
export function captureScroll(): () => void {
  const positions = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
    .filter(element => element.getClientRects().length > 0).slice(0, 8)
    .map(element => ({ id: element.dataset.navigationScroll!, top: element.scrollTop, left: element.scrollLeft }));
  return () => {
    cancelRestore?.();
    if (!positions.length) return;
    let frame = 0;
    const inputs = ['pointerdown', 'wheel', 'touchstart', 'keydown'] as const;
    const stop = () => {
      observer.disconnect(); cancelAnimationFrame(frame); clearTimeout(deadline);
      for (const input of inputs) document.removeEventListener(input, stop, true);
      if (cancelRestore === stop) cancelRestore = undefined;
    };
    const apply = () => {
      for (const position of positions) {
        const element = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
          .find(candidate => candidate.dataset.navigationScroll === position.id && candidate.getClientRects().length > 0);
        if (!element) continue;
        element.scrollTop = position.top; element.scrollLeft = position.left;
      }
    };
    // Later refetches can clamp a position that initially restored correctly.
    // Keep observing until the existing deadline, or until the user interacts.
    const observer = new MutationObserver(() => { cancelAnimationFrame(frame); frame = requestAnimationFrame(apply); });
    const deadline = setTimeout(stop, 5000);
    observer.observe(document.body, { childList: true, subtree: true, characterData: true });
    for (const input of inputs) document.addEventListener(input, stop, { capture: true, passive: true });
    cancelRestore = stop;
    frame = requestAnimationFrame(apply);
  };
}
export function cancelScrollRestore() { cancelRestore?.(); }
