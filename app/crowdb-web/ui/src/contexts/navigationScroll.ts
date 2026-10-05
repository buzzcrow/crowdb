// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

let cancelRestore: (() => void) | undefined;
export function captureScroll(): () => void {
  const positions = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
    .filter(element => element.getClientRects().length > 0).slice(0, 8)
    .map(element => ({ id: element.dataset.navigationScroll!, top: element.scrollTop, left: element.scrollLeft }));
  return () => {
    cancelRestore?.();
    const pending = new Map(positions.map(position => [position.id, position]));
    let frame = 0;
    const stop = () => { observer.disconnect(); cancelAnimationFrame(frame); clearTimeout(deadline); if (cancelRestore === stop) cancelRestore = undefined; };
    const apply = () => {
      for (const [id, position] of pending) {
        const element = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
          .find(candidate => candidate.dataset.navigationScroll === id && candidate.getClientRects().length > 0);
        if (!element) continue;
        element.scrollTop = position.top; element.scrollLeft = position.left;
        if (Math.abs(element.scrollTop - position.top) < 1 && Math.abs(element.scrollLeft - position.left) < 1) pending.delete(id);
      }
      if (!pending.size) stop();
    };
    // Refetched pages may not yet have their final scroll height at first paint.
    const observer = new MutationObserver(() => { cancelAnimationFrame(frame); frame = requestAnimationFrame(apply); });
    const deadline = setTimeout(stop, 5000);
    observer.observe(document.body, { childList: true, subtree: true });
    cancelRestore = stop;
    frame = requestAnimationFrame(apply);
  };
}
export function cancelScrollRestore() { cancelRestore?.(); }
