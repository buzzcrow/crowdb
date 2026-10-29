// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';

export function useClusterLayout(active: boolean, selected: boolean) {
  const [viewport, setViewport] = useState(window.innerWidth);
  const [sidebar, setSidebar] = useState(240);
  const [properties, setProperties] = useState(320);
  useEffect(() => {
    const resize = () => setViewport(window.innerWidth);
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, []);
  const drawer = active && viewport < 1100;
  const overlayNavigation = false;
  const hidden = false;
  const sidebarMax = active ? Math.min(360, viewport - 480 - (selected && !drawer ? 280 : 0)) : 600;
  const sidebarWidth = active ? Math.max(200, Math.min(sidebar, sidebarMax)) : sidebar;
  const propertiesMax = active ? Math.min(480, viewport - 480 - (hidden ? 0 : sidebarWidth)) : 600;
  const propertiesWidth = drawer ? Math.min(properties, viewport) : Math.max(active ? 280 : 220, Math.min(properties, propertiesMax));
  return { drawer, hidden, overlayNavigation, sidebarWidth, propertiesWidth, sidebarMax,
    propertiesMax, setSidebar, setProperties };
}
