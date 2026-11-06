/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { WEB_BASE } from "./src/ports";

// During `npm run dev`, Vite serves the SPA on port 5173 and proxies
// `/api/*` and `/healthz` to the default local Axum backend.
// For production, `npm run build`
// emits to `dist/`, which Axum serves directly via ServeDir.
const backend = `http://127.0.0.1:${WEB_BASE}`;
const proxy = { "/api": backend, "/healthz": backend, "/internal": backend };
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy,
  },
  preview: {
    proxy,
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    css: false,
  },
});
