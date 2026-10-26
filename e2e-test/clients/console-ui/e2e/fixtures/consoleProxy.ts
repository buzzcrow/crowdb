// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { createServer, request } from 'node:http';
import type { AddressInfo } from 'node:net';

/** Actual reverse proxy: every response comes from the owned console server. */
export async function consoleProxy(baseURL: string) {
  let dropSuffix: string | undefined;
  let dropped = 0;
  const server = createServer((incoming, outgoing) => {
    const path = (incoming.url ?? '/').replace(/^\/proxy\/api(?=\/|\?|$)/, '/api');
    const upstream = request(new URL(path, baseURL), { method: incoming.method, headers: incoming.headers }, response => {
      if (dropSuffix && path.endsWith(dropSuffix) && ['POST', 'PUT'].includes(incoming.method ?? '') && response.statusCode === 200) {
        dropSuffix = undefined;
        let prefix: Buffer | undefined;
        response.on('data', (chunk: Buffer) => { prefix ??= chunk.subarray(0, 1); });
        response.on('end', () => {
          dropped++;
          // Deliver real headers and a partial real body before disconnecting.
          // Closing an unused keep-alive socket lets Chromium transparently replay
          // the mutation, which would not exercise an unknown publication outcome.
          outgoing.writeHead(response.statusCode!, response.headers);
          outgoing.write(prefix ?? Buffer.alloc(0), () => outgoing.destroy());
        });
        return;
      }
      outgoing.writeHead(response.statusCode ?? 502, response.headers);
      response.pipe(outgoing);
    });
    upstream.on('error', error => { outgoing.writeHead(502); outgoing.end(String(error)); });
    incoming.pipe(upstream);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    dropNextSuccessfulResponse: (suffix: string) => { dropSuffix = suffix; },
    droppedResponses: () => dropped,
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: () => new Promise<void>((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve()); server.closeAllConnections();
    }),
  };
}
