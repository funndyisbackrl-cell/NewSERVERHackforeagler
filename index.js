import { DurableObject } from "cloudflare:workers";

const BACKEND = "ws://us-iad01.altare.global:25436/";
const KEEP_ALIVE_MS = 12 * 60 * 1000;

function normalizeCloseCode(code) {
  if (
    Number.isInteger(code) &&
    code >= 1000 &&
    code <= 4999 &&
    ![1004, 1005, 1006, 1015].includes(code)
  ) {
    return code;
  }

  return 1011;
}

function waitForOpen(socket, timeout = 15000) {
  return new Promise((resolve, reject) => {
    if (socket.readyState === WebSocket.OPEN) {
      resolve();
      return;
    }

    let settled = false;

    const cleanup = () => {
      clearTimeout(timer);
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("close", onClose);
    };

    const finish = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve();
    };

    const onOpen = () => finish();
    const onError = () => finish(new Error("Backend WebSocket error"));
    const onClose = () => finish(new Error("Backend WebSocket closed before opening"));

    const timer = setTimeout(() => {
      finish(new Error("Backend WebSocket timed out"));
    }, timeout);

    socket.addEventListener("open", onOpen);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onClose);
  });
}

export default {
  async fetch(request, env) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("McDonaldsMP Durable Object gateway is online!", {
        status: 200,
        headers: { "Content-Type": "text/plain; charset=utf-8" }
      });
    }

    if (request.method !== "GET") {
      return new Response("WebSocket connections must use GET.", { status: 400 });
    }

    const id = env.MCDONALDSMP_RELAY.idFromName("mcdonaldsmp-shared-relay");
    const stub = env.MCDONALDSMP_RELAY.get(id);
    return stub.fetch(request);
  }
};

export class McDonaldsMPRelay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sessions = new Map();
    this.keepAliveTimer = null;
  }

  startKeepAliveChain() {
    if (this.keepAliveTimer !== null || this.sessions.size === 0) return;

    this.keepAliveTimer = setTimeout(() => {
      this.keepAliveTimer = null;

      if (this.sessions.size > 0) {
        console.log("DO_KEEPALIVE_TIMER_TICK", {
          activeConnections: this.sessions.size,
          time: new Date().toISOString()
        });
        this.startKeepAliveChain();
      }
    }, KEEP_ALIVE_MS);
  }

  stopKeepAliveChainIfIdle() {
    if (this.sessions.size !== 0 || this.keepAliveTimer === null) return;

    clearTimeout(this.keepAliveTimer);
    this.keepAliveTimer = null;
  }

  closeSession(session, source, receivedCode = 1000) {
    if (session.closed) return;

    session.closed = true;
    this.sessions.delete(session.id);

    const forwardedCode = normalizeCloseCode(receivedCode);

    console.log("DO_RELAY_SESSION_CLOSED", {
      sessionId: session.id,
      source,
      receivedCode,
      forwardedCode,
      activeConnections: this.sessions.size,
      time: new Date().toISOString()
    });

    try {
      if (session.client.readyState === WebSocket.OPEN || session.client.readyState === WebSocket.CONNECTING) {
        session.client.close(forwardedCode);
      }
    } catch (error) {
      console.error("DO_CLIENT_CLOSE_ERROR", { message: error.message, sessionId: session.id });
    }

    try {
      if (session.backend.readyState === WebSocket.OPEN || session.backend.readyState === WebSocket.CONNECTING) {
        session.backend.close(forwardedCode);
      }
    } catch (error) {
      console.error("DO_BACKEND_CLOSE_ERROR", { message: error.message, sessionId: session.id });
    }

    this.stopKeepAliveChainIfIdle();
  }

  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected a WebSocket upgrade.", { status: 426 });
    }

    const incoming = new URL(request.url);
    const backendURL = new URL(BACKEND);
    backendURL.pathname = incoming.pathname || "/";
    backendURL.search = incoming.search;

    const requestedProtocols = request.headers.get("Sec-WebSocket-Protocol");
    const protocols = requestedProtocols
      ? requestedProtocols.split(",").map(value => value.trim()).filter(Boolean)
      : [];

    let backend;

    try {
      backend = protocols.length
        ? new WebSocket(backendURL.toString(), protocols)
        : new WebSocket(backendURL.toString());
      backend.binaryType = "arraybuffer";
      await waitForOpen(backend);

      if (backend.readyState !== WebSocket.OPEN) {
        throw new Error("Backend socket was not open after connection");
      }
    } catch (error) {
      console.error("DO_BACKEND_CONNECTION_FAILED", {
        message: error.message,
        backend: BACKEND,
        time: new Date().toISOString()
      });

      try {
        if (backend && (backend.readyState === WebSocket.OPEN || backend.readyState === WebSocket.CONNECTING)) {
          backend.close(1011);
        }
      } catch {}

      return new Response("Backend connection failed: " + error.message, { status: 502 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.binaryType = "arraybuffer";
    server.accept();

    const session = {
      id: crypto.randomUUID(),
      client: server,
      backend,
      closed: false
    };

    this.sessions.set(session.id, session);
    this.startKeepAliveChain();

    console.log("DO_RELAY_CONNECTED", {
      sessionId: session.id,
      backend: BACKEND,
      activeConnections: this.sessions.size,
      time: new Date().toISOString()
    });

    server.addEventListener("message", event => {
      if (session.closed) return;

      try {
        if (backend.readyState === WebSocket.OPEN) {
          backend.send(event.data);
        } else {
          this.closeSession(session, "CLIENT_MESSAGE_WHILE_BACKEND_CLOSED", 1011);
        }
      } catch (error) {
        console.error("DO_BACKEND_SEND_ERROR", {
          sessionId: session.id,
          message: error.message,
          time: new Date().toISOString()
        });
        this.closeSession(session, "BACKEND_SEND_ERROR", 1011);
      }
    });

    backend.addEventListener("message", event => {
      if (session.closed) return;

      try {
        if (server.readyState === WebSocket.OPEN) {
          server.send(event.data);
        } else {
          this.closeSession(session, "BACKEND_MESSAGE_WHILE_CLIENT_CLOSED", 1011);
        }
      } catch (error) {
        console.error("DO_CLIENT_SEND_ERROR", {
          sessionId: session.id,
          message: error.message,
          time: new Date().toISOString()
        });
        this.closeSession(session, "CLIENT_SEND_ERROR", 1011);
      }
    });

    server.addEventListener("close", event => {
      console.log("DO_CLIENT_SOCKET_CLOSED", {
        sessionId: session.id,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
        time: new Date().toISOString()
      });
      this.closeSession(session, "CLIENT_SIDE", event.code);
    });

    backend.addEventListener("close", event => {
      console.log("DO_BACKEND_SOCKET_CLOSED", {
        sessionId: session.id,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
        time: new Date().toISOString()
      });
      this.closeSession(session, "ALTARE_BACKEND", event.code);
    });

    server.addEventListener("error", () => {
      console.error("DO_CLIENT_SOCKET_ERROR", {
        sessionId: session.id,
        time: new Date().toISOString()
      });
      this.closeSession(session, "CLIENT_ERROR", 1011);
    });

    backend.addEventListener("error", () => {
      console.error("DO_BACKEND_SOCKET_ERROR", {
        sessionId: session.id,
        time: new Date().toISOString()
      });
      this.closeSession(session, "BACKEND_ERROR", 1011);
    });

    const headers = new Headers();
    if (backend.protocol) {
      headers.set("Sec-WebSocket-Protocol", backend.protocol);
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers
    });
  }
}
