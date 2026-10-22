import {
  pairingCoordinates,
  sealPairingMessage,
  openPairingMessage,
} from "../../phantom-core/src/pairing-code.js";

/** Encrypted request/reply over a normal doorway, before main-network admission. */
export async function openPairingChannel({
  code,
  identity,
  doorway,
  role = "client",
  onRequest,
  onEvent = () => {},
  onError = () => {},
  now = Date.now,
  timeoutMs = 15000,
} = {}) {
  if (
    !["host", "client"].includes(role) ||
    !identity?.publicKey ||
    typeof doorway?.open !== "function"
  )
    throw new TypeError("Pairing identity and doorway required");
  const coordinates = await pairingCoordinates(code),
    peerId = identity.publicKey.slice(0, 16),
    bootId = crypto.randomUUID();
  const pending = new Map(),
    peers = new Map(),
    seen = new Map();
  let session,
    closed = false,
    remote = null,
    queued = 0;
  const handle = "phantom-pairing-" + role;
  async function send(to, message) {
    if (closed) throw new Error("Pairing channel closed");
    const sealed = await sealPairingMessage(coordinates, {
      ...message,
      peerPublicKey: identity.publicKey,
      issuedAt: now(),
    });
    if (closed) return;
    session.sendSignal({
      type: "phantom_pairing",
      videoId: coordinates.roomId,
      from: peerId,
      to,
      bootId,
      sealed,
    });
  }
  async function receive(packet) {
    if (
      closed ||
      packet?.type !== "phantom_pairing" ||
      packet.videoId !== coordinates.roomId ||
      packet.to !== peerId ||
      !peers.has(packet.from)
    )
      return;
    const message = await openPairingMessage(coordinates, packet.sealed);
    if (
      closed ||
      message.peerPublicKey !== peers.get(packet.from)?.hex ||
      message.peerPublicKey?.slice(0, 16) !== packet.from ||
      !Number.isSafeInteger(message.issuedAt) ||
      Math.abs(now() - message.issuedAt) > 60000 ||
      typeof message.id !== "string" ||
      message.id.length > 128
    )
      return;
    if (message.kind === "response") {
      const task = pending.get(message.id);
      if (
        !task ||
        !task.targets.has(packet.from) ||
        (remote && remote !== packet.from)
      )
        return;
      remote ??= packet.from;
      pending.delete(message.id);
      clearTimeout(task.timer);
      message.ok
        ? task.resolve(message.data)
        : task.reject(Object.assign(new Error(message.error || "Pairing request denied"),{code:'MEMBERSHIP_REJECTED'}));
      return;
    }
    if (message.kind === "event") {
      if (role === "client" && packet.from === remote) onEvent(message.data);
      return;
    }
    if (
      message.kind !== "request" ||
      role !== "host" ||
      typeof onRequest !== "function"
    )
      return;
    const cacheKey = message.peerPublicKey + ":" + message.id;
    let response = seen.get(cacheKey);
    if (!response) {
      if (seen.size >= 256) seen.delete(seen.keys().next().value);
      response = (async () => {
        try {
          return {
            kind: "response",
            id: message.id,
            ok: true,
            data: await onRequest({
              action: message.action,
              data: structuredClone(message.data),
              peerPublicKey: message.peerPublicKey,
            }),
          };
        } catch (error) {
          return {
            kind: "response",
            id: message.id,
            ok: false,
            error: error.message,
          };
        }
      })();
      seen.set(cacheKey, response);
    }
    await send(packet.from, await response);
  }
  function flush() {
    if (closed || role !== "client") return;
    const targets = remote
      ? [remote]
      : [...peers.values()]
          .filter((p) => p.handle === "phantom-pairing-host")
          .map((p) => p.hKey);
    for (const task of pending.values())
      for (const to of targets) {
        task.targets.add(to);
        if (peers.has(to)) void send(to, task.message).catch(onError);
      }
  }
  session = doorway.open({
    roomId: coordinates.roomId,
    peerId,
    onOpen() {
      queueMicrotask(() => {
        if (!closed)
          session?.announce({
            roomId: coordinates.roomId,
            peerId,
            handle,
            bootId,
            t: now(),
          });
      });
    },
    onDiscovery(hint) {
      peers.clear();
      for (const row of (hint.peers ?? []).slice(0, 256))
        if (row.hKey !== peerId && /^[0-9a-f]{64}$/.test(row.hex))
          peers.set(row.hKey, row);
      flush();
    },
    onSignal(packet) {
      if (queued >= 32) return;
      queued++;
      void receive(packet)
        .catch(onError)
        .finally(() => queued--);
    },
    onClose() {
      onError(new Error("Pairing signaling disconnected"));
    },
    onError,
  });
  // Some adapters synchronously report open before returning their session.
  queueMicrotask(() => {
    if (!closed)
      session.announce({
        roomId: coordinates.roomId,
        peerId,
        handle,
        bootId,
        t: now(),
      });
  });
  const retry = setInterval(() => {
    if (!closed) {
      session.reconcile?.({});
      flush();
    }
  }, 1000);
  retry.unref?.();
  return {
    request(action, data, { signal } = {}) {
      if (closed || signal?.aborted)
        return Promise.reject(new Error("Pairing request cancelled"));
      if (pending.size >= 16)
        return Promise.reject(new Error("Too many pairing requests"));
      const id = crypto.randomUUID(),
        message = { kind: "request", id, action, data };
      return new Promise((resolve, reject) => {
        let timer;
        const finish = (fn) => (value) => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", abort);
          fn(value);
        };
        const abort = () => {
          pending.delete(id);
          finish(reject)(new Error("Pairing request cancelled"));
        };
        timer = setTimeout(() => {
          pending.delete(id);
          finish(reject)(
            new Error(
              "Pairing timed out. Check the code and keep the inviting device open.",
            ),
          );
        }, timeoutMs);
        pending.set(id, {
          message,
          targets: new Set(),
          timer,
          resolve: finish(resolve),
          reject: finish(reject),
        });
        signal?.addEventListener("abort", abort, { once: true });
        flush();
      });
    },
    async notify(peerPublicKey, data) {
      const to = peerPublicKey.slice(0, 16);
      if (peers.get(to)?.hex === peerPublicKey)
        await send(to, { kind: "event", id: crypto.randomUUID(), data });
    },
    close() {
      if (closed) return;
      closed = true;
      clearInterval(retry);
      for (const task of pending.values())
        task.reject(new Error("Pairing channel closed"));
      pending.clear();
      seen.clear();
      session.close();
    },
  };
}
