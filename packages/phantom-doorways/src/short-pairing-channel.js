import {
  shortCodeRoom,
  createShortPairingHost,
  createShortPairingClient,
} from "./short-pairing-prototype.js";

/** Experimental adapter. Requires an identity-authenticating doorway (e.g. Nostr).
 * Never use unverified discovery rows or an unauthenticated signal transport.
 */
export async function openShortPairingChannel({
  code,
  identity,
  doorway,
  pake,
  role = "client",
  invitation,
  timeoutMs = 15000,
  ttlMs = 240000,
  maxAttempts = 5,
  signal,
  onError = () => {},
  onConsumed = () => {},
} = {}) {
  if (
    !["host", "client"].includes(role) ||
    !/^[a-f0-9]{64}$/.test(identity?.publicKey) ||
    typeof doorway?.open !== "function" ||
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs < 1 ||
    !Number.isSafeInteger(maxAttempts) ||
    maxAttempts < 1 ||
    maxAttempts > 5
  )
    throw Error("Invalid short pairing channel");
  if (signal?.aborted) throw Error("Pairing cancelled");
  const roomId = await shortCodeRoom(code),
    peerId = identity.publicKey.slice(0, 16),
    bootId = crypto.randomUUID();
  const host =
    role === "host"
      ? createShortPairingHost({
          code,
          hostPublicKey: identity.publicKey,
          invitation,
          pake,
          ttlMs,
          maxAttempts,
        })
      : null;
  const peers = new Map(),
    tasks = new Map(),
    candidates = new Map(),
    responses = new Map();
  let consumed = false,
    closed = false,
    session,
    queue = 0,
    attempts = 0,
    timer,
    retry,
    resolveResult,
    rejectResult;
  const result = new Promise((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });
  // Hosts have no result consumer; client callers may attach after open returns.
  result.catch(() => {});
  function close(error = new Error("Pairing channel closed")) {
    if (closed) return;
    closed = true;
    clearTimeout(timer);
    clearInterval(retry);
    signal?.removeEventListener("abort", abort);
    for (const task of tasks.values()) {
      clearTimeout(task.timer);
      task.reject(error);
    }
    tasks.clear();
    for (const client of candidates.values()) client.close();
    candidates.clear();
    host?.close();
    peers.clear();
    responses.clear();
    session?.close();
    rejectResult(error);
  }
  const abort = () => close(new Error("Pairing cancelled"));
  function send(to, message) {
    if (closed) throw Error("Pairing channel closed");
    session.sendSignal({
      type: "phantom_short_pairing_v3",
      videoId: roomId,
      from: peerId,
      to,
      bootId,
      ...message,
    });
  }
  function request(to, action, data) {
    return new Promise((resolve, reject) => {
      if (closed) return reject(Error("Pairing channel closed"));
      const id = crypto.randomUUID(),
        message = { id, action, data, kind: "request" };
      const task = {
        to,
        message,
        resolve,
        reject,
        timer: setTimeout(() => {
          tasks.delete(id);
          reject(Error("Pairing candidate timed out"));
        }, timeoutMs),
      };
      tasks.set(id, task);
      try {
        send(to, message);
      } catch (error) {
        clearTimeout(task.timer);
        tasks.delete(id);
        reject(error);
      }
    });
  }
  async function tryCandidate(row) {
    if (closed || attempts >= maxAttempts || candidates.has(row.hex)) return;
    attempts++;
    const client = createShortPairingClient({
      code,
      hostPublicKey: row.hex,
      peerPublicKey: identity.publicKey,
      pake,
    });
    candidates.set(row.hex, client);
    try {
      const challenge = await request(row.hKey, "hello", {
        hello: client.hello,
      });
      const response = await client.answer(challenge);
      const sealed = await request(row.hKey, "finish", {
        hello: client.hello,
        response,
      });
      const value = await client.open(sealed);
      if (!closed) {
        resolveResult({ invitation: value, hostPublicKey: row.hex });
        close();
      }
    } catch {
      /* A wrong-code candidate is expected in a shared bucket. The overall deadline reports failure. */
    } finally {
      client.close();
    }
  }
  function discover(hint) {
    if (closed) return;
    peers.clear();
    for (const row of (hint.peers ?? []).slice(0, 256)) {
      if (
        row.hKey !== peerId &&
        /^[a-f0-9]{64}$/.test(row.hex) &&
        row.hKey === row.hex.slice(0, 16)
      )
        peers.set(row.hKey, row);
    }
    if (role === "client")
      for (const row of peers.values())
        if (row.handle === "phantom-short-pairing-host-v3")
          queueMicrotask(() => {
            void tryCandidate(row);
          });
  }
  async function receive(packet) {
    if (
      closed ||
      packet?.type !== "phantom_short_pairing_v3" ||
      packet.videoId !== roomId ||
      packet.to !== peerId ||
      !peers.has(packet.from) ||
      typeof packet.id !== "string" ||
      packet.id.length > 64 ||
      JSON.stringify(packet).length > 20000
    )
      return;
    const row = peers.get(packet.from);
    if (packet.bootId !== row.bootId) return;
    if (packet.kind === "response") {
      const task = tasks.get(packet.id);
      if (!task || task.to !== packet.from) return;
      tasks.delete(packet.id);
      clearTimeout(task.timer);
      packet.ok
        ? task.resolve(packet.data)
        : task.reject(Error("Pairing candidate rejected"));
      return;
    }
    if (
      !host ||
      packet.kind !== "request" ||
      !["hello", "finish"].includes(packet.action)
    )
      return;
    const cacheKey = row.hex + ":" + packet.id;
    let response = responses.get(cacheKey);
    if (!response) {
      // Never evict cached responses into renewed work; stop accepting new RPCs.
      if (responses.size >= maxAttempts * 4) return;
      response = (async () => {
        try {
          const data =
            packet.action === "hello"
              ? host.begin(row.hex, packet.data?.hello)
              : await host.finish(
                  row.hex,
                  packet.data?.hello,
                  packet.data?.response,
                );
          if (packet.action === "finish" && !consumed) {
            consumed = true;
            try {
              onConsumed();
            } catch (error) {
              onError(error);
            }
          }
          return { kind: "response", id: packet.id, ok: true, data };
        } catch {
          return { kind: "response", id: packet.id, ok: false };
        }
      })();
      responses.set(cacheKey, response);
    }
    const answer = await response;
    if (!closed) send(packet.from, answer);
  }
  function announce() {
    if (!closed && session)
      session.announce({
        roomId,
        peerId,
        handle: "phantom-short-pairing-" + role + "-v3",
        bootId,
        t: Date.now(),
      });
  }
  try {
    session = doorway.open({
      roomId,
      peerId,
      onOpen() {
        queueMicrotask(announce);
      },
      onDiscovery: discover,
      onSignal(packet) {
        if (closed || queue >= 16) return;
        queue++;
        void receive(packet)
          .catch((error) => {
            if (!closed) onError(error);
          })
          .finally(() => queue--);
      },
      onClose() {
        close(Error("Pairing signaling disconnected"));
      },
      onError,
    });
  } catch (error) {
    close(error);
    throw error;
  }
  if (closed) {
    session.close();
    return {
      result,
      close,
      get attempts() {
        return 0;
      },
      get closed() {
        return true;
      },
    };
  }
  queueMicrotask(announce);
  timer = setTimeout(
    () => close(Error("Pairing expired or timed out")),
    role === "host" ? ttlMs : timeoutMs,
  );
  retry = setInterval(() => {
    if (closed) return;
    try {
      session.reconcile?.({});
      for (const task of tasks.values()) send(task.to, task.message);
    } catch (error) {
      onError(error);
    }
  }, 1000);
  retry.unref?.();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  return {
    result,
    close,
    get attempts() {
      return role === "host" ? host.attempts : attempts;
    },
    get closed() {
      return closed;
    },
  };
}
