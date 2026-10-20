import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { openShortPairingChannel } from "../packages/phantom-doorways/src/short-pairing-channel.js";
const pake = createRequire(import.meta.url)("spake2-wasm");
function relay({ dropFirst = false } = {}) {
  const rooms = new Map();
  let dropped = 0;
  function roster(room) {
    const rows = [...room.values()].filter((x) => x.row).map((x) => x.row);
    for (const x of room.values()) x.config.onDiscovery({ peers: rows });
  }
  return {
    rooms,
    forIdentity(publicKey) {
      return {
        open(config) {
          let room = rooms.get(config.roomId);
          if (!room) rooms.set(config.roomId, (room = new Map()));
          const self = { config, row: null };
          room.set(config.peerId, self);
          queueMicrotask(() => config.onOpen());
          return {
            announce(row) {
              if (!self.row)
                self.row = { ...row, hKey: row.peerId, hex: publicKey };
              roster(room);
            },
            reconcile() {
              roster(room);
            },
            sendSignal(packet) {
              assert.equal(packet.from, publicKey.slice(0, 16));
              if (dropFirst && dropped < 2 && packet.kind === "response") {
                dropped++;
                return;
              }
              const target = room.get(packet.to);
              if (target)
                queueMicrotask(() => {
                  target.config.onSignal(structuredClone(packet));
                  target.config.onSignal(structuredClone(packet));
                });
            },
            close() {
              room.delete(config.peerId);
              roster(room);
            },
          };
        },
      };
    },
  };
}
// Distinct prefixes are required, just like actual SDK peer IDs.
const id = (n) => ({
  publicKey: n.toString(16).padStart(16, "0") + "0".repeat(48),
});
async function open(bus, n, opts) {
  const who = id(n);
  return openShortPairingChannel({
    code: "7KH9T2",
    identity: who,
    doorway: bus.forIdentity(who.publicKey),
    pake,
    ...opts,
  });
}
test("same bucket hosts do not authenticate by discovery order; correct PAKE wins", async () => {
  const bus = relay(),
    wrong = await open(bus, 1, {
      role: "host",
      code: "7K0000",
      invitation: "wrong",
    }),
    right = await open(bus, 2, { role: "host", invitation: "right" }),
    client = await open(bus, 3, {});
  try {
    assert.deepEqual(await client.result, {
      invitation: "right",
      hostPublicKey: id(2).publicKey,
    });
    assert.equal(client.attempts, 2);
    assert.equal(right.attempts, 1);
    assert.equal(wrong.attempts, 1);
    assert.equal(client.closed, true);
  } finally {
    client.close();
    right.close();
    wrong.close();
  }
});
test("dropped reply retries same RPC without spending another host guess", async () => {
  let consumed = 0;
  const bus = relay({ dropFirst: true }),
    host = await open(bus, 1, {
      role: "host",
      invitation: "ok",
      onConsumed: () => consumed++,
    }),
    client = await open(bus, 2, {});
  try {
    assert.equal((await client.result).invitation, "ok");
    assert.equal(host.attempts, 1);
    assert.equal(client.attempts, 1);
    assert.equal(consumed, 1);
  } finally {
    client.close();
    host.close();
  }
});
test("client limits hostile discovery candidates across repeated roster updates", async () => {
  const bus = relay(),
    hosts = [];
  let client;
  try {
    for (let n = 1; n <= 8; n++)
      hosts.push(
        await open(bus, n, {
          role: "host",
          code: "7K0000",
          invitation: "wrong",
        }),
      );
    client = await open(bus, 9, { timeoutMs: 80, maxAttempts: 3 });
    await assert.rejects(client.result, /timed out/);
    assert.equal(client.attempts, 3);
    assert.equal(
      hosts.reduce((sum, h) => sum + h.attempts, 0),
      3,
    );
  } finally {
    client?.close();
    for (const h of hosts) h.close();
  }
});
test("abort closes discovery and rejects pending result without late completion", async () => {
  const bus = relay(),
    controller = new AbortController(),
    client = await open(bus, 1, { signal: controller.signal });
  controller.abort();
  await assert.rejects(client.result, /cancelled/);
  assert.equal(client.closed, true);
  assert.equal([...bus.rooms.values()][0].size, 0);
  client.close();
});
test("host expiry and pre-aborted request leave no open session", async () => {
  const bus = relay(),
    host = await open(bus, 1, { role: "host", invitation: "ok", ttlMs: 10 });
  await assert.rejects(host.result, /expired/);
  assert.equal(host.closed, true);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    open(bus, 2, { signal: controller.signal }),
    /cancelled/,
  );
  assert.equal([...bus.rooms.values()][0].size, 0);
});
test("exchange runs through the Nostr doorway adapter with identity-bound routes", async () => {
  const { createNostrDoorway } = await import(
    "../packages/phantom-doorways/src/nostr-doorway.js"
  );
  const rooms = new Map();
  function busFactory(config) {
    let room = rooms.get(config.roomId);
    if (!room) rooms.set(config.roomId, (room = new Set()));
    const self = { config, row: null };
    room.add(self);
    const publish = (row) => {
      self.row = row;
      for (const target of room)
        if (target !== self)
          queueMicrotask(() =>
            target.config.onAnnouncement({
              row,
              s7pPublicKey: config.identity.publicKey,
            }),
          );
    };
    queueMicrotask(() => config.onStatus({ type: "open" }));
    return {
      announce({ row }) {
        publish(row);
        for (const target of room)
          if (target !== self && target.row)
            queueMicrotask(() =>
              config.onAnnouncement({
                row: target.row,
                s7pPublicKey: target.config.identity.publicKey,
              }),
            );
      },
      sendSignal(target, control) {
        const recipient = [...room].find(
          (x) => x.config.identity.publicKey === target.s7pPublicKey,
        );
        if (recipient)
          queueMicrotask(() =>
            recipient.config.onSignal({
              s7pPublicKey: config.identity.publicKey,
              control: structuredClone(control),
            }),
          );
      },
      depart() {
        room.delete(self);
      },
      close() {
        room.delete(self);
      },
    };
  }
  const make = async (n, options) => {
    const identity = id(n);
    return openShortPairingChannel({
      code: "7KH9T2",
      identity,
      pake,
      doorway: createNostrDoorway({ identity, relays: [], busFactory }),
      ...options,
    });
  };
  const host = await make(1, { role: "host", invitation: "nostr-path" }),
    client = await make(2, {});
  try {
    assert.equal((await client.result).invitation, "nostr-path");
    assert.equal(host.attempts, 1);
  } finally {
    client.close();
    host.close();
  }
});
