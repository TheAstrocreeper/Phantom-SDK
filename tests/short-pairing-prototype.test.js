import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  createShortCode,
  parseShortCode,
  shortCodeRoom,
  createShortPairingHost,
  createShortPairingClient,
} from "../packages/phantom-doorways/src/short-pairing-prototype.js";
const pake = createRequire(import.meta.url)("spake2-wasm");
const hostKey = "1".repeat(64),
  peerKey = "2".repeat(64),
  code = "7KH9T2",
  invitation = "P1-INTERNAL-LONG-INVITATION";
const host = (o) =>
  createShortPairingHost({
    code,
    hostPublicKey: hostKey,
    invitation,
    pake,
    ...o,
  });
const client = (o) =>
  createShortPairingClient({
    code,
    hostPublicKey: hostKey,
    peerPublicKey: peerKey,
    pake,
    ...o,
  });
test("six-character formatting and room locator never disclose a check for the secret", async () => {
  for (let i = 0; i < 20; i++)
    assert.match(createShortCode(), /^[0-9A-HJKMNP-TV-Z]{6}$/);
  assert.equal(parseShortCode("7k h9t2").code, code);
  assert.equal(await shortCodeRoom(code), await shortCodeRoom("7K0000"));
  assert.notEqual(await shortCodeRoom(code), await shortCodeRoom("8KH9T2"));
  assert.throws(() => parseShortCode("7K!9T2"));
});
test("real Rust/WASM SPAKE2 confirms both keys and delivers only the encrypted invitation", async () => {
  const h = host(),
    c = client();
  try {
    const challenge = h.begin(peerKey, c.hello),
      response = await c.answer(challenge),
      sealed = await h.finish(peerKey, c.hello, response);
    assert.equal(await c.open(sealed), invitation);
    assert.equal(h.used, true);
    assert.equal(
      JSON.stringify({ challenge, response, sealed }).includes(invitation),
      false,
    );
    assert.equal(
      JSON.stringify({ challenge, response, sealed }).includes("H9T2"),
      false,
    );
    assert.throws(() => h.begin(peerKey, c.hello), /expired or used/);
  } finally {
    h.close();
    c.close();
  }
});
test("wrong secret cannot retrieve invitation and shared attempt budget applies across identities", async () => {
  const h = host({ maxAttempts: 2 });
  try {
    for (let i = 0; i < 2; i++) {
      const key = String(i + 3).repeat(64),
        c = client({ code: "7K0000", peerPublicKey: key });
      try {
        await assert.rejects(
          h.finish(key, c.hello, await c.answer(h.begin(key, c.hello))),
          /did not match/,
        );
      } finally {
        c.close();
      }
    }
    assert.equal(h.used, false);
    assert.equal(h.attempts, 2);
    assert.throws(() => h.begin(peerKey, "a".repeat(32)), /attempt limit/);
  } finally {
    h.close();
  }
});
test("expiry and close prevent subsequent pairing", async () => {
  let clock = 0;
  const h = host({ now: () => clock, ttlMs: 100 });
  const c = client();
  const response = await c.answer(h.begin(peerKey, c.hello));
  clock = 100;
  await assert.rejects(h.finish(peerKey, c.hello, response), /expired/);
  h.close();
  c.close();
  assert.throws(() => h.begin(peerKey, "a".repeat(32)), /expired/);
});
test("identity substitution and recorded response replay cannot confirm a different handshake", async () => {
  const h = host(),
    c = client();
  try {
    const response = await c.answer(h.begin(peerKey, c.hello));
    await assert.rejects(
      h.finish("3".repeat(64), c.hello, response),
      /Unknown/,
    );
    const other = client();
    try {
      h.begin(peerKey, other.hello);
      await assert.rejects(
        h.finish(peerKey, other.hello, response),
        /did not match/,
      );
    } finally {
      other.close();
    }
    const sealed = await h.finish(peerKey, c.hello, response);
    sealed.ciphertext =
      sealed.ciphertext.slice(0, -2) +
      (sealed.ciphertext.endsWith("00") ? "01" : "00");
    await assert.rejects(c.open(sealed));
  } finally {
    h.close();
    c.close();
  }
});
test("duplicate hello and concurrent finalization count once and return one result", async () => {
  const h = host(),
    c = client();
  try {
    const a = h.begin(peerKey, c.hello);
    assert.deepEqual(h.begin(peerKey, c.hello), a);
    const response = await c.answer(a);
    const [x, y] = await Promise.all([
      h.finish(peerKey, c.hello, response),
      h.finish(peerKey, c.hello, response),
    ]);
    assert.deepEqual(x, y);
    assert.equal(h.attempts, 1);
    assert.equal(await c.open(x), invitation);
  } finally {
    h.close();
    c.close();
  }
});
test("browser WASM glue runs using Web Crypto and exchanges the invitation", async () => {
  const { readFile } = await import("node:fs/promises");
  const { pathToFileURL } = await import("node:url");
  const entry = pathToFileURL(
    createRequire(import.meta.url).resolve("spake2-wasm"),
  );
  const glue = await readFile(new URL("../mjs/index_bg.js", entry), "utf8"),
    wasmBytes = await readFile(new URL("../mjs/index_bg.wasm", entry));
  const adapted = glue.replace(
    "import * as wasm from './index_bg.wasm';",
    "let wasm; export function initialize(exports){wasm=exports;}",
  );
  const originalSelf = globalThis.self;
  globalThis.self = globalThis;
  try {
    const browserPake = await import(
      "data:text/javascript;base64," + Buffer.from(adapted).toString("base64")
    );
    const { instance } = await WebAssembly.instantiate(wasmBytes, {
      "./index_bg.js": browserPake,
    });
    browserPake.initialize(instance.exports);
    const h = host({ pake: browserPake }),
      c = client({ pake: browserPake });
    try {
      const response = await c.answer(h.begin(peerKey, c.hello));
      assert.equal(
        await c.open(await h.finish(peerKey, c.hello, response)),
        invitation,
      );
    } finally {
      h.close();
      c.close();
    }
  } finally {
    if (originalSelf === undefined) delete globalThis.self;
    else globalThis.self = originalSelf;
  }
});
test("only one of two correct simultaneous peers can consume the invitation", async () => {
  const h = host(),
    a = client(),
    key = "3".repeat(64),
    b = client({ peerPublicKey: key });
  try {
    const ar = await a.answer(h.begin(peerKey, a.hello)),
      br = await b.answer(h.begin(key, b.hello));
    const outcomes = await Promise.allSettled([
      h.finish(peerKey, a.hello, ar),
      h.finish(key, b.hello, br),
    ]);
    assert.equal(outcomes.filter((o) => o.status === "fulfilled").length, 1);
    assert.equal(h.used, true);
  } finally {
    h.close();
    a.close();
    b.close();
  }
});

test("manual entry normalizes case and ambiguous glyphs without accepting longer codes", () => {
  assert.equal(parseShortCode("ol-i2ab").code, "0112AB");
  for (const value of ["12345", "1234567", "ABCDU2", "4827-193604", ""])
    assert.throws(() => parseShortCode(value), /six-character/);
});
test("routing bucket is independent of all four secret characters", async () => {
  const room = await shortCodeRoom("7K0000");
  for (const code of ["7KZ000", "7K0Z00", "7K00Z0", "7K000Z"])
    assert.equal(await shortCodeRoom(code), room);
});
