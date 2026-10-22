/** Experimental PAKE bootstrap. Not connected to production pairing.
 * `pake` is an injected SPAKE2 implementation, not a home-grown primitive.
 */
const enc = new TextEncoder(),
  dec = new TextDecoder();
const hex = (b) =>
  Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const bytes = (s) =>
  Uint8Array.from(s.match(/../g) ?? [], (x) => parseInt(x, 16));
const validKey = (s) => typeof s === "string" && /^[a-f0-9]{64}$/.test(s);
const validNonce = (s) => typeof s === "string" && /^[a-f0-9]{32}$/.test(s);
const nonce = () => hex(crypto.getRandomValues(new Uint8Array(16)));
function randomBelow(n) {
  const limit = Math.floor(2 ** 32 / n) * n;
  let v;
  do {
    v = crypto.getRandomValues(new Uint32Array(1))[0];
  } while (v >= limit);
  return v % n;
}
// Two public routing characters, four secret characters: 20 secret bits.
const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
export function createShortCode() {
  return Array.from({ length: 6 }, () => alphabet[randomBelow(32)]).join("");
}
export function parseShortCode(input) {
  if (typeof input !== "string" || input.length > 32)
    throw Error("Enter the six-character pairing code");
  const code = input
    .replace(/[\s-]/g, "")
    .toUpperCase()
    .replace(/O/g, "0")
    .replace(/[IL]/g, "1");
  if (code.length !== 6 || Array.from(code).some((c) => !alphabet.includes(c)))
    throw Error("Enter the six-character pairing code");
  return { locator: code.slice(0, 2), secret: code.slice(2), code };
}
export async function shortCodeRoom(code) {
  const { locator } = parseShortCode(code);
  return hex(
    new Uint8Array(
      await crypto.subtle.digest(
        "SHA-256",
        enc.encode("phantom-short-pairing-v3:" + locator),
      ),
    ),
  );
}
const context = (locator, host, peer, hello, challenge) =>
  JSON.stringify([
    "phantom-short-pairing-v3",
    locator,
    host,
    peer,
    hello,
    challenge,
  ]);
async function derive(shared, ctx) {
  try {
    const root = await crypto.subtle.importKey("raw", shared, "HKDF", false, [
      "deriveKey",
    ]);
    const salt = await crypto.subtle.digest("SHA-256", enc.encode(ctx));
    return {
      confirm: await crypto.subtle.deriveKey(
        {
          name: "HKDF",
          hash: "SHA-256",
          salt,
          info: enc.encode("client-confirm"),
        },
        root,
        { name: "HMAC", hash: "SHA-256", length: 256 },
        false,
        ["sign", "verify"],
      ),
      seal: await crypto.subtle.deriveKey(
        {
          name: "HKDF",
          hash: "SHA-256",
          salt,
          info: enc.encode("host-invitation"),
        },
        root,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"],
      ),
    };
  } finally {
    shared.fill(0);
  }
}
function dispose(state) {
  if (state?.ptr) state.free();
}
export function createShortPairingHost({
  code,
  hostPublicKey,
  invitation,
  pake,
  now = Date.now,
  ttlMs = 240000,
  maxAttempts = 5,
} = {}) {
  if (
    !validKey(hostPublicKey) ||
    typeof invitation !== "string" ||
    invitation.length > 8192 ||
    !Number.isSafeInteger(ttlMs) ||
    ttlMs <= 0 ||
    !Number.isSafeInteger(maxAttempts) ||
    maxAttempts < 1 ||
    maxAttempts > 10
  )
    throw Error("Invalid short pairing configuration");
  const { locator, secret } = parseShortCode(code),
    expiresAt = now() + ttlMs,
    rows = new Map();
  let attempts = 0,
    used = false,
    closed = false;
  const active = () => {
    if (closed || used || now() >= expiresAt)
      throw Error("Pairing invitation expired or used");
  };
  function begin(peer, hello) {
    active();
    if (!validKey(peer) || !validNonce(hello))
      throw Error("Invalid pairing request");
    const id = peer + ":" + hello;
    if (rows.has(id)) return rows.get(id).challenge;
    if (attempts >= maxAttempts) throw Error("Pairing attempt limit reached");
    attempts++;
    const n = nonce(),
      ctx = context(locator, hostPublicKey, peer, hello, n),
      state = pake.start(ctx, secret),
      challenge = { nonce: n, message: hex(pake.msg(state)) };
    rows.set(id, { state, ctx, challenge });
    return challenge;
  }
  async function finish(peer, hello, packet) {
    const row = rows.get(peer + ":" + hello);
    if (!row) throw Error("Unknown pairing request");
    if (row.pending) return row.pending;
    active();
    if (
      !/^[a-f0-9]{66}$/.test(packet?.message) ||
      !/^[a-f0-9]{64}$/.test(packet?.confirmation)
    )
      throw Error("Invalid pairing response");
    row.pending = (async () => {
      try {
        const state = row.state;
        row.state = null;
        const keys = await derive(
          pake.finish(state, bytes(packet.message)),
          row.ctx,
        );
        if (
          !(await crypto.subtle.verify(
            "HMAC",
            keys.confirm,
            bytes(packet.confirmation),
            enc.encode(row.ctx),
          ))
        )
          throw Error("Pairing code did not match");
        active();
        used = true;
        const iv = crypto.getRandomValues(new Uint8Array(12));
        const ciphertext = await crypto.subtle.encrypt(
          { name: "AES-GCM", iv, additionalData: enc.encode(row.ctx) },
          keys.seal,
          enc.encode(
            JSON.stringify({ invitation, peerPublicKey: peer, hostPublicKey }),
          ),
        );
        if (closed || now() >= expiresAt)
          throw Error("Pairing invitation expired or used");
        return { iv: hex(iv), ciphertext: hex(new Uint8Array(ciphertext)) };
      } finally {
        dispose(row.state);
        row.state = null;
      }
    })();
    return row.pending;
  }
  return {
    begin,
    finish,
    get attempts() {
      return attempts;
    },
    get used() {
      return used;
    },
    expiresAt,
    close() {
      closed = true;
      for (const row of rows.values()) dispose(row.state);
      rows.clear();
    },
  };
}
export function createShortPairingClient({
  code,
  hostPublicKey,
  peerPublicKey,
  pake,
} = {}) {
  if (!validKey(hostPublicKey) || !validKey(peerPublicKey))
    throw Error("Invalid pairing identity");
  const { locator, secret } = parseShortCode(code),
    hello = nonce();
  let ctx,
    keys,
    started = false,
    closed = false;
  return {
    hello,
    async answer(challenge) {
      if (closed || started) throw Error("Pairing client already used");
      if (
        !validNonce(challenge?.nonce) ||
        !/^[a-f0-9]{66}$/.test(challenge?.message)
      )
        throw Error("Invalid pairing challenge");
      started = true;
      ctx = context(
        locator,
        hostPublicKey,
        peerPublicKey,
        hello,
        challenge.nonce,
      );
      let state = pake.start(ctx, secret);
      try {
        const message = hex(pake.msg(state));
        const shared = pake.finish(state, bytes(challenge.message));
        state = null;
        keys = await derive(shared, ctx);
        if (closed) throw Error("Pairing client closed");
        return {
          message,
          confirmation: hex(
            new Uint8Array(
              await crypto.subtle.sign("HMAC", keys.confirm, enc.encode(ctx)),
            ),
          ),
        };
      } finally {
        dispose(state);
      }
    },
    async open(packet) {
      if (
        closed ||
        !keys ||
        !/^[a-f0-9]{24}$/.test(packet?.iv) ||
        !/^([a-f0-9]{2}){16,8400}$/.test(packet?.ciphertext)
      )
        throw Error("Invalid pairing result");
      const text = await crypto.subtle.decrypt(
        {
          name: "AES-GCM",
          iv: bytes(packet.iv),
          additionalData: enc.encode(ctx),
        },
        keys.seal,
        bytes(packet.ciphertext),
      );
      if (closed) throw Error("Pairing client closed");
      const result = JSON.parse(dec.decode(text));
      if (
        result.peerPublicKey !== peerPublicKey ||
        result.hostPublicKey !== hostPublicKey ||
        typeof result.invitation !== "string"
      )
        throw Error("Pairing identity mismatch");
      return result.invitation;
    },
    close() {
      closed = true;
      keys = null;
    },
  };
}
