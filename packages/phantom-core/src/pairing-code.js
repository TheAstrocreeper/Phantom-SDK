const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const utf8 = (value) => new TextEncoder().encode(value);
const hex = (bytes) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
function base32(bytes) {
  let value = 0,
    bits = 0,
    result = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      result += alphabet[(value >>> bits) & 31];
    }
  }
  if (bits) result += alphabet[(value << (5 - bits)) & 31];
  return result;
}
async function checksum(body, crypto) {
  const d = new Uint8Array(
    await crypto.subtle.digest("SHA-256", utf8("phantom-code-v1:" + body)),
  );
  return alphabet[d[0] >>> 3] + alphabet[((d[0] & 7) << 2) | (d[1] >>> 6)];
}
export async function createPairingCode({ crypto = globalThis.crypto } = {}) {
  const body = base32(crypto.getRandomValues(new Uint8Array(16)));
  const encoded = body + (await checksum(body, crypto));
  return "P1-" + encoded.match(/.{4}/g).join("-");
}
export async function parsePairingCode(
  input,
  { crypto = globalThis.crypto } = {},
) {
  if (typeof input !== "string" || input.length > 128)
    throw new TypeError("Enter a Phantom pairing code");
  const code = input
    .toUpperCase()
    .replace(/[\s-]/g, "")
    .replaceAll("O", "0")
    .replace(/[IL]/g, "1");
  if (!code.startsWith("P1") || code.length !== 30)
    throw new TypeError(
      "Pairing code must contain P1 and 28 letters or numbers",
    );
  const body = code.slice(2, 28),
    check = code.slice(28);
  if (
    [...(body + check)].some((c) => !alphabet.includes(c)) ||
    (await checksum(body, crypto)) !== check
  )
    throw new TypeError("Pairing code has a typing error");
  let value = 0,
    bits = 0;
  const bytes = [];
  for (const c of body) {
    value = (value << 5) | alphabet.indexOf(c);
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((value >>> bits) & 255);
    }
  }
  const seed = new Uint8Array(bytes);
  if (seed.length !== 16 || base32(seed) !== body)
    throw new TypeError("Noncanonical pairing code");
  return { code: "P1-" + (body + check).match(/.{4}/g).join("-"), seed };
}
/** These coordinates open an encrypted bootstrap exchange; they are not the sharing room ID. */
export async function pairingCoordinates(
  code,
  { crypto = globalThis.crypto } = {},
) {
  const { seed } = await parsePairingCode(code, { crypto });
  const derive = async (label) =>
    new Uint8Array(
      await crypto.subtle.digest(
        "SHA-256",
        new Uint8Array([...utf8(label), ...seed]),
      ),
    );
  const roomId = hex(await derive("phantom-pairing-room-v1:"));
  const key = await crypto.subtle.importKey(
    "raw",
    await derive("phantom-pairing-key-v1:"),
    "AES-GCM",
    false,
    ["encrypt", "decrypt"],
  );
  return { roomId, key };
}
export async function sealPairingMessage(
  { roomId, key },
  message,
  { crypto = globalThis.crypto } = {},
) {
  const plaintext = utf8(JSON.stringify(message));
  if (plaintext.length > 30000)
    throw new TypeError("Pairing message too large");
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: "AES-GCM", iv, additionalData: utf8(roomId) },
      key,
      plaintext,
    ),
  );
  return { iv: hex(iv), ciphertext: hex(ciphertext) };
}
export async function openPairingMessage(
  { roomId, key },
  sealed,
  { crypto = globalThis.crypto } = {},
) {
  if (
    !/^[0-9a-f]{24}$/.test(sealed?.iv) ||
    typeof sealed.ciphertext !== "string" ||
    sealed.ciphertext.length > 60032 ||
    !/^(?:[0-9a-f]{2}){16,}$/.test(sealed.ciphertext)
  )
    throw new TypeError("Invalid encrypted pairing message");
  const bytes = (hex) =>
    Uint8Array.from(hex.match(/../g), (pair) => parseInt(pair, 16));
  const plaintext = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: bytes(sealed.iv), additionalData: utf8(roomId) },
    key,
    bytes(sealed.ciphertext),
  );
  return JSON.parse(new TextDecoder().decode(plaintext));
}
