/**
 * Offline bootstrap format. Routing is public; the password authenticates SPAKE2.
 * 16 base32 symbols = IPv4 (32) + port (16) + PAKE password (25) + CRC (7).
 * The password MUST be used by a PAKE with key confirmation, never as a bearer
 * credential or directly as an encryption key. This codec does not pair devices.
 */
const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const PASSWORD_LIMIT = 1n << 25n;
const invalid = () => new Error('Enter the complete 16-character LAN pairing code.');
function normalize(value, length) {
  if (typeof value !== 'string' || value.length > 80) throw invalid();
  const text = value.replace(/[\s-]/g, '').toUpperCase().replace(/O/g, '0').replace(/[IL]/g, '1');
  if (text.length !== length || [...text].some(char => !ALPHABET.includes(char))) throw invalid();
  return text;
}
function encode(value, length) {
  let result = '';
  for (let i = 0; i < length; i++) {result = ALPHABET[Number(value & 31n)] + result; value >>= 5n;}
  return result;
}
function decode(value) {
  let result = 0n;
  for (const char of value) result = (result << 5n) | BigInt(ALPHABET.indexOf(char));
  return result;
}
function privateAddress(host) {
  if (typeof host !== 'string' || !/^(?:0|[1-9]\d{0,2})(?:\.(?:0|[1-9]\d{0,2})){3}$/.test(host)) throw Error('A private IPv4 address is required.');
  const bytes = host.split('.').map(Number);
  if (bytes.some(value => value > 255) || !(bytes[0] === 10 || (bytes[0] === 172 && bytes[1] >= 16 && bytes[1] <= 31) || (bytes[0] === 192 && bytes[1] === 168))) throw Error('A private IPv4 address is required.');
  return bytes.reduce((value, byte) => (value << 8n) | BigInt(byte), 0n);
}
// CRC-7, polynomial x^7 + x^3 + 1, over the 73 payload bits. Typo detection only.
function checksum(payload) {
  let crc = 0;
  for (let bit = 72n; bit >= 0n; bit--) {
    const feedback = ((crc >> 6) & 1) ^ Number((payload >> bit) & 1n);
    crc = (crc << 1) & 127;
    if (feedback) crc ^= 9;
  }
  return BigInt(crc);
}
export function newLanPairingPassword() {
  const bytes = crypto.getRandomValues(new Uint8Array(4));
  const value = bytes.reduce((result, byte) => (result << 8n) | BigInt(byte), 0n);
  return encode(value & (PASSWORD_LIMIT - 1n), 5);
}
export function encodeLanPairingCode({host, port, password}) {
  const address = privateAddress(host);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) throw Error('A valid local service port is required.');
  const secret = decode(normalize(password, 5));
  const payload = (address << 41n) | (BigInt(port) << 25n) | secret;
  return encode((payload << 7n) | checksum(payload), 16).match(/.{4}/g).join('-');
}
export function parseLanPairingCode(input) {
  const code = normalize(input, 16), packed = decode(code), payload = packed >> 7n;
  if ((packed & 127n) !== checksum(payload)) throw invalid();
  const address = payload >> 41n;
  const host = [24n, 16n, 8n, 0n].map(shift => Number((address >> shift) & 255n)).join('.');
  privateAddress(host);
  const port = Number((payload >> 25n) & 65535n);
  if (port < 1024) throw invalid();
  return {host, port, password:encode(payload & (PASSWORD_LIMIT - 1n), 5)};
}
