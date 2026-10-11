// Compatibility codec. These are observations, never committed membership proofs.
export const DISCOVERY_TYPES = new Set(['concierge_route','concierge_bootstrap_needed','concierge_update','roster','anchor','bootstrap_anchor']);
export function decodeDiscovery(packet) {
  if (!packet || !DISCOVERY_TYPES.has(packet.type)) throw new TypeError('Unknown discovery packet');
  const { type, videoId, epoch, syscon1, syscon2, list, ...extensions } = packet;
  return { kind: type, roomId: videoId, epoch, leader: syscon1, mirror: syscon2, peers: list, extensions };
}
export function encodeDiscovery(hint, roomId) {
  if (!hint || !DISCOVERY_TYPES.has(hint.kind)) throw new TypeError('Unknown discovery kind');
  if (hint.roomId !== undefined && hint.roomId !== roomId) throw new Error('Discovery room mismatch');
  return { ...hint.extensions, type: hint.kind, videoId: hint.roomId ?? roomId,
    epoch: hint.epoch, syscon1: hint.leader, syscon2: hint.mirror, list: hint.peers };
}
