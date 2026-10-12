/** WebSocket discovery/signaling transport. */
export function createWebSocketDoorway({ url, WebSocket = globalThis.WebSocket }) {
  const endpoint = new URL(url);
  if (!['ws:', 'wss:'].includes(endpoint.protocol)) {
    throw new TypeError('Doorway URL must use ws:// or wss://');
  }
  if (!WebSocket) throw new Error('WebSocket implementation required');
  return Object.freeze({
    protocol: 'phantom-concierge-v1',
    url: endpoint.href.endsWith('/') ? endpoint.href : endpoint.href + '/',
    WebSocket,
  });
}
export { createDoorway, bridgeDoorway, adaptWebSocketDoorway, validateDoorway, DoorwayConfigurationError } from './interface.js';

export {createHttpControlStore} from './control-store.js';
export {withAdmissionDoorway} from './admission-doorway.js';

// Retain exported aliases for callers of the initial API.
export {createWebSocketDoorway as createLegacyDoorway};
export {adaptWebSocketDoorway as adaptLegacyDoorway} from './interface.js';
