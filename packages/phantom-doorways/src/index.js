/** Existing SIX7 doorway wire format. Names are not reserved npm packages. */
export function createLegacyDoorway({ url, WebSocket = globalThis.WebSocket }) {
  const endpoint = new URL(url);
  if (!['ws:', 'wss:'].includes(endpoint.protocol)) {
    throw new TypeError('Doorway URL must use ws:// or wss://');
  }
  if (!WebSocket) throw new Error('WebSocket implementation required');
  return Object.freeze({
    protocol: 'six7-concierge-v1',
    url: endpoint.href.endsWith('/') ? endpoint.href : endpoint.href + '/',
    WebSocket,
  });
}
export { createDoorway, bridgeDoorway, adaptLegacyDoorway, validateDoorway, DoorwayConfigurationError } from './interface.js';

export {createHttpControlStore} from './control-store.js';
export {withAdmissionDoorway} from './admission-doorway.js';
