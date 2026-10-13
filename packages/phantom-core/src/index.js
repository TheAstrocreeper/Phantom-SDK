import { projectNetworkState } from './network-state.js';
import { resolvePeerIdentity, validatePeerName } from './identity.js';
export { createPeerIdentity } from './identity.js';
import { bridgeDoorway } from '../../phantom-doorways/src/interface.js';
import {isConciergeProtocol,migrateStorage,normalizeCommand} from './compatibility.js';
import { createNetworkEngine } from './network-engine.js';

function memoryStorage() {
  const values = new Map();
  return {
    getItem: key => values.get(String(key)) ?? null,
    setItem: (key, value) => values.set(String(key), String(value)),
    removeItem: key => values.delete(String(key)),
  };
}

function eventSurface() {
  const listeners = new Map();
  return {
    addEventListener(type, callback) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(callback);
    },
    dispatchEvent(event) {
      for (const callback of listeners.get(event.type) || []) callback(event);
    },
    clear() { listeners.clear(); }
  };
}

/** Public peer-network facade with injectable transport and runtime. */
export function createPhantom({ doorway, origin = 'https://phantom.local',
    avatar = null, storage = memoryStorage(), sessionStorage = memoryStorage(),
    runtime = globalThis, observeBrowserLifecycle = false, transportRecoveryGraceMs, canSendFile = () => true, authorizeSyscon1 = () => true, canAdmitPeer = () => true } = {}) {
  if (['phantom-doorway-v1','phantom-doorway-v2'].includes(doorway?.protocol)) doorway = bridgeDoorway(doorway);
  if (!isConciergeProtocol(doorway?.protocol)) {
    throw new Error('A Phantom concierge doorway is required');
  }
  transportRecoveryGraceMs ??= doorway.presence === 'discovery-hints' ? 60000 : 10000;
  if(!Number.isFinite(transportRecoveryGraceMs)||transportRecoveryGraceMs<1000||transportRecoveryGraceMs>120000)throw new TypeError('transportRecoveryGraceMs must be between 1000 and 120000');
  migrateStorage(storage);
  migrateStorage(sessionStorage);
  // Browser timer functions require their Window receiver, even when
  // supplied through a plain runtime configuration object.
  const timerTarget = runtime.timerTarget ?? globalThis;
  const timers = Object.fromEntries(
    ["setTimeout", "clearTimeout", "setInterval", "clearInterval"]
      .map(name => [name, runtime[name].bind(timerTarget)])
  );
  const listeners = new Set();
  const stateListeners=new Set();
  let networkState={state:"idle",reason:"not_joined",nextAction:"join",peerId:null};
  let networkSignature=JSON.stringify(networkState);
  const window = eventSurface();
  const document = { ...eventSurface(), visibilityState: observeBrowserLifecycle ? runtime.document?.visibilityState ?? 'visible' : 'visible' };
  const timeouts = new Set();
  const intervals = new Set();
  let disposed = false;
  let joined = false;
  let joining = false;
  let cancelJoin = null;
  let publicIdentity = null;
  const host = {
    window, document, origin, avatar, allowDisplayNames: true, localStorage: storage, sessionStorage,
    location: { href: origin }, doorwayUrl: doorway.url,
    doorwayPresence: doorway.presence ?? 'live-sockets', transportRecoveryGraceMs,
    WebSocket: doorway.WebSocket, RTCPeerConnection: runtime.RTCPeerConnection,
    crypto: runtime.crypto, canSendFile, authorizeSyscon1,
    canAdmitPeer: peer => { try { return canAdmitPeer(peer) === true; } catch { return false; } },
    emit(type, payload) {
      if (disposed) return;
      if(type==='SNAPSHOT') {
        const projected=projectNetworkState(payload);
        const signature=JSON.stringify(projected);
        if(signature!==networkSignature){networkState=projected;networkSignature=signature;for(const listener of stateListeners){try{listener(structuredClone(networkState));}catch(error){console.error('Phantom state listener failed',error);}}}
      }
      for (const listener of listeners) {
        try { listener({ type, payload }); }
        catch (error) { console.error('Phantom event listener failed', error); }
      }
    },
    setTimeout(callback, delay, ...args) {
      const id = timers.setTimeout(() => {
        timeouts.delete(id);
        if (!disposed) callback(...args);
      }, delay);
      timeouts.add(id);
      return id;
    },
    clearTimeout(id) { timers.clearTimeout(id); timeouts.delete(id); },
    setInterval(callback, delay, ...args) {
      const id = timers.setInterval(() => {
        if (!disposed) callback(...args);
      }, delay);
      intervals.add(id);
      return id;
    },
    clearInterval(id) { timers.clearInterval(id); intervals.delete(id); },
  };
  const engine = createNetworkEngine(host);
  const lifecycleBindings = [];
  if (observeBrowserLifecycle) {
    const browserWindow = runtime.window ?? runtime;
    function bind(target, type, callback) {
      if (typeof target?.addEventListener !== 'function' || typeof target?.removeEventListener !== 'function') return;
      target.addEventListener(type, callback);
      lifecycleBindings.push(() => target.removeEventListener(type, callback));
    }
    for (const type of ['online', 'pageshow']) bind(browserWindow, type, () => {
      if (!disposed) window.dispatchEvent({type});
    });
    bind(runtime.document, 'visibilitychange', () => {
      if (disposed) return;
      document.visibilityState = runtime.document.visibilityState;
      document.dispatchEvent({type:'visibilitychange'});
    });
  }
  function command(type, payload) {
    if (disposed) throw new Error('Phantom instance disposed');
    engine.command(normalizeCommand(type), payload);
  }
  return {
    getState() {return structuredClone(networkState);},
    onState(listener) {
      if(disposed)throw new Error('Phantom instance disposed');
      if(typeof listener!=='function')throw new TypeError('State listener required');
      stateListeners.add(listener);return ()=>stateListeners.delete(listener);
    },
    onEvent(listener) {
      if (disposed) throw new Error('Phantom instance disposed');
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    async join({ roomId, name, identity: savedIdentity, handle, identityHash } = {}) {
      if (disposed || joined || joining) throw new Error('Create a fresh instance to join another room');
      if (typeof roomId !== 'string' || !roomId.trim()) throw new Error('roomId required');
      if (name !== undefined) {
        if (handle !== undefined) throw new Error('Use name or legacy handle, not both');
        handle = validatePeerName(name);
        if (savedIdentity !== undefined && identityHash !== undefined) throw new Error('Use identity or identityHash, not both');
        publicIdentity = resolvePeerIdentity(savedIdentity ?? (identityHash === undefined ? undefined : {version:1,id:identityHash}), {crypto:runtime.crypto});
        identityHash = publicIdentity.id;
      } else {
        if (savedIdentity !== undefined) throw new Error('Saved identity requires name');
        if (typeof handle !== 'string' || !handle.startsWith('@') || handle.length < 2) throw new Error('Supply name, or a legacy handle beginning with @');
      }
      if (identityHash !== undefined && !/^[0-9a-f]{64}$/.test(identityHash)) {
        throw new Error('identityHash must be 64 lowercase hex characters');
      }
      joining = true;
      command('CORE_CTX', { roomId: roomId.trim() });
      const ready = new Promise((resolve, reject) => {
        const listener = event => {
          if (event.type === 'SNAPSHOT' && event.payload?.reason === 'CORE_SET_SELF_hash_failed') { listeners.delete(listener); cancelJoin = null; reject(new Error('Identity hashing failed')); return; }
          if (event.type !== 'IDENTITY_READY') return;
          listeners.delete(listener);
          cancelJoin = null;
          if (event.payload.hKey) resolve(event.payload);
          else reject(new Error('Identity initialization failed'));
        };
        listeners.add(listener);
        cancelJoin = () => { listeners.delete(listener); reject(new Error('Phantom instance disposed')); };
      });
      command('CORE_SET_SELF', { handle, hKey: identityHash });
      try {
        const identity = await ready;
        command('CORE_SET_ENABLED', { enabled: true });
        command('CORE_BOOTSTRAP', {});
        joined = true;
        return identity; // Identity ready, not a claim of committed room admission.
      } finally { joining = false; }
    },
    invite(peer) { command('VEIL_INVITE', { hKey: peer }); },
    accept(peer) { command('VEIL_ACCEPT', { from: peer }); },
    send(peer, text) { command('SEND_DM', { to: peer, text }); },
    exportIdentity() {
      if (!publicIdentity) throw new Error("No standalone identity initialized; join with name first");
      return {...publicIdentity};
    },
    disconnectDiscovery() { command("CORE_DISCONNECT_DISCOVERY", {}); },
    snapshot: () => engine.snapshot(),
    command,
    dispose() {
      if (disposed) return;
      for (const unbind of lifecycleBindings.splice(0)) unbind();
      engine.dispose();
      disposed = true;
      cancelJoin?.();
      for (const id of timeouts) timers.clearTimeout(id);
      for (const id of intervals) timers.clearInterval(id);
      timeouts.clear(); intervals.clear(); listeners.clear();
      networkState={...networkState,state:'closed',reason:'disposed',nextAction:'create_instance'};
      for(const listener of stateListeners){try{listener(structuredClone(networkState));}catch{}}
      stateListeners.clear();
      window.clear(); document.clear();
    },
  };
}

export { createAdminIdentity, importAdminIdentity, createAdminAuthorization } from './authorization.js';

export {createControlStore} from './admin-lifecycle.js';
export {createMemoryChunkStore} from './files.js';

export {createAdminController} from './admin-controller.js';
export {createAdmissionCheckpoint, createAdmissionVerifier, AdmissionError} from './admission.js';
