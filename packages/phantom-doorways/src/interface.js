import { DISCOVERY_TYPES, decodeDiscovery, encodeDiscovery } from './discovery.js';
export class DoorwayConfigurationError extends TypeError {
  constructor(code, field, message) {
    super(message); this.name='DoorwayConfigurationError'; this.code=code; this.field=field;
  }
}
export function validateDoorway(doorway) {
  if(!['phantom-doorway-v1','phantom-doorway-v2'].includes(doorway?.protocol))
    throw new DoorwayConfigurationError('DOORWAY_PROTOCOL','protocol','Use a Phantom doorway adapter (v1 or v2).');
  if(typeof doorway.open!=='function')
    throw new DoorwayConfigurationError('DOORWAY_OPEN_REQUIRED','open','The doorway must provide open(context).');
  if(!['discovery-hints','live-sockets'].includes(doorway.presence??'discovery-hints'))
    throw new DoorwayConfigurationError('DOORWAY_PRESENCE','presence','Presence must be discovery-hints or live-sockets.');
  return doorway;
}
function validateSession(session,typed) {
  const required=typed?['announce','reconcile','sendSignal','close']:['send','close'];
  const missing=required.filter(method=>typeof session?.[method]!=='function');
  if(missing.length)throw new DoorwayConfigurationError('DOORWAY_SESSION_METHODS','session',`Doorway session is missing: ${missing.join(', ')}.`);
  if(session.publishAuthority!==undefined && typeof session.publishAuthority!=='function')
    throw new DoorwayConfigurationError('DOORWAY_SESSION_METHODS','session.publishAuthority','publishAuthority must be a function when supplied.');
}
/** Transport boundary. Discovery hints do not confer room authority. */
export function createDoorway({ open, discovery = false, presence = 'discovery-hints' }) {
  if(typeof discovery!=='boolean')throw new DoorwayConfigurationError('DOORWAY_DISCOVERY','discovery','discovery must be true or false.');
  return Object.freeze(validateDoorway({ protocol: discovery ? 'phantom-doorway-v2' : 'phantom-doorway-v1', presence, open }));
}

/** Temporary compatibility bridge; engine election rules stay in the engine. */
export function bridgeDoorway(doorway) {
  validateDoorway(doorway);
  class Socket {
    static CONNECTING=0; static OPEN=1; static CLOSING=2; static CLOSED=3;
    constructor(url) {
      this.readyState=0; this.pending=[];
      const u=new URL(url);
      this.roomId=u.searchParams.get('v');
      this.typed=doorway.protocol==='phantom-doorway-v2';
      queueMicrotask(() => {
        if(this.readyState!==0)return;
        try {
          this.session=doorway.open({ roomId:u.searchParams.get('v'), peerId:u.searchParams.get('to'),
            onOpen:()=>{if(this.readyState!==0)return;this.readyState=1;this.onopen?.();},
            onDiscovery:hint=>{if(this.readyState===1)this.onmessage?.({data:JSON.stringify(encodeDiscovery(hint,this.roomId))});},
            onSignal:packet=>{if(this.readyState===1)this.onmessage?.({data:JSON.stringify(packet)});},
            onPacket:packet=>{if(this.readyState===1)this.onmessage?.({data:JSON.stringify(packet)});},
            onClose:detail=>{if(this.readyState===3)return;this.readyState=3;this.onclose?.(detail);},
            onStatus:status=>{if(this.readyState!==3)this.onstatus?.(status);},
            onPublication:receipt=>{if(this.readyState!==3)this.onpublication?.(receipt);},
            onError:error=>{if(this.readyState!==3)this.onerror?.(error);}
          });
          validateSession(this.session,this.typed);
          for(const packet of this.pending)this.dispatch(packet);
          this.pending=[];
        } catch(error) { try{this.session?.close?.();}catch{} this.pending=[]; this.onerror?.(error); if(this.readyState!==3){this.readyState=3;this.onclose?.({code:1006,reason:'doorway_open_failed',wasClean:false});} }
      });
    }
    dispatch(packet) {
      if(!this.typed)return this.session.send(packet);
      if(packet.type==='concierge_connect'){const {type,videoId,from,...identity}=packet;return this.session.announce({roomId:videoId,peerId:from,...identity});}
      if(packet.type==='doorway_reconcile')return this.session.reconcile(packet);
      return this.session.sendSignal(packet);
    }
    send(raw) {if(this.readyState!==1)throw new Error('Doorway not open');const packet=JSON.parse(raw);if(this.session)this.dispatch(packet);else this.pending.push(packet);}
    publishAuthority(snapshot) {if(this.readyState!==1)throw new Error("Doorway not open");this.session?.publishAuthority?.(snapshot);}
    close() {if(this.readyState===3)return;this.readyState=3;this.session?.close();this.pending=[];}
  }
  return {protocol:'six7-concierge-v1',presence:doorway.presence??'discovery-hints',url:'ws://phantom-adapter.invalid/',WebSocket:Socket};
}

/** Wrap existing CF/custom WebSocket or local fixture for incremental migration. */
export function adaptLegacyDoorway(legacy) {
  if(legacy?.protocol!=='six7-concierge-v1')throw new TypeError('Legacy doorway required');
  return createDoorway({discovery:true,presence:legacy.presence??'live-sockets',open({roomId,peerId,authorization,signAdmission,onOpen,onDiscovery,onSignal,onClose,onError}) {
    const url=new URL('ws',legacy.url);url.searchParams.set('v',roomId);url.searchParams.set('to',peerId);
    const socket=new legacy.WebSocket(url.href);
    let challenge,pendingAnnouncement;
    function announce(value){
      if(authorization&&!challenge){pendingAnnouncement=value;return;}
      const {roomId:room,peerId:key,...identity}=value;
      if(authorization){
        const {admissionProof,...payload}=value;
        identity.connectionChallenge=challenge;
        identity.admissionProof=signAdmission('announce',{...payload,connectionChallenge:challenge});
      }
      socket.send(JSON.stringify({...identity,type:'concierge_connect',videoId:room,from:key}));
    }
    socket.onopen=onOpen;
    socket.onmessage=event=>{try{const packet=JSON.parse(event.data);
      if(authorization&&packet.type==='hello'&&typeof packet.connectionChallenge==='string'&&packet.connectionChallenge.length<=128){challenge=packet.connectionChallenge;if(pendingAnnouncement){const value=pendingAnnouncement;pendingAnnouncement=null;announce(value);}return;}
      if(DISCOVERY_TYPES.has(packet.type))onDiscovery(decodeDiscovery(packet));else onSignal(packet);
    }catch(error){onError(error);}};
    socket.onclose=onClose;socket.onerror=onError;
    return {
      announce,
      reconcile:packet=>socket.send(JSON.stringify(packet)),
      sendSignal:packet=>socket.send(JSON.stringify(packet)),
      publishAuthority:snapshot=>socket.send(JSON.stringify({type:'doorway_authority',videoId:roomId,from:peerId,snapshot})),close:()=>socket.close()
    };
  }});
}
