import {createLeadershipAdmin} from './leadership-admin.js';
import {createNetworkDistribution} from './network-distribution.js';
import {createAdminLifecycle} from './admin-lifecycle.js';
import {createFileDistribution} from './files.js';
import {createPhantom} from './index.js';
import {createInvite, parseInvite} from './invite.js';
/** Dependency-injected entry facade: adapters remain outside core. */
export function createPhantomEntry({openContext, relays, doorways, adminPublicKey, coreFactory=createPhantom}={}) {
  if(typeof openContext !== 'function') throw new TypeError('openContext factory required');
  async function start(invite,{name='Peer',timeoutMs=null,signal,onState,onEvent}={},waitForAdmission=true) {
    const context=parseInvite(invite);
    if(timeoutMs!==null&&(!Number.isFinite(timeoutMs)||timeoutMs<=0)) throw new TypeError('Positive admission timeout required');
    if(signal?.aborted) throw new Error('Join aborted');
    const config=await openContext(context);
    if(signal?.aborted){config.admissionCheckpoint?.close();throw new Error('Join aborted');}
    const retireGraceMs=config.retireGraceMs??30000;if(!Number.isFinite(retireGraceMs)||retireGraceMs<0||retireGraceMs>60000)throw new TypeError('Invalid retirement grace period');
    let core,files,distribution,retireTimer,adminGate;
    let retired=false,offAdmission=()=>{};
    const lifecycle=await createAdminLifecycle({networkId:context.networkId,adminPublicKey:context.adminPublicKey,
      adminIdentity:config.adminIdentity,store:config.controlStore,crypto:config.runtime?.crypto??globalThis.crypto,
      send:(record,to)=>core?.command('PHANTOM_ADMIN_SEND',{record,to}),
      emit:(type,record)=>{if(type==='revoked'){files?.stop(record.target);distribution?.stop(record.target);}emit(type,record);},
      onRetire:()=>{retired=true;adminGate?.retire();files?.close();distribution?.close();emit('state',{...core.getState(),state:'retired',reason:'signed_network_retirement',nextAction:'none'});
        // SysCon 1 carries the order during the drain window; ordinary peers leave promptly.
        retireTimer=setTimeout(()=>{core?.dispose();},core.getState().role==='syscon1'?retireGraceMs:250);
      }});
    if(lifecycle?.retired)throw new Error('Network permanently retired');
    adminGate=createLeadershipAdmin({networkId:context.networkId,readOrders:config.readAdminOrders,
      applyOrder:record=>lifecycle?.apply(record)??false,retired:()=>retired,emit:(type,payload)=>emit(type,payload)});
    core=coreFactory({...config,authorizeSyscon1:(context,key,resume)=>adminGate.authorize(context,key,resume),canSendFile:file=>!retired&&!lifecycle?.isRevoked(file.id)&&(!file.distributionId||!lifecycle?.isRevoked(file.distributionId))});
    const handlers=new Map();let closed=false;let peers=[];
    if(typeof onState==='function')handlers.set('state',new Set([onState]));
    if(typeof onEvent==='function')handlers.set('event',new Set([onEvent]));
    const emit=(type,data)=>{for(const fn of handlers.get(type)||[])try{fn(data);}catch(e){console.error(e);}};
    const offEvent=core.onEvent(event=>{
      if(closed)return;
      if(event.type==='PHANTOM_ADMIN')void lifecycle?.apply(event.payload.record).catch(error=>emit('controlError',{error:error.message}));
      if(event.type==='PHANTOM_ADMIN_LINK_READY'||event.type==='P2P_CHAT_READY'){const peer=event.payload?.from;if(peer)lifecycle?.sync(peer);}
      if(event.type==='PRESENCE_ROSTER') peers=structuredClone(event.payload?.list||[]);
      emit('event',event);
      if(event.type==='PEER_CONNECTION_STATE')emit('peerConnection',event.payload);
      if(event.type==='INCOMING_MSG'&&!event.payload?.sys&&typeof event.payload?.text==='string') emit('message',{from:event.payload.from,text:event.payload.text});
    });
    const adminContext=()=>{const snapshot=core.snapshot?.(),c=snapshot?.concierge;if(!c)return core.getState();return {peerId:snapshot.self?.hKey,leader:c.syscon1,role:c.localRoleCandidate||c.role};};
    const offState=core.onState(state=>{if(closed)return;adminGate.observe(adminContext());config.onNetworkState?.(state);if(retired)lifecycle?.sync();else emit('state',state);});
    const active=()=>{if(closed)throw new Error('Network closed');if(retired)throw new Error('Network permanently retired');};
    const canUsePeer=peer=>!config.canUsePeer||config.canUsePeer(peer)===true;
    files=createFileDistribution({core,canUsePeer,crypto:config.runtime?.crypto??globalThis.crypto,emit,
      allowed:(id,target)=>!retired&&!closed&&!lifecycle?.isRevoked(id)&&(!target||!lifecycle?.isRevoked(target)),...Object.fromEntries(Object.entries(config.files??{}).filter(([key])=>['chunkStore','requestTimeoutMs','requestWindow','maxAttemptsPerPeer','maxFileBytes','maxChunkBytes'].includes(key)))});
    if(config.distribution)distribution=createNetworkDistribution({core,canUsePeer,networkId:context.networkId,adminPublicKey:context.adminPublicKey,lifecycle,runtime:config.runtime,emit,allowed:(id,target)=>!retired&&!closed&&!lifecycle?.isRevoked(id)&&(!target||!lifecycle?.isRevoked(target)),checkAdmin:()=>adminGate.wait(adminContext()),options:{...config.distribution,...(config.rtcConfiguration!==undefined?{rtcConfiguration:config.rtcConfiguration}:{})}});
    const network={invite,networkId:context.networkId,
      refreshNetworkPermissions:()=>{active();return core.refreshNetworkPermissions?.()??Promise.resolve({localNetwork:{state:'unknown',name:null,reason:'permissions_api_unavailable',nextAction:'none'}});},
      prepareDistributionFile:(file,options)=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.prepare(file,options);},
      requestTransferCapabilities:peer=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.requestCapabilities(peer);},
      getTransferCapabilities:peer=>{active();return distribution?.getCapabilities(peer)??null;},
      proposeTransferPlan:(peer,settings)=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.proposePlan(peer,settings);},
      getTransferPlan:peer=>{active();return distribution?.getPlan(peer)??null;},
      prepareAgreedDistributionFile:(file,peer,settings)=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.prepareAgreed(file,peer,settings);},
      distributeFile:(file,options)=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.publish(file,options);},
      getDistributionState:()=>distribution?.snapshot()??null,
      getDistributedFile:id=>{active();if(!distribution)throw new Error('Enable distribution in SDK configuration');return distribution.getFile(id);},
      getAdminState:()=>adminGate.snapshot(),
      publishFile:(file,options)=>{active();return files.publish(file,options);},
      shareFile:(peer,id)=>{active();return files.share(peer,id);},
      requestFile:(id,options)=>{active();return files.request(id,options);},
      requestChunks:(id,indexes,options)=>{active();return files.request(id,{...options,indexes});},
      cancelFile:id=>files.cancel(id),
      getFileManifest:id=>files.getManifest(id),
      revoke:target=>{active();if(!lifecycle)throw new Error('Network has no configured admin');return lifecycle.issue('content.revoke',target);},
      retire:()=>{active();if(!lifecycle)throw new Error('Network has no configured admin');return lifecycle.issue('network.retire',context.networkId);},
      submitAdminOrder:record=>{active();if(core.getState().role!=='syscon1')throw new Error('Admin orders are delivered to SysCon 1');if(!lifecycle)throw new Error('Network has no configured admin');return lifecycle.apply(record);},
      applyControl:record=>{if(closed)throw new Error('Network closed');if(!lifecycle)throw new Error('Network has no configured admin');return lifecycle.apply(record);},
      getControls:()=>lifecycle?.records()??[],
      isRevoked:target=>lifecycle?.isRevoked(target)??false,
      on(type,fn){if(closed)throw new Error('Network closed');if(typeof fn!=='function')throw new TypeError('Listener required');if(!handlers.has(type))handlers.set(type,new Set());handlers.get(type).add(fn);return()=>handlers.get(type)?.delete(fn);},
      getState:()=>retired?{...core.getState(),state:'retired',reason:'signed_network_retirement',nextAction:'none'}:core.getState(),snapshot:()=>core.snapshot(),getPeers:()=>structuredClone(peers),getPeerConnections:()=>core.getPeerConnections(),
      invitePeer:id=>{active();return core.invite(id);},acceptPeer:id=>{active();return core.accept(id);},send:(id,text)=>{active();return core.send(id,text);},
      leave:()=>{active();if(!config.leaveMembership)throw new Error('Configure membership leave before using leave');return config.leaveMembership();},
      refreshMembership:()=>{active();if(!config.refreshMembership)throw new Error('Membership refresh is not configured');return config.refreshMembership();},
      close(){
        if(closed)return;closed=true;
        // Every owner is released even when another owner's close throws.
        for(const dispose of [offAdmission,()=>config.admissionCheckpoint?.close(),()=>clearTimeout(retireTimer),
          ()=>distribution?.close(),()=>files.close(),offEvent,offState,()=>core.dispose(),()=>adminGate.close(),()=>handlers.clear()]) {
          try{dispose();}catch(error){try{onEvent?.({type:'CONNECTION_CLEANUP_ERROR',payload:{message:error.message}});}catch{}}
        }
      }
    };
    if(config.admissionCheckpoint)offAdmission=config.admissionCheckpoint.onChange(event=>{
      if(closed)return;
      const local=config.identity.id.slice(0,16);
      if(!config.admissionCheckpoint.allows(local)){emit('membershipLost',{reason:event.type});network.close();return;}
      const peers=new Set([...network.getPeers().map(p=>p.hKey),...core.getPeerConnections().map(p=>p.peerId)]);
      if(event.peerPublicKey)peers.add(event.peerPublicKey.slice(0,16));
      for(const peer of peers)if(!canUsePeer(peer)){files.dropPeer(peer);distribution?.dropPeer(peer);}
      core.command('PHANTOM_ADMISSION_REFRESH',{});
      emit('membershipChanged',event);
    });
    try {
      if(config.admissionCheckpoint&&!config.admissionCheckpoint.allows(config.identity.id.slice(0,16)))throw new Error('Local membership inactive');
      await new Promise((resolve,reject)=>{
        let done=false,off=()=>{},storageReady=!distribution;
        const attemptId=globalThis.crypto.randomUUID();
        const finish=error=>{
          if(done)return;done=true;clearTimeout(timer);
          try{off();}catch{}
          signal?.removeEventListener('abort',abort);
          if(error) {
            error.code??=signal?.aborted?'JOIN_CANCELLED':'TRANSPORT_TERMINAL';
            error.attempt={id:attemptId,status:signal?.aborted?'cancelled':'failed',reason:core.getState().reason??error.message};
            emit('event',{type:'ADMISSION_ATTEMPT_TERMINAL',payload:error.attempt});
          }
          error?reject(error):resolve();
        };
        const check=state=>{if(retired)finish(new Error('Network permanently retired'));else if(['failed','closed'].includes(state.state))finish(new Error(state.reason));else if(storageReady&&state.state==='admitted')finish();};
        const abort=()=>finish(new Error('Join aborted'));
        // Only a caller-requested cancellation deadline may end this wait.
        const timer=timeoutMs===null?null:setTimeout(()=>finish(Object.assign(new Error('Network admission timed out'),{code:'ADMISSION_TIMEOUT'})),timeoutMs);
        off=core.onState(check);signal?.addEventListener('abort',abort,{once:true});
        if(signal?.aborted){abort();return;}
        Promise.resolve().then(()=>{
          if(done||closed)return;
          return core.join({roomId:context.networkId,name,identity:config.identity});
        }).then(async()=>{
          if(done||closed)return;
          await distribution?.restore();
          if(done||closed)return;
          storageReady=true;return waitForAdmission?check(core.getState()):finish();
        }).catch(finish);
      });
      return network;
    } catch(error){
      if(error?.code==='ADMISSION_TIMEOUT')error.diagnostics={state:core.getState(),rtc:core.snapshot()?.rtcDiagnostics??null};
      network.close();
      if(error.attempt){error.attempt={...error.attempt,status:'cleaned'};try{onEvent?.({type:'ADMISSION_ATTEMPT_CLEANED',payload:error.attempt});}catch{}}
      throw error;
    }
  }
  return {join:(invite,options)=>start(invite,options,true),create:options=>start(createInvite({relays,doorways,adminPublicKey}),options,false),
    startCreated:(invite,options)=>start(invite,options,false)};
}
