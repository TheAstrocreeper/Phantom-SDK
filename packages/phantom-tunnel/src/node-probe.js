import {spawn} from 'node:child_process';

/** Desktop reference binding. Only bounded control/receipts cross process pipes;
 * the native worker generates, streams and hashes all probe payload bytes.
 * binary/bind/STUN are trusted local configuration, never peer-supplied paths.
 */
export function createNativeProbeAdapter({binary,bindAddress='0.0.0.0',stun=null}) {
  if(typeof binary!=='string'||!binary||typeof bindAddress!=='string'||(stun&&(!stun.host||!Number.isInteger(stun.port)||stun.port<1||stun.port>65535)))throw new TypeError('Invalid native worker configuration');
  return {
    prepare({localPeer,remotePeer,sessionId,bytes,initiator,signal}) {
      if(!/^[0-9a-f]{16}$/.test(localPeer)||!/^[0-9a-f]{16}$/.test(remotePeer)||!/^[0-9a-f]{32}$/.test(sessionId)||!Number.isInteger(bytes)||bytes<1||bytes>2147483648)throw new TypeError('Invalid native scope');
      signal?.throwIfAborted();
      const args=[localPeer,remotePeer,sessionId,initiator?'connect':'accept',String(bytes),bindAddress];
      if(stun)args.push(stun.host+':'+stun.port);
      const worker=spawn(binary,args,{stdio:['pipe','pipe','pipe'],windowsHide:true});
      let buffer='',offerSeen=false,connectedSeen=false,complete=null,ended=false,remoteSent=false;
      let resolveOffer,rejectOffer,resolveConnected,rejectConnected,resolveResult,rejectResult;
      const offerReady=new Promise((a,b)=>{resolveOffer=a;rejectOffer=b;});
      const connected=new Promise((a,b)=>{resolveConnected=a;rejectConnected=b;});connected.catch(()=>{});
      const result=new Promise((a,b)=>{resolveResult=a;rejectResult=b;});result.catch(()=>{});
      const fail=()=>{const error=new Error('Native probe failed');rejectOffer(error);rejectConnected(error);rejectResult(error);};
      function close(){if(ended)return;worker.kill();}
      const abort=()=>{fail();close();};signal?.addEventListener('abort',abort,{once:true});
      worker.on('error',()=>{ended=true;signal?.removeEventListener('abort',abort);fail();});
      worker.stdin.on('error',()=>{fail();close();});
      worker.stderr.resume(); // Never forward uncontrolled native stderr to page logs.
      worker.stdout.on('data',chunk=>{
        buffer+=chunk;
        if(buffer.length>32768){fail();close();return;}
        let index;
        while((index=buffer.indexOf('\n'))>=0){
          const line=buffer.slice(0,index);buffer=buffer.slice(index+1);
          try {
            const event=JSON.parse(line);
            if(event.type==='offer'&&!offerSeen){
              if(event.probeVersion!==2)throw Error('Native probe version mismatch');
              offerSeen=true;
              resolveOffer({offer:event.offer,close,connect(remote){
                if(remoteSent||ended)throw new Error('Native session consumed');remoteSent=true;
                const text=JSON.stringify(remote);if(text.length>16384)throw new Error('Native offer too large');
                worker.stdin.end(text+'\n');return connected;
              }});
            }else if(event.type==='connected'&&offerSeen&&!connectedSeen){
              connectedSeen=true;resolveConnected({result,close});
            }else if(event.type==='complete'&&connectedSeen&&!complete){
              if(event.bytes!==bytes||!/^[0-9a-f]{64}$/.test(event.sha256)||!Number.isFinite(event.elapsedSeconds)||event.elapsedSeconds<=0)throw Error('Invalid receipt');
              complete={bytes:event.bytes,sha256:event.sha256,elapsedSeconds:event.elapsedSeconds,MBps:event.MBps,localReceiveDrops:event.localReceiveDrops,localSendDrops:event.localSendDrops};
            }else throw Error('Invalid native phase');
          }catch{fail();close();}
        }
      });
      worker.on('close',code=>{ // Wait for stdout to drain before checking the receipt.
        ended=true;signal?.removeEventListener('abort',abort);
        if(code===0&&complete&&connectedSeen)resolveResult(complete);else fail();
      });
      if(signal?.aborted)abort();
      return offerReady;
    },
  };
}
