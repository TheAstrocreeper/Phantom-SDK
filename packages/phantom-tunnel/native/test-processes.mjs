import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';

// Only control offers travel through these process pipes. Actual probe bytes
// travel between separate libjuice UDP sockets through Quinn on both sides.
test('two native processes establish ICE/QUIC and verify a 32 MiB stream', {timeout:30000}, async t => {
  const exe=process.env.PHANTOM_NATIVE_PROBE ?? fileURLToPath(new URL('./target/debug/examples/native_probe'+(process.platform==='win32'?'.exe':''),import.meta.url));
  const session=randomBytes(16).toString('hex'); const bytes=32*1024*1024;
  const peers=[];
  function launch(local,remote,mode) {
    const child=spawn(exe,[local,remote,session,mode,String(bytes),'127.0.0.1'],{stdio:['pipe','pipe','pipe']});
    const phases=[];
    t.after(()=>{if(child.exitCode===null){t.diagnostic(local+' native phases before cancellation: '+phases.join(','));child.kill();}});
    let buffer='',stderr=''; const events=[];
    let gotOffer,failOffer;
    const offer=new Promise((resolve,reject)=>{gotOffer=resolve;failOffer=reject;});
    // Avoid unhandled rejections while waiting for both processes.
    offer.catch(()=>{});
    const complete=new Promise((resolve,reject)=>{
      child.on('error',error=>{failOffer(error);reject(error);});
      child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-8192);});
      child.stdout.on('data',chunk=>{
        buffer+=chunk;if(buffer.length>32768){child.kill();reject(Error('Native control buffer limit'));return;}
        let index;
        while((index=buffer.indexOf('\n'))>=0){
          const line=buffer.slice(0,index);buffer=buffer.slice(index+1);
          try {
            const event=JSON.parse(line);phases.push(event.type);
            if(event.type==='offer') {
              assert.equal(event.offer.from,local);assert.equal(event.offer.to,remote);assert.equal(event.offer.sessionId,session);
              gotOffer(event.offer);
            } else events.push(event);
          }catch(error){child.kill();failOffer(error);reject(error);}
        }
      });
      child.on('close',(code,signal)=>{ // stdout must be drained before validating completion.
        if(code!==0){const error=Error(`Native probe failed (${code??signal}): ${stderr}`);failOffer(error);reject(error);return;}
        const result=events.find(e=>e.type==='complete');
        if(!result){const error=Error('Missing completion receipt');failOffer(error);reject(error);return;}
        resolve({result,events});
      });
    });complete.catch(()=>{});
    const peer={child,offer,complete};peers.push(peer);return peer;
  }
  const a=launch('alice','bob','connect'),b=launch('bob','alice','accept');
  const [ao,bo]=await Promise.all([a.offer,b.offer]);
  a.child.stdin.end(JSON.stringify(bo)+'\n');b.child.stdin.end(JSON.stringify(ao)+'\n');
  const results=await Promise.all([a.complete,b.complete]);
  const hash=createHash('sha256');for(let n=0;n<bytes;n+=65536)hash.update(Buffer.alloc(65536,0xa7));
  const expected=hash.digest('hex');
  for(const {result,events} of results){
    assert.equal(result.bytes,bytes);assert.equal(result.sha256,expected);
    assert(events.some(e=>e.type==='connected'&&e.carrier==='ice-quic'));
  }
});
