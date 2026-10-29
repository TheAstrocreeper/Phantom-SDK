import test from 'node:test';
import assert from 'node:assert/strict';
import {createScoutFileWorkload} from '../packages/phantom-core/src/scout-file-workload.js';
test('late frames from a retired synthetic file cannot enter the next verification trial',()=>{
 const received=[],errors=[],channel={readyState:'open',bufferedAmount:0,send(){}};
 const driver=createScoutFileWorkload({peer:'1111111111111111',lanes:[0],channels:[channel],pcs:[{sctp:{maxMessageSize:262144}}],bytes:1024,chunkSize:1024,batchSize:1,requestWindow:1,scopeId:'current-trial',onChunk:(...args)=>received.push(args),onError:error=>errors.push(error)});
 try{driver.receive(0,JSON.stringify({kind:'binary',file:{id:'old-file',index:0,bytes:1024,distributionId:'retired-trial'}}));driver.receive(0,new ArrayBuffer(1024));driver.receive(0,JSON.stringify({kind:'message',sys:'PHANTOM_FILE_REQUEST',file:{id:'old-file',index:0}}));assert.deepEqual(received,[]);assert.deepEqual(errors,[]);}finally{driver.close();}
});
