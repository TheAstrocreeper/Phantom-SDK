import {test} from 'node:test';
import assert from 'node:assert/strict';
import {relayMetadata} from './metadata.mjs';
test('metadata preserves declared limitations and caps response size',async()=>{
 const doc={limitation:{auth_required:true,max_message_length:1234}};
 const result=await relayMetadata('wss://example.test/',{fetchImpl:async(url,options)=>{assert.equal(url.href,'https://example.test/');assert.equal(options.headers.Accept,'application/nostr+json');return new Response(JSON.stringify(doc));}});
 assert.deepEqual(result.document,doc);
 const capped=await relayMetadata('wss://example.test/',{maxBytes:2,fetchImpl:async()=>new Response('too large')});assert.equal(capped.error,'metadata_too_large');
});
