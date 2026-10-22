import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createLanPairingHost,createLanPairingClient,createShortPairingHost,createShortPairingClient} from '@phantom-local/doorways/local';
const pake=createRequire(import.meta.url)('spake2-wasm');
const hostPublicKey='a'.repeat(64),peerPublicKey='b'.repeat(64),route='http://192.168.0.214:46139',password='2W9X7';
const base='L2:192.168.0.214:46139:'+ 'A'.repeat(32);
const invitation=base+':'+ 'c'.repeat(64)+':'+hostPublicKey+':P1-test';
const options={route,password,hostPublicKey,peerPublicKey,pake};
async function exchange(host,client) {
  const challenge=host.begin(peerPublicKey,client.hello);
  const packet=await client.answer(challenge);
  return client.open(await host.finish(peerPublicKey,client.hello,packet));
}
test('LAN PAKE confirms both identities and opens one encrypted invitation',async()=>{
  const host=createLanPairingHost({...options,invitation});
  const client=createLanPairingClient(options);
  try {
    assert.equal(await exchange(host,client),invitation);
    assert.equal(host.used,true);
    assert.throws(()=>host.begin('d'.repeat(64),'f'.repeat(32)),/expired or used/);
  } finally{host.close();client.close();}
});
test('wrong password, changed route and changed host identity cannot retrieve credentials',async()=>{
  for(const override of [{password:'2W9X6'},{route:'http://192.168.0.215:46139'},{hostPublicKey:'d'.repeat(64)}]){
    const host=createLanPairingHost({...options,invitation}),client=createLanPairingClient({...options,...override});
    try{await assert.rejects(exchange(host,client));assert.equal(host.used,false);}
    finally{host.close();client.close();}
  }
});
test('five attempts include parallel callers; unauthenticated clients do not own the host',async()=>{
  const host=createLanPairingHost({...options,invitation});
  const wrong=Array.from({length:4},()=>createLanPairingClient({...options,password:'00000'}));
  try {
    await Promise.all(wrong.map(client=>assert.rejects(exchange(host,client))));
    const client=createLanPairingClient(options);
    try{assert.equal(await exchange(host,client),invitation);assert.equal(host.attempts,5);}
    finally{client.close();}
  }finally{host.close();wrong.forEach(client=>client.close());}
  const limited=createLanPairingHost({...options,invitation});
  try{
    for(let i=0;i<5;i++)limited.begin(peerPublicKey,i.toString(16).padStart(32,'0'));
    assert.throws(()=>limited.begin(peerPublicKey,'f'.repeat(32)),/limit/);
  }finally{limited.close();}
});
test('simultaneous valid finishes can consume only one invitation',async()=>{
  const host=createLanPairingHost({...options,invitation});
  const clients=[createLanPairingClient(options),createLanPairingClient(options)];
  try{
    const packets=await Promise.all(clients.map(c=>c.answer(host.begin(peerPublicKey,c.hello))));
    const results=await Promise.allSettled(clients.map((c,i)=>host.finish(peerPublicKey,c.hello,packets[i])));
    assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
  }finally{host.close();clients.forEach(c=>c.close());}
});
test('expiry and cancellation are checked on actions without polling',async()=>{
  for(const end of ['expire','close']){
    let now=0;
    const host=createLanPairingHost({...options,invitation,now:()=>now,ttlMs:100});
    const client=createLanPairingClient(options);
    try{
      const packet=await client.answer(host.begin(peerPublicKey,client.hello));
      if(end==='expire')now=100;else host.close();
      await assert.rejects(host.finish(peerPublicKey,client.hello,packet));
    }finally{host.close();client.close();}
  }
});
test('online six-character PAKE remains compatible',async()=>{
  const host=createShortPairingHost({code:'ABC123',hostPublicKey,invitation,pake});
  const client=createShortPairingClient({code:'ABC123',hostPublicKey,peerPublicKey,pake});
  try{assert.equal(await exchange(host,client),invitation);}finally{host.close();client.close();}
});

test('PIN leading zeroes, fixed global budget and transcript binding',async()=>{
  const {createPinPairingHost,createPinPairingClient}=await import('@phantom-local/doorways/local');
  const room='c'.repeat(48),pinOptions={room,pin:'0007',hostPublicKey,peerPublicKey,pake};
  for(const pin of [7,'007','00007','abcd'])assert.throws(()=>createPinPairingClient({...pinOptions,pin}));
  for(const override of [{pin:'0008'},{room:'d'.repeat(48)},{hostPublicKey:'d'.repeat(64)}]){
    const host=createPinPairingHost({...pinOptions,invitation}),client=createPinPairingClient({...pinOptions,...override});
    try{await assert.rejects(exchange(host,client));assert.equal(host.used,false);}finally{host.close();client.close();}
  }
  const limited=createPinPairingHost({...pinOptions,invitation,maxAttempts:10});
  try{
    for(let i=0;i<3;i++)limited.begin(peerPublicKey,i.toString(16).padStart(32,'0'));
    assert.throws(()=>limited.begin(peerPublicKey,'f'.repeat(32)),/limit/);
  }finally{limited.close();}
  const host=createPinPairingHost({...pinOptions,invitation});
  const clients=[createPinPairingClient(pinOptions),createPinPairingClient(pinOptions)];
  try{
    const packets=await Promise.all(clients.map(c=>c.answer(host.begin(peerPublicKey,c.hello))));
    const results=await Promise.allSettled(clients.map((c,i)=>host.finish(peerPublicKey,c.hello,packets[i])));
    assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
    const winner=results.findIndex(r=>r.status==='fulfilled');
    assert.equal(await clients[winner].open(results[winner].value),invitation);
  }finally{host.close();clients.forEach(c=>c.close());}
});
