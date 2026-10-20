import test from "node:test";
import assert from "node:assert/strict";
import { createNativeSaver } from "../src/native-save.js";
function bridge(handler) {
  const packets = [];
  const api = {
    postMessage(text) {
      const packet = JSON.parse(text);
      packets.push(packet);
      queueMicrotask(() =>
        handler(packet, (result, ok = true) =>
          api.onmessage({
            data: JSON.stringify({
              id: packet.id,
              ok,
              result,
              error: "write failed",
            }),
          }),
        ),
      );
    },
  };
  return { api, packets };
}
test("native saves stream ordered bounded chunks with one acknowledgement in flight", async () => {
  const bytes = crypto.getRandomValues(new Uint8Array(60000)),
    blob = new Blob([bytes, bytes, bytes]);
  const received = [];
  let inFlight = 0,
    max = 0;
  const b = bridge((packet, reply) => {
    inFlight++;
    max = Math.max(max, inFlight);
    setTimeout(() => {
      if (packet.action === "chunk")
        received.push(Buffer.from(packet.data, "base64"));
      inFlight--;
      reply(packet.action === "begin" ? { token: "native-token" } : true);
    }, 1);
  });
  assert.equal(await createNativeSaver(b.api).save(blob, "test.bin"), true);
  assert.equal(max, 1);
  assert.deepEqual(
    Buffer.concat(received),
    Buffer.from(await blob.arrayBuffer()),
  );
  assert.deepEqual(
    b.packets.filter((p) => p.action === "chunk").map((p) => p.index),
    [0, 1, 2],
  );
  assert.ok(received.every((b) => b.length <= 65536));
  assert.equal(b.packets.at(-1).action, "finish");
});
test("cancelled picker and empty files have no data chunks", async () => {
  const cancelled = bridge((packet, reply) => reply(null));
  assert.equal(
    await createNativeSaver(cancelled.api).save(new Blob(["x"]), "x"),
    false,
  );
  assert.equal(cancelled.packets.length, 1);
  const empty = bridge((packet, reply) =>
    reply(packet.action === "begin" ? { token: "t" } : true),
  );
  assert.equal(
    await createNativeSaver(empty.api).save(new Blob([]), "empty"),
    true,
  );
  assert.deepEqual(
    empty.packets.map((p) => p.action),
    ["begin", "finish"],
  );
});
test("reset rejects a pending save, ignores late replies and prevents concurrent pickers", async () => {
  let finish;
  const b = bridge((packet, reply) => {
    if (packet.action === "begin") finish = reply;
    else reply(true);
  });
  const saver = createNativeSaver(b.api);
  const first = saver.save(new Blob(["x"]), "one");
  await assert.rejects(saver.save(new Blob(["y"]), "two"), /current save/);
  await new Promise((resolve) => setTimeout(resolve, 0));
  saver.cancel();
  await assert.rejects(first, /cancelled/);
  finish({ token: "late" });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(
    b.packets.some((p) => p.action === "chunk"),
    false,
  );
});
test("write failure aborts destination and never reports successful finish", async () => {
  const b = bridge((packet, reply) =>
    reply(
      packet.action === "begin" ? { token: "t" } : true,
      packet.action !== "chunk",
    ),
  );
  await assert.rejects(
    createNativeSaver(b.api).save(new Blob(["data"]), "x"),
    /write failed/,
  );
  assert.ok(b.packets.some((p) => p.action === "cancel"));
  assert.equal(
    b.packets.some((p) => p.action === "finish"),
    false,
  );
});

test('cancel while reading a chunk cannot submit a late write',async()=>{
 let release,reading;
 const started=new Promise(resolve=>reading=resolve);
 const b=bridge((packet,reply)=>reply(packet.action==='begin'?{token:'t'}:true));
 const blob={size:1,type:'application/octet-stream',slice(){return {arrayBuffer(){reading();return new Promise(resolve=>release=resolve);}};}};
 const saver=createNativeSaver(b.api),save=saver.save(blob,'x');await started;saver.cancel();release(new Uint8Array([1]).buffer);
 await assert.rejects(save,error=>error.code==='SAVE_CANCELLED');assert.equal(b.packets.some(p=>p.action==='chunk'),false);
});

test('native connection permission is acknowledged before networking can start',async()=>{
 const b=bridge((packet,reply)=>reply(true));await createNativeSaver(b.api).prepareConnection();assert.equal(b.packets[0].action,'network');
 const denied=bridge((packet,reply)=>reply(null,false));await assert.rejects(createNativeSaver(denied.api).prepareConnection(),/write failed/);
});

test('session lease sends transfer state and old close keeps its own token',async()=>{
 const b=bridge((packet,reply)=>reply(true)), saver=createNativeSaver(b.api);
 const old=await saver.startSession(), current=await saver.startSession();
 old.setTransferActive(true); old.close(); current.setTransferActive(true); current.setTransferActive(false); current.close();
 await new Promise(resolve=>setTimeout(resolve,0));
 const starts=b.packets.filter(p=>p.action==='startSession');
 assert.notEqual(starts[0].token,starts[1].token);
 assert.deepEqual(b.packets.filter(p=>p.action==='endSession').map(p=>p.token),starts.map(p=>p.token));
 assert.deepEqual(b.packets.filter(p=>p.action==='transfer').map(p=>p.active),[true,true,false]);
 const count=b.packets.length;old.setTransferActive(true);old.close();assert.equal(b.packets.length,count);
});

test('network callback from an older native session cannot own a replacement',async()=>{
 const b=bridge((packet,reply)=>reply(true)),saver=createNativeSaver(b.api);
 const first=await saver.startSession(),old=b.packets.find(p=>p.action==='startSession').token;
 assert.equal(saver.ownsSession(old),true);
 const second=await saver.startSession(),current=b.packets.filter(p=>p.action==='startSession').at(-1).token;
 first.close();assert.equal(saver.ownsSession(old),false);assert.equal(saver.ownsSession(current),true);
 assert.equal(saver.ownsSession(undefined),false);second.close();assert.equal(saver.ownsSession(current),false);
});


test('native close waits for its acknowledgement and repeated close shares completion',async()=>{
 let finish;
 const b=bridge((packet,reply)=>{if(packet.action==='endSession')finish=reply;else reply(true);});
 const saver=createNativeSaver(b.api),session=await saver.startSession();
 const closing=session.close();assert.equal(session.close(),closing);
 let done=false;closing.then(()=>done=true);await new Promise(setImmediate);
 assert.equal(done,false);assert.equal(saver.ownsSession(b.packets[0].token),false);
 finish(true);await closing;assert.equal(done,true);
 assert.equal(b.packets.filter(p=>p.action==='endSession').length,1);
});
