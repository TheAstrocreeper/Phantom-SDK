import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as wait} from 'node:timers/promises';
import {createRequire} from 'node:module';
import {createPairingCode} from '../packages/phantom-core/src/pairing-code.js';
import {openPairingChannel} from '../packages/phantom-doorways/src/pairing-channel.js';
import {openShortPairingChannel} from '../packages/phantom-doorways/src/short-pairing-channel.js';
const pake = createRequire(import.meta.url)('spake2-wasm');
const identity = n => ({publicKey:n.toString(16).padStart(16,'0')+'0'.repeat(48)});

// Push discovery, as the authenticated CF adapter does; count every client write.
function wire({dropResponse=false}={}) {
  const rooms = new Map(), traffic = [];
  let dropped = false;
  return {
    traffic,
    doorway(who) {
      return {open(context) {
        let room = rooms.get(context.roomId);
        if (!room) rooms.set(context.roomId,room=new Map());
        const self={context,row:null};
        room.set(context.peerId,self);
        const roster=()=> {
          const peers=[...room.values()].flatMap(p=>p.row?[p.row]:[]);
          for(const peer of room.values()) peer.context.onDiscovery({peers});
        };
        queueMicrotask(context.onOpen);
        return {
          announce(row) {
            traffic.push('announce');
            self.row={...row,hKey:row.peerId,hex:who.publicKey};
            roster();
          },
          reconcile() {traffic.push('reconcile');roster();},
          sendSignal(packet) {
            traffic.push('signal');
            if(dropResponse&&!dropped&&packet.from===identity(1).publicKey.slice(0,16)) {
              dropped=true;return;
            }
            queueMicrotask(()=>room.get(packet.to)?.context.onSignal(packet));
          },
          close() {room.delete(context.peerId);roster();},
        };
      }};
    },
  };
}

test('waiting invitation and retained membership channel emit no idle traffic; late discovery works', async () => {
  const bus=wire(), code=await createPairingCode(), hostId=identity(1), guestId=identity(2);
  const host=await openPairingChannel({code,identity:hostId,doorway:bus.doorway(hostId),role:'host',onRequest:({action})=>action});
  let guest;
  try {
    await wait(20);
    const waiting=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,waiting,'waiting host must not poll');
    guest=await openPairingChannel({code,identity:guestId,doorway:bus.doorway(guestId)});
    assert.equal(await guest.request('context',{}),'context');
    await wait(20);
    const connected=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,connected,'completed RPC must retire retry traffic');
    assert.equal(await guest.request('leave',{}),'leave','retained channel remains usable');
  } finally {guest?.close();host.close();}
});

test('short-code host stays quiet while waiting and after consumption; late guest pairs', async () => {
  const bus=wire(), hostId=identity(1), guestId=identity(2);
  const host=await openShortPairingChannel({code:'7KH9T2',identity:hostId,doorway:bus.doorway(hostId),role:'host',invitation:'invitation',pake});
  let guest;
  try {
    await wait(20);
    const waiting=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,waiting,'short-code host must not poll');
    guest=await openShortPairingChannel({code:'7KH9T2',identity:guestId,doorway:bus.doorway(guestId),pake});
    assert.equal((await guest.result).invitation,'invitation');
    await wait(20);
    const paired=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,paired,'consumed short-code host must not poll');
  } finally {guest?.close();host.close();}
});

test('outstanding encrypted RPC recovers a dropped reply, then stops retrying', async () => {
  const bus=wire({dropResponse:true}), code=await createPairingCode(), hostId=identity(1), guestId=identity(2);
  let calls=0;
  const host=await openPairingChannel({code,identity:hostId,doorway:bus.doorway(hostId),role:'host',onRequest:()=>++calls});
  const guest=await openPairingChannel({code,identity:guestId,doorway:bus.doorway(guestId)});
  try {
    await wait(20);
    assert.equal(await guest.request('redeem',{}),1);
    assert.equal(calls,1,'retry reuses the existing request id');
    assert.ok(bus.traffic.includes('reconcile'));
    await wait(20);
    const completed=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,completed);
  } finally {guest.close();host.close();}
});

test('cancellation and request expiry stop retry traffic without closing the channel', async () => {
  const bus=wire(), who=identity(2), code=await createPairingCode();
  const guest=await openPairingChannel({code,identity:who,doorway:bus.doorway(who),timeoutMs:40});
  try {
    const controller=new AbortController();
    const pending=guest.request('context',{}, {signal:controller.signal});
    controller.abort();
    await assert.rejects(pending,/cancelled/);
    await assert.rejects(guest.request('context',{}),/timed out/);
    await wait(20);
    const finished=bus.traffic.length;
    await wait(1100);
    assert.equal(bus.traffic.length,finished);
  } finally {guest.close();}
});
