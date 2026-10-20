import {createRequire} from 'node:module';
const testPake=createRequire(import.meta.url)('spake2-wasm');
import test from "node:test";
import assert from "node:assert/strict";
import {
  createPairingCode,
  parsePairingCode,
  pairingCoordinates,
  sealPairingMessage,
  openPairingMessage,
} from "../packages/phantom-core/src/pairing-code.js";
import { createAdminIdentity } from "../packages/phantom-core/src/authorization.js";
import {
  createMembershipAuthority,
  createMemoryMembershipStore,
} from "../packages/phantom-core/src/membership.js";
import {
  createMembershipPairingHost,
  connectMembershipPairing,
  verifyMembershipPeerRequest,
} from "../packages/phantom-doorways/src/membership-pairing.js";
import { createNostrIdentity } from "../packages/phantom-doorways/src/nostr-crypto.js";
import { encodeInvite } from "../packages/phantom-core/src/invite.js";
import { createDoorway } from "../packages/phantom-doorways/src/interface.js";
import { createAdmissionCheckpoint } from "../packages/phantom-core/src/admission.js";
const room = "a".repeat(64);
function bus() {
  const rooms = new Map();
  return () =>
    createDoorway({
      discovery: true,
      presence: "live-sockets",
      open(context) {
        let state = rooms.get(context.roomId);
        if (!state)
          rooms.set(
            context.roomId,
            (state = {
              members: new Map(),
              epoch: 0,
              leader: null,
              mirror: null,
            }),
          );
        const members = state.members;
        queueMicrotask(context.onOpen);
        const update = () => {
          const keys = [...members.keys()].sort(),
            leader = keys.includes(state.leader) ? state.leader : keys[0],
            mirror =
              keys.includes(state.mirror) && state.mirror !== leader
                ? state.mirror
                : keys.find((k) => k !== leader);
          if (leader !== state.leader || mirror !== state.mirror) state.epoch++;
          state.leader = leader;
          state.mirror = mirror;
          for (const peer of members.values())
            peer.context.onDiscovery({
              kind: "concierge_update",
              roomId: context.roomId,
              epoch: state.epoch,
              leader,
              mirror,
              peers: [...members.values()].map((p) => p.row),
            });
        };
        return {
          announce(v) {
            members.set(context.peerId, {
              context,
              row: {
                hKey: context.peerId,
                hex:
                  v.hex ??
                  context.authorization?.peerPublicKey ??
                  context.peerId.padEnd(64, "0"),
                handle: v.handle,
                from: context.peerId,
                bootId: v.bootId,
                roomJoinedAt: v.t,
                admissionProof: v.admissionProof,
              },
            });
            update();
          },
          reconcile() {
            update();
          },
          sendSignal(p) {
            queueMicrotask(() => members.get(p.to)?.context.onSignal(p));
          },
          close() {
            if (members.get(context.peerId)?.context === context) {
              members.delete(context.peerId);
              update();
            }
          },
        };
      },
    });
}
// The real Nostr doorway cryptographically binds this full key before routing.
function factory(bus, identity) {
  return () => {
    const raw = bus();
    return {
      ...raw,
      open(context) {
        const session = raw.open(context);
        return {
          ...session,
          announce: (v) => session.announce({ ...v, hex: identity.publicKey }),
        };
      },
    };
  };
}
async function fixture({pake,onCodeUsed}={}) {
  const admin = await createAdminIdentity(),
    identity = createNostrIdentity(),
    authority = await createMembershipAuthority({
      networkId: room,
      adminIdentity: admin,
      store: createMemoryMembershipStore(),
      verifyPeerRequest: verifyMembershipPeerRequest,
    });
  const wire = bus(),
    invite = encodeInvite({
      networkId: room,
      adminPublicKey: admin.publicKey,
      doorways: [{ type: "nostr", relays: ["wss://relay.example"] }],
    });
  const host = createMembershipPairingHost({
    identity,
    authority,
    invite,
    doorwayFactory: factory(wire, identity),
    canIssueInvite: () => true,
    onCodeUsed,
    pake,
  });
  const clients = [];
  async function client(code, identity = createNostrIdentity()) {
    const result = await connectMembershipPairing({
      code,
      identity,
      doorwayFactory: factory(wire, identity),
      pake,
    });
    clients.push(result);
    return { client: result, identity };
  }
  return {
    host,
    authority,
    client,
    wire,
    close() {
      for (const c of clients) c.close();
      host.close();
    },
  };
}
test("pairing code round-trips 128 random bits with a typing checksum", async () => {
  const code = await createPairingCode(),
    parsed = await parsePairingCode(code.toLowerCase().replaceAll("-", " "));
  assert.equal(parsed.code, code);
  assert.equal(parsed.seed.length, 16);
  assert.match(code, /^P1-(?:[0-9A-HJKMNP-TV-Z]{4}-){6}[0-9A-HJKMNP-TV-Z]{4}$/);
  await assert.rejects(
    parsePairingCode(code.slice(0, -1) + (code.endsWith("0") ? "1" : "0")),
  );
});
test("pairing encryption is scoped to the code, authenticates payloads, and uses fresh nonces", async () => {
  const a = await pairingCoordinates(await createPairingCode()),
    b = await pairingCoordinates(await createPairingCode());
  const first = await sealPairingMessage(a, { hello: "world" }),
    second = await sealPairingMessage(a, { hello: "world" });
  assert.notEqual(first.iv, second.iv);
  assert.deepEqual(await openPairingMessage(a, first), { hello: "world" });
  await assert.rejects(openPairingMessage(b, first));
  await assert.rejects(
    openPairingMessage(a, {
      ...first,
      ciphertext:
        (first.ciphertext[0] === "0" ? "1" : "0") + first.ciphertext.slice(1),
    }),
  );
});
test("code enrollment uses the authority; another identity cannot reuse it; leave needs a fresh code", async () => {
  const f = await fixture();
  let checkpoint;
  try {
    const invitation = await f.host.newInvite(),
      a = await f.client(invitation.code);
    checkpoint = createAdmissionCheckpoint({
      policy: a.client.admission,
      networkId: room,
      peerPublicKey: a.identity.publicKey,
    });
    const grant = await checkpoint.authorize();
    assert.equal(
      await f.authority.verify({
        networkId: room,
        peerPublicKey: a.identity.publicKey,
        credential: grant,
      }),
      true,
    );
    await assert.rejects(f.client(invitation.code), /already used/);
    const next = await a.client.newInvite();
    assert.notEqual(next.code, invitation.code);
    const request = {
      peerPublicKey: a.identity.publicKey,
      membershipId: grant.membershipId,
      proof: a.identity.signPeerProof({
        networkId: room,
        kind: "membership.leave",
        credential: null,
        payload: {
          action: "membership.leave",
          membershipId: grant.membershipId,
        },
        issuedAt: Date.now(),
        nonce: crypto.randomUUID(),
      }),
    };
    await a.client.admission.leave({
      networkId: room,
      peerPublicKey: a.identity.publicKey,
      credential: grant,
      request,
    });
    await assert.rejects(f.client(invitation.code, a.identity), /inactive/);
    const returned = await f.client(next.code, a.identity),
      second = createAdmissionCheckpoint({
        policy: returned.client.admission,
        networkId: room,
        peerPublicKey: a.identity.publicKey,
      });
    try {
      assert.notEqual(
        (await second.authorize()).membershipId,
        grant.membershipId,
      );
    } finally {
      second.close();
    }
  } finally {
    checkpoint?.close();
    f.close();
  }
});

test(
  "pairing supports verified transfers, abrupt recovery, Remove and repeated Leave/rejoin",
  { timeout: 60000 },
  async () => {
    const { createBrowserPhantom, createBrowserIdentity } = await import(
      "../packages/phantom-doorways/src/browser-entry.js"
    );
    const { createMembershipAdmission } = await import(
      "../packages/phantom-core/src/membership-admission.js"
    );
    const { memoryRTC } = await import("./fixtures/memory-rtc.js");
    const { createShareSession } = await import(
      "../demos/phantom-share/src/session.js"
    );
    const admin = await createAdminIdentity(),
      // Lower guest key owns the messaging invite after recovering. This
      // exposes a stale accepted relationship on the surviving owner.
      seeds = [createBrowserIdentity(),createBrowserIdentity()].sort((a,b)=>createNostrIdentity(a).publicKey.localeCompare(createNostrIdentity(b).publicKey)),
      ownerSeed = seeds[1],
      ownerIdentity = createNostrIdentity(ownerSeed),
      guestSeed = seeds[0],
      guestIdentity = createNostrIdentity(guestSeed),
      wire = bus(),
      RTC = class extends (memoryRTC()) { async setRemoteDescription(value) { await new Promise(r=>setTimeout(r,25)); return super.setRemoteDescription(value); } },
      store = createMemoryMembershipStore();
    const networks = [],
      sessions = [];
    let authority, host, client, pairingOffline=false;
    const trace=[];
    const record = side => ({type,payload={}}) => {trace.push({side,type,reason:payload.reason,peer:payload.peer,from:payload.from,state:payload.state});if(trace.length>100)trace.shift();};
    const previousOffers=new Map();
    const pairingFactory=identity=>()=>{
      const adapter=factory(wire,identity)();
      return {...adapter,open(context){const session=adapter.open(context);return {...session,sendSignal(packet){if(pairingOffline)throw Error('Fixture pairing relays offline');const sent=session.sendSignal(packet); if(packet.type==='rtc_offer'){const key=packet.from+':'+packet.to,previous=previousOffers.get(key);previousOffers.set(key,structuredClone(packet));setTimeout(()=>{try{session.sendSignal(packet);if(previous)session.sendSignal(previous);}catch{}},5);}return sent;}};}};
    };
    const sign = (identity, networkId, action, fields) => ({
      peerPublicKey: identity.publicKey,
      proof: identity.signPeerProof({
        networkId,
        kind: action,
        credential: null,
        payload: { action, ...fields },
        issuedAt: Date.now(),
        nonce: crypto.randomUUID(),
      }),
    });
    const ownerAdmission = createMembershipAdmission({
      authorityPublicKey: admin.publicKey,
      authorize: async ({ networkId }) => {
        authority = await createMembershipAuthority({
          networkId,
          adminIdentity: admin,
          store,
          verifyPeerRequest: verifyMembershipPeerRequest,
        });
        const i = await authority.issueInvite(),
          inviteId = Array.from(
            new Uint8Array(
              await crypto.subtle.digest(
                "SHA-256",
                new TextEncoder().encode(i.token),
              ),
            ),
            (b) => b.toString(16).padStart(2, "0"),
          ).join("");
        return authority.redeem({
          token: i.token,
          request: sign(ownerIdentity, networkId, "membership.redeem", {
            inviteId,
          }),
        });
      },
      readRevocations: () => authority.getRevocations(),
      subscribeRevocations: ({ onRevocation }) =>
        authority.onRevocation(onRevocation),
    });
    function sdk(identity, admission, adminIdentity) {
      const values = new Map();
      return createBrowserPhantom({
        identity,
        admission,
        adminIdentity,
        messaging: "automatic",
        distribution: { intervalMs: 10 },
        doorways: [{ type: "memory" }],
        adapters: { memory: ({ identity }) => pairingFactory(identity)() },
        runtime: {
          crypto,
          RTCPeerConnection: RTC,
          setTimeout,
          clearTimeout,
          setInterval,
          clearInterval,
          localStorage: {
            getItem: (k) => values.get(k) ?? null,
            setItem: (k, v) => values.set(k, v),
          },
        },
      });
    }
    const wait = async (check, label) => {
      const until = Date.now() + 10000;
      while (Date.now() < until) {
        if (check()) return;
        await new Promise((r) => setTimeout(r, 10));
      }
      if(label === "abrupt disconnect recovery") console.error(JSON.stringify({states:networks.map(n=>({state:n.getState(),peers:n.getPeers(),connections:n.getPeerConnections()})),trace}));
      throw Error("Timed out: " + label);
    };
    try {
      const owner = await sdk(ownerSeed, ownerAdmission, admin).create({
        name: "Owner", onEvent:record("owner"),
      });
      networks.push(owner);
      host = createMembershipPairingHost({
        identity: ownerIdentity,
        authority,
        invite: owner.invite,
        doorwayFactory: pairingFactory(ownerIdentity),
      });
      host.bindNetwork(owner);
      const invite = await host.newInvite();
      client = await connectMembershipPairing({
        code: invite.code,
        identity: guestIdentity,
        doorwayFactory: pairingFactory(guestIdentity),
      });
      let guest = await sdk(guestSeed, client.admission).join(client.invite, {
        name: "Guest",
        timeoutMs: 10000,
      });
      networks.push(guest);
      client.bindNetwork(guest);
      await wait(
        () =>
          [owner, guest].every((n) =>
            n.getPeerConnections().some((p) => p.state === "ready"),
          ),
        "ready peers",
      );
      pairingOffline=true;
      // Remain admitted beyond the original 15-second lease with bootstrap signaling unavailable.
      await new Promise(r=>setTimeout(r,16000));
      assert.equal(guest.getState().state,'admitted');
      await guest.refreshMembership();
      const errors = [],
        ownerSession = createShareSession({
          network: owner,
          admin,
          adminPublicKey: admin.publicKey,
          onError: (e) => errors.push(e),
        }),
        guestSession = createShareSession({
          network: guest,
          adminPublicKey: admin.publicKey,
          onError: (e) => errors.push(e),
        });
      sessions.push(ownerSession, guestSession);
      // Owner proof and hello travel over the actual SDK messaging channels.
      await new Promise((r) => setTimeout(r, 30));
      const bytes = new TextEncoder().encode(
        "member publication after code pairing".repeat(1024),
      );
      const asset = await guestSession.publish(new Blob([bytes]), [
        owner.getState().peerId,
      ]);
      await wait(
        () => ownerSession.assets.get(asset.id)?.status === "received",
        "verified owner receipt",
      );
      assert.deepEqual(
        new Uint8Array(
          await ownerSession.assets.get(asset.id).blob.arrayBuffer(),
        ),
        bytes,
      );
      assert.equal(asset.proof.publicKey, admin.publicKey);
      assert.deepEqual(errors, []);
      // Abrupt local reset cannot notify the authority. A fresh invite must
      // recover this identity's active grant over the actual pairing protocol.
      pairingOffline = false;
      guest.close(); client.close();
      const recovery = await host.newInvite();
      client = await connectMembershipPairing({code:recovery.code,identity:guestIdentity,doorwayFactory:pairingFactory(guestIdentity)});
      guest = await sdk(guestSeed, client.admission).join(client.invite,{name:"Recovered guest",timeoutMs:10000,onEvent:record("recovered")});
      networks.push(guest); client.bindNetwork(guest);
      await wait(()=>[owner,guest].every(n=>n.getPeerConnections().some(p=>p.state==="ready")),"abrupt disconnect recovery");
      await guest.refreshMembership();
      const recoveredSession = createShareSession({network:guest,adminPublicKey:admin.publicKey,onError:e=>errors.push(e)});
      sessions.push(recoveredSession);
      // Respect the owner's 500 ms publication authorization rate limit.
      await new Promise(resolve=>setTimeout(resolve,600));
      const recoveredBytes = new TextEncoder().encode("verified transfer after abrupt recovery");
      const recoveredAsset = await recoveredSession.publish(new Blob([recoveredBytes]),[owner.getState().peerId]);
      await wait(()=>ownerSession.assets.get(recoveredAsset.id)?.status==="received","verified transfer after recovery");
      assert.deepEqual(new Uint8Array(await ownerSession.assets.get(recoveredAsset.id).blob.arrayBuffer()),recoveredBytes);
      assert.deepEqual(errors,[]);
      await host.removePeer(guest.getState().peerId);
      await wait(
        () => guest.getState().state === "closed",
        "removed paired member closes",
      );
      pairingOffline = false;
      client.close();
      for (let cycle = 0; cycle < 2; cycle++) {
        const fresh = await host.newInvite();
        client = await connectMembershipPairing({code:fresh.code,identity:guestIdentity,doorwayFactory:pairingFactory(guestIdentity)});
        const returned = await sdk(guestSeed, client.admission).join(client.invite,{name:"Guest",timeoutMs:10000});
        networks.push(returned);
        client.bindNetwork(returned);
        await wait(()=>[owner,returned].every(n=>n.getPeerConnections().some(p=>p.state==="ready")),"fresh rejoin ready cycle "+cycle);
        await returned.leave();
        await wait(()=>returned.getState().state==="closed","left member closes cycle "+cycle);
        client.close();
      }

    } finally {
      for (const s of sessions) s.close();
      for (const n of networks) n.close();
      client?.close();
      host?.close();
    }
  },
);

test("an unauthenticated host announcement cannot pin the client away from the real inviter", async () => {
  const f = await fixture(),
    identity = createNostrIdentity(),
    fake = createNostrIdentity();
  let client, checkpoint, rogue;
  try {
    const invitation = await f.host.newInvite(),
      { roomId } = await pairingCoordinates(invitation.code);
    rogue = factory(f.wire, fake)().open({
      roomId,
      peerId: fake.publicKey.slice(0, 16),
      onOpen() {},
      onDiscovery() {},
      onSignal() {},
      onClose() {},
      onError() {},
    });
    rogue.announce({
      roomId,
      peerId: fake.publicKey.slice(0, 16),
      handle: "phantom-pairing-host",
      bootId: "fake",
      t: Date.now(),
    });
    const raw = factory(f.wire, identity);
    client = await connectMembershipPairing({
      code: invitation.code,
      identity,
      doorwayFactory: () => {
        const door = raw();
        return {
          ...door,
          open: (context) =>
            door.open({
              ...context,
              onDiscovery: (hint) =>
                context.onDiscovery({
                  ...hint,
                  peers: [...hint.peers].sort(
                    (a, b) =>
                      (b.hKey === fake.publicKey.slice(0, 16)) -
                      (a.hKey === fake.publicKey.slice(0, 16)),
                  ),
                }),
            }),
        };
      },
    });
    checkpoint = createAdmissionCheckpoint({
      policy: client.admission,
      networkId: room,
      peerPublicKey: identity.publicKey,
    });
    assert.equal(
      (await checkpoint.authorize()).peerPublicKey,
      identity.publicKey,
    );
  } finally {
    checkpoint?.close();
    client?.close();
    rogue?.close();
    f.close();
  }
});

test('an enrollment abandoned before transport admission releases its grant for a fresh invite',async()=>{
 const f=await fixture();let checkpoint;
 try{
  const first=await f.host.newInvite(),a=await f.client(first.code);
  checkpoint=createAdmissionCheckpoint({policy:a.client.admission,networkId:room,peerPublicKey:a.identity.publicKey});
  const grant=await checkpoint.authorize();
  await a.client.releaseMembership();
  assert.equal(await f.authority.verify({networkId:room,peerPublicKey:a.identity.publicKey,credential:grant}),false);
  const fresh=await f.host.newInvite(),returned=await f.client(fresh.code,a.identity);
  const next=createAdmissionCheckpoint({policy:returned.client.admission,networkId:room,peerPublicKey:a.identity.publicKey});
  try{assert.notEqual((await next.authorize()).membershipId,grant.membershipId);}finally{next.close();}
 }finally{checkpoint?.close();f.close();}
});

test('six-character enrollment and high-entropy link reach the same single-use membership',async()=>{
 const used=[];const f=await fixture({pake:testPake,onCodeUsed:event=>used.push(event.code)});let checkpoint,secondCheckpoint;
 try{
  const invite=await f.host.newInvite();assert.match(invite.code,/^[0-9A-HJKMNP-TV-Z]{6}$/);
  assert.match(invite.linkCode,/^P1-/);assert.equal(invite.codeExpiresAt,invite.expiresAt);
  assert.ok(invite.expiresAt-Date.now()<=240000);
  assert.ok(invite.expiresAt-Date.now()>230000, "code and link remain valid for four minutes");
  const first=await f.client(invite.code.toLowerCase());
  assert.deepEqual(used,[invite.code]);
  checkpoint=createAdmissionCheckpoint({policy:first.client.admission,networkId:room,peerPublicKey:first.identity.publicKey});
  const grant=await checkpoint.authorize();assert.equal(grant.peerPublicKey,first.identity.publicKey);
  // QR/link cannot grant a different identity after the short-code enrollment.
  await assert.rejects(f.client(invite.linkCode),/already used/);
  await first.client.releaseMembership();
  const fresh=await f.host.newInvite(),second=await f.client(fresh.linkCode,first.identity);
  secondCheckpoint=createAdmissionCheckpoint({policy:second.client.admission,networkId:room,peerPublicKey:second.identity.publicKey});
  assert.notEqual((await secondCheckpoint.authorize()).membershipId,grant.membershipId);
 }finally{checkpoint?.close();secondCheckpoint?.close();f.close();}
});
