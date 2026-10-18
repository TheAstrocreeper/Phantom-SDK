import { openMembershipPeerChannel } from "./membership-peer-channel.js";
import {
  createPairingCode,
  parsePairingCode,
  pairingCoordinates,
} from "../../phantom-core/src/pairing-code.js";
import { createMembershipAdmission } from "../../phantom-core/src/membership-admission.js";
import { parseInvite } from "../../phantom-core/src/invite.js";
import { verifyPeerProof } from "./peer-proof.js";
import { openPairingChannel } from "./pairing-channel.js";
const digest = async (token) =>
  Array.from(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token)),
    ),
    (b) => b.toString(16).padStart(2, "0"),
  ).join("");
function signed(identity, networkId, action, fields = {}) {
  return {
    peerPublicKey: identity.publicKey,
    ...(fields.membershipId ? { membershipId: fields.membershipId } : {}),
    proof: identity.signPeerProof({
      networkId,
      kind: action,
      credential: null,
      payload: { action, ...fields },
      issuedAt: Date.now(),
      nonce: crypto.randomUUID(),
    }),
  };
}
export function verifyMembershipPeerRequest({
  networkId,
  action,
  inviteId,
  membershipId,
  request,
}) {
  return (
    request?.peerPublicKey === request?.proof?.peerPublicKey &&
    verifyPeerProof(request.proof, {
      networkId,
      kind: action,
      payload: {
        action,
        ...(inviteId ? { inviteId } : {}),
        ...(membershipId ? { membershipId } : {}),
      },
    })
  );
}
/** A local authority serves enrollment over existing doorway channels, not a code lookup backend. */
export function createMembershipPairingHost({
  identity,
  authority,
  invite,
  doorwayFactory,
  canIssueInvite = () => false,
  onClaim = () => {},
  onError = () => {},
  maxChannels = 16,
} = {}) {
  const context = parseInvite(invite);
  if (
    context.networkId !== authority?.networkId ||
    typeof doorwayFactory !== "function" ||
    typeof canIssueInvite !== "function"
  )
    throw new TypeError(
      "Matching authority, invitation and doorway factory required",
    );
  const channels = new Set(),
    credentials = new Map();
  let peerChannel;
  let closed = false,
    reservations = 0;
  const off = authority.onRevocation((record) => {
    for (const entry of channels)
      if (entry.claimedKey) {
        try { peerChannel?.notify(entry.claimedKey,{type:'revocation',record}); } catch {}
        void entry.channel
          .notify(entry.claimedKey, { type: "revocation", record })
          .catch(onError);
        if (
          entry.claimedKey === record.peerPublicKey &&
          entry.membershipId === record.membershipId
        ) {
          clearTimeout(entry.timer);
          entry.timer = setTimeout(() => {
            entry.channel.close();
            channels.delete(entry);
          }, 16000);
          entry.timer.unref?.();
        }
      }
  });
  async function newInvite() {
    if (closed) throw new Error("Inviter closed");
    if (channels.size + reservations >= maxChannels)
      throw new Error(
        "Too many open pairing channels. Wait for unused invitations to expire.",
      );
    reservations++;
    try {
      const invitation = await authority.issueInvite(),
        code = await createPairingCode(),
        coordinates = await pairingCoordinates(code);
      const entry = {
        code,
        invitation,
        channel: null,
        claimedKey: null,
        timer: null,
      };
      const checkPeer = (request, peerPublicKey, action, fields = {}) => {
        if (
          request?.peerPublicKey !== peerPublicKey ||
          !verifyMembershipPeerRequest({
            networkId: context.networkId,
            action,
            request,
            ...fields,
          })
        )
          throw new Error("Invalid signed membership request");
      };
      entry.channel = await openPairingChannel({
        code,
        identity,
        role: "host",
        doorway: doorwayFactory(),
        onError,
        onRequest: entry.handleRequest = async ({ action, data, peerPublicKey }) => {
          if (closed) throw new Error("Inviter closed");
          if (entry.claimedKey && entry.claimedKey !== peerPublicKey)
            throw new Error("Invitation already used");
          if (!entry.claimedKey && Date.now() >= invitation.expiresAt)
            throw new Error("Invitation expired");
          if (action === "context") {
            if (
              data?.request?.peerPublicKey !== peerPublicKey ||
              !verifyPeerProof(data.request.proof, {
                networkId: coordinates.roomId,
                kind: "pairing.context",
                payload: { action: "pairing.context" },
              }) ||
              data.request.proof.peerPublicKey !== peerPublicKey
            )
              throw new Error("Invalid pairing context request");
            if (
              entry.claimedKey &&
              (entry.membershipId !==
                credentials.get(peerPublicKey)?.membershipId ||
                !(await authority.verify({
                  networkId: context.networkId,
                  peerPublicKey,
                  credential: credentials.get(peerPublicKey),
                })))
            )
              throw new Error(
                "Membership inactive; request a fresh invitation",
              );
            return {
              invite,
              authorityPublicKey: authority.authorityPublicKey,
              hostPeerPublicKey: identity.publicKey,
              token: invitation.token,
              expiresAt: invitation.expiresAt,
            };
          }
          if (action === "redeem") {
            const inviteId = await digest(invitation.token);
            checkPeer(data?.request, peerPublicKey, "membership.redeem", {
              inviteId,
            });
            const credential = await authority.redeem({
              token: invitation.token,
              request: data.request,
            });
            entry.claimedKey = peerPublicKey;
            entry.membershipId = credential.membershipId;
            credentials.set(peerPublicKey, credential);
            try {
              onClaim({ code, peerPublicKey, credential });
            } catch (error) {
              onError(error);
            }
            return credential;
          }
          if (!entry.claimedKey)
            throw new Error(
              "Redeem the invitation before requesting membership services",
            );
          if (action === "reconnect") {
            checkPeer(data?.request, peerPublicKey, "membership.reconnect");
            const credential = await authority.reconnect({
              request: data.request,
            });
            credentials.set(peerPublicKey, credential);
            return credential;
          }
          if (action === "leave") {
            checkPeer(data?.request, peerPublicKey, "membership.leave", {
              membershipId: data.request?.membershipId,
            });
            return authority.leave({ request: data.request });
          }
          if (action === "revocations") {
            checkPeer(data?.request, peerPublicKey, "membership.status", {
              membershipId: data.request?.membershipId,
            });
            if (
              credentials.get(peerPublicKey)?.membershipId !==
              data.request.membershipId
            )
              throw new Error("Membership generation changed");
            return authority.getRevocations();
          }
          if (action === "invite") {
            checkPeer(data?.request, peerPublicKey, "membership.invite", {
              membershipId: data.request?.membershipId,
            });
            if (
              data.request.membershipId !== data.credential?.membershipId ||
              !(await authority.verify({
                networkId: context.networkId,
                peerPublicKey,
                credential: data.credential,
              })) ||
              (await canIssueInvite({
                peerPublicKey,
                credential: data.credential,
              })) !== true
            )
              throw new Error("Invitation permission denied");
            return newInvite();
          }
          throw new Error("Unknown membership request");
        },
      });
      if (closed) {
        entry.channel.close();
        throw new Error("Inviter closed");
      }
      channels.add(entry);
      entry.timer = setTimeout(
        () => {
          if (!entry.claimedKey) {
            entry.channel.close();
            channels.delete(entry);
          }
        },
        Math.max(1, invitation.expiresAt - Date.now()),
      );
      entry.timer.unref?.();
      return { code, expiresAt: invitation.expiresAt };
    } finally {
      reservations--;
    }
  }
  return {
    bindNetwork(network) {
      if(closed||network.networkId!==context.networkId||network.getState().peerId!==identity.publicKey.slice(0,16))throw Error('Wrong membership network');
      peerChannel?.close();
      peerChannel=openMembershipPeerChannel({network,onRequest:input=>{
        const entry=[...channels].find(entry=>entry.claimedKey===input.peerPublicKey&&entry.membershipId===credentials.get(input.peerPublicKey)?.membershipId);
        if(!entry)throw Error('No enrolled membership');
        return entry.handleRequest(input);
      }});
    },
    newInvite,
    async removePeer(peerId) {
      const credential = [...credentials.values()].find(
        (c) => c.peerPublicKey.slice(0, 16) === peerId,
      );
      if (!credential)
        throw new Error("No enrolled membership found for this peer");
      return authority.remove(
        credential.peerPublicKey,
        credential.membershipId,
      );
    },
    close() {
      if (closed) return;
      closed = true;
      off();
      peerChannel?.close();
      for (const entry of channels) {
        clearTimeout(entry.timer);
        entry.channel.close();
      }
      channels.clear();
    },
  };
}
export async function connectMembershipPairing({
  code,
  identity,
  doorwayFactory,
  signal,
  onError = () => {},
  onControlPath = () => {},
} = {}) {
  code = (await parsePairingCode(code)).code;
  let policy, credential, context, peerChannel;
  const early = [],
    listeners = new Set();
  const channel = await openPairingChannel({
    code,
    identity,
    doorway: doorwayFactory(),
    onError,
    onEvent: (event) => {
      if (event?.type !== "revocation") return;
      if (!policy) {
        early.push(event.record);
        return;
      }
      for (const fn of listeners) fn(event.record);
    },
  });
  const request = async (action,data,options) => {
    if(peerChannel?.ready()){
      try{onControlPath({path:'peer',action});return await peerChannel.request(action,data,options);}catch(error){if(options?.signal?.aborted)throw error;}
    }
    onControlPath({path:'pairing',action});return channel.request(action,data,options);
  };
  try {
    const firstRequest = {
      request: signed(
        identity,
        (await pairingCoordinates(code)).roomId,
        "pairing.context",
      ),
    };
    // Unconsumed codes need no room knowledge; for reconnect the stored room is supplied below.
    context = await channel.request("context", firstRequest, { signal });
    const parsed = parseInvite(context.invite);
    if (context.authorityPublicKey !== parsed.adminPublicKey)
      throw new Error(
        "Pairing authority does not match the sharing invitation",
      );
    const authorize = async ({ networkId, peerPublicKey, signal }) => {
      if (
        networkId !== parsed.networkId ||
        peerPublicKey !== identity.publicKey
      )
        throw new Error("Pairing membership scope mismatch");
      if (credential) return credential;
      const request = signed(identity, networkId, "membership.redeem", {
        inviteId: await digest(context.token),
      });
      credential = await channel.request("redeem", { request }, { signal });
      return credential;
    };
    policy = createMembershipAdmission({
      authorityPublicKey: context.authorityPublicKey,
      authorize,
      readRevocations: ({ networkId, signal }) =>
        request(
          "revocations",
          {
            request: signed(identity, networkId, "membership.status", {
              membershipId: credential?.membershipId,
            }),
          },
          { signal },
        ),
      subscribeRevocations: ({ onRevocation }) => {
        listeners.add(onRevocation);
        for (const record of early.splice(0)) onRevocation(record);
        return () => listeners.delete(onRevocation);
      },
      leave: ({ request: proof }) => request("leave", { request: proof }),
    });
    return {
      invite: context.invite,
      bindNetwork(network) {
        if(network.networkId!==parsed.networkId||network.getState().peerId!==identity.publicKey.slice(0,16)||!/^[0-9a-f]{64}$/.test(context.hostPeerPublicKey))throw Error('Invalid membership host');
        peerChannel?.close();peerChannel=openMembershipPeerChannel({network,remotePublicKey:context.hostPeerPublicKey,onEvent:event=>{
          if(event?.type==='revocation')for(const fn of listeners)fn(event.record);
        }});
      },
      admission: policy,
      newInvite: () =>
        request("invite", {
          credential,
          request: signed(identity, parsed.networkId, "membership.invite", {
            membershipId: credential?.membershipId,
          }),
        }),
      close: () => {peerChannel?.close();channel.close();},
    };
  } catch (error) {
    channel.close();
    throw error;
  }
}
