import { createShortCode, parseShortCode } from "./short-pairing-prototype.js";
import { openShortPairingChannel } from "./short-pairing-channel.js";
import { openMembershipPeerChannel } from "./membership-peer-channel.js";
import { createMembershipControlRequest } from "./membership-control-request.js";
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
  onCodeUsed = () => {},
  onError = () => {},
  maxChannels = 16,
  pake,
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
  const usedLocators = new Set();
  const channels = new Set(),
    credentials = new Map();
  let peerChannel;
  let closed = false,
    reservations = 0;
  const off = authority.onRevocation((record) => {
    for (const entry of channels)
      if (entry.claimedKey) {
        try {
          peerChannel?.notify(entry.claimedKey, { type: "revocation", record });
        } catch {}
        void entry.channel
          .notify(entry.claimedKey, { type: "revocation", record })
          .catch(onError);
        if (
          entry.claimedKey === record.peerPublicKey &&
          entry.membershipId === record.membershipId
        ) {
          clearTimeout(entry.timer);
          entry.timer = setTimeout(() => {
            entry.shortChannel?.close();
            entry.channel.close();
            channels.delete(entry);
            if (pake) usedLocators.delete(entry.code.slice(0, 2));
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
    let locator,
      retained = false;
    try {
      const invitation = await authority.issueInvite(
          pake ? { ttlMs: 240000 } : undefined,
        ),
        code = await createPairingCode();
      let displayCode = code;
      if (pake) {
        for (let i = 0; i < 64; i++) {
          const candidate = createShortCode(),
            route = candidate.slice(0, 2);
          if (!usedLocators.has(route)) {
            displayCode = candidate;
            locator = route;
            usedLocators.add(route);
            break;
          }
        }
        if (!locator)
          throw Error("Could not allocate a pairing route; try again");
      }
      const coordinates = await pairingCoordinates(code);
      const entry = {
        code: displayCode,
        invitation,
        shortChannel: null,
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
        onRequest: (entry.handleRequest = async ({
          action,
          data,
          peerPublicKey,
        }) => {
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
            if (data.reconnectRequest != null)
              checkPeer(data.reconnectRequest, peerPublicKey, "membership.reconnect");
            const credential = await authority.redeem({
              reconnectRequest: data.reconnectRequest,
              token: invitation.token,
              request: data.request,
            });
            entry.claimedKey = peerPublicKey;
            entry.membershipId = credential.membershipId;
            credentials.set(peerPublicKey, credential);
            try {
              entry.shortChannel?.close();
              onClaim({ code: displayCode, peerPublicKey, credential });
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
        }),
      });
      if (pake && !closed) {
        try {
          entry.shortChannel = await openShortPairingChannel({
            code: displayCode,
            identity,
            doorway: doorwayFactory(),
            pake,
            role: "host",
            invitation: code,
            onError,
            onConsumed: () => onCodeUsed({ code: displayCode }),
            ttlMs: Math.max(
              1,
              Math.min(240000, invitation.expiresAt - Date.now()),
            ),
          });
        } catch (error) {
          entry.channel.close();
          throw error;
        }
      }
      if (closed) {
        entry.shortChannel?.close();
        entry.channel.close();
        throw new Error("Inviter closed");
      }
      channels.add(entry);
      retained = true;
      entry.timer = setTimeout(
        () => {
          if (!entry.claimedKey) {
            entry.shortChannel?.close();
            entry.channel.close();
            channels.delete(entry);
            if (pake) usedLocators.delete(entry.code.slice(0, 2));
          }
        },
        Math.max(1, invitation.expiresAt - Date.now()),
      );
      entry.timer.unref?.();
      return {
        code: displayCode,
        ...(pake
          ? { linkCode: code, codeExpiresAt: invitation.expiresAt }
          : {}),
        expiresAt: invitation.expiresAt,
      };
    } finally {
      if (!retained && locator) usedLocators.delete(locator);
      reservations--;
    }
  }
  return {
    bindNetwork(network) {
      if (
        closed ||
        network.networkId !== context.networkId ||
        network.getState().peerId !== identity.publicKey.slice(0, 16)
      )
        throw Error("Wrong membership network");
      peerChannel?.close();
      peerChannel = openMembershipPeerChannel({
        network,
        onRequest: (input) => {
          const entry = [...channels].find(
            (entry) =>
              entry.claimedKey === input.peerPublicKey &&
              entry.membershipId ===
                credentials.get(input.peerPublicKey)?.membershipId,
          );
          if (!entry) throw Error("No enrolled membership");
          return entry.handleRequest(input);
        },
      });
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
        entry.shortChannel?.close();
        entry.channel.close();
      }
      channels.clear();
      usedLocators.clear();
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
  onRefresh = () => {},
  pake,
} = {}) {
  if (typeof code === "string" && code.replace(/[\s-]/g, "").length === 6) {
    if (!pake) throw Error("Short-code pairing is unavailable");
    code = parseShortCode(code).code;
    const bootstrap = await openShortPairingChannel({
      code,
      identity,
      doorway: doorwayFactory(),
      pake,
      signal,
      onError,
    });
    try {
      code = (await bootstrap.result).invitation;
    } finally {
      bootstrap.close();
    }
  }
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
  const request = createMembershipControlRequest({
    getPeer: () => peerChannel,
    pairing: channel,
    onControlPath,
  });
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
      const reconnectRequest = signed(identity, networkId, "membership.reconnect");
      credential = await channel.request("redeem", { request, reconnectRequest }, { signal });
      return credential;
    };
    policy = createMembershipAdmission({
      onRefresh,
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
      leave: ({ request: proof, signal }) => request("leave", { request: proof }, {signal}),
    });
    return {
      invite: context.invite,
      bindNetwork(network) {
        if (
          network.networkId !== parsed.networkId ||
          network.getState().peerId !== identity.publicKey.slice(0, 16) ||
          !/^[0-9a-f]{64}$/.test(context.hostPeerPublicKey)
        )
          throw Error("Invalid membership host");
        peerChannel?.close();
        peerChannel = openMembershipPeerChannel({
          network,
          remotePublicKey: context.hostPeerPublicKey,
          onEvent: (event) => {
            if (event?.type === "revocation")
              for (const fn of listeners) fn(event.record);
          },
        });
      },
      admission: policy,
      async releaseMembership({signal} = {}) {
        if (!credential) return;
        const proof = signed(identity, parsed.networkId, "membership.leave", {
          membershipId: credential.membershipId,
        });
        await policy.leave({
          signal,
          networkId: parsed.networkId,
          peerPublicKey: identity.publicKey,
          credential,
          request: proof,
        });
      },
      newInvite: () =>
        request("invite", {
          credential,
          request: signed(identity, parsed.networkId, "membership.invite", {
            membershipId: credential?.membershipId,
          }),
        }),
      close: () => {
        peerChannel?.close();
        channel.close();
      },
    };
  } catch (error) {
    channel.close();
    throw error;
  }
}
