import { createAdminIdentity } from "../../../packages/phantom-core/src/authorization.js";
import { createMembershipAuthority } from "../../../packages/phantom-core/src/membership.js";
import { createMembershipAdmission } from "../../../packages/phantom-core/src/membership-admission.js";
import { createNostrIdentity } from "../../../packages/phantom-doorways/src/nostr-crypto.js";
import { createNostrDoorway } from "../../../packages/phantom-doorways/src/nostr-doorway.js";
import { createBrowserPhantom } from "../../../packages/phantom-doorways/src/browser-entry.js";
import { createBrowserMembershipStore } from "../../../packages/phantom-doorways/src/browser-membership-store.js";
import {
  createMembershipPairingHost,
  connectMembershipPairing,
  verifyMembershipPeerRequest,
} from "../../../packages/phantom-doorways/src/membership-pairing.js";
import { getDefaultRelays } from "../../../packages/phantom-doorways/src/defaults.js";
const digest = async (token) =>
  Array.from(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token)),
    ),
    (b) => b.toString(16).padStart(2, "0"),
  ).join("");
const sign = (identity, networkId, action, fields = {}) => ({
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
});
export async function connectShare({
  seed,
  code,
  name,
  signal,
  onState,
  onEvent,
  onClaim,
  onError,
} = {}) {
  const identity = createNostrIdentity(seed),
    relays = getDefaultRelays(),
    doorwayFactory = () => createNostrDoorway({ identity, relays });
  let authority, control, network, admin;
  try {
    if (code) {
      control = await connectMembershipPairing({
        onControlPath: ({path,action,...detail}) => onEvent?.({type:"MEMBERSHIP_CONTROL_PATH",payload:{type:path,reason:action,...detail}}),
        onRefresh: detail => onEvent?.({type:"MEMBERSHIP_REFRESH",payload:detail}),
        code,
        identity,
        doorwayFactory,
        signal,
        onError,
      });
      const sdk = createBrowserPhantom({
        identity: seed,
        admission: control.admission,
        messaging: "automatic",
        // Assign 2 MiB per scheduling turn; only eight 64 KiB requests remain in flight.
        distribution: { batchSize: 32, requestWindow: 8 },
      });
      network = await sdk.join(control.invite, {
        name,
        signal,
        timeoutMs: 60000,
        onState,
        onEvent,
      });
    } else {
      admin = await createAdminIdentity();
      const store = createBrowserMembershipStore();
      const admission = createMembershipAdmission({
        onRefresh: detail => onEvent?.({type:"MEMBERSHIP_REFRESH",payload:detail}),
        authorityPublicKey: admin.publicKey,
        authorize: async ({ networkId }) => {
          authority ??= await createMembershipAuthority({
            networkId,
            adminIdentity: admin,
            store,
            verifyPeerRequest: verifyMembershipPeerRequest,
          });
          const invitation = await authority.issueInvite();
          return authority.redeem({
            token: invitation.token,
            request: sign(identity, networkId, "membership.redeem", {
              inviteId: await digest(invitation.token),
            }),
          });
        },
        readRevocations: () => authority.getRevocations(),
        subscribeRevocations: ({ onRevocation }) =>
          authority.onRevocation(onRevocation),
        leave: ({ request }) => authority.leave({ request }),
      });
      const sdk = createBrowserPhantom({
        identity: seed,
        adminIdentity: admin,
        admission,
        relays,
        messaging: "automatic",
        // Assign 2 MiB per scheduling turn; only eight 64 KiB requests remain in flight.
        distribution: { batchSize: 32, requestWindow: 8 },
      });
      network = await sdk.create({ name, signal, onState, onEvent });
      control = createMembershipPairingHost({
        identity,
        authority,
        invite: network.invite,
        doorwayFactory,
        canIssueInvite: () => true,
        onClaim,
        onError,
      });
    }
    control.bindNetwork(network);
    return {
      network,
      admin,
      isOwner: !!admin,
      newInvite: () => control.newInvite(),
      removePeer: (id) => {
        if (!admin)
          throw new Error("Only the session owner can remove a device");
        return control.removePeer(id);
      },
      close: () => {
        network.close();
        control.close();
      },
    };
  } catch (error) {
    network?.close();
    control?.close();
    throw error;
  }
}
export function codeFromInput(value) {
  const text = value.trim();
  if (/^P1/i.test(text)) return text;
  try {
    const url = new URL(text);
    const code = new URLSearchParams(url.hash.slice(1)).get("pair");
    if (code) return code;
  } catch {}
  throw new Error(
    "Enter a pairing code or a current Phantom Share invitation link.",
  );
}
