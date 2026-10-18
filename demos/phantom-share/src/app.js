import { attachRampTest } from "./ramp-test.js";
import { leaveAndReset } from "./session-cleanup.js";
import { createDiagnostics } from "./diagnostics.js";
import { createBrowserIdentity as createBrowserIdentity } from "../../../packages/phantom-doorways/src/browser-entry.js";
import { parseInvite as parseInvite } from "../../../packages/phantom-core/src/invite.js";
import { createIcons as createIcons, icons as icons } from "lucide";
import QRCode from "qrcode";
import {
  selectRecipients as selectRecipients,
  createShareSession as createShareSession,
} from "./session.js";
import { connectShare, codeFromInput } from "./membership.js";
let diagnosticStorage;
try {
  diagnosticStorage = sessionStorage;
} catch {}
const diagnostics = createDiagnostics({ storage: diagnosticStorage });
const recordEnvironment = (type) => {
  diagnostics.record(type, {
    visibility: document.visibilityState,
    online: navigator.onLine,
  });
  diagnostics.flush();
};
for (const type of ["pageshow", "online", "offline"])
  window.addEventListener(type, () => recordEnvironment(type));
for (const type of ["visibilitychange", "freeze", "resume"])
  document.addEventListener(type, () => recordEnvironment(type));
window.addEventListener("error", (e) =>
  diagnostics.record("javascript-error", { message: e.message }),
);
window.addEventListener("unhandledrejection", (e) =>
  diagnostics.record("unhandled-rejection", {
    message: e.reason?.message || String(e.reason),
  }),
);
setInterval(() => diagnostics.flush(), 1000);
let rampControl = null,
  rampBusy = false,
  lastRampReport = null;
try {
  lastRampReport = JSON.parse(
    diagnosticStorage?.getItem("phantom-ramp-report") || "null",
  );
} catch {}
const JT = { default: QRCode };
var byId = (e) => document.getElementById(e),
  recipientModes = new Map(),
  downloadUrls = new Map(),
  network = null,
  sharing = null,
  admin = null,
  connecting = !1,
  invite = "",
  shareUrl = "",
  pendingCode = "",
  releaseIdentityLock = null,
  joinController = null,
  connectionGeneration = 0,
  renderIcons = () =>
    createIcons({ icons: icons, attrs: { "aria-hidden": "true" } }),
  escapeHtml = (e) =>
    String(e).replace(
      /[&<>"']/g,
      (a) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[a],
    ),
  displayName = () => localStorage.getItem("phantom-share-name") || "",
  setStatus = (e) => {
    byId("connection-status").textContent = e;
  },
  showError = (e) => {
    diagnostics.record("app-error", { message: e?.message || String(e) });
    setStatus(e?.message || String(e));
  },
  $T = (e) =>
    e >= 1048576
      ? (e / 1048576).toFixed(1) + " MB"
      : Math.max(1, Math.ceil(e / 1024)) + " KB",
  savedIdentity;
try {
  ((savedIdentity =
    JSON.parse(localStorage.getItem("phantom-share-identity") || "null") ||
    createBrowserIdentity()),
    localStorage.setItem(
      "phantom-share-identity",
      JSON.stringify(savedIdentity),
    ));
} catch {
  setStatus(
    "Browser storage is unavailable. Enable site storage before connecting.",
  );
}
function peers() {
  if (!network) return [];
  let e = network.getState().peerId,
    a = new Map(network.getPeerConnections().map((o) => [o.peerId, o]));
  return network
    .getPeers()
    .filter((o) => o.hKey !== e)
    .map((o) => ({
      id: o.hKey,
      name: o.handle || "Device " + o.hKey.slice(-4),
      connection: a.get(o.hKey)?.state || "connecting",
    }));
}
function readyPeers() {
  return peers().filter((e) => e.connection === "ready");
}
function preparationActivity() {
  return [...(sharing?.assets.values() || [])].find(
    (a) => a.outgoing && ["preparing", "authorizing"].includes(a.status),
  );
}
function updateConnectionStatus(state) {
  const asset = preparationActivity();
  setStatus(
    asset
      ? asset.status === "preparing"
        ? `Preparing ${asset.name} · ${asset.total ? Math.round((asset.received / asset.total) * 100) : 0}%`
        : `Authorizing ${asset.name}…`
      : connectionText(state),
  );
}
function canShare() {
  if (!sharing || !network || leaving || rampBusy || preparationActivity())
    return false;
  const selected = selectRecipients(peers(), recipientModes),
    ready = new Set(readyPeers().map((p) => p.id));
  return selected.length > 0 && selected.every((id) => ready.has(id));
}
function renderAvailability() {
  const available = canShare(),
    ready = readyPeers().length;
  const zone = byId("portal-drop-zone");
  zone.classList.toggle("drop-unavailable", !available);
  zone.setAttribute("aria-disabled", String(!available));
  zone.tabIndex = available ? 0 : -1;
  byId("file-input").disabled = !available;
  if (!available) zone.classList.remove("portal-drop-active");
  byId("session-heading").textContent = ready
    ? "Connected"
    : connecting
      ? "Connecting…"
      : network
        ? peers().length
          ? "Connecting to devices…"
          : "No devices connected"
        : "Not connected";
  byId("session-dot").classList.toggle("status-idle", !ready);
  byId("leave-action").disabled = leaving || (!network && !connecting);
  byId("leave-label").textContent = leaving
    ? "Leaving…"
    : connecting
      ? "Cancel"
      : network && !peers().length
        ? "Leave / reset"
        : "Leave";
}
function updateTargetLabel() {
  let e = peers(),
    a = e.filter((o) => recipientModes.get(o.id) === "direct");
  byId("active-target-name").textContent = a.length
    ? a.map((o) => o.name).join(", ")
    : e.length === 1
      ? e[0].name
      : "Everyone";
}
function setRecipientMode(e, a) {
  (recipientModes.set(e, recipientModes.get(e) === a ? "default" : a),
    renderPeers());
}
function renderPeers() {
  let e = peers();
  ((byId("peer-count-text").textContent =
    e.length + " connected device" + (e.length === 1 ? "" : "s")),
    byId("routing-legend").classList.toggle("hidden", e.length < 2));
  let a = byId("orbital-peers-container");
  ((a.innerHTML = e.length
    ? e
        .map((o) => {
          let l = recipientModes.get(o.id) || "default";
          return `<div class="glass-card rounded-2xl p-4 border transition-all flex flex-col justify-between gap-3 ${l === "exclude" ? "opacity-40 grayscale border-white/5 bg-black/40" : l === "direct" ? "border-phantom-neonBlue bg-phantom-spectral/30" : "border-white/10"}"><div class="flex items-center gap-2.5"><div class="w-8 h-8 rounded-xl border border-white/10 flex items-center justify-center bg-white/5 text-phantom-neonBlue"><i data-lucide="monitor" class="w-4 h-4"></i></div><div class="min-w-0"><h4 class="text-xs font-bold text-white truncate">${escapeHtml(o.name)}</h4><span class="text-[10px] font-mono text-phantom-ghostBlue">${o.id.slice(-4)} \xB7 ${escapeHtml(o.connection)}</span></div></div>${e.length > 1 ? `<div class="pt-2 border-t border-white/5 flex items-center justify-between gap-2"><span class="text-[10px] font-mono text-phantom-mutedIvory">${l.toUpperCase()}</span><div class="flex gap-1"><button data-peer="${o.id}" data-mode="direct" aria-pressed="${l === "direct"}" class="px-2 py-1 rounded text-[10px] font-mono ${l === "direct" ? "bg-phantom-neonBlue text-black" : "bg-white/5 text-phantom-mutedIvory"}">Direct</button><button data-peer="${o.id}" data-mode="exclude" aria-pressed="${l === "exclude"}" class="px-2 py-1 rounded text-[10px] font-mono ${l === "exclude" ? "bg-lime-500/20 text-lime-200" : "bg-white/5 text-phantom-mutedIvory"}">Exclude</button></div></div>` : ""}${shareConnection?.isOwner ? `<button data-peer="${o.id}" data-mode="remove" class="text-[10px] font-mono text-phantom-mutedIvory self-end hover:text-white" aria-label="Remove ${escapeHtml(o.name)}">Remove</button>` : ""}</div>`;
        })
        .join("")
    : '<div class="col-span-full text-center py-4 font-mono text-xs text-phantom-mutedIvory">' +
      (network
        ? "Waiting for someone to join. Share your invitation."
        : "Connect to start sharing.") +
      "</div>"),
    updateTargetLabel(),
    renderAvailability(),
    renderIcons());
}
function renderTransfers() {
  let e = [...(sharing?.assets.values() || [])].reverse();
  byId("transfer-count-badge").textContent =
    e.filter((o) => !["sent", "received", "failed"].includes(o.status)).length +
    " active";
  let a = byId("transfers-container");
  if (
    ((a.innerHTML =
      e
        .slice(0, 1)
        .map((o) => {
          let l = 0,
            p = o.status;
          if (o.outgoing)
            ((l =
              o.status === "preparing"
                ? o.total
                  ? Math.round((o.received / o.total) * 100)
                  : 0
                : o.recipients.length
                  ? Math.round((o.delivered.size / o.recipients.length) * 100)
                  : 0),
              (p =
                o.status === "sharing"
                  ? `${o.delivered.size}/${o.recipients.length} recipients received`
                  : o.status));
          else {
            let h = network
              ?.getDistributionState()
              ?.assets.find((g) => g.id === o.id)
              ?.holders.find((g) => g.peer === network.getState().peerId);
            l =
              o.status === "received"
                ? 100
                : o.total
                  ? Math.round(
                      (Math.max(o.received || 0, h?.indexes.length || 0) /
                        o.total) *
                        100,
                    )
                  : 0;
          }
          return `<div class="glass-card rounded-2xl p-4 border border-white/10 flex flex-col gap-2"><div class="flex items-center justify-between gap-3"><div class="min-w-0"><h4 class="text-xs font-bold text-white truncate">${escapeHtml(o.name)}</h4><span class="text-[10px] font-mono text-phantom-mutedIvory">${$T(o.size)} \xB7 ${escapeHtml(o.error || p)}</span></div>${o.blob && !o.outgoing ? `<button data-download="${o.id}" class="px-3 py-1 rounded-lg bg-phantom-spectral/40 border border-phantom-spectral text-phantom-neonBlue text-xs">Save</button>` : ""}</div><progress class="w-full h-1 accent-lime-300" max="100" value="${l}" aria-label="${o.status === "preparing" ? "File preparation" : o.outgoing ? "Recipients completed" : "File received"}"></progress></div>`;
        })
        .join("") ||
      '<div class="text-center py-3 font-mono text-xs text-phantom-mutedIvory/60">No transfers yet.</div>'),
    e.length > 1)
  ) {
    let o = document.createElement("button");
    ((o.textContent = `View all ${e.length} transfers`),
      (o.className = "text-xs text-phantom-neonBlue"),
      (o.onclick = () => showHistory(e)),
      a.append(o));
  }
}
function showHistory(e) {
  let a = byId("history-dialog");
  byId("history-list").replaceChildren();
  for (let o of e) {
    let l = document.createElement("div");
    l.className = "history-item";
    let p = document.createElement("span");
    if (
      ((p.textContent = o.name + " \xB7 " + $T(o.size) + " \xB7 " + o.status),
      l.append(p),
      o.blob && !o.outgoing)
    ) {
      let u = document.createElement("button");
      ((u.textContent = "Save"),
        (u.onclick = () => saveFile(o.id)),
        l.append(u));
    }
    byId("history-list").append(l);
  }
  a.showModal();
}
function saveFile(e) {
  let a = sharing?.assets.get(e);
  if (!a?.blob) return;
  let o = downloadUrls.get(e);
  o || ((o = URL.createObjectURL(a.blob)), downloadUrls.set(e, o));
  let l = document.createElement("a");
  ((l.href = o), (l.download = a.name), l.click());
}
async function acquireIdentityLock() {
  navigator.locks &&
    (await new Promise((e, a) => {
      navigator.locks
        .request("phantom-share-active-identity", { ifAvailable: !0 }, (o) => {
          if (!o) {
            a(
              Error(
                "Phantom Share is already connected in another tab. Leave there first.",
              ),
            );
            return;
          }
          return new Promise((l) => {
            ((releaseIdentityLock = l), e());
          });
        })
        .catch(a);
    }));
}
let shareConnection = null,
  currentInvitation = null,
  invitationTimer = null,
  leaving = false,
  invitationPending = null;
async function ensureInvitation() {
  const connection = shareConnection;
  if (!connection) return;
  if (!currentInvitation || currentInvitation.expiresAt <= Date.now()) {
    if (!invitationPending) {
      const pending = connection
        .newInvite()
        .then((value) => {
          if (shareConnection === connection) currentInvitation = value;
        })
        .finally(() => {
          if (invitationPending === pending) invitationPending = null;
        });
      invitationPending = pending;
    }
    await invitationPending;
  }
  if (shareConnection !== connection || !currentInvitation) return;
  const shown = currentInvitation,
    url = new URL(location.href);
  url.search = "";
  url.hash = new URLSearchParams({ pair: shown.code }).toString();
  shareUrl = url.href;
  byId("invite-link").value = shareUrl;
  byId("invite-code").value = shown.code;
  byId("invite-expiry").textContent =
    "One device · expires in " +
    Math.max(1, Math.ceil((shown.expiresAt - Date.now()) / 60000)) +
    " min";
  await JT.default.toCanvas(byId("invite-qr"), shareUrl, {
    width: 164,
    margin: 2,
    errorCorrectionLevel: "L",
  });
  if (shareConnection !== connection) return;
  if (currentInvitation !== shown) return ensureInvitation();
  clearTimeout(invitationTimer);
  invitationTimer = setTimeout(
    () => {
      currentInvitation = null;
      byId("invite-code").value = "Expired";
      byId("invite-link").value = "";
      if (!byId("invite-modal").classList.contains("hidden"))
        void ensureInvitation().catch(showError);
    },
    Math.max(1, shown.expiresAt - Date.now()),
  );
}
async function connect(code = "") {
  if (connecting || network) return;
  const generation = ++connectionGeneration;
  diagnostics.record("connect-start", {
    generation,
    role: code ? "guest" : "owner",
  });
  connecting = true;
  joinController = new AbortController();
  byId("connect-action").disabled = true;
  renderAvailability();
  setStatus("Connecting…");
  try {
    if (!window.isSecureContext || !crypto.subtle)
      throw Error("Open Phantom Share over HTTPS or localhost.");
    if (!savedIdentity) throw Error("Site storage is required.");
    await acquireIdentityLock();
    const connection = await connectShare({
      seed: savedIdentity,
      code,
      name: displayName(),
      signal: joinController.signal,
      onEvent: (event) => {
        if (
          /MEMBERSHIP|PRESENCE|CONNECTION|ADMISSION|RTC|SIGNAL|RECOVER|CLOSED|ERROR/.test(
            event.type,
          )
        )
          diagnostics.record(event.type, event.payload);
      },
      onState: (state) => {
        diagnostics.record("network-state", state);
        if (generation === connectionGeneration) updateConnectionStatus(state);
      },
      onError: showError,
      onClaim: ({ code: claimed }) => {
        if (currentInvitation?.code === claimed) {
          currentInvitation = null;
          byId("invite-code").value = "Used";
          byId("invite-link").value = "";
          if (!byId("invite-modal").classList.contains("hidden"))
            void ensureInvitation().catch(showError);
        }
      },
    });
    if (generation !== connectionGeneration) {
      connection.close();
      return;
    }
    shareConnection = connection;
    network = connection.network;
    rampControl = attachRampTest({
      network,
      onRequest: (peer, settings) => {
        if (hasActiveTransfers() || document.visibilityState !== "visible")
          return false;
        return confirm(
          `Run a raw connection ramp test? This profile may use up to ${settings?.maxStageBytes === 1073741824 ? "18" : "3.5"} GiB of data. Keep this page visible.`,
        );
      },
      onUpdate: updateRampTest,
    });
    admin = connection.admin;
    invite = network.invite;
    const context = parseInvite(invite);
    sharing = createShareSession({
      network: network,
      admin: admin,
      adminPublicKey: context.adminPublicKey,
      onChange: () => {
        if (rampBusy && hasActiveTransfers()) rampControl?.cancel();
        for (const asset of sharing?.assets.values() || [])
          diagnostics.record("transfer-state", {
            id: asset.id,
            size: asset.size,
            status: asset.status,
            received: asset.received,
            total: asset.total,
            outgoing: asset.outgoing,
          });
        renderPeers();
        renderTransfers();
        if (preparationActivity()) updateConnectionStatus(network.getState());
      },
      onError: showError,
    });
    network.on("state", (state) => {
      if (generation !== connectionGeneration) return;
      diagnostics.record("network-state", state);
      if (["failed", "retired", "closed"].includes(state.state) && !leaving) {
        cleanupSession(
          state.state === "retired"
            ? "Session retired. Connect to start again."
            : "Session ended. Connect to start again.",
        );
        return;
      }
      renderPeers();
      updateConnectionStatus(state);
    });
    network.on("peerConnection", () => {
      renderPeers();
      if (network) updateConnectionStatus(network.getState());
    });
    for (const type of [
      "filePreparationProgress",
      "filePublished",
      "fileProgress",
      "fileRetry",
      "fileError",
      "fileComplete",
      "distributionProgress",
      "distributionFileEvent",
      "distributionConnectionReused",
      "distributionTransferTiming",
      "distributionBatchStarted",
      "distributionBatchComplete",
      "distributionJob",
      "distributionJobFailed",
      "distributionError",
      "distributionComplete",
      "distributionConnectionOpened",
      "distributionConnectionClosed",
      "distributionConnectionFailed",
      "controlError",
    ])
      network.on(type, (data) => diagnostics.record(type, data));
    network.on("membershipChanged", (event) => {
      diagnostics.record("membershipChanged", event);
      renderPeers();
    });
    network.on("event", (event) => {
      if (event.type === "PRESENCE_ROSTER") {
        diagnostics.record("peer-summary", {
          connected: peers().length,
          ready: readyPeers().length,
        });
        renderPeers();
      }
    });
    network.on("membershipLost", (event) => {
      diagnostics.record("membershipLost", event);
      diagnostics.flush();
      if (!leaving && generation === connectionGeneration) {
        const reason =
          event.reason === "revoked"
            ? "Access ended. Ask for a fresh invitation."
            : "Membership could not be confirmed. Reconnect to continue.";
        cleanupSession(reason);
      }
    });
    const url = new URL(location.href);
    url.search = "";
    url.hash = code ? new URLSearchParams({ pair: code }).toString() : "";
    history.replaceState(null, "", url);
    pendingCode = "";
    byId("join-section").hidden = true;
    byId("leave-action").disabled = false;
    byId("connect-action").querySelector("span").textContent = "Invite";
    renderPeers();
    renderTransfers();
    updateConnectionStatus(network.getState());
  } catch (error) {
    if (generation === connectionGeneration) {
      shareConnection?.close();
      shareConnection = null;
      sharing?.close();
      sharing = null;
      network?.close();
      network = null;
      releaseIdentityLock?.();
      releaseIdentityLock = null;
      showError(error);
    }
  } finally {
    if (generation === connectionGeneration) {
      connecting = false;
      byId("connect-action").disabled = false;
      renderAvailability();
    }
  }
}
function connectionText(e) {
  if (
    network &&
    !peers().length &&
    !["failed", "retired", "closed"].includes(e.state)
  )
    return "No devices connected · Invite a device or Leave to reset";
  return e.state === "admitted"
    ? "Connected \xB7 keep this tab open while sharing"
    : e.state === "failed"
      ? "Connection failed. " + (e.reason || "Try again.")
      : e.state === "retired"
        ? "This network has been retired."
        : e.state === "recovering" || e.state === "reconnecting"
          ? "Connection interrupted \xB7 recovering\u2026"
          : e.state === "closed"
            ? "Disconnected"
            : "Connecting \xB7 " + (e.reason || e.state || "finding peers");
}
async function openInviteModal() {
  if (!displayName()) {
    promptName();
    return;
  }
  byId("join-section").hidden = !!network;
  byId("share-section").hidden = !network;
  byId("invite-modal").classList.remove("hidden");
  byId("invite-modal").classList.add("flex");
  if (network)
    try {
      await ensureInvitation();
    } catch (error) {
      showError(error);
    }
}
function closeInviteModal() {
  (byId("invite-modal").classList.add("hidden"),
    byId("invite-modal").classList.remove("flex"));
}
async function copyShareLink() {
  try {
    (await navigator.clipboard.writeText(shareUrl),
      (byId("toast").textContent = "Invitation copied"),
      byId("toast").classList.remove("opacity-0"));
  } catch {
    (byId("invite-link").select(),
      (byId("toast").textContent = "Select and copy the invitation above"),
      byId("toast").classList.remove("opacity-0"));
  }
}
function cleanupSession(message = "You left the session.") {
  diagnostics.record("session-reset", { message });
  diagnostics.flush();
  rampControl?.close();
  rampControl = null;
  rampBusy = false;
  ++connectionGeneration;
  joinController?.abort();
  clearTimeout(invitationTimer);
  currentInvitation = null;
  invitationPending = null;
  shareConnection?.close();
  shareConnection = null;
  sharing?.close();
  sharing = null;
  network?.close();
  network = null;
  releaseIdentityLock?.();
  releaseIdentityLock = null;
  connecting = false;
  recipientModes.clear();
  for (const url of downloadUrls.values()) URL.revokeObjectURL(url);
  downloadUrls.clear();
  invite = "";
  shareUrl = "";
  pendingCode = "";
  byId("file-input").value = "";
  byId("connect-action").disabled = false;
  byId("leave-action").disabled = true;
  byId("connect-action").querySelector("span").textContent = "Connect";
  const url = new URL(location.href);
  url.hash = "";
  url.search = "";
  history.replaceState(null, "", url);
  closeInviteModal();
  renderPeers();
  renderTransfers();
  setStatus(message);
}
async function leaveNetwork() {
  if (!network && !connecting) return;
  if (connecting && !network) {
    cleanupSession("Connection cancelled.");
    return;
  }
  leaving = true;
  renderAvailability();
  const current = network;
  try {
    await leaveAndReset({
      leave: () => current.leave(),
      onError: (error) =>
        diagnostics.record("leave-unconfirmed", { message: error.message }),
      reset: (confirmed) =>
        cleanupSession(
          confirmed
            ? "You left the session."
            : "Session closed locally; Leave could not be confirmed.",
        ),
    });
  } finally {
    leaving = false;
    invitationPending = null;
    renderAvailability();
  }
}
async function removeDevice(id) {
  if (!shareConnection?.isOwner) return;
  try {
    await shareConnection.removePeer(id);
    recipientModes.delete(id);
    renderPeers();
    setStatus("Device removed. Returning requires a fresh invitation.");
  } catch (error) {
    showError(error);
  }
}
async function handleFileSelect(e) {
  (await sendFiles(Array.from(e.target.files)), (e.target.value = ""));
}
async function sendFiles(e) {
  if (!sharing) {
    setStatus("Connect someone before sending.");
    return;
  }
  let a = selectRecipients(peers(), recipientModes),
    o = new Set(readyPeers().map((l) => l.id));
  if (!a.length || a.some((l) => !o.has(l))) {
    setStatus("Selected recipients must be ready before sending.");
    return;
  }
  const sendingSession = sharing,
    generation = connectionGeneration;
  for (let l of e)
    try {
      if (generation !== connectionGeneration || sendingSession !== sharing)
        return;
      (diagnostics.record("file-selected", { size: l.size, type: l.type }),
        setStatus("Preparing " + l.name + "\u2026"),
        await sendingSession.publish(l, a),
        generation === connectionGeneration && setStatus("Sharing " + l.name));
    } catch (p) {
      if (generation === connectionGeneration) showError(p);
    }
}
function triggerFileInput() {
  if (!canShare()) return;
  byId("file-input").click();
}
function handleDragOver(e) {
  if (!canShare()) {
    e.preventDefault();
    return;
  }
  (e.preventDefault(),
    byId("portal-drop-zone").classList.add("portal-drop-active"));
}
function handleDragLeave(e) {
  (e.preventDefault(),
    byId("portal-drop-zone").classList.remove("portal-drop-active"));
}
async function handleDrop(e) {
  (e.preventDefault(),
    handleDragLeave(e),
    canShare() && (await sendFiles(Array.from(e.dataTransfer.files))));
}
function promptName() {
  (byId("name-dialog").showModal(),
    (byId("display-name").value = displayName()),
    byId("display-name").focus());
}
Object.assign(window, {
  openInviteModal: openInviteModal,
  closeInviteModal: closeInviteModal,
  copyShareLink: copyShareLink,
  leaveNetwork: leaveNetwork,
  triggerFileInput: triggerFileInput,
  handleFileSelect: handleFileSelect,
  handleDragOver: handleDragOver,
  handleDragLeave: handleDragLeave,
  handleDrop: handleDrop,
});
byId("file-input").addEventListener("click", (e) => e.stopPropagation());
byId("portal-drop-zone").setAttribute("role", "button");
byId("portal-drop-zone").setAttribute("tabindex", "0");
byId("portal-drop-zone").addEventListener("keydown", (e) => {
  (e.key === "Enter" || e.key === " ") &&
    (e.preventDefault(), triggerFileInput());
});
byId("invite-modal").addEventListener("keydown", (e) => {
  e.key === "Escape" && closeInviteModal();
});
byId("orbital-peers-container").addEventListener("click", (e) => {
  let a = e.target.closest("[data-peer]");
  a &&
    (a.dataset.mode === "remove"
      ? removeDevice(a.dataset.peer)
      : setRecipientMode(a.dataset.peer, a.dataset.mode));
});
byId("transfers-container").addEventListener("click", (e) => {
  let a = e.target.closest("[data-download]");
  a && saveFile(a.dataset.download);
});
byId("create-network").onclick = async () => {
  await connect();
  if (network) {
    byId("join-section").hidden = true;
    byId("share-section").hidden = false;
    await ensureInvitation().catch(showError);
  }
};
byId("join-network").onclick = async () => {
  try {
    await connect(codeFromInput(byId("paste-invite").value));
    if (network) closeInviteModal();
  } catch (error) {
    showError(error);
  }
};
byId("profile-action").onclick = promptName;
byId("name-form").onsubmit = async (e) => {
  e.preventDefault();
  let a = byId("display-name").value.trim();
  if (!a || a.length > 128 || /[\x00-\x1f\x7f]/.test(a)) {
    setStatus("Enter a name of 1\u2013128 characters.");
    return;
  }
  if (network) {
    setStatus("Leave the session before changing your name.");
    return;
  }
  try {
    (localStorage.setItem("phantom-share-name", a),
      (byId("your-name").textContent = a),
      byId("name-dialog").close(),
      pendingCode ? await connect(pendingCode) : await openInviteModal());
  } catch (o) {
    showError(o);
  }
};
byId("name-dialog").addEventListener("cancel", (e) => {
  displayName() || e.preventDefault();
});
function hasActiveTransfers() {
  return [...(sharing?.assets.values() || [])].some(
    (asset) => !["sent", "received", "failed"].includes(asset.status),
  );
}
function updateRampTest(update) {
  rampBusy = update.status === "Complete" ? false : update.busy;
  byId("ramp-status").textContent = update.status;
  if (update.busy && !byId("ramp-dialog").open) byId("ramp-dialog").showModal();
  if (update.status === "Preparing raw test") {
    byId("ramp-results").textContent = "";
    byId("ramp-download").disabled = true;
  }
  byId("ramp-start").disabled = !!rampBusy;
  byId("ramp-stop").disabled = !rampBusy;
  if (update.row) {
    const r = update.row;
    byId("ramp-results").textContent +=
      `${(r.windowBytes / 1048576).toFixed(2)} MiB window · ${(r.receiverBytesPerSecond / 1e6).toFixed(2)} MB/s · RTT ${r.rttMedianMs?.toFixed(1) ?? "n/a"} ms${r.limitedByByteBudget ? " · BYTE BUDGET REACHED" : ""}\n`;
    diagnostics.record("ramp-stage", {
      bytes: r.receivedBytes,
      elapsedMs: r.completionMs,
    });
  }
  if (update.report) {
    lastRampReport = {
      ...update.report,
      testConditions: byId("ramp-context").value,
      scope:
        "Separate raw RTC connection; compare selected routes. Per-path experimental results, not universal SDK defaults.",
    };
    try {
      diagnosticStorage?.setItem(
        "phantom-ramp-report",
        JSON.stringify(lastRampReport),
      );
    } catch {}
    byId("ramp-download").disabled = false;
    for (const direction of update.report.directions)
      byId("ramp-results").textContent +=
        `${direction.direction}: ${JSON.stringify(direction.recommendation)}\n`;
  }
  renderAvailability();
}
byId("connection-test").onclick = () => {
  byId("ramp-peer").replaceChildren(
    ...readyPeers().map((peer) => new Option(peer.name, peer.id)),
  );
  byId("ramp-start").disabled =
    rampBusy || !readyPeers().length || hasActiveTransfers();
  byId("ramp-download").disabled = rampBusy || !lastRampReport;
  byId("ramp-dialog").showModal();
};
byId("ramp-start").onclick = () => {
  if (!rampControl || hasActiveTransfers()) {
    byId("ramp-status").textContent =
      "Connect a ready peer and finish file transfers first.";
    return;
  }
  byId("ramp-results").textContent = "";
  void rampControl
    .start(byId("ramp-peer").value, {
      extended: byId("ramp-profile").value === "extended",
    })
    .catch((error) => {
      byId("ramp-status").textContent = error.message;
    });
};
byId("ramp-stop").onclick = () => rampControl?.cancel();
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible" && rampBusy) rampControl?.cancel();
});
byId("ramp-download").onclick = () => {
  if (!lastRampReport) return;
  const report = { ...lastRampReport, diagnostics: diagnostics.report() };
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(report, null, 2)], { type: "application/json" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download =
    "phantom-ramp-" + new Date().toISOString().replace(/[:.]/g, "-") + ".json";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 60000);
};
byId("download-logs").onclick = () => {
  recordEnvironment("export");
  diagnostics.record("connection-summary", {
    state: network?.getState()?.state,
    connected: peers().length,
    ready: readyPeers().length,
  });
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(diagnostics.report(), null, 2)], {
      type: "application/json",
    }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download =
    "phantom-share-" + new Date().toISOString().replace(/[:.]/g, "-") + ".json";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 60000);
};
window.addEventListener("pagehide", () => {
  recordEnvironment("pagehide");
  (shareConnection?.close(),
    sharing?.close(),
    network?.close(),
    releaseIdentityLock?.());
});
setInterval(() => {
  network && renderTransfers();
}, 1e3);
(async () => {
  (renderIcons(),
    (byId("your-name").textContent = displayName() || "Set name"),
    renderPeers(),
    renderTransfers());
  try {
    ((pendingCode =
      new URLSearchParams(location.hash.slice(1)).get("pair") || ""),
      displayName()
        ? pendingCode
          ? await connect(pendingCode)
          : setStatus(
              "Connect to start \xB7 files stay in this tab until saved.",
            )
        : promptName());
  } catch (e) {
    showError(e);
  }
})();
