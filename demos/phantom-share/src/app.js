import {saveBlob,cancelNativeSave,ownsNativeSession} from './native-save.js';
function reportSaveError(error){if(error.code!=='SAVE_CANCELLED')showError(error);}
import { createQRScanner } from "./qr-scanner.js";
import { requestRampApproval } from "./ramp-approval.js";
import { attachRampTest } from "./ramp-test.js";
import { leaveAndReset, closeSessionOwners } from "./session-cleanup.js";
import { createDiagnostics } from "./diagnostics.js";
import { createBrowserIdentity as createBrowserIdentity } from "../../../packages/phantom-doorways/src/browser-entry.js";
import { parseInvite as parseInvite } from "../../../packages/phantom-core/src/invite.js";
import { createIcons as createIcons, icons as icons } from "lucide";
import QRCode from "qrcode";
import {
  selectRecipients as selectRecipients,
  createShareSession as createShareSession,
} from "./session.js";
import { createConnectionQueue } from "./connection-queue.js";
import { connectShare, codeFromInput } from "./membership.js";
let diagnosticStorage;
try {
  diagnosticStorage = sessionStorage;
} catch {}
const diagnostics = createDiagnostics({ storage: diagnosticStorage });
const connectionQueue = createConnectionQueue({onState: state => diagnostics.record("session-lifecycle", {state})});
let nativeForeground = true;
const appVisible = () => document.visibilityState === "visible" &&
  (!globalThis.PhantomNative || nativeForeground);
const recordEnvironment = (type) => {
  diagnostics.record(type, {
    visibility: document.visibilityState,
    ...(globalThis.PhantomNative ? {nativeForeground} : {}),
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
  const ending = connectionQueue.phase === "ending";
  byId("join-network").disabled = connecting || ending || !!network;
  byId("join-network").textContent = connecting ? "Joining…" : "Join session";
  byId("join-network").setAttribute("aria-busy", String(connecting));
  byId("paste-invite").disabled = connecting || ending;
  byId("scan-qr").disabled = connecting || ending;
  byId("pairing-back").disabled = connecting || ending;
  byId("choose-create").disabled = connecting || ending;
  byId("choose-join").disabled = connecting || ending || !!network;
  byId("cancel-join").hidden = !connecting || pairingIntent !== "connect";
  const nameLocked = !!network || connecting || leaving;
  byId("profile-action").hidden = nameLocked;
  byId("session-name").hidden = !nameLocked;
  byId("session-name").textContent = displayName();
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
  const rows = peers();
  byId("peer-count-text").textContent = rows.length + " device" + (rows.length === 1 ? "" : "s");
  byId("routing-legend").classList.add("hidden");
  byId("orbital-peers-container").innerHTML = rows.length ? rows.map(peer => {
    const mode = recipientModes.get(peer.id) || "default";
    const label = escapeHtml(peer.name);
    return `<div class="share-peer ${mode === "exclude" ? "is-excluded" : ""}">
      <div class="share-peer-info"><span class="share-avatar"><i data-lucide="monitor"></i></span><div class="share-peer-name"><h4 title="${label}">${label}</h4><span class="share-peer-state">${escapeHtml(peer.connection)}</span></div></div>
      <div class="share-peer-actions"><select class="share-recipient-select" data-recipient-peer="${peer.id}" aria-label="Recipients for ${label}">${[["default","All peers"],["direct","Only this device"],["exclude","Exclude this device"]].map(([value,title]) => `<option value="${value}"${mode === value ? " selected" : ""}>${title}</option>`).join("")}</select>
      ${shareConnection?.isOwner ? `<button class="share-remove" data-peer="${peer.id}" data-mode="remove" aria-label="Remove ${label}" title="Remove device">Remove</button>` : ""}</div></div>`;
  }).join("") : `<div class="share-empty">${network ? "Waiting for someone to join. Share your invitation." : "Connect to start sharing."}</div>`;
  updateTargetLabel();
  renderAvailability();
  renderIcons();
}
function renderTransfers() {
  const assets = [...(sharing?.assets.values() || [])].reverse();
  const terminal = new Set(["sent", "received", "failed", "interrupted"]);
  const active = assets.filter(asset => !terminal.has(asset.status));
  const complete = assets.filter(asset => terminal.has(asset.status));
  const shown = [...active.slice(0, 2), ...complete.slice(0, active.length ? 1 : 2)];
  byId("transfer-count-badge").textContent = active.length + " active";
  const container = byId("transfers-container");
  container.innerHTML = shown.map(asset => {
    let progress = 0, status = asset.status;
    if (asset.outgoing) {
      progress = asset.status === "preparing" ? (asset.total ? Math.round(asset.received / asset.total * 100) : 0)
        : asset.recipients.length ? Math.round(asset.delivered.size / asset.recipients.length * 100) : 0;
      if (status === "sharing") status = `${asset.delivered.size}/${asset.recipients.length} recipients received`;
    } else {
      const holder = network?.getDistributionState()?.assets.find(row => row.id === asset.id)?.holders.find(row => row.peer === network.getState().peerId);
      progress = asset.status === "received" ? 100 : asset.total ? Math.round(Math.max(asset.received || 0, holder?.indexes.length || 0) / asset.total * 100) : 0;
    }
    progress = Math.max(0, Math.min(100, progress));
    const downloadable = asset.blob && !asset.outgoing;
    const filename = downloadable ? `<button class="share-file-link" data-download="${asset.id}" title="Save ${escapeHtml(asset.name)}">${escapeHtml(asset.name)}</button>` : `<span class="share-filename" title="${escapeHtml(asset.name)}">${escapeHtml(asset.name)}</span>`;
    const label = asset.status === "preparing" ? "File preparation" : asset.outgoing ? "Recipients completed" : "File received";
    return `<article class="share-transfer"><div class="share-transfer-label"><span>${asset.outgoing ? "Sending" : "Receiving"}${asset.status === "received" ? " · complete" : asset.status === "sent" ? " · complete" : ""}</span><span>${progress}%</span></div>
      <div class="share-file-row"><span class="share-file-icon"><i data-lucide="file"></i></span><div class="share-file-info">${filename}<span class="share-file-detail">${$T(asset.size)} · ${escapeHtml(asset.error || status)}</span></div>${downloadable ? `<button data-download="${asset.id}" class="share-save">Save</button>` : ""}</div>
      <div class="share-progress" role="progressbar" aria-label="${label}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${progress}"><span style="width:${progress}%"></span></div></article>`;
  }).join("") || '<div class="share-empty">No transfers yet.</div>';
  if (assets.length > shown.length) {
    const history = document.createElement("button");
    history.textContent = `View all ${assets.length} transfers`;
    history.className = "share-history";
    history.onclick = () => showHistory(assets);
    container.append(history);
  }
  renderIcons();
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
  if(globalThis.PhantomNative?.postMessage){void saveBlob(a.blob,a.name).catch(reportSaveError);return;}
  let o = downloadUrls.get(e);
  o || ((o = URL.createObjectURL(a.blob)), downloadUrls.set(e, o));
  let l = document.createElement("a");
  ((l.href = o), (l.download = a.name));
  document.body.append(l); l.click(); l.remove();
}
async function acquireIdentityLock(generation) {
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
          if (generation !== connectionGeneration) {
            a(new Error("Connection cancelled."));
            return;
          }
          return new Promise((l) => {
            ((releaseIdentityLock = l), e());
          });
        })
        .catch(a);
    }));
}
let pairingIntent = null;
let pairingCreatesSession = false, pairingPanelGeneration = 0;
const scanner = createQRScanner({
  video: byId("scanner-video"), canvas: byId("scanner-canvas"),
  onStatus: (text) => { byId("scanner-status").textContent = text; },
  onCode: (text) => {
    try {
      const code = codeFromInput(text);
      byId("paste-invite").value = code;
      byId("scanner-status").textContent = "Invitation scanned. Joining…";
      byId("scanner-panel").hidden = true;
      void connect(code).then((connected) => { if (connected) closeInviteModal(); });
    } catch { byId("scanner-status").textContent = "This QR is not a Phantom invitation. Point at an invitation QR."; throw new Error("This QR is not a Phantom invitation. Tap Scan QR to try again."); }
  },
});
byId("scan-qr").onclick = () => { byId("scanner-panel").hidden = false; void scanner.start(); };
byId("stop-scan").onclick = () => { scanner.stop(); byId("scanner-panel").hidden = true; byId("scanner-status").textContent = "Camera stopped. You can enter an invitation below."; };
document.addEventListener("visibilitychange", () => { if (!appVisible()) { scanner.stop(); byId("scanner-panel").hidden = true; } });
let shareConnection = null,
  currentInvitation = null,
  invitationTimer = null,
  leaving = false,
  invitationPending = null;
async function ensureInvitation() {
  const connection = shareConnection;
  if (!connection) return;
  if (!currentInvitation || Math.min(currentInvitation.expiresAt,currentInvitation.codeExpiresAt??Infinity) <= Date.now()) {
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
    url = new URL(document.querySelector('meta[name="phantom-public-url"]')?.content || location.href);
  url.search = "";
  url.hash = new URLSearchParams({ pair: shown.linkCode ?? shown.code }).toString();
  shareUrl = url.href;
  byId("invite-link").value = shareUrl;
  byId("invite-code").value = shown.code;
  byId("invite-expiry").textContent =
    "One device · expires in " +
    Math.max(1, Math.ceil((Math.min(shown.expiresAt,shown.codeExpiresAt??Infinity) - Date.now()) / 60000)) +
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
    Math.max(1, Math.min(shown.expiresAt,shown.codeExpiresAt??Infinity) - Date.now()),
  );
}
async function connect(code = "") {
  if (connecting || network || connectionQueue.busy) return;
  scanner.stop();
  byId("scanner-panel").hidden = true;
  const submittedInput = byId("paste-invite").value;
  const generation = ++connectionGeneration;
  if (code) byId("join-status").textContent = "Joining… Keep this screen open.";
  diagnostics.record("connect-start", {
    generation,
    role: code ? "guest" : "owner",
  });
  connecting = true;
  const controller = joinController = new AbortController();
  byId("connect-action").disabled = true;
  renderAvailability();
  setStatus("Connecting…");
  try {
    if (!window.isSecureContext || !crypto.subtle)
      throw Error("Open Phantom Share over HTTPS or localhost.");
    if (!savedIdentity) throw Error("Site storage is required.");
    const connection = await connectionQueue.run(async () => {
      await acquireIdentityLock(generation);
      if (controller.signal.aborted || generation !== connectionGeneration) throw Error("Connection cancelled.");
      return connectShare({
        seed: savedIdentity,
        code,
        name: displayName(),
        signal: controller.signal,
        onEvent: (event) => {
          if (generation !== connectionGeneration) return;
          if (
            /MEMBERSHIP|PRESENCE|CONNECTION|ADMISSION|RTC|SIGNAL|OFFER_ORDER|RECOVER|CLOSED|ERROR/.test(
              event.type,
            )
          )
            diagnostics.record(event.type, event.payload);
        },
        onState: (state) => {
          if (generation !== connectionGeneration) return;
          diagnostics.record("network-state", state);
          updateConnectionStatus(state);
        },
        onError: (error) => {
          if (generation === connectionGeneration) showError(error);
        },
        onClaim: ({ code: claimed }) => {
          if (generation !== connectionGeneration) return;
          if (currentInvitation?.code === claimed) {
            currentInvitation = null;
            byId("invite-code").value = "Used";
            byId("invite-link").value = "";
            if (!byId("invite-modal").classList.contains("hidden"))
              void ensureInvitation().catch(showError);
          }
        },
      });
    }, controller.signal);
    if (generation !== connectionGeneration) {
      await connection.close();
      return;
    }
    shareConnection = connection;
    network = connection.network;
    rampControl = attachRampTest({
      network,
      onDiagnostic: (type, data) => diagnostics.record(type, data),
      onRequest: (peer, settings, { signal } = {}) => {
        if (hasActiveTransfers() || !appVisible())
          return false;
        return requestRampApproval({
          root: byId("ramp-approval"),
          accept: byId("ramp-accept"),
          decline: byId("ramp-decline"),
          label: byId("ramp-approval-text"),
          signal,
          message: `The other device requests a connection test. Up to ${settings?.lowWaterBytes === 4194304 ? "0.125" : settings?.flowControls?.length === 2 ? "1.25" : settings?.maxStageBytes === 1073741824 ? "18" : "3.5"} GiB of generated traffic may be sent. Keep both pages visible.`,
        });
      },
      onUpdate: (update) => {
        if (generation === connectionGeneration) updateRampTest(update);
      },
    });
    admin = connection.admin;
    invite = network.invite;
    const context = parseInvite(invite);
    sharing = createShareSession({
      network: network,
      admin: admin,
      adminPublicKey: context.adminPublicKey,
      onChange: () => {
        if (generation !== connectionGeneration) return;
        if (rampBusy && hasActiveTransfers())
          rampControl?.cancel("File transfer started");
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
        shareConnection?.setTransferActive(hasActiveTransfers());
        if (preparationActivity()) updateConnectionStatus(network.getState());
      },
      onError: (error) => {
        if (generation === connectionGeneration) showError(error);
      },
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
      if (generation !== connectionGeneration) return;
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
      if (generation !== connectionGeneration) return;
      diagnostics.record("membershipChanged", event);
      renderPeers();
    });
    network.on("event", (event) => {
      if (generation !== connectionGeneration) return;
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
    byId("connect-action").hidden = false;
    renderPeers();
    renderTransfers();
    updateConnectionStatus(network.getState());
    if (code) byId("join-status").textContent = "Connected.";
    return true;
  } catch (error) {
    if (generation === connectionGeneration) {
      cleanupSession(error.message);
      showError(error);
      if (code) {
        pairingIntent = "connect";
        byId("paste-invite").value = submittedInput || code;
        await openPairingModal();
        byId("join-status").textContent = error.message;
      }
    }
    return false;
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
    !readyPeers().length && !connecting &&
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
async function openConnectionMenu() {
  pairingIntent = "choice";
  return openPairingModal();
}
async function openConnectModal() {
  pairingIntent = "connect";
  return openPairingModal();
}
async function openInviteModal() {
  pairingCreatesSession = !network && !connecting;
  pairingIntent = "invite";
  return openPairingModal();
}
async function openPairingModal() {
  const panelGeneration = ++pairingPanelGeneration;
  if (!displayName()) {
    promptName();
    return;
  }
  const choosing = pairingIntent === "choice";
  const joining = pairingIntent === "connect";
  byId("connection-choice").hidden = !choosing;
  byId("pairing-back").hidden = choosing;
  byId("choose-join").disabled = !!network;
  scanner.stop();
  byId("scanner-panel").hidden = true;
  byId("scanner-status").textContent = "";
  byId("join-status").textContent = "";
  byId("pairing-title").textContent = choosing ? "Connect" : joining ? "Join" : "Create";
  byId("pairing-description").textContent = choosing ? "Create a connection or join another device." : joining
    ? "Enter a pairing code, paste a link, or scan a QR."
    : "Scan this QR on the other device, or share the code or link.";
  byId("join-section").hidden = !joining;
  byId("share-section").hidden = choosing || joining;
  byId("invite-modal").classList.remove("hidden");
  byId("invite-modal").classList.add("flex");
  if (choosing || joining) return;
  byId("invite-code").value = "Generating…";
  byId("invite-link").value = "";
  const qr = byId("invite-qr");
  qr.getContext("2d")?.clearRect(0, 0, qr.width, qr.height);
  try {
    if (!network) await connect();
    if (panelGeneration !== pairingPanelGeneration) return;
    if (network) await ensureInvitation();
    else byId("invite-code").value = "Unable to create invitation. Try Invite again.";
  } catch (error) {
    byId("invite-code").value = "Unable to create invitation. Try Invite again.";
    showError(error);
  }
}
function closeInviteModal() {
  ++pairingPanelGeneration;
  pairingCreatesSession = false;
  scanner.stop();
  byId("scanner-panel").hidden = true;
  (byId("invite-modal").classList.add("hidden"),
    byId("invite-modal").classList.remove("flex"));
}
function dismissPairingModal() {
  const cancelCreated = pairingCreatesSession && !peers().length;
  closeInviteModal();
  if (cancelCreated) cleanupSession("Creation cancelled. Connect to create or join.");
}
async function backFromPairingModal() {
  dismissPairingModal();
  const generation = connectionGeneration;
  await connectionQueue.settled();
  if (generation === connectionGeneration) return openConnectionMenu();
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
  // Invalidate callbacks and detach live references before closing transports.
  ++connectionGeneration;
  const old = {
    ramp: rampControl, controller: joinController, connection: shareConnection,
    session: sharing, network, release: releaseIdentityLock,
  };
  rampControl = null;
  rampBusy = false;
  joinController = null;
  shareConnection = null;
  sharing = null;
  network = null;
  admin = null;
  releaseIdentityLock = null;
  clearTimeout(invitationTimer);
  invitationTimer = null;
  currentInvitation = null;
  invitationPending = null;
  const dispose = (action) => {
    try { action(); }
    catch (error) { diagnostics.record("reset-error", { message: error.message }); }
  };
  dispose(() => old.controller?.abort());
  // Register ending synchronously, before any UI can start a replacement.
  void connectionQueue.cleanup(async () => {
    try {
      await closeSessionOwners([old.ramp,old.session,old.connection,old.network],
        error => diagnostics.record("reset-error", {message:error.message}));
    } finally { dispose(() => old.release?.()); }
  });
  connecting = false;
  leaving = false;
  renderAvailability();
  recipientModes.clear();
  for (const url of downloadUrls.values()) URL.revokeObjectURL(url);
  downloadUrls.clear();
  cancelNativeSave();
  invite = "";
  shareUrl = "";
  pendingCode = "";
  byId("file-input").value = "";
  for (const id of ["invite-code", "invite-link", "paste-invite"])
    byId(id).value = "";
  byId("invite-expiry").textContent = "";
  byId("scanner-status").textContent = "";
  const qr = byId("invite-qr");
  qr.getContext("2d")?.clearRect(0, 0, qr.width, qr.height);
  for (const id of ["ramp-dialog", "history-dialog"])
    document.getElementById(id)?.close?.();
  byId("ramp-approval").classList.add("hidden");
  byId("toast").classList.add("opacity-0");
  byId("portal-drop-zone").classList.remove("portal-drop-active");
  byId("connect-action").disabled = connectionQueue.busy;
  byId("leave-action").disabled = true;
  byId("connect-action").hidden = false;
  byId("connect-action").querySelector("span").textContent = "Connect";
  const url = new URL(location.href);
  url.hash = "";
  url.search = "";
  history.replaceState(null, "", url);
  closeInviteModal();
  renderPeers();
  renderTransfers();
  if (connectionQueue.busy) {
    const resetGeneration = connectionGeneration;
    setStatus("Cleaning up connection…");
    void connectionQueue.settled().then(() => {
      diagnostics.record("connection-cleanup", {generation:resetGeneration,status:"complete"});
      diagnostics.flush();
      if (resetGeneration !== connectionGeneration) return;
      byId("connect-action").disabled = false;
      setStatus(message);
      renderAvailability();
    });
  } else setStatus(message);
}
async function leaveNetwork() {
  if (!network && !connecting) return;
  if (connecting && !network) {
    cleanupSession("Connection cancelled.");
    return;
  }
  leaving = true;
  renderAvailability();
  const current = network, generation = connectionGeneration;
  try {
    await leaveAndReset({
      leave: () => current.leave(),
      onError: (error) =>
        diagnostics.record("leave-unconfirmed", { message: error.message }),
      reset: (confirmed) => {
        if (generation !== connectionGeneration || network !== current) return;
        cleanupSession(
          confirmed
            ? "You left the session."
            : "Session closed locally; Leave could not be confirmed.",
        );
      },
    });
  } finally {
    // A revoked/closed session may already have reset and been replaced.
    if (generation === connectionGeneration && network === current) {
      leaving = false;
      renderAvailability();
    }
  }
}
async function removeDevice(id) {
  if (!shareConnection?.isOwner) return;
  const connection = shareConnection, generation = connectionGeneration;
  try {
    await connection.removePeer(id);
    if (generation !== connectionGeneration || connection !== shareConnection) return;
    recipientModes.delete(id);
    renderPeers();
    setStatus("Device removed. Returning requires a fresh invitation.");
  } catch (error) {
    if (generation === connectionGeneration) showError(error);
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
  if (network || connecting || leaving) return;
  (byId("name-dialog").showModal(),
    (byId("display-name").value = displayName()),
    byId("display-name").focus());
}
Object.assign(window, {
  openConnectionMenu,
  openConnectModal,
  openInviteModal: openInviteModal,
  closeInviteModal: dismissPairingModal,
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
  e.key === "Escape" && dismissPairingModal();
});
byId("orbital-peers-container").addEventListener("click", (e) => {
  let a = e.target.closest("button[data-peer]");
  a &&
    (a.dataset.mode === "remove"
      ? removeDevice(a.dataset.peer)
      : setRecipientMode(a.dataset.peer, a.dataset.mode));
});
byId("orbital-peers-container").addEventListener("change", (e) => {
  const select = e.target.closest("select[data-recipient-peer]");
  if (!select || !["default", "direct", "exclude"].includes(select.value)) return;
  setRecipientMode(select.dataset.recipientPeer, select.value);
  const replacement = [...byId("orbital-peers-container").querySelectorAll("select[data-recipient-peer]")].find(el => el.dataset.recipientPeer === select.dataset.recipientPeer);
  replacement?.focus({ preventScroll: true });
});
byId("transfers-container").addEventListener("click", (e) => {
  let a = e.target.closest("[data-download]");
  a && saveFile(a.dataset.download);
});
byId("choose-create").onclick = openInviteModal;
byId("choose-join").onclick = openConnectModal;
byId("pairing-back").onclick = backFromPairingModal;
byId("join-network").onclick = async () => {
  if (connecting || network || connectionQueue.busy) return;
  byId("join-status").textContent = "";
  try {
    const code = codeFromInput(byId("paste-invite").value);
    if (await connect(code)) closeInviteModal();
  } catch (error) {
    byId("join-status").textContent = error.message;
    showError(error);
  }
};
byId("cancel-join").onclick = () => {
  if (!connecting || network) return;
  cleanupSession("Joining cancelled.");
  pairingIntent = "connect";
  void openPairingModal().then(() => {
    byId("join-status").textContent = "Joining cancelled. Enter an invitation to try again.";
  });
};
byId("profile-action").onclick = promptName;
byId("name-form").onsubmit = async (e) => {
  e.preventDefault();
  let a = byId("display-name").value.trim();
  if (!a || a.length > 128 || /[\x00-\x1f\x7f]/.test(a)) {
    setStatus("Enter a name of 1\u2013128 characters.");
    return;
  }
  if (network || connecting || leaving) {
    setStatus("Leave the session before changing your name.");
    return;
  }
  try {
    (localStorage.setItem("phantom-share-name", a),
      (byId("your-name").textContent = a),
      byId("name-dialog").close(),
      pendingCode ? await connect(pendingCode) : pairingIntent ? await openPairingModal() : setStatus("Invite a device, or Connect using an invitation."));
  } catch (o) {
    showError(o);
  }
};
byId("name-dialog").addEventListener("cancel", (e) => {
  displayName() || e.preventDefault();
});
function hasActiveTransfers() {
  return [...(sharing?.assets.values() || [])].some(
    (asset) => !["sent", "received", "failed", "interrupted"].includes(asset.status),
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
      `${r.flowControl === "native-buffer" ? "Native buffer" : "Receiver credit"} · ${(r.windowBytes / 1048576).toFixed(2)} MiB window · ${(r.receiverBytesPerSecond / 1e6).toFixed(2)} MB/s · channel response ${r.rttMedianMs?.toFixed(1) ?? "n/a"} ms${r.limitedByByteBudget ? " · BYTE BUDGET REACHED" : ""}\n`;
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
      comparePacing: byId("ramp-profile").value === "compare",
      eventDriven: byId("ramp-profile").value === "event",
    })
    .catch((error) => {
      byId("ramp-status").textContent = error.message;
    });
};
byId("ramp-stop").onclick = () => rampControl?.cancel();
document.addEventListener("visibilitychange", () => {
  if (!appVisible() && rampBusy)
    rampControl?.cancel("Test stopped because this page became hidden");
});
byId("ramp-download").onclick = () => {
  if (!lastRampReport) return;
  const report = { ...lastRampReport, diagnostics: diagnostics.report() };
  void saveBlob(new Blob([JSON.stringify(report,null,2)],{type:'application/json'}),
    'phantom-ramp-'+new Date().toISOString().replace(/[:.]/g,'-')+'.json').catch(reportSaveError);
};
byId("download-logs").onclick = () => {
  recordEnvironment("export");
  diagnostics.record("connection-summary", {
    state: network?.getState()?.state,
    connected: peers().length,
    ready: readyPeers().length,
  });
  void saveBlob(new Blob([JSON.stringify(diagnostics.report(),null,2)],{type:'application/json'}),
    'phantom-share-'+new Date().toISOString().replace(/[:.]/g,'-')+'.json').catch(reportSaveError);
};
window.addEventListener("phantom-network-change", event => {
  if (!ownsNativeSession(event.detail?.token)) return;
  diagnostics.record("network-change", {reason:event.detail.reason});
  cleanupSession("Network changed. Connect again.");
});
window.addEventListener("phantom-foreground",()=>{nativeForeground=true;recordEnvironment("native-foreground");});
window.addEventListener("phantom-background",()=>{nativeForeground=false;scanner.stop();byId("scanner-panel").hidden=true;if(rampBusy)rampControl?.cancel("Test stopped because app was backgrounded");recordEnvironment("native-background");});
window.addEventListener("pagehide", () => {
  cancelNativeSave();
  scanner.stop();
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
