import { createBrowserPhantom } from "../../packages/phantom-doorways/src/browser-entry.js";
import { createAdminIdentity } from "../../packages/phantom-core/src/authorization.js";
import { createWebSocketDoorway, adaptWebSocketDoorway } from "../../packages/phantom-doorways/src/index.js";
const $ = (id) => document.getElementById(id), events = [], urls = /* @__PURE__ */ new Set(), received = /* @__PURE__ */ new Map();
let network, controller, opening = false, state, snapshot, dropped = 0, reportUrl;
const build = __SDK_TEST_BUILD__;
function record(type, data) {
  events.push({ at: (/* @__PURE__ */ new Date()).toISOString(), type, data });
  if (events.length > 1e3) {
    events.shift();
    dropped++;
  }
  $("log").textContent = events.slice(-15).map((e) => JSON.stringify(e)).join("\n");
}
function failure(error) {
  record("error", { message: error.message, code: error.code, diagnostics: error.diagnostics });
  $("notice").textContent = error.message;
}
function controls() {
  $("setup").disabled = opening || !!network;
  $("leave").disabled = !opening && !network;
  for (const id of ["send", "publish"]) $(id).disabled = !network;
}
function render() {
  if (network) {
    state = network.getState();
    snapshot = network.snapshot();
  }
  $("state").textContent = JSON.stringify({ state, connections: network?.getPeerConnections(), distribution: network?.getDistributionState() }, null, 2);
  if (!network) return;
  const selected = new Set([...document.querySelectorAll("#peers input:checked")].map((i) => i.value));
  $("peers").replaceChildren();
  for (const p of network.getPeers()) {
    if (p.hKey === state.peerId) continue;
    const label = document.createElement("label"), input = document.createElement("input");
    input.type = "checkbox";
    input.value = p.hKey;
    input.checked = selected.has(p.hKey);
    label.append(input, document.createTextNode(`${p.hKey} ${p.handle ?? ""} ${network.getPeerConnections().find((c) => c.peerId === p.hKey)?.state ?? "no messaging connection"}`));
    $("peers").append(label, document.createElement("br"));
  }
}
async function hash(blob) {
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer())), (b) => b.toString(16).padStart(2, "0")).join("");
}
async function localFile(id) {
  if (received.has(id)) return;
  const blob = await network.getDistributedFile(id), sha256 = await hash(blob), url = URL.createObjectURL(blob);
  urls.add(url);
  received.set(id, { bytes: blob.size, sha256 });
  const a = document.createElement("a");
  a.href = url;
  a.download = network.getDistributionState().assets?.find((m) => m.id === id)?.name ?? "received.bin";
  a.textContent = `Download ${id} (${blob.size} bytes; SHA-256 ${sha256})`;
  $("files").append(a, document.createElement("br"));
  record("file.local", { id, bytes: blob.size, sha256 });
}
async function start(join) {
  opening = true;
  controller = new AbortController();
  controls();
  try {
    const url = $("doorway").value.trim(), rtc = $("rtc").value.trim();
    if (url && !["ws:", "wss:"].includes(new URL(url).protocol)) throw Error("Doorway must use ws or wss");
    const sdk = createBrowserPhantom({ messaging: "automatic", distribution: {}, ...rtc ? { rtcConfiguration: JSON.parse(rtc) } : {}, ...!join ? { adminIdentity: await createAdminIdentity() } : {}, ...url ? { doorways: [{ type: "test-websocket", url }] } : {}, adapters: { "test-websocket": ({ spec }) => adaptWebSocketDoorway(createWebSocketDoorway({ url: spec.url })) } });
    const options = { name: $("name").value, signal: controller.signal, timeoutMs: Number($("deadline").value) * 1e3, onState: (s) => {
      state = s;
      record("state", s);
      render();
    }, onEvent: (e) => {
      if (e.type === "SNAPSHOT") {
        snapshot = e.payload;
        record("snapshot.reason", { reason: e.payload.reason });
        render();
      } else record("event", e);
    } };
    network = await (join ? sdk.join($("invite").value.trim(), options) : sdk.create(options));
    $("invite").value = network.invite;
    network.on("message", (m) => record("message", m));
    for (const type of ["peerConnection", "distributionProgress", "distributionConnectionFailed", "distributionConnectionClosed", "distributionJob", "distributionJobFailed", "distributionError", "distributionComplete"]) network.on(type, (data) => {
      record(type, type === "distributionComplete" ? { id: data.id, bytes: data.blob.size } : data);
      render();
      if (type === "distributionComplete") void localFile(data.id).catch(failure);
    });
    record("opened", { networkId: network.networkId });
    render();
  } catch (error) {
    failure(error);
  } finally {
    opening = false;
    controls();
  }
}
$("create").onclick = () => void start(false);
$("join").onclick = () => void start(true);
$("leave").onclick = () => {
  controller?.abort();
  if (network) {
    render();
    record("before.close", { state, snapshot });
    network.close();
    state = network.getState();
    snapshot = network.snapshot();
    network = null;
  }
  record("closed", { state });
  render();
  controls();
};
$("send").onclick = () => {
  try {
    network.send($("target").value.trim(), $("text").value);
    record("message.submitted", { to: $("target").value.trim(), text: $("text").value });
  } catch (error) {
    failure(error);
  }
};
$("publish").onclick = async () => {
  try {
    const file = $("file").files[0], recipients = [...document.querySelectorAll("#peers input:checked")].map((i) => i.value);
    if (!file || !recipients.length) throw Error("Choose a file and at least one recipient");
    const sha256 = await hash(file), manifest = await network.distributeFile(file, { recipients, name: file.name, type: file.type });
    record("file.published", { manifest, recipients, sha256 });
  } catch (error) {
    failure(error);
  }
};
$("snapshot").onclick = () => {
  render();
  record("snapshot.manual", { state, snapshot });
};
$("report").onclick = () => {
  try {
    render();
    const report = { build, browser: { userAgent: navigator.userAgent, secureContext: isSecureContext, origin: location.origin }, limits: { events: 1e3 }, dropped, state, snapshot, distribution: network?.getDistributionState(), received: [...received], events };
    if (reportUrl) {
      URL.revokeObjectURL(reportUrl);
      urls.delete(reportUrl);
    }
    reportUrl = URL.createObjectURL(new Blob([JSON.stringify(report, null, 2)], { type: "application/json" }));
    urls.add(reportUrl);
    const a = $("reportLink");
    a.href = reportUrl;
    a.download = "phantom-sdk-test.json";
    a.hidden = false;
    a.click();
    $("notice").textContent = "Report generated. If download was blocked, use the report link.";
  } catch (error) {
    failure(error);
  }
};
window.addEventListener("error", (e) => failure(e.error ?? Error(e.message)));
window.addEventListener("unhandledrejection", (e) => failure(e.reason));
window.addEventListener("pagehide", () => {
  controller?.abort();
  network?.close();
  for (const url of urls) URL.revokeObjectURL(url);
});
record("loaded", { build });
controls();
