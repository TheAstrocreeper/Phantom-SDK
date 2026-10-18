import http from "node:http";
import { readFile, mkdir } from "node:fs/promises";
import path from "node:path";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
const playwright = process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES
  ? process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES + "/playwright/index.mjs"
  : "playwright";
const { chromium } = await import(playwright);
const root = new URL("../dist/", import.meta.url).pathname,
  out = process.env.PHANTOM_BROWSER_OUTPUT ?? "/tmp/phantom-share-browser";
await mkdir(out, { recursive: true });
const server = http.createServer(async (req, res) => {
  try {
    const file = path.resolve(
      root,
      "." +
        new URL(req.url, "http://localhost").pathname.replace(
          /\/$/,
          "/index.html",
        ),
    );
    if (!file.startsWith(root)) throw Error();
    const body = await readFile(file);
    res.setHeader(
      "Content-Type",
      file.endsWith(".js")
        ? "text/javascript"
        : file.endsWith(".css")
          ? "text/css"
          : "text/html",
    );
    res.end(body);
  } catch {
    res.statusCode = 404;
    res.end();
  }
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const url = "http://127.0.0.1:" + server.address().port;
const browser = await chromium.launch({
  headless: true,
  ...(process.env.PHANTOM_CHROMIUM_EXECUTABLE
    ? { executablePath: process.env.PHANTOM_CHROMIUM_EXECUTABLE }
    : {}),
  args: [
    "--no-sandbox",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--disable-features=WebRtcHideLocalIpsWithMdns",
    "--allow-loopback-in-peer-connection",
  ],
});
const sockets = new Map(),
  events = new Map(),
  errors = [];
function matches(event, filter) {
  return (
    (!filter.kinds || filter.kinds.includes(event.kind)) &&
    (!filter["#t"] ||
      event.tags.some((t) => t[0] === "t" && filter["#t"].includes(t[1]))) &&
    (!filter.since || event.created_at >= filter.since)
  );
}
async function page(name, viewport = { width: 1280, height: 800 }) {
  const context = await browser.newContext({ viewport, acceptDownloads: true });
  try {
    await context.grantPermissions(["local-network-access"]);
  } catch {}
  await context.addInitScript(() => {
    const Base = window.RTCPeerConnection;
    window.__pcs = [];
    window.__rtcErrors = [];
    window.RTCPeerConnection = class extends Base {
      constructor(config) {
        super(config);
        window.__pcs.push(this);
      }
      createDataChannel(...args) {
        try {
          return super.createDataChannel(...args);
        } catch (e) {
          window.__rtcErrors.push(e.message);
          throw e;
        }
      }
      async createOffer(...args) {
        try {
          return await super.createOffer(...args);
        } catch (e) {
          window.__rtcErrors.push(e.message);
          throw e;
        }
      }
      async setLocalDescription(...args) {
        try {
          return await super.setLocalDescription(...args);
        } catch (e) {
          window.__rtcErrors.push(e.message);
          throw e;
        }
      }
    };
  });
  await context.addInitScript(
    (name) => localStorage.setItem("phantom-share-name", name),
    name,
  );
  await context.route("https://fonts.googleapis.com/**", (route) =>
    route.abort(),
  );
  await context.route("https://fonts.gstatic.com/**", (route) => route.abort());
  await context.routeWebSocket(/wss:\/\/.*/, (socket) => {
    const subscriptions = new Map();
    sockets.set(socket, subscriptions);
    socket.onClose(() => sockets.delete(socket));
    socket.onMessage((raw) => {
      let p;
      try {
        p = JSON.parse(raw);
      } catch {
        return;
      }
      if (p[0] === "REQ") {
        subscriptions.set(p[1], p[2]);
        for (const event of [...events.values()]
          .filter((e) => matches(e, p[2]))
          .slice(-100))
          socket.send(JSON.stringify(["EVENT", p[1], event]));
        socket.send(JSON.stringify(["EOSE", p[1]]));
      }
      if (p[0] === "CLOSE") subscriptions.delete(p[1]);
      if (p[0] === "EVENT") {
        const event = p[1];
        socket.send(JSON.stringify(["OK", event.id, true, ""]));
        if (events.has(event.id)) return;
        events.set(event.id, event);
        if (events.size > 1000) events.delete(events.keys().next().value);
        for (const [target, subs] of sockets)
          for (const [id, filter] of subs)
            if (matches(event, filter))
              target.send(JSON.stringify(["EVENT", id, event]));
      }
    });
  });
  const p = await context.newPage();
  p.on("pageerror", (error) => errors.push(name + ": " + error.message));
  return { context, page: p };
}
try {
  const owner = await page("Owner"),
    guest = await page("Guest");
  await owner.page.goto(url);
  await owner.page.locator("#connect-action").click();
  await owner.page.locator("#create-network").click();
  await owner.page.waitForFunction(
    () => document.getElementById("invite-code").value.startsWith("P1-"),
    {},
    { timeout: 30000 },
  );
  const first = await owner.page.locator("#invite-code").inputValue();
  await owner.page.screenshot({ path: out + "/pairing-desktop.png" });
  await guest.page.goto(url + "/#" + new URLSearchParams({ pair: first }));
  if (process.env.PHANTOM_BROWSER_UI_ONLY === "1") {
    await owner.page.waitForFunction(
      (first) =>
        document.getElementById("invite-code").value.startsWith("P1-") &&
        document.getElementById("invite-code").value !== first,
      first,
      { timeout: 30000 },
    );
    assert.ok(
      !(await guest.page
        .locator("#connection-status")
        .textContent()
        .then((t) => /invalid|denied|failed/i.test(t))),
    );
    console.log(
      "real browser code enrollment completed; native RTC checks skipped in UI-only mode",
    );
  } else {
    for (const p of [owner.page, guest.page])
      await p.waitForFunction(
        () =>
          document
            .getElementById("orbital-peers-container")
            .textContent.includes("ready"),
        {},
        { timeout: Number(process.env.PHANTOM_READY_TIMEOUT_MS ?? 60000) },
      );
    console.log("pairing established in two browser contexts");
    await owner.page.evaluate(() => window.closeInviteModal());
    const bytes = Buffer.from(
      "Phantom pairing browser transfer\n".repeat(2048),
    );
    await owner.page
      .locator("#file-input")
      .setInputFiles({
        name: "pairing-test.txt",
        mimeType: "text/plain",
        buffer: bytes,
      });
    await guest.page
      .locator("[data-download]")
      .first()
      .waitFor({ state: "visible", timeout: 60000 });
    const downloadPromise = guest.page.waitForEvent("download");
    await guest.page.locator("[data-download]").first().click();
    const download = await downloadPromise;
    const received = await readFile(await download.path());
    assert.equal(
      createHash("sha256").update(received).digest("hex"),
      createHash("sha256").update(bytes).digest("hex"),
    );
    console.log("browser transfer saved with matching SHA-256");
    await owner.page.locator('[data-mode="remove"]').click();
    await guest.page.waitForFunction(
      () =>
        document
          .getElementById("connection-status")
          .textContent.includes("Access ended"),
      {},
      { timeout: 20000 },
    );
    await guest.page.locator("#connect-action").click();
    await guest.page.locator("#paste-invite").fill(first);
    await guest.page.locator("#join-network").click();
    await guest.page.waitForFunction(
      () =>
        /inactive|already used|fresh invitation/i.test(
          document.getElementById("connection-status").textContent,
        ),
      {},
      { timeout: 20000 },
    );
    console.log("removed device cannot use its old code");
    await owner.page.locator("#connect-action").click();
    await owner.page.waitForFunction(
      () => document.getElementById("invite-code").value.startsWith("P1-"),
      {},
      { timeout: 20000 },
    );
    const fresh = await owner.page.locator("#invite-code").inputValue();
    assert.notEqual(fresh, first);
    await guest.page.locator("#paste-invite").fill(fresh);
    await guest.page.locator("#join-network").click();
    await guest.page.waitForFunction(
      () =>
        document
          .getElementById("orbital-peers-container")
          .textContent.includes("ready"),
      {},
      { timeout: 60000 },
    );
    await guest.page.locator("#leave-action").click();
    await guest.page.waitForFunction(
      () =>
        document
          .getElementById("connection-status")
          .textContent.includes("You left"),
      {},
      { timeout: 20000 },
    );
    console.log("fresh code admits the same device; Leave succeeds");
  }
  const mobile = await page("Mobile", { width: 390, height: 844 });
  await mobile.page.goto(url);
  await mobile.page.waitForFunction(()=>document.getElementById('portal-drop-zone').getAttribute('aria-disabled')==='true');
  assert.equal(await mobile.page.locator('#file-input').isDisabled(),true);
  assert.equal(await mobile.page.locator('#session-heading').textContent(),'Not connected');
  await mobile.page.locator('#portal-drop-zone').click({force:true});
  assert.equal(await mobile.page.locator('#invite-modal').evaluate(el=>el.classList.contains('hidden')),true);
  await mobile.page.locator('#connection-test').click();
  assert.equal(await mobile.page.locator('#ramp-start').isDisabled(),true);
  assert.equal(await mobile.page.locator('#ramp-dialog').evaluate(el=>el.open),true);
  await mobile.page.screenshot({path:out+'/ramp-panel-mobile.png'});
  await mobile.page.locator('#ramp-dialog').evaluate(el=>el.close());
  const logDownload = mobile.page.waitForEvent('download');
  await mobile.page.locator('#download-logs').click();
  const download = await logDownload;
  const report = JSON.parse(await readFile(await download.path(), 'utf8'));
  assert.equal(report.build, 'share-preparation-v7');
  assert.ok(report.rows.some(row=>row.type==='export'));
  assert.ok(report.rows.some(row=>row.type==='page-start'));
  console.log('disconnected mobile page exports diagnostic JSON');
  await mobile.page.locator("#connect-action").click();
  await mobile.page.screenshot({ path: out + "/pairing-mobile.png" });
  const previousInvite=await owner.page.locator("#invite-code").inputValue();
  await owner.page.evaluate(()=>window.closeInviteModal());
  await owner.page.locator('#leave-action').click();
  await owner.page.waitForFunction(()=>document.getElementById('session-heading').textContent==='Not connected');
  assert.equal(await owner.page.locator('#connect-action').isDisabled(),false);
  assert.equal(await owner.page.locator('#file-input').isDisabled(),true);
  assert.equal(await owner.page.locator('#leave-action').isDisabled(),true);
  await owner.page.locator('#connect-action').click();
  await owner.page.locator('#create-network').click();
  await owner.page.waitForFunction(previous=>{const code=document.getElementById('invite-code').value;return code.startsWith('P1-')&&code!==previous;},previousInvite);
  console.log('Leave resets local session and permits a new session; empty drop zone stays disabled');
  assert.deepEqual(errors, []);
  console.log(
    "browser " +
      (process.env.PHANTOM_BROWSER_UI_ONLY === "1"
        ? "enrollment/layout"
        : "full RTC") +
      " smoke passed; relay traffic was routed through a local fixture",
  );
} catch (error) {
  for (const context of browser.contexts())
    for (const p of context.pages()) {
      console.log(
        "page state",
        await p
          .locator("#connection-status")
          .textContent()
          .catch(() => ""),
      );
      console.log("RTC errors", await p.evaluate(() => window.__rtcErrors));
      console.log(
        "RTC",
        await p
          .evaluate(() =>
            window.__pcs?.map((pc) => ({
              state: pc.connectionState,
              ice: pc.iceConnectionState,
              signaling: pc.signalingState,
              local: pc.localDescription?.sdp
                .split("\r\n")
                .filter((l) => l.startsWith("a=candidate")),
              remote: pc.remoteDescription?.sdp
                .split("\r\n")
                .filter((l) => l.startsWith("a=candidate")),
            })),
          )
          .catch(() => []),
      );
    }
  console.log("page errors", errors);
  throw error;
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
