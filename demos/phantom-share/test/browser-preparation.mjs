/** Real app DOM/session and SDK preparation, with controlled network and preparation gates. No native RTC speed claim. */
import http from "node:http";
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { build } from "esbuild";
const { chromium } = await import(
  process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES + "/playwright/index.mjs"
);
const root = new URL("../", import.meta.url);
const fixture = `
import {createAdminIdentity} from '../../../packages/phantom-core/src/authorization.js';
import {createInvite,parseInvite} from '../../../packages/phantom-core/src/invite.js';
import {createFileDistribution} from '../../../packages/phantom-core/src/files.js';
export const codeFromInput=x=>x;
export async function connectShare(){
 const admin=await createAdminIdentity(),invite=createInvite({adminPublicKey:admin.publicKey,doorways:[{type:'memory'}]}),handlers=new Map(),peer='2'.repeat(16),self='1'.repeat(16);let manifest;
 const emit=(type,data)=>{for(const fn of handlers.get(type)||[])fn(data);};window.__uiEmit=emit;
 const fileService=createFileDistribution({core:{onEvent(){return()=>{};},command(){}},emit:(type,data)=>emit('distributionFileEvent',{fileEvent:type,...data})});
 const network={async leave(){},invite,networkId:parseInvite(invite).networkId,getState:()=>({peerId:self,state:'admitted',role:'syscon1',admitted:true}),getPeers:()=>[{hKey:self},{hKey:peer,handle:'Receiver'}],getPeerConnections:()=>[{peerId:peer,state:'ready'}],getDistributionState:()=>({assets:[]}),
 on(type,fn){if(!handlers.has(type))handlers.set(type,new Set());handlers.get(type).add(fn);return()=>handlers.get(type)?.delete(fn);},send(to,text){(window.__messages??=[]).push(JSON.parse(text));},close(){fileService.close();},
 async prepareDistributionFile(file,options){const gate=new Promise((resolve,reject)=>{window.__prep={release:resolve,reject,emitState:()=>emit('state',network.getState()),progress:(processed,total)=>emit('distributionFileEvent',{fileEvent:'filePreparationProgress',processed,total}),publications:window.__publications||0};});await gate;manifest=await fileService.publish(file,options);window.__fixtureManifest=manifest;return manifest;},
 async distributeFile(){window.__publications=(window.__publications||0)+1;return manifest;},async getDistributedFile(){throw Error('No fixture file');}};
 return {network,admin,close:()=>network.close()};
}`;
const bundle = await build({
  entryPoints: [new URL("src/app.js", root).pathname],
  bundle: true,
  format: "esm",
  write: false,
  plugins: [
    {
      name: "controlled-network",
      setup(b) {
        if (process.env.PHANTOM_PREPARATION_BASELINE)
          b.onLoad(
            { filter: /\/demos\/phantom-share\/src\/(app|session)\.js$/ },
            (args) => ({
              contents: execFileSync(
                "git",
                [
                  "show",
                  process.env.PHANTOM_PREPARATION_BASELINE +
                    ":demos/phantom-share/src/" +
                    args.path.split("/").at(-1),
                ],
                { encoding: "utf8" },
              ),
              loader: "js",
              resolveDir: new URL("src/", root).pathname,
            }),
          );
        b.onLoad(
          { filter: /\/demos\/phantom-share\/src\/membership\.js$/ },
          () => ({
            contents: fixture,
            loader: "js",
            resolveDir: new URL("src/", root).pathname,
          }),
        );
      },
    },
  ],
});
const server = http.createServer(async (req, res) => {
  try {
    const pathname = new URL(req.url, "http://local").pathname;
    res.setHeader(
      "content-type",
      pathname === "/app.js"
        ? "text/javascript"
        : pathname === "/style.css"
          ? "text/css"
          : "text/html",
    );
    res.end(
      pathname === "/app.js"
        ? bundle.outputFiles[0].text
        : await readFile(
            new URL(
              pathname === "/style.css" ? "src/style.css" : "src/index.html",
              root,
            ),
          ),
    );
  } catch {
    res.statusCode = 500;
    res.end();
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
let browser;
try {
  browser = await chromium.launch({
    headless: true,
    executablePath: process.env.PHANTOM_CHROMIUM_EXECUTABLE,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
  });
  const page = await browser.newPage({ viewport: { width: 390, height: 844 } }),
    errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.addInitScript(() =>
    localStorage.setItem("phantom-share-name", "Preparation test"),
  );
  await page.route("https://fonts.googleapis.com/**", (r) => r.abort());
  await page.route("https://fonts.gstatic.com/**", (r) => r.abort());
  await page.goto(
    "http://127.0.0.1:" + server.address().port + "/#pair=fixture",
  );
  await page.waitForFunction(
    () => !document.getElementById("file-input").disabled,
  );
  const file = {
    name: "32MiB.mp4",
    mimeType: "video/mp4",
    buffer: Buffer.alloc(32 * 1024 * 1024, 19),
  };
  await page.locator("#file-input").setInputFiles(file);
  await page.waitForFunction(() => !!window.__prep);
  assert.match(
    await page.locator("#transfers-container").textContent(),
    /32MiB.mp4.*preparing/s,
  );
  assert.equal(await page.locator("#file-input").isDisabled(), true);
  await page.evaluate(() => {
    window.__prep.progress(256, 512);
    window.__prep.emitState();
  });
  assert.equal(
    await page.locator('[aria-label="File preparation"]').getAttribute("value"),
    "50",
  );
  assert.match(
    await page.locator("#connection-status").textContent(),
    /Preparing.*50%/,
  );
  await page.evaluate(() => window.__prep.release());
  await page.waitForFunction(() => window.__publications === 1);
  await page.waitForFunction(
    () => !document.getElementById("file-input").disabled,
  );
  assert.match(
    await page.locator("#transfers-container").textContent(),
    /recipients received/,
  );
  assert.equal(
    await page.locator('[aria-label="File preparation"]').count(),
    0,
  );
  console.log(
    "32 MiB selection creates preparation row immediately; 50% survives connection updates; actual SDK preparation publishes once",
  );
  await page.evaluate(() => {
    const id = "sha256:" + "a".repeat(64);
    window.__uiEmit("distributionFileEvent", {
      fileEvent: "fileOffer",
      manifest: {
        id,
        name: "Incoming.mp4",
        size: 33554432,
        type: "video/mp4",
        total: 512,
      },
    });
    window.__uiEmit("distributionProgress", {
      id,
      index: 7,
      received: 8,
      total: 512,
    });
  });
  assert.equal(
    await page.locator('[aria-label="File received"]').getAttribute("value"),
    "2",
  );
  console.log(
    "Receiver bar advances from persisted chunk progress before inventory batch completion",
  );
  await page
    .locator("#file-input")
    .setInputFiles({ ...file, name: "failed.mp4", buffer: Buffer.alloc(1024) });
  await page.waitForFunction(
    () => document.getElementById("file-input").disabled,
  );
  await page.evaluate(() =>
    window.__prep.reject(Error("Controlled preparation read failure")),
  );
  await page.waitForFunction(
    () => !document.getElementById("file-input").disabled,
  );
  assert.match(
    await page.locator("#transfers-container").textContent(),
    /Controlled preparation read failure/,
  );
  assert.equal(await page.evaluate(() => window.__publications), 1);
  assert.deepEqual(errors, []);
  console.log(
    "Preparation failure remains visible, releases picker and does not publish",
  );
  await page.evaluate(() => {
    const peer = "2".repeat(16);
    window.__uiEmit("message", {
      from: peer,
      text: JSON.stringify({
        app: "phantom-share/1",
        type: "receipt",
        id: window.__fixtureManifest.id,
      }),
    });
    window.__uiEmit("distributionComplete", {
      id: "sha256:" + "a".repeat(64),
      blob: new Blob(["controlled UI fixture"]),
    });
  });
  let nativeDialogs = 0;
  page.on("dialog", (dialog) => {
    nativeDialogs++;
    void dialog.dismiss();
  });
  async function incomingTest(compare = false) {
    return page.evaluate(async (compare) => {
      const pc = new RTCPeerConnection();
      pc.createDataChannel("phantom-ramp");
      await pc.setLocalDescription(await pc.createOffer());
      window.__approvalProbe = pc;
      const id = crypto.randomUUID();
      window.__approvalId = id;
      window.__uiEmit("message", {
        from: "2".repeat(16),
        text: JSON.stringify({
          app: "phantom-ramp/1",
          kind: "offer",
          id,
          description: pc.localDescription,
          settings: compare
            ? {
                durationMs: 3000,
                maxStageBytes: 67108864,
                windows: [262144, 1048576, 4194304, 1048576, 262144],
                flowControls: ["receiver-credit", "native-buffer"],
              }
            : { durationMs: 3000, maxStageBytes: 268435456 },
        }),
      });
      return id;
    }, compare);
  }
  async function remoteStop() {
    await page.evaluate(() => {
      window.__uiEmit("message", {
        from: "2".repeat(16),
        text: JSON.stringify({
          app: "phantom-ramp/1",
          kind: "stop",
          id: window.__approvalId,
          reason: "Phone stopped during approval",
        }),
      });
      window.__approvalProbe.close();
    });
  }
  const canceledId = await incomingTest();
  await page.locator("#ramp-accept").waitFor({ state: "visible" });
  assert.match(await page.locator("#ramp-approval-text").textContent(), /3.5/);
  await remoteStop();
  await page.waitForFunction(
    () => document.getElementById("ramp-approval").hidden,
  );
  await page.evaluate(() => document.getElementById("ramp-accept").click());
  assert.equal(
    await page.evaluate(
      (id) => window.__messages.some((m) => m.kind === "answer" && m.id === id),
      canceledId,
    ),
    false,
  );
  await incomingTest(true);
  await page.locator("#ramp-accept").waitFor({ state: "visible" });
  assert.match(await page.locator("#ramp-approval-text").textContent(), /1.25/);
  await page.locator("#ramp-accept").click();
  await page.waitForFunction(() =>
    window.__messages.some(
      (m) => m.kind === "answer" && m.id === window.__approvalId,
    ),
  );
  await remoteStop();
  assert.equal(nativeDialogs, 0);
  assert.match(
    await page.locator("#ramp-status").textContent(),
    /Phone stopped during approval/,
  );
  await page.evaluate(() => document.getElementById("ramp-dialog").close());
  console.log(
    "Remote-initiated 3.5 GiB approval is nonblocking; cancel invalidates acceptance; accepted offer answers; peer stop reason survives",
  );
  const logsReady = page.waitForEvent("download");
  await page.locator("#download-logs").click();
  const downloaded = await logsReady;
  const diagnostics = JSON.parse(
    await readFile(await downloaded.path(), "utf8"),
  );
  assert.ok(diagnostics.rows.some((r) => r.type === "ramp-approval-request"));
  assert.ok(
    diagnostics.rows.some(
      (r) => r.type === "ramp-approval-result" && r.data.admitted === true,
    ),
  );
  assert.ok(
    diagnostics.rows.some(
      (r) =>
        r.type === "ramp-stop" &&
        r.data.reason.includes("Phone stopped during approval"),
    ),
  );
  assert.equal(JSON.stringify(diagnostics).includes("a=ice-ufrag"), false);
  console.log(
    "Download logs retains approval and exact stop reason without raw signaling",
  );

  await page.locator("#file-input").setInputFiles({
    ...file,
    name: "cancelled.mp4",
    buffer: Buffer.alloc(1024),
  });
  await page.waitForFunction(
    () => document.getElementById("file-input").disabled,
  );
  await page.evaluate(async () => {
    const pending = window.__prep;
    await window.leaveNetwork();
    pending.release();
  });
  await page.waitForTimeout(100);
  assert.match(
    await page.locator("#connection-status").textContent(),
    /You left the session/,
  );
  assert.equal(await page.evaluate(() => window.__publications), 1);
  assert.deepEqual(errors, []);
  console.log(
    "Leaving during preparation fences late publication and status updates",
  );
} finally {
  await browser?.close();
  await new Promise((r) => server.close(r));
}
