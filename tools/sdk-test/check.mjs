import assert from "node:assert/strict";
import http from "node:http";
import { readFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { chromium } from "../swarm/node_modules/playwright/index.mjs";
import { startServer } from "../swarm/server.mjs";
const bundle = readFileSync(new URL("./dist/app.js", import.meta.url), "utf8"), html = readFileSync(new URL("./index.html", import.meta.url), "utf8");
const server = await startServer(bundle, () => {
}), output = new URL("./reports/",import.meta.url).pathname;
mkdirSync(output,{recursive:true});
const appServer=http.createServer((req,res)=>{res.setHeader("Content-Type",req.url==="/app.js"?"text/javascript":"text/html");res.end(req.url==="/app.js"?bundle:html);});
await new Promise(resolve=>appServer.listen(0,"127.0.0.1",resolve));
const appUrl=`http://127.0.0.1:${appServer.address().port}`;
let browser;
const pages = [];
try {
  browser = await chromium.launch({ headless: true, ...process.env.PHANTOM_CHROMIUM_PATH ? { executablePath: process.env.PHANTOM_CHROMIUM_PATH } : {}, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
  for (let i = 0; i < 3; i++) {
    const context = await browser.newContext({ acceptDownloads: true, ...(i===1?{viewport:{width:390,height:844},isMobile:true,hasTouch:true}: {}) });
    const p = await context.newPage();
    p.on("console",m=>{if(m.type()==="error")console.error("browser:",m.text());});
    p.on("pageerror", (e) => {
      throw e;
    });
    await p.addInitScript(()=>{
      window.__testChannels=[];const Native=window.RTCPeerConnection;
      window.RTCPeerConnection=new Proxy(Native,{construct(Target,args){const pc=new Target(...args),create=pc.createDataChannel.bind(pc);pc.createDataChannel=(...args)=>{const dc=create(...args);window.__testChannels.push(dc);return dc;};pc.addEventListener("datachannel",e=>window.__testChannels.push(e.channel));return pc;}});
    });
    await p.goto(appUrl);
    await p.locator("#advanced").evaluate(e=>e.open=true);
    await p.locator("#doorway").fill(server.url.replace("http:", "ws:"));
    await p.locator("#rtc").fill('{"iceServers":[]}');
    await p.locator("#name").fill("peer-" + i);
    pages.push(p);
  }
  const [source, receiver, excluded] = pages;
  // The fixture loopback HTTP origin cannot be scanned by a real phone; QR encoding is
  // verified separately and the fragment join flow is exercised in the mobile viewport.

  await source.locator("#create").click();
  await source.waitForFunction(() => document.querySelector("#invite").value.length > 0);
  const invite = await source.locator("#invite").inputValue();
  for (const p of [receiver, excluded]) {
    if(p===receiver){const link=await source.locator("#joinLink").inputValue();await p.goto("about:blank");await p.goto(link);await p.locator("#advanced").evaluate(e=>e.open=true);await p.locator("#rtc").fill('{"iceServers":[]}');await p.locator("#advanced").evaluate(e=>e.open=false);}
    else await p.locator("#invite").fill(invite);
    await p.locator("#join").click();
  }
  for (const p of pages) await p.waitForFunction(() => {
    try {
      const d = JSON.parse(document.querySelector("#state").textContent);
      return d.state.state === "admitted" && d.connections.length === 2 && d.connections.every((c) => c.state === "ready");
    } catch {
      return false;
    }
  }, {}, { timeout: 6e4 });
  const state = (p) => p.locator("#state").textContent().then(JSON.parse), receiverId = (await state(receiver)).state.peerId;
  assert.equal(await receiver.locator("#publish").isDisabled(),true,"joining peer cannot publish without admin key");
  assert.equal(await source.locator("#publish").isEnabled(),true,"creator can publish");
  await source.locator("#target").selectOption("");
  assert.equal(await source.locator("#send").isDisabled(),true,"blank target cannot submit");
  await source.locator("#target").selectOption(receiverId);
  await source.locator("#text").fill("native-page-message");
  await source.locator("#send").click();
  await receiver.waitForFunction(() => document.querySelector("#messages").textContent.includes("native-page-message"));
  const bytes = Buffer.from(Array.from({ length: 262144 }, (_, i) => (i * 31 + (i >>> 8) + 17) % 256)), expected = createHash("sha256").update(bytes).digest("hex");
  await source.locator(`#peers input[value="${receiverId}"]`).check();
  await source.locator("#file").setInputFiles({ name: "fixture.bin", mimeType: "application/octet-stream", buffer: bytes });
  await source.locator("#publish").click();
  await receiver.locator("#files a").waitFor({ timeout: 6e4 });
  const fileDownload = receiver.waitForEvent("download");
  await receiver.locator("#files a").click();
  const file = await fileDownload, path = join(output, "received.bin");
  await file.saveAs(path);
  assert.equal(createHash("sha256").update(readFileSync(path)).digest("hex"), expected);
  const reports = [];
  for (let i = 0; i < pages.length; i++) {
    const p = pages[i], download = p.waitForEvent("download");
    await p.locator("#report").click();
    const d = await download, path2 = join(output, `report-${i}.json`);
    await d.saveAs(path2);
    const report = JSON.parse(readFileSync(path2));
    assert.ok(report.build.revision && Object.keys(report.build.sourceHashes).length > 0);
    assert.equal(report.events.some((e) => e.type === "error"), false);
    reports.push(report);
  }
  assert.equal(reports[1].received[0][1].sha256, expected);
  assert.deepEqual(reports[2].received, []);
  assert.equal(await excluded.locator("#files a").count(), 0);
  assert.equal(reports[2].distribution.assets.some((a) => a.holders.some((h) => h.peer === reports[2].state.peerId && h.indexes.length > 0)), false);
  assert.equal(await source.locator("#publish").isEnabled(),true,"export must preserve creator publishing");
  await source.locator("#run").click();
  await source.waitForFunction(()=>document.querySelector("#results").textContent.includes("Baseline complete"),{}, {timeout:90000});
  const combined=source.waitForEvent("download");
  await source.locator("#combinedReport").click();
  const combinedPath=join(output,"combined.json");await (await combined).saveAs(combinedPath);
  const combinedReport=JSON.parse(readFileSync(combinedPath));
  assert.equal(combinedReport.companions.length,1);
  assert.equal(combinedReport.companions[0][0],receiverId);
  assert.ok(combinedReport.results.includes("PASS · 256 KiB"));
  assert.equal(await receiver.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,"mobile must fit viewport");
  // No network fault was injected: the recovery check must not invent a success.
  await source.locator("#armRecovery").click();
  await source.locator("#verifyRecovery").waitFor({state:"visible"});
  await source.waitForFunction(()=>!document.querySelector("#verifyRecovery").disabled);
  await source.locator("#verifyRecovery").click();
  await source.waitForFunction(()=>document.querySelector("#results").textContent.includes("INCONCLUSIVE"),{}, {timeout:60000});
  await source.locator("#armRecovery").click();
  await source.waitForFunction(()=>!document.querySelector("#verifyRecovery").disabled);
  await receiver.evaluate(()=>{const dc=window.__testChannels.find(c=>c.label==="six7"&&c.readyState==="open");if(!dc)throw Error("Native messaging channel not found");dc.close();});
  await source.locator("#verifyRecovery").click();
  await source.waitForFunction(()=>document.querySelector("#results").textContent.includes("PASS · Messaging responded after an observed interruption"),{}, {timeout:120000});
  // A reload keeps the failed/armed-session diagnostics, not private SDK state.
  await receiver.reload();
  await receiver.waitForFunction(()=>document.querySelector("#journalStatus").textContent.includes("restored"));
  await receiver.locator("#join").click();
  await receiver.waitForFunction(()=>{try{return JSON.parse(document.querySelector("#state").textContent).state.state==="admitted";}catch{return false;}},{},{timeout:60000});
  await source.waitForFunction(()=>!document.querySelector("#collect").disabled);
  await source.locator("#collect").click();
  await source.waitForFunction(()=>document.querySelector("#collectionStatus").textContent.includes("collected"),{}, {timeout:60000});
  await source.waitForFunction(()=>!document.querySelector("#collect").disabled);
  const retainedDownload=source.waitForEvent("download");await source.locator("#combinedReport").click();
  const retainedPath=join(output,"retained.json");await(await retainedDownload).saveAs(retainedPath);
  const retainedReport=JSON.parse(readFileSync(retainedPath));
  assert.ok(retainedReport.companions.some(([,r])=>r.retained?.events.some(e=>e.type==="test.recoveryArmed")),"rejoined companion report includes pre-reload evidence");
  assert.ok(retainedReport.controller.events.some(e=>e.type==="test.recoveryResult"&&e.data.message.startsWith("PASS")));
  await receiver.locator("#clearJournal").click();
  assert.equal(await receiver.evaluate(()=>sessionStorage.getItem("phantom-test-diagnostics-v1")),null);

  for (const p of pages) await p.locator("#leave").click();
  const post = receiver.waitForEvent("download");
  await receiver.locator("#report").click();
  const postPath = join(output, "after-leave.json");
  await (await post).saveAs(postPath);
  assert.ok(JSON.parse(readFileSync(postPath)).events.some((e) => e.type === "before.close"));
  console.log(JSON.stringify({ status: "passed", browser: browser.version(), bytes: bytes.length, sha256: expected, checks: ["three native peers", "message delivered", "selected receiver downloaded exact bytes", "excluded peer no download", "three JSON report downloads", "report after leave", "baseline acknowledgments and file hash", "report export preserves publish authority", "mobile fragment join and layout", "no-interruption recovery is inconclusive", "native channel-close recovery observed and acknowledged", "retained diagnostics collected after reload/rejoin"], reports: output }));
} catch (error) {
  for (let i = 0; i < pages.length; i++) console.error("PAGE", i, await pages[i].locator("#state").textContent(), await pages[i].locator("#notice").textContent(), (await pages[i].locator("#log").textContent()).slice(-3500));
  throw error;
} finally {
  await browser?.close();
  await server.close();
  await new Promise(resolve=>appServer.close(resolve));
}
