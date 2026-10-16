import QRCode from "../swarm/node_modules/qrcode/lib/browser.js";
import {createJournal} from "./journal.js";
import { createBrowserPhantom } from "../../packages/phantom-doorways/src/browser-entry.js";
import { createAdminIdentity } from "../../packages/phantom-core/src/authorization.js";
import { createWebSocketDoorway, adaptWebSocketDoorway } from "../../packages/phantom-doorways/src/index.js";
const $ = (id) => document.getElementById(id), events = [], messages = [], urls = /* @__PURE__ */ new Set(), received = /* @__PURE__ */ new Map();
let network, controller, opening = false, state, snapshot, dropped = 0, reportUrl, canPublish = false;
const build = __SDK_TEST_BUILD__;
let journal, retained=null, saveTimer, journalWarning="", closing=false, currentTest=null;
try {journal=createJournal(sessionStorage,"phantom-test-diagnostics-v1");retained=journal.previous;} catch(e) {journalWarning=e.message;}
function persist(){
 clearTimeout(saveTimer);
 if(!journal)return;
 const value={version:1,networkId:network?.networkId??retained?.networkId??null,peerId:state?.peerId,at:new Date().toISOString(),state,events:events.slice(-250),results:$("results").textContent,rtc:snapshot?.rtcDiagnostics??null};
 // Bound the stored log and native RTC history without silently exceeding browser quotas.
 if(JSON.stringify(value).length>330000)value.rtc=null;
 while(value.events.length&&JSON.stringify(value).length>330000)value.events.shift();
 if(!journal.save(value)){journalWarning=journal.error;$("journalStatus").textContent="Log retention unavailable: "+journalWarning;}
 else $("journalStatus").textContent="Diagnostics retained in this tab across reloads. Clear them below when finished.";
}
function schedulePersist(){clearTimeout(saveTimer);saveTimer=setTimeout(persist,250);}

function record(type, data) {
  events.push({ at: (/* @__PURE__ */ new Date()).toISOString(), type, data });
  if (events.length > 1e3) {
    events.shift();
    dropped++;
  }
  if(type === "message") {
    messages.push(data);if(messages.length>100)messages.shift();
    $("messages").textContent=messages.map(m=>`${m.from}: ${m.text}`).join("\n");
  }
  $("log").textContent = events.slice(-15).map((e) => JSON.stringify(e)).join("\n");
  schedulePersist();
}
function failure(error) {
  record("error", { message: error.message, code: error.code, diagnostics: error.diagnostics });
  $("notice").textContent = error.message;
}
function controls() {
  $("setup").disabled = opening || !!network;
  $("leave").disabled = !opening && !network;
  $("send").disabled = !network || !network.getPeerConnections().some(p=>p.peerId === $("target").value && p.state === "ready");
  $("publish").disabled = !network || !canPublish;
  const ready=!!network?.getPeerConnections().some(p=>p.peerId===$("target").value&&p.state==="ready");
  $("run").disabled = !canPublish || !ready || running;
  $("armRecovery").disabled = !canPublish || !ready || running || !!recovery;
  $("verifyRecovery").disabled = !network || !recovery || running;
  $("collect").disabled = !network || running;
  $("leave").disabled = !opening && !network;
  $("file").disabled = !network || !canPublish;
}
function render() {
  if (network) {
    state = network.getState();
    snapshot = network.snapshot();
  }
  $("summary").textContent = !network ? (opening ? "Connecting…" : "Not connected") : `${canPublish ? "Controller" : "Companion"} · ${state.state} · ${network.getPeerConnections().filter(p=>p.state==="ready").length} ready peer(s)`;
  $("state").textContent = JSON.stringify({ state, connections: network?.getPeerConnections(), distribution: network?.getDistributionState() }, null, 2);
  if (!network) return;
  const selected = new Set([...document.querySelectorAll("#peers input:checked")].map((i) => i.value));
  const target=$("target").value;
  $("target").replaceChildren(new Option("Choose a peer", ""));
  $("peers").replaceChildren();
  for (const p of network.getPeers()) {
    if (p.hKey === state.peerId) continue;
    const connection=network.getPeerConnections().find(c=>c.peerId===p.hKey);
    const option=new Option(`${p.hKey} ${p.handle??""} (${connection?.state??"not ready"})`,p.hKey);
    option.disabled=connection?.state!=="ready";$("target").append(option);
    const label = document.createElement("label"), input = document.createElement("input");
    input.type = "checkbox";
    input.value = p.hKey;
    input.checked = selected.has(p.hKey) || (network.getPeers().filter(p=>p.hKey!==state.peerId).length===1);
    label.append(input, document.createTextNode(`${p.hKey} ${p.handle ?? ""} ${network.getPeerConnections().find((c) => c.peerId === p.hKey)?.state ?? "no messaging connection"}`));
    $("peers").append(label, document.createElement("br"));
  }
  if([...$("target").options].some(o=>o.value===target&&!o.disabled))$("target").value=target;
  const ready=[...$("target").options].filter(o=>o.value&&!o.disabled);
  if(!$("target").value&&ready.length===1)$("target").value=ready[0].value;
  controls();
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
  $("fileStatus").textContent=`Received ${blob.size} bytes; SHA-256 ${sha256}. Download below.`;
}
async function start(join) {
  opening = true;
  controller = new AbortController();
  controls();
  try {
    if (!isSecureContext || !crypto.subtle) throw Error("Open this page over HTTPS, or use http://localhost:8000 on the PC. This address cannot run browser cryptography.");
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
    canPublish=!join;
    $("publishHelp").textContent=canPublish?"This creating page holds the admin key and can publish new files. Select recipients below.":"This joining page can receive files. Publish new files from the creating page; this page has no admin signing key.";
    $("invite").value = network.invite;
    closing=false;
    network.on("message", (m) => { if(!handleControl(m)) record("message", m); });
    showJoinLink();
    requestWake();
    for (const type of ["peerConnection", "distributionProgress", "distributionConnectionFailed", "distributionConnectionClosed", "distributionJob", "distributionJobFailed", "distributionError", "distributionComplete"]) network.on(type, (data) => {
      record(type, type === "distributionComplete" ? { id: data.id, bytes: data.blob.size } : data);
      if(type==="peerConnection"){
        if(recovery&&data.peerId===recovery.peer&&data.state!=="ready")recovery.interruption=true;
        if(data.state==="ready"&&canPublish&&deferredReports.has(data.peerId))void collectPeer(data.peerId).catch(()=>{});
      }
      render();
      if (type === "distributionComplete") {void localFile(data.id).catch(failure);void verifyFixture(data.id);}
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
$("target").onchange=controls;
$("create").onclick = () => void start(false);
$("join").onclick = () => void start(true);
$("leave").onclick = () => {
  closing=true;persist();cancelRequests("Test page left the network");fixtureChecks.clear();recovery=null;reportCache.clear();
  controller?.abort();
  if (network) {
    render();
    record("before.close", { state, snapshot });
    network.close();
    state = network.getState();
    snapshot = network.snapshot();
    network = null;
  }
  void wake?.release();
  record("closed", { state });
  render();
  controls();
};
$("send").onclick = () => {
  try {
    if(!network?.getPeerConnections().some(p=>p.peerId===$("target").value&&p.state==="ready"))throw Error("Select a ready peer before sending");
    network.send($("target").value.trim(), $("text").value);
    record("message.submitted", { to: $("target").value.trim(), text: $("text").value });
    $("notice").textContent="Submitted to "+$("target").value+". Check received messages on that peer for delivery.";
  } catch (error) {
    failure(error);
  }
};
$("publish").onclick = async () => {
  try {
    if(!canPublish)throw Error("Publish new files from the creating page, which holds the admin key");
    const file = $("file").files[0], recipients = [...document.querySelectorAll("#peers input:checked")].map((i) => i.value);
    if (!file) throw Error("Choose a file first, or use Run baseline to send a generated test file.");
    if (!recipients.length) throw Error("Select a recipient in the peer list above.");
    const sha256 = await hash(file), manifest = await network.distributeFile(file, { recipients, name: file.name, type: file.type });
    record("file.published", { manifest, recipients, sha256 });
    $("fileStatus").textContent=`Published ${file.name}: ${file.size} bytes; SHA-256 ${sha256}. Waiting for recipient completion.`;
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
    const report = { retained, results:$("results").textContent, build, browser: { userAgent: navigator.userAgent, secureContext: isSecureContext, origin: location.origin }, limits: { events: 1e3 }, dropped, state, snapshot, distribution: network?.getDistributionState(), received: [...received], events };
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
// Harness messages use the public SDK. They add acknowledgments for tests only.
let running=false, wake, recovery=null;
const pending=new Map(), companionReports=new Map(), fixtureChecks=new Map(), reportCache=new Map(), deferredReports=new Set(), collecting=new Set();
const prefix="PHANTOM_TEST_V1:", partSize=6000;
function testStatus(text){$("results").textContent=text;record("test.status",{text});}
function compactReport(){
 const report={build,browser:{userAgent:navigator.userAgent,secureContext:isSecureContext,origin:location.origin},networkId:network?.networkId,state:network?.getState()??state,rtc:snapshot?.rtcDiagnostics,distribution:network?.getDistributionState(),received:[...received],events:events.slice(-250),retained,journalWarning,results:$("results").textContent,truncated:[]};
 while(report.events.length&&JSON.stringify(report).length>1100000){report.events.shift();if(!report.truncated.includes("events"))report.truncated.push("events");}
 if(JSON.stringify(report).length>1100000){report.rtc=null;report.truncated.push("rtc");}
 return report;
}
function sendControl(peer,data){if(!network||closing)throw Error("Network is closed");network.send(peer,prefix+JSON.stringify(data));}
function cancelRequests(reason){for(const p of [...pending.values()])p.reject(Error(reason));}
function request(peer,kind,data={},timeout=15000){
 if(pending.size>=16)return Promise.reject(Error("Too many pending test commands"));
 const id=crypto.randomUUID();
 return new Promise((resolve,reject)=>{const finish=(fn,v)=>{clearTimeout(timer);pending.delete(id);fn(v);};const timer=setTimeout(()=>finish(reject,Error(`${kind}: companion did not respond within ${timeout/1000}s`)),timeout);
 pending.set(id,{peer,resolve:v=>finish(v.error?reject:resolve,v.error?Error(v.error):v),reject:e=>finish(reject,e)});
 try{sendControl(peer,{kind,id,...data});}catch(e){finish(reject,e);}});
}
function handleControl(m){
 if(typeof m.text!=="string"||!m.text.startsWith(prefix))return false;
 try{const q=JSON.parse(m.text.slice(prefix.length));if(!q||typeof q.id!=="string"||q.id.length>100)return true;
 const reply=data=>sendControl(m.from,{kind:"reply",id:q.id,...data});
 if(q.kind==="reply"){const p=pending.get(q.id);if(p?.peer===m.from)p.resolve(q);return true;}
 if(q.kind==="ping"){record("test.ping",{from:m.from,runId:q.runId});reply({ok:true});}
 else if(q.kind==="arm"){currentTest=q.runId;record("test.recoveryArmed",{from:m.from,runId:q.runId});persist();$("companionStatus").textContent="Network-switch check armed. Switch Wi-Fi/cellular, then return here without reloading. The PC will verify and collect logs.";reply({ok:true});}
 else if(q.kind==="fixture"){
 if(typeof q.fileId!=="string"||!Number.isSafeInteger(q.bytes)||q.bytes<0||q.bytes>1048576||typeof q.sha256!=="string"||!/^[a-f0-9]{64}$/.test(q.sha256))throw Error("Invalid test fixture request");
 if(fixtureChecks.size>=4)throw Error("Too many outstanding fixtures");
 fixtureChecks.set(q.fileId,{...q,peer:m.from,expires:Date.now()+65000,busy:false});void verifyFixture(q.fileId);
 }else if(q.kind==="report"){
 persist();const text=JSON.stringify(compactReport());if(text.length>1500000)throw Error("Report exceeds transfer limit; export locally");
 const token=crypto.randomUUID();while(reportCache.size>=2)reportCache.delete(reportCache.keys().next().value);
 reportCache.set(token,{peer:m.from,text,expires:Date.now()+120000});reply({token,parts:Math.ceil(text.length/partSize),length:text.length});
 }else if(q.kind==="report.part"){
 const c=reportCache.get(q.token);if(!c||c.peer!==m.from||c.expires<Date.now()||!Number.isInteger(q.index)||q.index<0||q.index>=Math.ceil(c.text.length/partSize))throw Error("Report part expired or invalid");
 reply({text:c.text.slice(q.index*partSize,(q.index+1)*partSize)});
 }
 }catch(e){record("test.protocolError",{message:e.message,from:m.from});try{const q=JSON.parse(m.text.slice(prefix.length));sendControl(m.from,{kind:"reply",id:q.id,error:e.message});}catch{}}
 return true;
}
async function verifyFixture(id){
 const q=fixtureChecks.get(id);if(!q||q.busy)return;if(Date.now()>q.expires){fixtureChecks.delete(id);record("test.fixtureExpired",{id});return;}
 q.busy=true;
 try{const blob=await network.getDistributedFile(id),sha256=await hash(blob),ok=sha256===q.sha256&&blob.size===q.bytes;
 sendControl(q.peer,{kind:"reply",id:q.id,ok,sha256,bytes:blob.size});fixtureChecks.delete(id);
 $("companionStatus").textContent=ok?"Test file verified. Keep this page open; reports are collected on the PC.":"File verification failed.";
 record("test.fileVerified",{id,ok,sha256,bytes:blob.size});
 }catch{/* Missing bytes are retried after completion or the bounded harness poll. */}finally{q.busy=false;}
}
async function collectPeer(peer){
 if(collecting.has(peer))return;collecting.add(peer);deferredReports.add(peer);$("collectionStatus").textContent="Collecting companion diagnostics…";
 try{const head=await request(peer,"report");if(!Number.isInteger(head.parts)||head.parts<1||head.parts>250||head.length>1500000)throw Error("Invalid report size");
 let text="";for(let i=0;i<head.parts;i++){const part=await request(peer,"report.part",{token:head.token,index:i});if(typeof part.text!=="string"||part.text.length>partSize)throw Error("Invalid report part");text+=part.text;}
 if(text.length!==head.length)throw Error("Incomplete companion report");const report=JSON.parse(text);if(report.networkId!==network.networkId)throw Error("Companion report belongs to another network");
 companionReports.set(peer,{collectedAt:new Date().toISOString(),report});deferredReports.delete(peer);record("test.reportCollected",{peer,parts:head.parts,characters:text.length});
 $("collectionStatus").textContent=`${companionReports.size} companion report(s) collected. Download the combined report on this device.`;
 return report;
 }catch(e){$("collectionStatus").textContent="Companion report pending. It will retry when messaging becomes ready; Collect reports retries manually.";throw e;}finally{collecting.delete(peer);}
}
async function requestWake(){try{if(!wake||wake.released)wake=await navigator.wakeLock?.request("screen");}catch{} }
document.addEventListener("visibilitychange",()=>{persist();if(document.visibilityState==="visible"&&network)void requestWake();});
function showJoinLink(){
 const u=new URL(location.href);u.hash=new URLSearchParams({phantom:JSON.stringify({invite:network.invite,doorway:$("doorway").value.trim()})}).toString();
 $("joinLink").value=u.href;$("joinHelp").textContent=location.protocol==="https:"?"Scan this QR with the phone camera, open the link, then tap Join. Keep that page open. Reports come back to this device.":"For the phone, host these same files over HTTPS first. This localhost link works only on this computer.";
 $("qr").hidden=location.protocol!=="https:";
 if(!$("qr").hidden)QRCode.toCanvas($("qr"),u.href,{width:360,margin:4,errorCorrectionLevel:"L"}).catch(e=>{$("joinHelp").textContent="QR generation failed: "+e.message+". Copy the link below.";});
}
$("copyJoin").onclick=async()=>{try{await navigator.clipboard.writeText($("joinLink").value);$("notice").textContent="Join link copied.";}catch{$("joinLink").select();$("notice").textContent="Select and copy the join link.";}};
$("shareJoin").onclick=async()=>{try{if(navigator.share)await navigator.share({title:"Phantom companion",url:$("joinLink").value});else $("copyJoin").click();}catch(e){if(e.name!=="AbortError")failure(e);}};
async function baseline(peer){
 currentTest=crypto.randomUUID();testStatus("1/3 · Checking message round trip…");await request(peer,"ping",{runId:currentTest});
 testStatus("PASS · Message round trip\n2/3 · Sending and verifying 256 KiB…");
 // A new publication identity forces a fresh transfer on every run.
 const bytes=Uint8Array.from({length:262144},(_,i)=>(i*31+(i>>>8)+17)%256),blob=new Blob([bytes]),sha256=await hash(blob);
 const manifest=await network.distributeFile(blob,{recipients:[peer],distributionId:currentTest,name:"phantom-baseline.bin",type:"application/octet-stream"});
 const result=await request(peer,"fixture",{fileId:manifest.id,sha256,bytes:blob.size},65000);
 if(!result.ok)throw Error("Received file bytes or SHA-256 did not match.");
 testStatus("PASS · Message round trip\nPASS · 256 KiB received with matching SHA-256\n3/3 · Collecting companion report…");await collectPeer(peer);
 testStatus("PASS · Message round trip\nPASS · 256 KiB received with matching SHA-256\nPASS · Companion report collected\nBaseline complete. Download combined report on this device.");
}
function selectedPeer(){const peer=$("target").value;if(!peer||!network?.getPeerConnections().some(p=>p.peerId===peer&&p.state==="ready"))throw Error("Select a ready companion first.");return peer;}
async function perform(task){if(running)return;running=true;controls();try{await task();}catch(e){testStatus($("results").textContent+"\nFAILED · "+e.message);failure(e);}finally{running=false;persist();controls();}}
$("run").onclick=()=>void perform(()=>baseline(selectedPeer()));
$("collect").onclick=()=>void perform(async()=>{const peers=network.getPeerConnections().filter(p=>p.state==="ready");if(!peers.length)throw Error("No ready companion; diagnostics remain on its device until it reconnects.");for(const p of peers)await collectPeer(p.peerId);});
$("armRecovery").onclick=()=>void perform(async()=>{
 const peer=selectedPeer(),runId=crypto.randomUUID();await request(peer,"arm",{runId});
 recovery={peer,runId,armedAt:Date.now(),interruption:false};currentTest=runId;
 testStatus("ARMED · Change the phone network, then return to its page without reloading.\nOn this PC, click Verify after switching. No phone export is needed once messaging returns.");
});
$("verifyRecovery").onclick=()=>void perform(async()=>{
 const check=recovery;if(!check)throw Error("Arm a recovery check first.");
 testStatus("Waiting up to 90 seconds for the companion to respond…");const end=Date.now()+90000;let reply=false;
 while(Date.now()<end&&network&&!closing){
 if(network.getPeerConnections().some(p=>p.peerId===check.peer&&p.state==="ready"))try{await request(check.peer,"ping",{runId:check.runId},3000);reply=true;break;}catch{check.interruption=true;}
 else check.interruption=true;
 await new Promise(resolve=>setTimeout(resolve,500));
 }
 recovery=null;
 if(!reply){deferredReports.add(check.peer);record("test.recoveryFailed",check);throw Error("Companion did not recover within 90 seconds. Logs remain on that tab; report collection is queued for messaging recovery.");}
 await collectPeer(check.peer);
 const message=check.interruption?"PASS · Messaging responded after an observed interruption. Companion report collected.":"INCONCLUSIVE · Companion responds, but this harness observed no interruption. This does not establish network-change recovery.";
 record("test.recoveryResult",{...check,message});testStatus(message+"\nDownload the combined report here. This check does not prove a new network route or file-transfer recovery.");
});
$("combinedReport").onclick=()=>{
 persist();const a=document.createElement("a"),url=URL.createObjectURL(new Blob([JSON.stringify({controller:compactReport(),companions:[...companionReports].map(([peer,value])=>[peer,value.report]),collection:[...companionReports].map(([peer,value])=>({peer,collectedAt:value.collectedAt})),pendingCompanions:[...deferredReports],results:$("results").textContent},null,2)],{type:"application/json"}));urls.add(url);a.href=url;a.download="phantom-baseline.json";a.click();
};
$("clearJournal").onclick=()=>{clearTimeout(saveTimer);journal?.clear();retained=null;events.length=0;$("journalStatus").textContent="Retained diagnostics cleared. Active tests will record new events.";};
const timer=setInterval(()=>{for(const id of fixtureChecks.keys())void verifyFixture(id);for(const [id,value] of reportCache)if(value.expires<Date.now())reportCache.delete(id);},1000);
window.addEventListener("pagehide",()=>{persist();closing=true;cancelRequests("Page hidden or unloaded");controller?.abort();network?.close();clearInterval(timer);void wake?.release();for(const url of urls)URL.revokeObjectURL(url);});
window.addEventListener("pageshow",e=>{if(e.persisted){$("notice").textContent="This page was restored after leaving. Reload and rejoin to collect retained diagnostics.";}});
try{const q=new URLSearchParams(location.hash.slice(1)).get("phantom");if(q){const data=JSON.parse(q);$("invite").value=data.invite;$("doorway").value=data.doorway??"";$("name").value="Mobile companion";document.body.classList.add("companion");$("companionStatus").textContent="Tap Join, then keep this page open. The PC runs checks and collects reports.";}}catch(e){failure(Error("Invalid join link: "+e.message));}
if(retained)$("journalStatus").textContent="Previous diagnostics restored. They will be included in reports after you rejoin.";
else if(journalWarning)$("journalStatus").textContent="Log retention unavailable: "+journalWarning;
record("loaded",{build});controls();
