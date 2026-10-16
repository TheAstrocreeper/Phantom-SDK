import QRCode from '../swarm/node_modules/qrcode/lib/browser.js';
import {createJournal} from './journal.js';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createWebSocketDoorway,adaptWebSocketDoorway} from '../../packages/phantom-doorways/src/index.js';
const $=id=>document.getElementById(id),build=__SDK_TEST_BUILD__;
const events=[],messages=[],received=new Map(),remoteReports=new Map(),pending=new Map(),reportCache=new Map(),urls=new Set();
let network,controller,state,snapshot,opening=false,creator=false,dropped=0,saveTimer,journal,retained=null,storageError='',exporting=false,lastPublication=null;
try{journal=createJournal(sessionStorage,'phantom-test-diagnostics-v1');retained=journal.previous;}catch(e){storageError=e.message;}
const prefix='PHANTOM_LOGS_V1:',partSize=6000;
function record(type,data){events.push({at:new Date().toISOString(),type,data});if(events.length>1000){events.shift();dropped++;}scheduleSave();$('log').textContent=events.slice(-20).map(e=>JSON.stringify(e)).join('\n');}
function save(){clearTimeout(saveTimer);if(!journal)return;const value={networkId:network?.networkId??retained?.networkId,state,at:new Date().toISOString(),events:events.slice(-200),rtc:snapshot?.rtcDiagnostics??null};if(JSON.stringify(value).length>320000)value.rtc=null;while(value.events.length&&JSON.stringify(value).length>320000)value.events.shift();if(!journal.save(value))storageError=journal.error;}
function scheduleSave(){clearTimeout(saveTimer);saveTimer=setTimeout(save,250);}
function fail(e){const error=e instanceof Error?e:Error(String(e));$('notice').textContent=error.message;record('error',{message:error.message,code:error.code,diagnostics:error.diagnostics});}
function connections(){return network?.getPeerConnections()??[];}
function controls(){const ready=connections().some(p=>p.peerId===$('target').value&&p.state==='ready');$('setup').disabled=opening||!!network;$('leave').disabled=!opening&&!network;$('send').disabled=!ready;$('publish').disabled=!network||!creator||!ready||!$('file').files.length;$('file').disabled=!network||!creator;$('report').disabled=exporting;}
function render(){
 if(network){state=network.getState();snapshot=network.snapshot();}
 const rows=connections(),ready=rows.filter(p=>p.state==='ready').length;
 const status=network?state?.state:opening?'connecting':state?.state==='closed'?'disconnected':'idle';
 $('summary').textContent=({idle:'Not connected',discovering:'Finding peers',connecting:'Connecting',admitted:'Connected',recovering:'Reconnecting',disconnected:'Disconnected',closed:'Disconnected',failed:'Connection failed'})[status]??status;
 $('statusDot').dataset.state=status;
 $('peerCount').textContent=`${ready} ready / ${rows.length} peers`;
 $('self').textContent=state?.peerId??'—';$('role').textContent=network?(creator?'Creator':'Joined peer'):'—';
 $('connectionDetail').textContent=network?({admitted:'Ready to send and receive.',recovering:'Connection interrupted. Waiting for recovery.',connecting:'Setting up the peer connection.',discovering:'Looking for a peer.'})[state?.state]??'Waiting for the connection.':'Create a session or join one.';
 updateTransferStatus();
 $('state').textContent=JSON.stringify({state,connections:rows,distribution:network?.getDistributionState()},null,2);
 const old=$('target').value;$('target').replaceChildren(new Option(rows.length?'Choose a peer':'Waiting for a peer…',''));
 for(const p of rows){const member=network.getPeers().find(m=>m.hKey===p.peerId);const option=new Option(`${member?.handle??'Peer'} · ${p.state} · ${p.peerId.slice(0,6)}`,p.peerId);option.disabled=p.state!=='ready';$('target').append(option);}
 if(rows.some(p=>p.peerId===old&&p.state==='ready'))$('target').value=old;
 else if(ready===1)$('target').value=rows.find(p=>p.state==='ready').peerId;
 $('publishHelp').textContent=network&&!creator?'Receive files here. Send files from the page that created this session.':'Files go to the selected peer. Received files appear below.';
 controls();
}
function updateTransferStatus(){if(!network||!lastPublication)return;const asset=network.getDistributionState().assets?.find(a=>a.id===lastPublication.id),held=asset?.holders?.find(h=>h.peer===lastPublication.peer)?.indexes?.length??0;if(asset&&held===asset.total)$('fileStatus').textContent=`Delivered ${lastPublication.name} · peer reports all ${held} chunks received`;}
const refreshTimer=setInterval(updateTransferStatus,500);
async function hash(blob){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer()))].map(b=>b.toString(16).padStart(2,'0')).join('');}
function addMessage(from,text){messages.push({from,text});if(messages.length>100)messages.shift();$('messages').replaceChildren();for(const m of messages){const row=document.createElement('div');row.className='message';const label=document.createElement('strong');label.textContent=m.from;const body=document.createElement('span');body.textContent=m.text;row.append(label,body);$('messages').append(row);}}
async function localFile(id){if(received.has(id))return;const blob=await network.getDistributedFile(id),sha256=await hash(blob),url=URL.createObjectURL(blob);urls.add(url);received.set(id,{bytes:blob.size,sha256});const a=document.createElement('a');a.href=url;a.download='received.bin';a.textContent=`Download received file · ${(blob.size/1024).toFixed(1)} KB`;$('files').append(a);$('fileStatus').textContent=`Received ${(blob.size/1024).toFixed(1)} KB · verified local bytes`;record('file.received',{id,bytes:blob.size,sha256});}
function showInvite(){
 const u=new URL(location.href);u.hash=new URLSearchParams({phantom:JSON.stringify({invite:network.invite,doorway:$('doorway').value.trim()})});$('joinLink').value=u.href;$('sharing').hidden=!creator;
 $('joinHelp').textContent=location.protocol==='https:'?'Scan with your phone camera, then tap Join.':'Phone joining needs this page hosted over HTTPS. Localhost works on this PC only.';
 $('qr').hidden=location.protocol!=='https:';
 QRCode.toCanvas($('qr'),u.href,{width:400,margin:4,errorCorrectionLevel:'L'}).catch(e=>{$('joinHelp').textContent='Could not draw QR. Use Copy link. '+e.message;});
}
async function start(join){
 if(opening||network)return;opening=true;creator=!join;document.body.classList.toggle('receiveOnly',join);controller=new AbortController();$('notice').textContent='';render();
 try{
 if(!isSecureContext||!crypto.subtle)throw Error('Open this page over HTTPS, or http://localhost:8000 on this PC.');
 const url=$('doorway').value.trim(),rtc=$('rtc').value.trim();if(url&&!['ws:','wss:'].includes(new URL(url).protocol))throw Error('Doorway must use ws or wss.');
 const sdk=createBrowserPhantom({messaging:'automatic',distribution:{},...rtc?{rtcConfiguration:JSON.parse(rtc)}:{},...creator?{adminIdentity:await createAdminIdentity()}:{},...url?{doorways:[{type:'test-websocket',url}]}:{},adapters:{'test-websocket':({spec})=>adaptWebSocketDoorway(createWebSocketDoorway({url:spec.url}))}});
 const options={name:$('name').value.trim()||'Peer',signal:controller.signal,timeoutMs:Number($('deadline').value)*1000,onState:s=>{state=s;record('state',s);render();},onEvent:e=>{if(e.type==='SNAPSHOT'){snapshot=e.payload;record('snapshot.reason',{reason:e.payload.reason});render();}else if(typeof e.payload?.text==='string'&&e.payload.text.startsWith(prefix)){record('log.frame',{type:e.type,characters:e.payload.text.length});}else record('event',e);}};
 network=await(join?sdk.join($('invite').value.trim(),options):sdk.create(options));
 network.on('message',m=>{if(!handleLogMessage(m)){record('message',m);addMessage(m.from.slice(0,6),m.text);}});
 network.on('peerConnection',data=>{record('peerConnection',data);render();});
 for(const type of ['distributionProgress','distributionJob','distributionJobFailed','distributionError','distributionConnectionFailed','distributionComplete'])network.on(type,data=>{
 record(type,type==='distributionComplete'?{id:data.id,bytes:data.blob.size}:data);
 if(type==='distributionProgress')$('fileStatus').textContent=`Receiving · ${data.received??0} / ${data.total??'?'} chunks`;
 if(type==='distributionJobFailed'||type==='distributionError'||type==='distributionConnectionFailed')$('fileStatus').textContent='Transfer issue · see logs';
 if(type==='distributionComplete')void localFile(data.id).catch(fail);render();});
 record('opened',{networkId:network.networkId,creator});showInvite();$('setupSection').classList.add('connected');$('joinPrompt').hidden=true;render();
 }catch(e){fail(e);}finally{opening=false;render();}
}
function makeReport(){const report={build,browser:{userAgent:navigator.userAgent,origin:location.origin,secureContext:isSecureContext},networkId:network?.networkId,state,snapshot,received:[...received],distribution:network?.getDistributionState(),events:[...events],dropped,retained,storageError,truncated:[]};
 while(report.events.length&&JSON.stringify(report).length>1100000){report.events.shift();if(!report.truncated.includes('events'))report.truncated.push('events');}if(JSON.stringify(report).length>1100000){report.snapshot=null;report.truncated.push('snapshot');}return report;}
function sendLog(peer,data){network.send(peer,prefix+JSON.stringify(data));}
function request(peer,kind,data={}){const id=crypto.randomUUID();return new Promise((resolve,reject)=>{const finish=(fn,v)=>{clearTimeout(timer);pending.delete(id);fn(v);};const timer=setTimeout(()=>finish(reject,Error('Companion log collection timed out')),5000);pending.set(id,{peer,resolve:v=>finish(v.error?reject:resolve,v.error?Error(v.error):v),reject:e=>finish(reject,e)});try{sendLog(peer,{kind,id,...data});}catch(e){finish(reject,e);}});}
function handleLogMessage(m){if(typeof m.text!=='string'||!m.text.startsWith(prefix))return false;try{const q=JSON.parse(m.text.slice(prefix.length));if(typeof q.id!=='string')return true;if(q.kind==='reply'){const p=pending.get(q.id);if(p?.peer===m.from)p.resolve(q);return true;}
 if(q.kind==='report'){save();const text=JSON.stringify(makeReport());if(text.length>1500000)throw Error('Report too large');while(reportCache.size>=2)reportCache.delete(reportCache.keys().next().value);const token=crypto.randomUUID();reportCache.set(token,{peer:m.from,text,until:Date.now()+120000});sendLog(m.from,{kind:'reply',id:q.id,token,length:text.length,parts:Math.ceil(text.length/partSize)});}
 if(q.kind==='part'){const c=reportCache.get(q.token);if(!c||c.peer!==m.from||c.until<Date.now()||!Number.isInteger(q.index)||q.index<0||q.index>=Math.ceil(c.text.length/partSize))throw Error('Invalid report part');sendLog(m.from,{kind:'reply',id:q.id,text:c.text.slice(q.index*partSize,(q.index+1)*partSize)});}
 }catch(e){record('log.collectionError',{message:e.message});}return true;}
async function collect(peer){const head=await request(peer,'report');if(!Number.isInteger(head.parts)||head.parts<1||head.parts>250)throw Error('Invalid report size');let text='';for(let i=0;i<head.parts;i++){const p=await request(peer,'part',{token:head.token,index:i});if(typeof p.text!=='string'||p.text.length>partSize)throw Error('Invalid report part');text+=p.text;}if(text.length!==head.length)throw Error('Incomplete report');const report=JSON.parse(text);if(report.networkId!==network.networkId)throw Error('Wrong network in companion report');remoteReports.set(peer,{collectedAt:new Date().toISOString(),report});}
$('create').onclick=()=>void start(false);$('join').onclick=()=>void start(true);$('target').onchange=controls;$('file').onchange=controls;
$('leave').onclick=()=>{controller?.abort();save();if(network){record('before.close',{state});network.close();state=network.getState();snapshot=network.snapshot();network=null;}for(const p of [...pending.values()])p.reject(Error('Network closed'));record('closed',{state});$('setupSection').classList.remove('connected');$('sharing').hidden=true;render();};
$('send').onclick=()=>{try{const text=$('text').value.trim();if(!text)throw Error('Enter a message.');network.send($('target').value,text);record('message.submitted',{to:$('target').value,text});addMessage('You',text);$('text').value='';$('notice').textContent='Message submitted. Delivery is visible on the receiving page.';}catch(e){fail(e);}};
$('text').onkeydown=e=>{if(e.key==='Enter'&&!$('send').disabled)$('send').click();};
$('publish').onclick=async()=>{try{const file=$('file').files[0],peer=$('target').value;if(!file||!peer||!creator)throw Error('Select a ready peer and a file on the creating page.');$('fileStatus').textContent='Sending…';const sha256=await hash(file),manifest=await network.distributeFile(file,{recipients:[peer],distributionId:crypto.randomUUID(),name:file.name,type:file.type});lastPublication={id:manifest.id,peer,name:file.name};record('file.published',{manifest,sha256,recipients:[peer]});$('fileStatus').textContent=`Published ${file.name} · ${(file.size/1024).toFixed(1)} KB. Waiting for receiver.`;}catch(e){$('fileStatus').textContent='Could not send file';fail(e);}};
$('report').onclick=async()=>{if(exporting)return;exporting=true;controls();const missing=[];try{$('notice').textContent='Preparing logs…';if(network&&creator)for(const p of connections().filter(p=>p.state==='ready')){try{await collect(p.peerId);}catch(e){missing.push({peer:p.peerId,error:e.message});}}
 save();const data={...makeReport(),companions:[...remoteReports].map(([peer,v])=>[peer,v.report]),collection:[...remoteReports].map(([peer,v])=>({peer,collectedAt:v.collectedAt})),unavailable:missing};const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));urls.add(url);const a=$('reportLink');a.href=url;a.download='phantom-logs.json';a.hidden=false;a.click();$('notice').textContent=missing.length?'Logs downloaded. A companion report was unavailable; details are included.':'Logs downloaded. Use the link below if the browser blocked the download.';
 }catch(e){fail(e);}finally{exporting=false;controls();}};
$('copyJoin').onclick=async()=>{try{await navigator.clipboard.writeText($('joinLink').value);$('notice').textContent='Join link copied.';}catch{$('joinLink').select();$('notice').textContent='Select and copy the link.';}};
$('clearJournal').onclick=()=>{clearTimeout(saveTimer);journal?.clear();retained=null;$('notice').textContent='Previous retained logs cleared.';};
window.addEventListener('error',e=>fail(e.error??e.message));window.addEventListener('unhandledrejection',e=>fail(e.reason));
document.addEventListener('visibilitychange',save);window.addEventListener('pagehide',()=>{clearInterval(refreshTimer);save();controller?.abort();network?.close();for(const url of urls)URL.revokeObjectURL(url);});
try{const q=new URLSearchParams(location.hash.slice(1)).get('phantom');if(q){const data=JSON.parse(q);$('invite').value=data.invite;$('doorway').value=data.doorway??'';$('name').value='Phone';document.body.classList.add('companion');$('joinPrompt').hidden=false;}}catch(e){fail(e);}
record('loaded',{build});render();
