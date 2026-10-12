import QRCode from 'qrcode';
import {createBoard,apply,ordered,COLORS} from './model.js';
import {openCanvas} from './session.js';
import {shareLink,readLink} from './link.js';
const $=id=>document.getElementById(id),board=createBoard(),canvas=$('canvas'),ctx=canvas.getContext('2d');
let session=null,link='',joiningInvite=null,starting=false,color=COLORS[0],drawing=false,points=[],lastPoint=null,flushTimer;
const traces=[];let activeController;
function self(){return session?.network.getState().peerId??'0000000000000000';}
function render(){
 const rect=canvas.getBoundingClientRect(),ratio=Math.min(devicePixelRatio||1,2);
 const w=Math.round(rect.width*ratio),h=Math.round(rect.height*ratio);if(canvas.width!==w||canvas.height!==h){canvas.width=w;canvas.height=h;}
 ctx.clearRect(0,0,w,h);ctx.lineCap='round';ctx.lineJoin='round';
 for(const stroke of ordered(board)){
  ctx.strokeStyle=stroke.color;ctx.fillStyle=stroke.color;ctx.lineWidth=stroke.width*w/900;
  const ps=stroke.points;if(ps.length===1){ctx.beginPath();ctx.arc(ps[0][0]*w,ps[0][1]*h,ctx.lineWidth/2,0,Math.PI*2);ctx.fill();continue;}
  ctx.beginPath();ctx.moveTo(ps[0][0]*w,ps[0][1]*h);for(const p of ps.slice(1))ctx.lineTo(p[0]*w,p[1]*h);ctx.stroke();
 }
 $('empty-note').hidden=board.strokes.size>0;
}
function local(op){if(apply(board,op)){session?.broadcast(op);render();}else feedback('Canvas is full. Clear it to keep drawing.');}
function flush(){if(!points.length)return;const ps=points;points=[];local({type:'stroke',epoch:{...board.epoch},id:crypto.randomUUID(),author:self(),clock:++board.clock,color,width:Number($('width').value),points:ps});lastPoint=ps.at(-1);}
function point(e){const r=canvas.getBoundingClientRect();return [Math.max(0,Math.min(1,(e.clientX-r.left)/r.width)),Math.max(0,Math.min(1,(e.clientY-r.top)/r.height))];}
canvas.addEventListener('pointerdown',e=>{if(e.button!==0)return;drawing=true;canvas.setPointerCapture(e.pointerId);points=[point(e)];lastPoint=null;flushTimer=setInterval(flush,45);e.preventDefault();});
canvas.addEventListener('pointermove',e=>{if(!drawing)return;if(!points.length&&lastPoint)points.push(lastPoint);points.push(point(e));if(points.length>=40)flush();});
function stop(){if(!drawing)return;drawing=false;clearInterval(flushTimer);flush();lastPoint=null;}
canvas.addEventListener('pointerup',stop);canvas.addEventListener('pointercancel',stop);canvas.addEventListener('lostpointercapture',stop);
$('clear').onclick=()=>{stop();const epoch={id:crypto.randomUUID(),author:self(),clock:++board.clock};local({type:'clear',epoch});};
for(const ink of COLORS){const b=document.createElement('button');b.className='swatch'+(ink===color?' selected':'');b.style.background=ink;b.setAttribute('aria-label','Use '+ink+' ink');b.setAttribute('aria-pressed',ink===color?'true':'false');b.onclick=()=>{stop();color=ink;for(const s of $('colors').children){s.classList.toggle('selected',s===b);s.setAttribute('aria-pressed',s===b?'true':'false');}};$('colors').append(b);}
new ResizeObserver(render).observe(canvas);
function feedback(text){$('feedback').textContent=text;}
function trace(event){
 traces.unshift(event);traces.length=Math.min(traces.length,50);$('trace').replaceChildren();
 for(const e of traces){const li=document.createElement('li'),t=document.createElement('time');t.textContent=new Date(e.at).toLocaleTimeString([], {hour12:false});li.append(t,document.createTextNode(e.type+' · '+e.detail));$('trace').append(li);}
}
function status(state){
 const count=(session?.ready.size??0)+1;
 $('peer-count').textContent=count+' '+(count===1?'screen':'screens');
 $('status').textContent=!session?(starting?'Connecting to room…':'Your canvas is ready'):count>1?'Connected · draw together':state.state==='failed'?'Connection needs attention':state.state==='recovering'?'Recovering peer connections…':'Room active · waiting for another screen';
 $('status-dot').classList.toggle('active',!!session&&state.state!=='failed');
 $('network-state').textContent=session?state.state:'Not connected';
 const snapshot=session?.network.snapshot();const peers=session?.network.getPeers()??[];
 const channels=(snapshot?.chatPairs??[]).filter(p=>p.dcState==='open');
 $('channels').textContent=String(channels.length);$('messages').textContent=(session?.stats.sent??0)+' / '+(session?.stats.received??0);
 $('peer-list').replaceChildren();
 const me=document.createElement('li');me.textContent=($('name').value.trim()||'Sketcher')+' · this screen';$('peer-list').append(me);
 for(const p of peers){if(p.hKey===state.peerId)continue;const li=document.createElement('li');li.textContent=(p.handle??p.hKey?.slice(0,8)??'Peer')+' · '+(session?.ready.has(p.hKey)?'drawing channel ready':'connecting');$('peer-list').append(li);}
 $('network-id').textContent=session?'Room '+session.network.networkId:'';
}
async function start(){
 if(starting||session)return;starting=true;activeController=new AbortController();$('start').disabled=true;feedback('Finding a doorway and connecting…');status({});
 try{
  const next=await openCanvas({invite:joiningInvite,name:$('name').value.trim()||'Sketcher',board,onChange:render,onStatus:status,onTrace:trace,signal:activeController.signal});
  session=next;link=await shareLink(next.network.invite,location.href.split('#')[0]);
  history.replaceState(null,'',link); // URL always opens this same room.
  $('invite-link').value=link;await QRCode.toCanvas($('qr'),link,{width:230,margin:2,errorCorrectionLevel:'M',color:{dark:'#181523',light:'#ffffff'}});
  $('invite-box').hidden=false;$('start').hidden=true;$('name').disabled=true;
  feedback(joiningInvite?'You joined. Draw with the other screen.':'Room created. Scan the QR with your phone.');status(next.network.getState());
 }catch(error){session?.close();session=null;feedback(/deadline|timed out/.test(error.message)?'The peer connection timed out. Keep the other screen open and retry.':error.message||'Could not connect. Try again.');$('start').textContent=joiningInvite?'Join this room ↗':'Create a room ↗';}
 finally{starting=false;$('start').disabled=false;status(session?.network.getState()??{});}
}
$('start').onclick=start;
$('copy').onclick=async()=>{try{await navigator.clipboard.writeText(link);feedback('Invite copied. Open it on another screen.');}catch{$('invite-link').focus();$('invite-link').select();feedback('Select and copy the invitation above.');}};
$('leave').onclick=()=>{stop();session?.close();session=null;joiningInvite=null;link='';const clean=new URL(location.href);clean.hash='';clean.searchParams.delete('p');clean.searchParams.delete('invite');history.replaceState(null,'',clean.href);$('invite-box').hidden=true;$('start').hidden=false;$('start').textContent='Create a room ↗';$('name').disabled=false;feedback('You left. Your local sketch is still here.');status({});};
$('inspect').onclick=()=>{const expanded=$('inspect').getAttribute('aria-expanded')!=='true';$('inspect').setAttribute('aria-expanded',String(expanded));$('inspector-body').hidden=!expanded;$('inspect-label').textContent=expanded?'Close inspector −':'Inspect live network +';};
window.addEventListener('pagehide',()=>{activeController?.abort();session?.close();});
try{joiningInvite=await readLink(location.search,location.hash);if(joiningInvite){$('start').textContent='Join this room ↗';$('share-description').textContent='An invitation is ready. Join to draw with the other screen.';await start();}}catch(e){feedback(e.message);}
render();
