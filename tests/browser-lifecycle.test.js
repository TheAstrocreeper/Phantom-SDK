import test from 'node:test';
import assert from 'node:assert/strict';
import {createPhantom} from '../packages/phantom-core/src/index.js';
class Surface{listeners=new Map();visibilityState='hidden';addEventListener(t,f){if(!this.listeners.has(t))this.listeners.set(t,new Set());this.listeners.get(t).add(f);}removeEventListener(t,f){this.listeners.get(t)?.delete(f);}dispatch(t){for(const f of this.listeners.get(t)??[])f({type:t});}count(){return [...this.listeners.values()].reduce((n,s)=>n+s.size,0);}}
for(const kind of ['online','pageshow','visibilitychange'])test(`real browser ${kind} observes an expired doorway attempt and disposal removes bindings`,async()=>{
 const win=new Surface(),doc=new Surface(),sockets=[];class Socket{static OPEN=1;static CONNECTING=0;constructor(){this.readyState=0;sockets.push(this);}send(){}close(){this.closed=true;this.readyState=3;}}
 const actualNow=Date.now;let clock=actualNow();Date.now=()=>clock;
 const core=createPhantom({observeBrowserLifecycle:true,runtime:{crypto:globalThis.crypto,window:win,document:doc,setTimeout,clearTimeout,setInterval,clearInterval},doorway:{protocol:'phantom-concierge-v1',url:'ws://fixture/',WebSocket:Socket}});
 try{await core.join({roomId:'test',name:'Tester'});assert.equal(win.count(),2);assert.equal(doc.count(),1);clock+=60000;if(kind==='visibilitychange'){doc.dispatch(kind);assert.equal(sockets[0].closed,undefined);doc.visibilityState='visible';doc.dispatch(kind);}else win.dispatch(kind);assert.equal(sockets[0].closed,true);assert.equal(core.snapshot().bootstrap.doorwayDeadlineObservedBy,kind==='visibilitychange'?'document_visible':'window_'+kind);}finally{core.dispose();Date.now=actualNow;}
 assert.equal(win.count(),0);assert.equal(doc.count(),0);
});
test('headless core never attaches to browser surfaces unless enabled',()=>{const win=new Surface(),doc=new Surface();const core=createPhantom({runtime:{...globalThis,crypto,window:win,document:doc,setTimeout,clearTimeout,setInterval,clearInterval},doorway:{protocol:'phantom-concierge-v1',url:'ws://fixture/',WebSocket:class{}}});try{assert.equal(win.count(),0);assert.equal(doc.count(),0);}finally{core.dispose();}});
