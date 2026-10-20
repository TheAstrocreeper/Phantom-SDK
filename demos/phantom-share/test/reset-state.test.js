import {createConnectionQueue} from '../src/connection-queue.js';
import {closeSessionOwners} from '../src/session-cleanup.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/app.js',import.meta.url),'utf8');
test('reset detaches before close, tolerates close failure, clears UI and releases resources once',async()=>{
 const elements=new Map(),closed=[];
 const get=id=>{if(!elements.has(id))elements.set(id,{value:'old invite',textContent:'old',classList:{add(){},remove(){}},querySelector(){return {};},getContext(){return {clearRect(){closed.push('qr');}};},close(){closed.push(id);}});return elements.get(id);};
 const ctx={connectionQueue:createConnectionQueue(),closeSessionOwners,renderAvailability(){},cancelNativeSave(){closed.push('save-cancel');},diagnostics:{record(){},flush(){}},connectionGeneration:4,rampControl:{close(){closed.push('ramp');throw Error('close failed');}},rampBusy:true,joinController:{abort(){closed.push('abort');}},shareConnection:{close(){closed.push('connection');assert.equal(ctx.network,null);}},sharing:{close(){closed.push('sharing');}},network:{close(){closed.push('network');}},admin:{},releaseIdentityLock(){closed.push('lock');},clearTimeout(){},invitationTimer:1,currentInvitation:{},invitationPending:{},connecting:true,leaving:true,recipientModes:new Map([['peer','direct']]),downloadUrls:new Map([['file','blob:test']]),URL:class extends URL {static revokeObjectURL(u){closed.push(u);}},invite:'old',shareUrl:'old',pendingCode:'old',byId:get,document:{getElementById:get},location:{href:'https://share.test/?p=old#pair=old'},history:{replaceState(_a,_b,u){ctx.cleanURL=u.href;}},closeInviteModal(){},renderPeers(){},renderTransfers(){},setStatus(){}};
 vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('function cleanupSession('),source.indexOf('async function leaveNetwork(')),ctx);ctx.cleanupSession();
 for(const k of ['rampControl','joinController','shareConnection','sharing','network','admin','releaseIdentityLock','currentInvitation','invitationPending','invitationTimer'])assert.equal(ctx[k],null,k);
 assert.ok(closed.includes('save-cancel'));
 assert.equal(ctx.connectionGeneration,5);assert.equal(ctx.connecting,false);assert.equal(ctx.leaving,false);assert.equal(ctx.recipientModes.size,0);assert.equal(ctx.downloadUrls.size,0);
 for(const id of ['file-input','invite-code','invite-link','paste-invite'])assert.equal(get(id).value,'');
 await ctx.connectionQueue.settled();
 for(const name of ['abort','ramp','sharing','connection','network','lock','blob:test','qr','history-dialog','ramp-dialog'])assert.ok(closed.includes(name),name);
 assert.equal(ctx.cleanURL,'https://share.test/');ctx.cleanupSession();await ctx.connectionQueue.settled();assert.equal(closed.filter(x=>x==='connection').length,1);
});
test('late canceled identity lock cannot replace a new session lock',async()=>{
 let callback;const ctx={navigator:{locks:{request(_n,_o,fn){callback=fn;return Promise.resolve();}}},connectionGeneration:1,releaseIdentityLock:null,Error,Promise};
 vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('async function acquireIdentityLock('),source.indexOf('let pairingIntent =')),ctx);
 const pending=ctx.acquireIdentityLock(1);ctx.connectionGeneration=2;const newer=()=>{};ctx.releaseIdentityLock=newer;callback({});await assert.rejects(pending,/cancelled/);assert.equal(ctx.releaseIdentityLock,newer);
});
test('late Leave completion cannot reset a replacement connection',async()=>{
 let finish;const old={leave(){}};let resets=0;
 const ctx={network:old,connecting:false,leaving:false,connectionGeneration:1,renderAvailability(){},diagnostics:{record(){}},cleanupSession(){resets++;},leaveAndReset:async({reset})=>{await new Promise(r=>finish=r);reset(true);}};
 vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('async function leaveNetwork('),source.indexOf('async function removeDevice(')),ctx);
 const pending=ctx.leaveNetwork();ctx.network={};ctx.connectionGeneration=2;finish();await pending;assert.equal(resets,0);
});
