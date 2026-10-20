import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/app.js',import.meta.url),'utf8');
function setup(named=true){const elements=new Map();let creates=0,invitations=0,prompts=0;const byId=id=>{if(!elements.has(id))elements.set(id,{hidden:false,value:'',textContent:'',classList:{add(){},remove(){}},getContext:()=>({clearRect(){}})});return elements.get(id);};const c={connecting:false,pairingCreatesSession:false,pairingPanelGeneration:0,pairingIntent:null,network:null,scanner:{stop(){}},displayName:()=>named?'User':'',promptName(){prompts++;},byId,async connect(){creates++;c.network={};},async ensureInvitation(){invitations++;byId('invite-code').value='PAIR';byId('invite-link').value='https://test/#pair=PAIR';},showError(e){throw e;}};vm.createContext(c);vm.runInContext(source.slice(source.indexOf('async function openConnectionMenu()'),source.indexOf('function closeInviteModal()')),c);return {c,byId,counts:()=>({creates,invitations,prompts})};}
test('Invite directly creates and displays invitation; repeated Invite reuses session',async()=>{const {c,byId,counts}=setup();await c.openInviteModal();assert.deepEqual(counts(),{creates:1,invitations:1,prompts:0});assert.equal(byId('join-section').hidden,true);assert.equal(byId('share-section').hidden,false);assert.equal(byId('invite-code').value,'PAIR');assert.match(byId('invite-link').value,/pair=PAIR/);await c.openInviteModal();assert.equal(counts().creates,1);});
test('Connect opens manual entry and camera option without creating an invitation or requesting camera access',async()=>{const {c,byId,counts}=setup();await c.openConnectModal();assert.deepEqual(counts(),{creates:0,invitations:0,prompts:0});assert.equal(byId('join-section').hidden,false);assert.equal(byId('share-section').hidden,true);assert.equal(byId('scanner-panel').hidden,true);});
test('name prompt preserves Invite intent so submission can complete the original action',async()=>{const {c,counts}=setup(false);await c.openInviteModal();assert.equal(c.pairingIntent,'invite');assert.equal(counts().prompts,1);assert.equal(counts().creates,0);});

test('the one Connect action opens Create/Join choices without creating a session',async()=>{const {c,byId,counts}=setup();await c.openConnectionMenu();assert.equal(byId('connection-choice').hidden,false);assert.equal(byId('join-section').hidden,true);assert.equal(byId('share-section').hidden,true);assert.equal(counts().creates,0);});

test('dismissed Create completion cannot paint invitation into a later Join panel',async()=>{
 const {c,byId,counts}=setup();let finish;
 c.connect=()=>new Promise(r=>finish=r);
 const creating=c.openInviteModal();c.pairingPanelGeneration++;
 await c.openConnectModal();finish();await creating;
 assert.equal(c.pairingIntent,'connect');assert.equal(byId('join-section').hidden,false);
 assert.equal(counts().invitations,0);
});
