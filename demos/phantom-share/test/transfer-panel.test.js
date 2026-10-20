import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/app.js',import.meta.url),'utf8');
function render(assets){
 const els=new Map(),byId=id=>{if(!els.has(id))els.set(id,{innerHTML:'',append(){}});return els.get(id);};
 const c={sharing:{assets:new Map(assets.map(a=>[a.id,a]))},network:null,byId,escapeHtml:s=>s.replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;'),$T:n=>String(n),renderIcons(){},showHistory(){},document:{createElement(){return {};}}};
 vm.createContext(c);vm.runInContext(source.slice(source.indexOf('function renderTransfers()'),source.indexOf('function showHistory(')),c);c.renderTransfers();return {html:byId('transfers-container').innerHTML,count:byId('transfer-count-badge').textContent};
}
const received={id:'received',name:'a"<script>.json',size:42,blob:{},status:'received',total:2,received:2};
const sending={id:'sending',name:'video.mp4',size:100,outgoing:true,status:'sharing',recipients:['one','two'],delivered:new Set(['one'])};
test('active transfer does not hide an earlier completed filename or Save',()=>{
 const r=render([received,sending]);assert.match(r.html,/video.mp4/);assert.match(r.html,/a&quot;&lt;script>.json/);
 assert.equal((r.html.match(/data-download="received"/g)||[]).length,2);
 assert.match(r.html,/aria-valuenow="50"/);assert.match(r.html,/aria-valuenow="100"/);assert.equal(r.count,'1 active');
 assert.equal(r.html.includes('<script>'),false);
});
test('terminal failures and interrupted jobs do not count as active',()=>{
 const r=render([{...sending,id:'failed',status:'failed'},{...sending,id:'interrupted',status:'interrupted'}]);
 assert.equal(r.count,'0 active');assert.equal((r.html.match(/class="share-transfer"/g)||[]).length,2);
});
test('empty transfer panel has no fake transfer or download button',()=>{
 const r=render([]);assert.match(r.html,/No transfers yet/);assert.equal(r.count,'0 active');assert.equal(r.html.includes('data-download'),false);
});
