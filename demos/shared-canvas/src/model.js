export const COLORS=['#191923','#7556f5','#00a884','#ef6c57','#f5b800'];
export const MAX_STROKES=4000;
export const INITIAL={id:'initial',clock:0,author:''};
const peer=x=>typeof x==='string'&&/^[0-9a-f]{16}$/.test(x);
const stamp=x=>x&&typeof x.id==='string'&&x.id.length<=100&&Number.isSafeInteger(x.clock)&&x.clock>=0&&(peer(x.author)||(x.id==='initial'&&x.clock===0&&x.author===''));
export function compare(a,b){return a.clock-b.clock||a.author.localeCompare(b.author)||a.id.localeCompare(b.id);}
export function createBoard(){return {epoch:{...INITIAL},clock:0,strokes:new Map()};}
export function apply(board,op){
 if(!op||!['stroke','clear'].includes(op.type)||!stamp(op.epoch))return false;
 if(op.type==='stroke'&&(!peer(op.author)||typeof op.id!=='string'||op.id.length>100||!Number.isSafeInteger(op.clock)||op.clock<0||!COLORS.includes(op.color)||!Number.isFinite(op.width)||op.width<1||op.width>24||!Array.isArray(op.points)||!op.points.length||op.points.length>48||!op.points.every(p=>Array.isArray(p)&&p.length===2&&p.every(n=>Number.isFinite(n)&&n>=0&&n<=1))))return false;
 if(compare(op.epoch,board.epoch)<0)return false;
 let changed=false;
 if(compare(op.epoch,board.epoch)>0){board.epoch={...op.epoch};board.strokes.clear();changed=true;}
 board.clock=Math.max(board.clock,op.epoch.clock,op.clock??0);
 if(op.type==='clear')return changed;
 if(board.strokes.has(op.id)||board.strokes.size>=MAX_STROKES)return changed;
 board.strokes.set(op.id,structuredClone(op));return true;
}
export function ordered(board){return [...board.strokes.values()].sort(compare);}
export function history(board){return [{type:'clear',epoch:{...board.epoch}},...ordered(board)];}
