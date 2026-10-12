import {parseInvite} from '../../../packages/phantom-core/src/invite.js';
const encode=b=>btoa(String.fromCharCode(...b)).replaceAll('+','-').replaceAll('/','_').replace(/=+$/,'');
const decode=s=>Uint8Array.from(atob(s.replaceAll('-','+').replaceAll('_','/')),c=>c.charCodeAt(0));
export async function shareLink(invite,base){
 parseInvite(invite);
 const url=new URL(base);url.hash='';url.searchParams.delete('p');url.searchParams.delete('invite');
 if(typeof CompressionStream!=='function'){url.searchParams.set('invite',invite);return url.href;}
 const data=await new Response(new Blob([invite]).stream().pipeThrough(new CompressionStream('deflate'))).arrayBuffer();
 url.searchParams.set('p',encode(new Uint8Array(data)));return url.href;
}
export async function readLink(query,hash=''){
 const search=new URLSearchParams(query.replace(/^[?#]/,''));
 const fragment=new URLSearchParams(hash.replace(/^#/,''));
 const params=search.has('p')||search.has('invite')?search:fragment;let invite=params.get('invite');
 if(params.has('p')){
  const value=params.get('p');if(value.length>12000)throw Error('Invite is too large');
  if(typeof DecompressionStream!=='function')throw Error('Open this invitation in a current browser.');
  const reader=new Blob([decode(value)]).stream().pipeThrough(new DecompressionStream('deflate')).getReader();
  let size=0;const parts=[];for(;;){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>8192){await reader.cancel();throw Error('Invite is too large');}parts.push(value);}
  invite=await new Blob(parts).text();
 }
 if(invite)parseInvite(invite);return invite;
}
