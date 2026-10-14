/** Permission evidence only: never probes the LAN, requests consent, or gates RTC. */
export function createNetworkPermissions({runtime=globalThis,onChange=()=>{}}={}) {
  let closed=false,revision=0,detach=()=>{};
  let current={state:'unknown',name:null,reason:'not_checked',nextAction:'none'};
  const snapshot=()=>({localNetwork:{...current}});
  function update(state,name,reason){
    if(closed)return;
    const next={state,name,reason,nextAction:state==='denied'?'review_local_network_if_connection_fails':state==='prompt'?'grant_local_network_if_needed':'none'};
    if(JSON.stringify(next)===JSON.stringify(current))return;
    current=next;onChange(snapshot());
  }
  async function refresh(){
    const version=++revision;
    detach();detach=()=>{};
    let permissions;
    try{permissions=runtime.navigator?.permissions;}catch{}
    if(typeof permissions?.query!=='function'){update('unknown',null,'permissions_api_unavailable');return snapshot();}
    for(const name of ['local-network','local-network-access']){
      try{
        const status=await permissions.query({name});
        if(closed||version!==revision)return snapshot();
        const changed=()=>{if(!closed&&version===revision)update(['granted','prompt','denied'].includes(status.state)?status.state:'unknown',name,'browser_permission');};
        if(typeof status.addEventListener==='function'){
          status.addEventListener('change',changed);
          detach=()=>status.removeEventListener?.('change',changed);
        }
        changed();return snapshot();
      }catch{
        if(closed||version!==revision)return snapshot();
      }
    }
    update('unknown',null,'permission_query_unavailable');return snapshot();
  }
  return {snapshot,refresh,dispose(){closed=true;++revision;detach();detach=()=>{};}};
}
