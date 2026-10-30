// Isolated-world bridge. Only this top-level, exact HTTPS origin may invoke it.
(()=>{
  const origin='https://phantomshare.app';
  if(window.top!==window||window.location.origin!==origin)return;
  let port=null;
  const reply=body=>window.postMessage({channel:'phantom-native/1',direction:'reply',body},origin);
  function open(){
    if(port)return port;
    const current=chrome.runtime.connect({name:'phantom-native/1'});port=current;
    current.onMessage.addListener(reply);
    current.onDisconnect.addListener(()=>{void chrome.runtime.lastError;if(port===current){port=null;reply({event:'unavailable'});}});
    return current;
  }
  window.addEventListener('message',event=>{
    if(event.source!==window||event.origin!==origin||event.data?.channel!=='phantom-native/1')return;
    if(event.data.direction==='disconnect'){try{port?.disconnect();}catch{}port=null;return;}
    if(event.data.direction!=='request')return;
    try{const body=event.data.body;if(JSON.stringify(body).length>16384)return;open().postMessage(body);}catch{reply({event:'unavailable'});}
  });
  window.addEventListener('pagehide',()=>{try{port?.disconnect();}catch{}port=null;});
})();
