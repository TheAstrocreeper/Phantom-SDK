import {SITE_ORIGIN,HOST_NAME,authorizedSender,boundedRequest} from './policy.js';
const ports=new Map();
chrome.runtime.onConnect.addListener(port=>{
  if(port.name!=='phantom-native/1'||!authorizedSender(port.sender,chrome.runtime.id)||ports.has(port.sender.tab.id)||ports.size>=4){port.disconnect();return;}
  const tab=port.sender.tab.id;let native,closed=false;
  const disconnect=()=>{if(closed)return;closed=true;ports.delete(tab);try{native?.disconnect();}catch{}try{port.disconnect();}catch{}};
  ports.set(tab,port);
  try{native=chrome.runtime.connectNative(HOST_NAME);}catch{disconnect();return;}
  port.onMessage.addListener(request=>{
    if(closed||!authorizedSender(port.sender,chrome.runtime.id)||!boundedRequest(request)){disconnect();return;}
    try{native.postMessage(request);}catch{disconnect();}
  });
  native.onMessage.addListener(reply=>{
    try{if(JSON.stringify(reply).length>32768)throw Error();port.postMessage(reply);}catch{disconnect();}
  });
  native.onDisconnect.addListener(()=>{void chrome.runtime.lastError;disconnect();});
  port.onDisconnect.addListener(()=>{void chrome.runtime.lastError;disconnect();});
});
