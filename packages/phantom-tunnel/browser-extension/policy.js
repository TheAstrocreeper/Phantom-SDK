export const SITE_ORIGIN='https://phantomshare.app';
export const HOST_NAME='app.phantomshare.tunnel';
export function authorizedSender(sender,extensionId) {
  try{return sender.id===extensionId&&Number.isInteger(sender.tab?.id)&&sender.frameId===0&&new URL(sender.url).origin===SITE_ORIGIN&&(!sender.origin||sender.origin===SITE_ORIGIN);}catch{return false;}
}
export function boundedRequest(body) {
  try{return body&&/^[0-9a-f]{32}$/.test(body.id)&&/^[0-9a-f]{32}$/.test(body.jobId)&&['prepare','connect','close'].includes(body.action)&&JSON.stringify(body).length<=16384;}catch{return false;}
}
