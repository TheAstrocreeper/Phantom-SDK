// Return a fresh object so one connection cannot mutate another's defaults.
export function defaultRTCConfiguration() {
  return {iceServers: [{urls: 'stun:stun.l.google.com:19302'}]};
}
