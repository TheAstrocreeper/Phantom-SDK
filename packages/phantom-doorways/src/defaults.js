import verified from './verified-relays.json' with {type:'json'};
export function getDefaultRelays(){return [...verified.relays];}
