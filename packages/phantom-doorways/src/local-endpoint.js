/** This HTTP adapter intentionally accepts only canonical RFC1918 IPv4 literals. */
export function validateLocalEndpoint(endpoint) {
  if(typeof endpoint!=='string')throw new TypeError('A private IPv4 endpoint is required');
  const match=/^http:\/\/((?:0|[1-9]\d{0,2})(?:\.(?:0|[1-9]\d{0,2})){3}):([1-9]\d{3,4})$/.exec(endpoint);
  if(!match)throw new TypeError('A private IPv4 endpoint is required');
  const parts=match[1].split('.').map(Number),port=Number(match[2]);
  if(parts.some(n=>n>255)||port<1024||port>65535||!(parts[0]===10||parts[0]===172&&parts[1]>=16&&parts[1]<=31||parts[0]===192&&parts[1]===168))throw new TypeError('A private IPv4 endpoint is required');
  return endpoint;
}
