/** Test-controller admin service: role election never transfers signing authority. */
export function createPublishAuthorizer({identity,networkId}) {
 if(!identity?.sign||typeof networkId!=='string'||!networkId)throw Error('Admin signer and network required');
 return async request=>{
  if(request?.networkId!==networkId||typeof request.target!=='string'||!/^sha256:[0-9a-f]{64}$/.test(request.target))throw Error('Invalid harness publish authorization request');
  return identity.sign({networkId,action:'content.publish',target:request.target,operationId:crypto.randomUUID()});
 };
}
